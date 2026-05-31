"""Source lifecycle management.

Shared by MCP tools and (future) web UI. Handles source registration,
listing, and soft-deletion.

Two source kinds:

- **File sources** — a single CSV/TSV, Parquet, or JSON/JSONL file
  addressed by an opaque URI (``s3://...`` in the container, a bare local
  path for direct dev runs). The URI is stored verbatim in
  ``connection_config['path']`` and handed straight to DuckDB at import
  time (DAT-389) — never resolved against the filesystem. Registered via
  `add_file_source`.
- **Recipe sources** — yaml declaring a backend (mssql, postgres, mysql,
  sqlite) plus named SELECT queries. The recipe yaml is a local file parsed
  at registration time. Credentials are resolved at pipeline-import time via
  `CredentialChain` keyed by source name (`DATARAUM_{NAME}_URL` env var).
  Registered via `add_recipe_source`.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.credentials import CredentialChain
from dataraum.core.models import Result
from dataraum.core.uri import uri_basename, uri_suffix
from dataraum.sources.db_recipe import Recipe, parse_recipe
from dataraum.storage.models import Source

_log = logging.getLogger(__name__)

# Source name pattern: lowercase, starts with letter, 2-49 chars total.
_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,48}$")

# File extensions for the file-source path.
_EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".tsv": "csv",
    ".parquet": "parquet",
    ".json": "json",
    ".jsonl": "json",
}

# Recipe yaml extensions for the database-source path.
RECIPE_EXTENSIONS: frozenset[str] = frozenset({".yaml", ".yml"})


@dataclass
class SourceInfo:
    """Public source information returned to callers (never contains secrets)."""

    name: str
    source_type: str
    status: str
    path: str | None = None
    backend: str | None = None
    columns: list[str] = field(default_factory=list)
    row_count_estimate: int | None = None
    discovered_schema: dict[str, Any] | None = None
    recipe_tables: list[str] = field(default_factory=list)


class SourceManager:
    """Manage data source lifecycle: add, list, remove."""

    def __init__(
        self,
        session: Session,
        credential_chain: CredentialChain,
        duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self._session = session
        self._credential_chain = credential_chain
        self._duckdb_conn = duckdb_conn

    def add_file_source(self, name: str, path: str) -> Result[SourceInfo]:
        """Register a single-file source (CSV/TSV/Parquet/JSON/JSONL).

        ``path`` is an opaque source URI (``s3://...`` in the container, a bare
        local path for direct dev runs). It is stored verbatim and handed to
        DuckDB at import time (DAT-389) — never resolved against the filesystem,
        so the registering process needs no access to the source bytes.
        Dispatch is on the URI suffix alone. Recipe yaml paths must use
        `add_recipe_source` instead.
        """
        validation = self._validate_new_name(name)
        if validation is not None:
            return validation

        suffix = uri_suffix(path)
        if suffix in RECIPE_EXTENSIONS:
            return Result.fail(f"Path '{path}' is a recipe yaml — call add_recipe_source instead.")

        source_type = _EXTENSION_MAP.get(suffix)
        if source_type is None:
            supported = ", ".join(sorted(_EXTENSION_MAP.keys()))
            return Result.fail(
                f"Unsupported file format: '{suffix}'. Supported extensions: {supported}"
            )

        columns, row_count = self._preview(path)

        source = Source(
            name=name,
            source_type=source_type,
            connection_config={"path": path},
            status="configured",
        )
        self._session.add(source)
        self._session.flush()

        return Result.ok(
            SourceInfo(
                name=name,
                source_type=source_type,
                status="configured",
                path=path,
                columns=columns,
                row_count_estimate=row_count,
            )
        )

    def _preview(self, source_uri: str) -> tuple[list[str], int | None]:
        """Best-effort column/row preview of a source URI.

        Opens a throwaway DuckDB connection with the object-store secret
        applied (DAT-389) so an ``s3://`` URI resolves over httpfs, then sniffs
        the schema. Any failure (unreachable URI, no S3 access in a unit-test
        process) is swallowed — the preview is advisory, not part of
        registration's contract.
        """
        try:
            conn = duckdb.connect()
            try:
                from dataraum.server.storage import apply_s3_secret

                apply_s3_secret(conn)
                return _read_file_preview(conn, source_uri)
            finally:
                conn.close()
        except Exception:
            _log.debug("Preview failed for %s", source_uri, exc_info=True)
            return [], None

    def add_recipe_source(self, name: str, recipe_path: str) -> Result[SourceInfo]:
        """Register a database source from a recipe yaml.

        Parses and validates the recipe at registration time. Does not
        touch the database — credentials and connectivity are checked
        lazily at pipeline-import time. This keeps `add_source` fast
        and avoids "register fails because env not loaded yet" friction.
        """
        validation = self._validate_new_name(name)
        if validation is not None:
            return validation

        parsed = parse_recipe(recipe_path)
        if not parsed.success or parsed.value is None:
            return Result.fail(parsed.error or "Recipe parse failed.")
        recipe: Recipe = parsed.value

        source = Source(
            name=name,
            source_type="db_recipe",
            backend=recipe.backend,
            connection_config={
                "recipe_path": str(recipe.source_path.resolve()),
                "backend": recipe.backend,
                "recipe_hash": recipe.recipe_hash,
                "tables": [{"name": t.name, "sql": t.sql} for t in recipe.tables],
            },
            status="configured",
        )
        self._session.add(source)
        self._session.flush()

        return Result.ok(
            SourceInfo(
                name=name,
                source_type="db_recipe",
                status="configured",
                path=str(recipe.source_path),
                backend=recipe.backend,
                recipe_tables=[t.name for t in recipe.tables],
            )
        )

    def list_sources(self, status_filter: str | None = None) -> list[SourceInfo]:
        """List all non-archived sources."""
        query = select(Source).where(Source.archived_at.is_(None))
        if status_filter:
            query = query.where(Source.status == status_filter)
        query = query.order_by(Source.name)

        sources = self._session.execute(query).scalars().all()

        return [
            SourceInfo(
                name=s.name,
                source_type=s.source_type,
                status=s.status or "unknown",
                path=(s.connection_config or {}).get("path")
                or (s.connection_config or {}).get("recipe_path"),
                backend=s.backend,
                discovered_schema=s.discovered_schema,
                recipe_tables=_recipe_table_names(s),
            )
            for s in sources
        ]

    def remove_source(self, name: str, purge: bool = False) -> Result[str]:
        """Soft-delete a source by setting archived_at."""
        source = self._get_source(name)
        if source is None:
            return Result.fail(f"Source '{name}' not found.")

        if purge:
            self._session.delete(source)
        else:
            source.archived_at = datetime.now(UTC)

        self._session.flush()

        cred_hint = ""
        if source.source_type == "db_recipe":
            cred_hint = f" Credentials in DATARAUM_{name.upper()}_URL are not removed by this call."

        return Result.ok(f"Source '{name}' {'deleted' if purge else 'archived'}.{cred_hint}")

    def _validate_new_name(self, name: str) -> Result[SourceInfo] | None:
        """Return a failure Result if the name is invalid or already used; else None."""
        if not _NAME_PATTERN.match(name):
            return Result.fail(
                f"Invalid source name '{name}'. Must match: lowercase, start with letter, "
                "2-49 chars, only a-z/0-9/_."
            )
        if self._get_source(name) is not None:
            return Result.fail(f"Source '{name}' already exists.")
        return None

    def _get_source(self, name: str) -> Source | None:
        """Look up a source by name (including archived)."""
        return self._session.execute(select(Source).where(Source.name == name)).scalar_one_or_none()


def _recipe_table_names(source: Source) -> list[str]:
    """Extract recipe table names from a Source's connection_config."""
    if source.source_type != "db_recipe":
        return []
    cfg = source.connection_config or {}
    return [t["name"] for t in cfg.get("tables", []) if isinstance(t, dict) and "name" in t]


def _read_file_preview(
    conn: duckdb.DuckDBPyConnection, source_uri: str
) -> tuple[list[str], int | None]:
    """Read column names and row count from a source URI.

    ``source_uri`` is opaque (``s3://...`` or a bare local path) and is passed
    verbatim to DuckDB over httpfs (DAT-389). ``conn`` must already carry the
    object-store secret for an ``s3://`` URI to resolve.
    """
    safe = source_uri.replace("'", "''")
    suffix = uri_suffix(source_uri)

    if suffix in (".csv", ".tsv"):
        try:
            result = conn.execute(f"SELECT * FROM read_csv_auto('{safe}') LIMIT 0")
        except Exception as e:
            err = str(e).lower()
            if "not utf-8 encoded" in err or "byte sequence mismatch" in err:
                msg = (
                    f"File is not UTF-8 encoded: {uri_basename(source_uri)}. "
                    "Re-save as UTF-8 (in Excel: Save As → CSV UTF-8)."
                )
                raise ValueError(msg) from e
            raise
        columns = [desc[0] for desc in result.description]
        try:
            count = conn.execute(f"SELECT count(*) FROM read_csv_auto('{safe}')").fetchone()
            return columns, count[0] if count else None
        except Exception:
            return columns, None

    if suffix == ".parquet":
        result = conn.execute(f"SELECT * FROM read_parquet('{safe}') LIMIT 0")
        columns = [desc[0] for desc in result.description]
        return columns, None

    if suffix in (".json", ".jsonl"):
        result = conn.execute(f"SELECT * FROM read_json_auto('{safe}') LIMIT 0")
        columns = [desc[0] for desc in result.description]
        try:
            count = conn.execute(f"SELECT count(*) FROM read_json_auto('{safe}')").fetchone()
            return columns, count[0] if count else None
        except Exception:
            return columns, None

    return [], None
