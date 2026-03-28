"""Source lifecycle management.

Shared by MCP tools and (future) web UI. Handles source registration,
validation, listing, and soft-deletion.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.credentials import CredentialChain
from dataraum.core.models import Result
from dataraum.sources.backends import (
    SUPPORTED_BACKENDS,
    TablePreview,
    validate_backend,
)
from dataraum.storage.models import Source

_log = logging.getLogger(__name__)

# Source name pattern: lowercase, starts with letter, 2-49 chars total.
_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,48}$")

# Max files per directory source. Prevents accidental data lake ingestion.
MAX_FILES_PER_SOURCE = 20

# Supported file extensions → source type.
_EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".tsv": "csv",
    ".parquet": "parquet",
    ".json": "json",
    ".jsonl": "json",
}


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
    credential_source: str | None = None
    credential_instructions: dict[str, Any] | None = None


class SourceManager:
    """Manage data source lifecycle: add, list, remove, validate."""

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
        """Register a local file source (CSV, Parquet, etc.).

        Args:
            name: Unique source name.
            path: File path (relative or absolute).

        Returns:
            Result with source info or error.
        """
        if not _NAME_PATTERN.match(name):
            return Result.fail(
                f"Invalid source name '{name}'. Must match: lowercase, start with letter, 2-49 chars, only a-z/0-9/_."
            )

        existing = self._get_source(name)
        if existing is not None:
            return Result.fail(f"Source '{name}' already exists.")

        # Validate path exists
        file_path = Path(path)
        if not file_path.exists():
            return Result.fail(f"Path not found: {path}")

        # Directory source: scan for supported files
        if file_path.is_dir():
            return self._add_directory_source(name, file_path)

        # Determine source type from extension
        suffix = file_path.suffix.lower()
        source_type = _EXTENSION_MAP.get(suffix)

        if source_type is None:
            supported = ", ".join(sorted(_EXTENSION_MAP.keys()))
            return Result.fail(
                f"Unsupported file format: '{suffix}'. Supported extensions: {supported}"
            )

        # Get column preview via DuckDB
        columns: list[str] = []
        row_count: int | None = None
        try:
            conn = duckdb.connect()
            try:
                columns, row_count = _read_file_preview(conn, file_path)
            finally:
                conn.close()
        except Exception:
            _log.debug("Preview failed for %s", path, exc_info=True)

        # Create Source record
        source = Source(
            name=name,
            source_type=source_type,
            connection_config={"path": str(file_path.resolve())},
            status="configured",
        )
        self._session.add(source)
        self._session.flush()

        return Result.ok(
            SourceInfo(
                name=name,
                source_type=source_type,
                status="configured",
                path=str(file_path),
                columns=columns,
                row_count_estimate=row_count,
            )
        )

    def _add_directory_source(self, name: str, directory: Path) -> Result[SourceInfo]:
        """Register a directory source by scanning for supported files.

        Args:
            name: Unique source name.
            directory: Path to directory.

        Returns:
            Result with source info including file count and format breakdown.
        """
        # Scan for supported files
        format_counts: dict[str, int] = {}
        total_files = 0
        for child in sorted(directory.iterdir()):
            if not child.is_file():
                continue
            fmt = _EXTENSION_MAP.get(child.suffix.lower())
            if fmt:
                format_counts[fmt] = format_counts.get(fmt, 0) + 1
                total_files += 1

        if total_files == 0:
            supported = ", ".join(sorted(_EXTENSION_MAP.keys()))
            return Result.fail(
                f"No supported data files found in '{directory}'. Supported extensions: {supported}"
            )

        if total_files > MAX_FILES_PER_SOURCE:
            return Result.fail(
                f"Directory contains {total_files} data files (max {MAX_FILES_PER_SOURCE}). "
                f"Split into multiple sources or reduce the number of files."
            )

        # Determine dominant format for source_type
        source_type = max(format_counts, key=lambda k: format_counts[k])

        # Preview columns from first file of dominant format
        columns: list[str] = []
        row_count: int | None = None
        for child in sorted(directory.iterdir()):
            if child.is_file() and _EXTENSION_MAP.get(child.suffix.lower()) == source_type:
                try:
                    conn = duckdb.connect()
                    try:
                        columns, row_count = _read_file_preview(conn, child)
                    finally:
                        conn.close()
                except Exception:
                    _log.debug("Preview failed for %s", child, exc_info=True)
                break

        source = Source(
            name=name,
            source_type=source_type,
            connection_config={"path": str(directory.resolve())},
            status="configured",
        )
        self._session.add(source)
        self._session.flush()

        # Build format breakdown string for discovery info
        breakdown = ", ".join(f"{count} {fmt}" for fmt, count in sorted(format_counts.items()))

        return Result.ok(
            SourceInfo(
                name=name,
                source_type=source_type,
                status="configured",
                path=str(directory),
                columns=columns,
                row_count_estimate=row_count,
                discovered_schema={
                    "file_count": total_files,
                    "formats": format_counts,
                    "breakdown": breakdown,
                },
            )
        )

    def add_database_source(
        self,
        name: str,
        backend: str,
        tables: list[str] | None = None,
        credential_ref: str | None = None,
    ) -> Result[SourceInfo]:
        """Register a database source.

        Resolves connection URL from CredentialChain. If found, validates
        via DuckDB ATTACH and discovers tables. If not found, returns
        setup instructions.

        Args:
            name: Unique source name.
            backend: DuckDB backend type (postgres, mysql, sqlite).
            tables: Optional table filter.
            credential_ref: Key for credential chain (defaults to name).

        Returns:
            Result with source info (may include credential_instructions).
        """
        if not _NAME_PATTERN.match(name):
            return Result.fail(
                f"Invalid source name '{name}'. Must match: lowercase, start with letter, 2-49 chars, only a-z/0-9/_."
            )

        if backend not in SUPPORTED_BACKENDS:
            return Result.fail(
                f"Unsupported backend: {backend}. Supported: {', '.join(sorted(SUPPORTED_BACKENDS))}"
            )

        existing = self._get_source(name)
        if existing is not None:
            return Result.fail(f"Source '{name}' already exists.")

        ref = credential_ref or name
        credential = self._credential_chain.resolve(ref)

        if credential is None:
            # No credentials — return instructions
            instructions = self._credential_chain.instructions_for(ref, backend)

            # Still create the source record with needs_credentials status
            source = Source(
                name=name,
                source_type=backend,
                status="needs_credentials",
                backend=backend,
                credential_ref=ref,
            )
            self._session.add(source)
            self._session.flush()

            return Result.ok(
                SourceInfo(
                    name=name,
                    source_type=backend,
                    status="needs_credentials",
                    backend=backend,
                    credential_instructions=instructions,
                )
            )

        # Credentials found — validate connection
        if self._duckdb_conn is None:
            return Result.fail("DuckDB connection required for database source validation.")

        validation = validate_backend(backend, credential.url, self._duckdb_conn)
        if not validation.success or validation.value is None:
            # Create source with error status
            source = Source(
                name=name,
                source_type=backend,
                status="error",
                backend=backend,
                credential_ref=ref,
            )
            self._session.add(source)
            self._session.flush()

            return Result.fail(f"Connection validation failed: {validation.error}")

        result = validation.value

        # Apply table filter if provided
        discovered_tables = result.tables
        if tables:
            table_set = set(tables)
            discovered_tables = [t for t in discovered_tables if t.name in table_set]

        # Build discovered schema dict
        schema_dict = _tables_to_schema_dict(discovered_tables, result.tables)

        source = Source(
            name=name,
            source_type=backend,
            status="validated",
            backend=backend,
            credential_ref=ref,
            connection_config={"tables": [t.name for t in discovered_tables]},
            discovered_schema=schema_dict,
            last_validated=datetime.now(UTC),
        )
        self._session.add(source)
        self._session.flush()

        return Result.ok(
            SourceInfo(
                name=name,
                source_type=backend,
                status="validated",
                backend=backend,
                discovered_schema=schema_dict,
                credential_source=credential.source,
            )
        )

    def list_sources(self, status_filter: str | None = None) -> list[SourceInfo]:
        """List all non-archived sources.

        Args:
            status_filter: Optional filter by status.

        Returns:
            List of source info objects.
        """
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
                path=s.connection_config.get("path") if s.connection_config else None,
                backend=s.backend,
                discovered_schema=s.discovered_schema,
            )
            for s in sources
        ]

    def remove_source(self, name: str, purge: bool = False) -> Result[str]:
        """Soft-delete a source by setting archived_at.

        Args:
            name: Source name to archive.
            purge: If True, hard-delete the source record.

        Returns:
            Result with confirmation message.
        """
        source = self._get_source(name)
        if source is None:
            return Result.fail(f"Source '{name}' not found.")

        if purge:
            self._session.delete(source)
        else:
            source.archived_at = datetime.now(UTC)

        self._session.flush()

        cred_hint = ""
        if source.credential_ref:
            cred_hint = (
                f" You may also want to remove the '{source.credential_ref}' entry "
                f"from {self._credential_chain.credentials_file}."
            )

        return Result.ok(f"Source '{name}' {'deleted' if purge else 'archived'}.{cred_hint}")

    def _get_source(self, name: str) -> Source | None:
        """Look up a source by name (including archived)."""
        return self._session.execute(select(Source).where(Source.name == name)).scalar_one_or_none()


def _read_file_preview(conn: duckdb.DuckDBPyConnection, path: Path) -> tuple[list[str], int | None]:
    """Read column names and row count from a file."""
    safe = str(path).replace("'", "''")
    suffix = path.suffix.lower()

    if suffix in (".csv", ".tsv"):
        try:
            result = conn.execute(f"SELECT * FROM read_csv_auto('{safe}') LIMIT 0")
        except Exception as e:
            err = str(e).lower()
            if "not utf-8 encoded" in err or "byte sequence mismatch" in err:
                msg = (
                    f"File is not UTF-8 encoded: {path.name}. "
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

    elif suffix == ".parquet":
        result = conn.execute(f"SELECT * FROM read_parquet('{safe}') LIMIT 0")
        columns = [desc[0] for desc in result.description]
        return columns, None

    elif suffix in (".json", ".jsonl"):
        result = conn.execute(f"SELECT * FROM read_json_auto('{safe}') LIMIT 0")
        columns = [desc[0] for desc in result.description]
        try:
            count = conn.execute(f"SELECT count(*) FROM read_json_auto('{safe}')").fetchone()
            return columns, count[0] if count else None
        except Exception:
            return columns, None

    return [], None


def _tables_to_schema_dict(
    included: list[TablePreview], all_tables: list[TablePreview]
) -> dict[str, Any]:
    """Build a schema dict from table previews."""
    return {
        "tables": [
            {
                "name": t.name,
                "columns": t.columns,
                "row_count_estimate": t.row_count_estimate,
            }
            for t in included
        ],
        "tables_excluded": len(all_tables) - len(included),
    }
