"""Backend extraction via DuckDB ATTACH.

Every supported database backend is accessed through a DuckDB extension.
This module materializes recipe queries into local raw tables and
harvests structural metadata (PK/FK/indexes) for downstream phases.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import duckdb

from dataraum.core.models import Result
from dataraum.sources.db_recipe import RecipeTable

_log = logging.getLogger(__name__)

# DuckDB extension names per backend.
BACKEND_EXTENSIONS: dict[str, str] = {
    "mssql": "mssql",
    "postgres": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
}

# DuckDB ATTACH type per backend.
BACKEND_ATTACH_TYPES: dict[str, str] = {
    "mssql": "MSSQL",
    "postgres": "POSTGRES",
    "mysql": "MYSQL",
    "sqlite": "SQLITE",
}

# Default schema used in `USE catalog.schema` after ATTACH. Backends with
# multi-schema catalogs (mssql, postgres) require an explicit schema so
# user SQL like `FROM dbo.Invoices` resolves. Backends where the catalog
# has a single canonical schema (sqlite) can use `USE catalog` alone, but
# we name the schema explicitly anyway for consistency.
BACKEND_DEFAULT_SCHEMA: dict[str, str] = {
    "mssql": "dbo",
    "postgres": "public",
    "mysql": "main",
    "sqlite": "main",
}

# Some extensions are community-maintained and need INSTALL FROM community.
# DuckDB's `core` repository hosts postgres/mysql/sqlite directly; mssql
# lives in the community repository.
_COMMUNITY_EXTENSIONS: frozenset[str] = frozenset({"mssql"})

SUPPORTED_BACKENDS = set(BACKEND_EXTENSIONS.keys())

_ATTACH_ALIAS = "src"


@dataclass
class ExtractedTable:
    """One materialized recipe table."""

    name: str  # Recipe table name (becomes raw_{name} in DuckDB)
    duckdb_table: str  # Actual DuckDB table name created
    row_count: int
    columns: list[tuple[str, str]]  # (column_name, duckdb_type)


@dataclass
class BackendExtractionResult:
    """Result of materializing all queries in a recipe."""

    tables: list[ExtractedTable] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def extract_backend(
    backend: str,
    url: str,
    queries: list[RecipeTable],
    duckdb_conn: duckdb.DuckDBPyConnection,
    raw_prefix: str = "raw_",
) -> Result[BackendExtractionResult]:
    """Materialize each recipe query into a `raw_{name}` table in DuckDB.

    Steps: INSTALL + LOAD the backend extension; ATTACH READ_ONLY;
    `USE src` so the user's SQL can reference attached tables by
    `schema.table` without an alias prefix; CREATE TABLE per query;
    restore catalog; DETACH.

    Fails loud at every step (DAT-274 pattern). On failure mid-extraction
    we still attempt to DETACH so the connection is left clean.

    Args:
        backend: One of `mssql`, `postgres`, `mysql`, `sqlite`.
        url: Connection URL resolved from the credential chain.
        queries: Recipe tables (name + sql), order preserved.
        duckdb_conn: DuckDB connection to materialize against.
        raw_prefix: Prefix applied to the recipe table name in DuckDB.

    Returns:
        Result with the list of extracted tables. Zero-row tables are
        surfaced as warnings, not errors.
    """
    if backend not in SUPPORTED_BACKENDS:
        return Result.fail(
            f"Unsupported backend: {backend}. Supported: {', '.join(sorted(SUPPORTED_BACKENDS))}"
        )
    if not queries:
        return Result.fail("extract_backend requires at least one query.")

    extension = BACKEND_EXTENSIONS[backend]
    attach_type = BACKEND_ATTACH_TYPES[backend]

    # 0. Snapshot the connection's default catalog BEFORE we switch into
    # the attached source. For in-memory connections this is "memory"; for
    # file-backed connections (the production case — session data.duckdb)
    # it's named after the file. Tables we create here must live in the
    # ORIGINAL catalog (not the read-only attached one), so all CREATE /
    # SELECT / information_schema queries qualify with this name.
    default_catalog_row = duckdb_conn.execute("SELECT current_catalog()").fetchone()
    default_catalog = default_catalog_row[0] if default_catalog_row else "memory"

    # 1. Install + load extension.
    try:
        if extension in _COMMUNITY_EXTENSIONS:
            duckdb_conn.execute(f"INSTALL {extension} FROM community")
        else:
            duckdb_conn.execute(f"INSTALL {extension}")
        duckdb_conn.execute(f"LOAD {extension}")
    except Exception as exc:
        return Result.fail(f"DuckDB extension '{extension}' failed to install/load: {exc}")

    # 2. ATTACH the source database READ_ONLY. Escape single quotes in
    # the URL so credentials containing apostrophes don't break out of
    # the string literal.
    safe_url = url.replace("'", "''")
    try:
        duckdb_conn.execute(
            f"ATTACH '{safe_url}' AS {_ATTACH_ALIAS} (TYPE {attach_type}, READ_ONLY)"
        )
    except Exception as exc:
        return Result.fail(f"ATTACH failed for {backend} source: {exc}")

    extracted: list[ExtractedTable] = []
    warnings: list[str] = []
    extraction_error: str | None = None
    try:
        # 3. Switch default catalog+schema so user SQL referencing
        # `schema.table` (e.g. `FROM dbo.Invoices`) resolves against the
        # attached database without an alias prefix. The original catalog
        # is restored in the finally block. Multi-schema backends (mssql,
        # postgres) require `USE catalog.schema` — `USE catalog` alone
        # fails with "no catalog + schema found". Some extensions (e.g.,
        # sqlite) defer connection errors to the first USE, so we surface
        # those as ATTACH-level failures.
        default_schema = BACKEND_DEFAULT_SCHEMA[backend]
        try:
            duckdb_conn.execute(f"USE {_ATTACH_ALIAS}.{default_schema}")
        except Exception as exc:
            # Some extensions (sqlite) defer connection errors to this
            # USE call, so the failure can mean either "ATTACH was a
            # no-op and now the connection is unreachable" or "the
            # default schema doesn't exist on this server".
            extraction_error = (
                f"Failed to enter {_ATTACH_ALIAS}.{default_schema} after ATTACH "
                f"for {backend} source: {exc}"
            )
        else:
            for q in queries:
                duckdb_table = f"{raw_prefix}{q.name}"
                try:
                    duckdb_conn.execute(
                        f'CREATE TABLE {default_catalog}.main."{duckdb_table}" AS {q.sql}'
                    )
                except Exception as exc:
                    extraction_error = f"Recipe table '{q.name}' SELECT failed: {exc}"
                    break

                row_count_row = duckdb_conn.execute(
                    f'SELECT count(*) FROM {default_catalog}.main."{duckdb_table}"'
                ).fetchone()
                row_count = int(row_count_row[0]) if row_count_row else 0

                col_rows = duckdb_conn.execute(
                    "SELECT column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_catalog = ? AND table_schema = 'main' "
                    "AND table_name = ? "
                    "ORDER BY ordinal_position",
                    [default_catalog, duckdb_table],
                ).fetchall()
                columns = [(str(r[0]), str(r[1])) for r in col_rows]

                if row_count == 0:
                    warnings.append(f"Recipe table '{q.name}' returned 0 rows.")

                extracted.append(
                    ExtractedTable(
                        name=q.name,
                        duckdb_table=duckdb_table,
                        row_count=row_count,
                        columns=columns,
                    )
                )
    finally:
        # Restore default catalog and DETACH. Suppress secondary errors —
        # the primary error (if any) is already captured.
        try:
            duckdb_conn.execute(f"USE {default_catalog}")
        except Exception:
            _log.debug("USE %s failed during cleanup", default_catalog, exc_info=True)
        try:
            duckdb_conn.execute(f"DETACH {_ATTACH_ALIAS}")
        except Exception:
            _log.debug("DETACH failed during cleanup", exc_info=True)

    if extraction_error is not None:
        return Result.fail(extraction_error)

    return Result.ok(BackendExtractionResult(tables=extracted, warnings=warnings))
