"""Backend validation via DuckDB ATTACH.

Every supported database backend is accessed through DuckDB extensions.
This module validates connections and discovers table schemas.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import duckdb

from dataraum.core.models import Result

_log = logging.getLogger(__name__)

# DuckDB extension names per backend.
BACKEND_EXTENSIONS: dict[str, str] = {
    "postgres": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
}

# DuckDB ATTACH type per backend.
BACKEND_ATTACH_TYPES: dict[str, str] = {
    "postgres": "POSTGRES",
    "mysql": "MYSQL",
    "sqlite": "SQLITE",
}

SUPPORTED_BACKENDS = set(BACKEND_EXTENSIONS.keys())


@dataclass
class TablePreview:
    """Preview of a discovered table."""

    name: str
    columns: list[str]
    row_count_estimate: int | None = None


@dataclass
class BackendValidationResult:
    """Result of backend validation."""

    tables: list[TablePreview] = field(default_factory=list)
    credential_source: str | None = None


def validate_backend(
    backend: str,
    url: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Result[BackendValidationResult]:
    """Test connection via DuckDB ATTACH and discover tables.

    Args:
        backend: Backend type (postgres, mysql, sqlite).
        url: Connection URL resolved from credential chain.
        duckdb_conn: DuckDB connection to use for ATTACH.

    Returns:
        Result containing table names and column previews, or error.
    """
    if backend not in SUPPORTED_BACKENDS:
        return Result.fail(f"Unsupported backend: {backend}. Supported: {', '.join(sorted(SUPPORTED_BACKENDS))}")

    attach_type = BACKEND_ATTACH_TYPES[backend]
    extension = BACKEND_EXTENSIONS[backend]
    alias = f"_validate_{backend}"

    try:
        # Install and load the extension
        duckdb_conn.execute(f"INSTALL {extension}")
        duckdb_conn.execute(f"LOAD {extension}")

        # ATTACH the database
        duckdb_conn.execute(f"ATTACH '{url}' AS {alias} (TYPE {attach_type}, READ_ONLY)")

        try:
            tables = discover_tables(duckdb_conn, alias)
            return Result.ok(BackendValidationResult(tables=tables))
        finally:
            # Always detach
            try:
                duckdb_conn.execute(f"DETACH {alias}")
            except Exception:
                pass

    except duckdb.IOException as e:
        return Result.fail(f"Connection failed: {e}")
    except duckdb.CatalogException as e:
        return Result.fail(f"Catalog error: {e}")
    except Exception as e:
        return Result.fail(f"Backend validation failed: {e}")


def discover_tables(
    duckdb_conn: duckdb.DuckDBPyConnection,
    alias: str,
) -> list[TablePreview]:
    """Discover tables in an attached database.

    Args:
        duckdb_conn: DuckDB connection with the database attached.
        alias: The ATTACH alias to query.

    Returns:
        List of table previews with column names.
    """
    tables: list[TablePreview] = []

    # Query information_schema for tables
    rows = duckdb_conn.execute(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_catalog = '{alias}' AND table_schema NOT IN ('information_schema', 'pg_catalog')"
    ).fetchall()

    for (table_name,) in rows:
        # Get columns for each table
        col_rows = duckdb_conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog = '{alias}' AND table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        ).fetchall()
        columns = [col[0] for col in col_rows]

        tables.append(TablePreview(name=table_name, columns=columns))

    return tables
