"""Export layer — write SQL results to files via DuckDB COPY.

Writes data to CSV or Parquet using DuckDB's native COPY (zero-copy,
no Python materialization). Caller provides a sidecar dict with
provenance metadata — export just writes it to disk alongside the data.

Usage:
    from dataraum.export import export_sql

    export_sql(
        sql="SELECT * FROM typed_orders",
        duckdb_conn=cursor,
        output_path=Path("./exports/orders.csv"),
        fmt="csv",
        sidecar={"confidence": "GREEN", "sql": "SELECT ..."},
    )
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    import duckdb

logger = get_logger(__name__)

ExportFormat = Literal["csv", "parquet"]


def export_sql(
    sql: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    output_path: Path,
    fmt: ExportFormat = "csv",
    sidecar: dict[str, Any] | None = None,
) -> Path:
    """Export SQL results to a file with metadata sidecar.

    Uses DuckDB's native COPY — data flows directly from DuckDB to disk
    without loading into Python memory.

    Args:
        sql: SQL query to execute and export.
        duckdb_conn: DuckDB connection.
        output_path: Destination file path (extension auto-corrected).
        fmt: Export format — csv or parquet.
        sidecar: Caller-provided metadata dict for the .meta.json file.
            Typically the MCP tool result minus rows/data.

    Returns:
        Path to the exported data file.
    """
    output_path = _ensure_extension(output_path, fmt)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # DuckDB COPY — zero-copy to disk
    copy_fmt = "CSV" if fmt == "csv" else "PARQUET"
    header = ", HEADER" if fmt == "csv" else ""
    copy_sql = f"COPY ({sql}) TO '{output_path}' (FORMAT {copy_fmt}{header})"
    duckdb_conn.execute(copy_sql)

    # Row count for sidecar
    count_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()
    row_count = count_result[0] if count_result else 0

    # Build sidecar: caller metadata + export metadata
    meta: dict[str, Any] = {
        "exported_at": datetime.now(UTC).isoformat(),
        "format": fmt,
        "row_count": row_count,
    }
    if sidecar:
        meta.update(sidecar)
    _write_sidecar(output_path, meta)

    logger.info("exported_sql", path=str(output_path), format=fmt, rows=row_count)
    return output_path


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _write_sidecar(data_path: Path, metadata: dict[str, Any]) -> None:
    """Write a metadata sidecar JSON file alongside the data file."""
    sidecar_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    with open(sidecar_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


def _ensure_extension(path: Path, fmt: ExportFormat) -> Path:
    """Ensure the file has the correct extension for the format."""
    expected = {"csv": ".csv", "parquet": ".parquet"}
    ext = expected[fmt]
    if path.suffix != ext:
        return path.with_suffix(ext)
    return path
