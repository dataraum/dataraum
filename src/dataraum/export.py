"""Export layer — materialize query and graph results to files.

Converts ExecutionResult / QueryResult data into persistent formats
(CSV, Parquet, JSON) with metadata sidecars that carry provenance.

Usage:
    from dataraum.export import export_query_result

    export_query_result(result, Path("./output/revenue.csv"), fmt="csv")
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    import duckdb

    from dataraum.query.models import QueryResult

logger = get_logger(__name__)

ExportFormat = Literal["csv", "parquet", "json"]


def export_query_result(
    result: QueryResult,
    output_path: Path,
    fmt: ExportFormat = "csv",
) -> Path:
    """Export a QueryResult to a file with metadata sidecar.

    Args:
        result: QueryResult from answer_question().
        output_path: Destination file path (extension auto-corrected if needed).
        fmt: Export format — csv, parquet, or json.

    Returns:
        Path to the exported data file.

    Raises:
        ValueError: If result has no data to export.
    """
    if not result.data or not result.columns:
        msg = "QueryResult has no tabular data to export"
        raise ValueError(msg)

    output_path = _ensure_extension(output_path, fmt)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    _write_data(result.columns, result.data, output_path, fmt)
    _write_sidecar(output_path, _query_result_metadata(result))

    logger.info(
        "exported_query_result",
        path=str(output_path),
        format=fmt,
        rows=len(result.data),
        columns=len(result.columns),
    )
    return output_path


def export_sql(
    sql: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    output_path: Path,
    fmt: ExportFormat = "csv",
    *,
    description: str | None = None,
) -> Path:
    """Export arbitrary SQL results to a file with metadata sidecar.

    Uses DuckDB's native COPY for CSV/Parquet (zero-copy, efficient).
    Falls back to fetch + serialize for JSON.

    Args:
        sql: SQL query to execute and export.
        duckdb_conn: DuckDB connection.
        output_path: Destination file path.
        fmt: Export format.
        description: Optional description for the sidecar.

    Returns:
        Path to the exported data file.
    """
    output_path = _ensure_extension(output_path, fmt)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "json":
        # JSON needs manual serialization
        rel = duckdb_conn.execute(sql)
        columns = [desc[0] for desc in rel.description]
        rows = rel.fetchall()
        data = [dict(zip(columns, row, strict=True)) for row in rows]
        _write_data(columns, data, output_path, "json")
        row_count = len(data)
    else:
        # CSV/Parquet via DuckDB COPY (most efficient path)
        copy_fmt = "CSV" if fmt == "csv" else "PARQUET"
        header = ", HEADER" if fmt == "csv" else ""
        copy_sql = f"COPY ({sql}) TO '{output_path}' (FORMAT {copy_fmt}{header})"
        duckdb_conn.execute(copy_sql)
        # Get row count for sidecar
        count_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()
        row_count = count_result[0] if count_result else 0

    sidecar = {
        "exported_at": datetime.now(UTC).isoformat(),
        "format": fmt,
        "sql": sql,
        "row_count": row_count,
    }
    if description:
        sidecar["description"] = description
    _write_sidecar(output_path, sidecar)

    logger.info("exported_sql", path=str(output_path), format=fmt, rows=row_count)
    return output_path


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _write_data(
    columns: list[str],
    data: list[dict[str, Any]],
    path: Path,
    fmt: ExportFormat,
) -> None:
    """Write tabular data to a file."""
    if fmt == "csv":
        import csv

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(data)
    elif fmt == "parquet":
        _write_parquet_via_duckdb(columns, data, path)
    elif fmt == "json":
        with open(path, "w") as f:
            json.dump(
                {"columns": columns, "data": data},
                f,
                indent=2,
                default=str,
            )


def _write_parquet_via_duckdb(
    columns: list[str],
    data: list[dict[str, Any]],
    path: Path,
) -> None:
    """Write data to parquet using DuckDB's native writer (no pyarrow needed)."""
    import csv as csv_mod
    import tempfile

    import duckdb

    # Write to temp CSV, then convert via DuckDB COPY
    with tempfile.NamedTemporaryFile(
        suffix=".csv", delete=False, mode="w", newline=""
    ) as f:
        writer = csv_mod.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
        tmp_csv = f.name

    conn = duckdb.connect()
    try:
        conn.execute(
            f"COPY (SELECT * FROM read_csv('{tmp_csv}', header=true)) "
            f"TO '{path}' (FORMAT PARQUET)"
        )
    finally:
        conn.close()
        Path(tmp_csv).unlink(missing_ok=True)


def _write_sidecar(data_path: Path, metadata: dict[str, Any]) -> None:
    """Write a metadata sidecar JSON file alongside the data file."""
    sidecar_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    with open(sidecar_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


def _ensure_extension(path: Path, fmt: ExportFormat) -> Path:
    """Ensure the file has the correct extension for the format."""
    expected = {"csv": ".csv", "parquet": ".parquet", "json": ".json"}
    ext = expected[fmt]
    if path.suffix != ext:
        return path.with_suffix(ext)
    return path


def _query_result_metadata(result: QueryResult) -> dict[str, Any]:
    """Build metadata sidecar dict from a QueryResult."""
    meta: dict[str, Any] = {
        "exported_at": datetime.now(UTC).isoformat(),
        "execution_id": result.execution_id,
        "question": result.question,
        "executed_at": result.executed_at.isoformat(),
        "sql": result.sql,
        "row_count": len(result.data) if result.data else 0,
        "column_count": len(result.columns) if result.columns else 0,
        "confidence": {
            "level": result.confidence_level.value,
            "label": result.confidence_level.label,
        },
    }
    if result.entropy_score is not None:
        meta["entropy_score"] = round(result.entropy_score, 3)
    if result.entropy_action:
        meta["entropy_action"] = result.entropy_action
    if result.assumptions:
        meta["assumptions"] = [
            {
                "dimension": a.dimension,
                "target": a.target,
                "assumption": a.assumption,
                "basis": a.basis.value,
                "confidence": round(a.confidence, 2),
            }
            for a in result.assumptions
        ]
    if result.contract_evaluation:
        meta["contract"] = {
            "name": result.contract,
            "evaluation": result.contract_evaluation.to_dict(),
        }
    if result.execution_steps:
        meta["execution_steps"] = [
            {"step_id": s.step_id, "sql": s.sql, "description": s.description}
            for s in result.execution_steps
        ]
    return meta
