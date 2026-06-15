"""Temporal slice analyzer — per-(slice, period) row counts and numeric sums.

The slice-series substrate for aggregation-lineage reconciliation (DAT-491):
for one physical slice table, one ``GROUP BY`` over its time column yields the
per-period row count and the SUM of every numeric column. ``Σ events ≈ Δ stock``
is arithmetic over these stored sums (linearity of SUM), so signed conventions
(debit−credit, …) are reconstructed downstream by the lineage processor.

Periods are derived from the data itself — the grain (day/week/month) buckets
the rows; empty periods simply don't appear (they carried no mass and never
contributed to reconciliation). The drift / completeness / volume-anomaly
analysis that used to live here was cut with ``ColumnDriftSummary`` (DAT-518):
its output had no reader.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.temporal_slicing.db_models import TemporalSliceAnalysis
from dataraum.analysis.temporal_slicing.models import (
    PeriodSums,
    TimeGrain,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage.upsert import upsert

logger = get_logger(__name__)

# DuckDB ``date_trunc`` unit + ``strftime`` label per grain. The label scheme is
# the cross-fact alignment key the lineage processor joins on, so it must be
# stable across slices of the same grain — ISO semantics give that.
_GRAIN_SQL: dict[TimeGrain, tuple[str, str]] = {
    TimeGrain.DAILY: ("day", "%Y-%m-%d"),
    TimeGrain.WEEKLY: ("week", "%G-W%V"),
    TimeGrain.MONTHLY: ("month", "%Y-%m"),
}

_NUMERIC_DUCKDB_TYPES = frozenset(
    {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE", "DECIMAL"}
)


def _period_end(period_start: date, grain: TimeGrain) -> date:
    """The exclusive upper bound of the period — for evidence/display only."""
    if grain is TimeGrain.DAILY:
        return period_start + timedelta(days=1)
    if grain is TimeGrain.WEEKLY:
        return period_start + timedelta(days=7)
    # MONTHLY
    if period_start.month == 12:
        return date(period_start.year + 1, 1, 1)
    return date(period_start.year, period_start.month + 1, 1)


def _numeric_columns(duckdb_conn: duckdb.DuckDBPyConnection, slice_table_name: str) -> list[str]:
    """The slice table's numeric columns — the per-period SUM targets (DAT-491)."""
    try:
        rows = duckdb_conn.execute(f'DESCRIBE "{slice_table_name}"').fetchall()
    except Exception as e:
        # No sums for this slice table → the lineage witness can never fire on
        # this fact; that abstention must be visible, never silent.
        logger.warning("slice_describe_failed", table=slice_table_name, error=str(e))
        return []
    return [r[0] for r in rows if str(r[1]).split("(")[0].upper() in _NUMERIC_DUCKDB_TYPES]


def compute_period_sums(
    slice_table_name: str,
    time_column: str,
    grain: TimeGrain,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Result[list[PeriodSums]]:
    """Per-period row count + numeric-column sums for one slice table.

    One ``GROUP BY`` over the time column — periods come from the data, not a
    pre-generated grid. Returns one :class:`PeriodSums` per populated period.

    Args:
        slice_table_name: Name of the slice table in DuckDB
        time_column: Name of the temporal column to bucket by
        grain: Time granularity (day/week/month)
        duckdb_conn: DuckDB connection

    Returns:
        Result wrapping the list of populated periods (possibly empty).
    """
    try:
        unit, label_fmt = _GRAIN_SQL[grain]
        numeric_columns = _numeric_columns(duckdb_conn, slice_table_name)
        sum_parts = "".join(f', SUM("{c}") AS sum_{i}' for i, c in enumerate(numeric_columns))

        sql = f"""
            SELECT
                CAST(date_trunc('{unit}', CAST("{time_column}" AS DATE)) AS DATE) AS period_start,
                COUNT(*) AS row_count
                {sum_parts}
            FROM "{slice_table_name}"
            WHERE "{time_column}" IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """
        rows = duckdb_conn.execute(sql).fetchall()

        periods: list[PeriodSums] = []
        for row in rows:
            period_start: date = row[0]
            row_count = int(row[1])
            column_sums = {
                col: float(row[2 + i])
                for i, col in enumerate(numeric_columns)
                if row[2 + i] is not None
            }
            periods.append(
                PeriodSums(
                    period_label=period_start.strftime(label_fmt),
                    period_start=period_start,
                    period_end=_period_end(period_start, grain),
                    row_count=row_count,
                    column_sums=column_sums,
                )
            )

        logger.debug(
            "period_sums_complete",
            table=slice_table_name,
            periods=len(periods),
            numeric_columns=len(numeric_columns),
        )
        return Result.ok(periods)

    except Exception as e:
        logger.error("period_sums_failed", table=slice_table_name, error=str(e))
        return Result.fail(f"Period-sum analysis failed: {e}")


def persist_period_sums(
    periods: list[PeriodSums],
    slice_table_name: str,
    time_column: str,
    session: Session,
    *,
    session_id: str,
    run_id: str | None = None,
) -> Result[int]:
    """Persist per-period sums as :class:`TemporalSliceAnalysis` rows.

    Run-versioned (DAT-448) form-(a) writer (DAT-502): rows dedup in-batch on
    ``uq_tsa_slice_period_run`` (slice_table_name, period_label, run_id), then
    UPSERT. A Temporal success-redelivery (same ``run_id``) converges in place
    (no run-scoped clear); a new run's rows coexist with prior runs'.

    Args:
        periods: The per-period sums from :func:`compute_period_sums`.
        slice_table_name: Name of the slice table.
        time_column: The temporal column the periods were bucketed by.
        session: Database session.
        session_id: Investigation session scope.
        run_id: The begin_session run stamped onto the rows.

    Returns:
        Result containing number of records upserted.
    """
    try:
        rows: dict[tuple[str, str, str | None], dict[str, Any]] = {}
        for p in periods:
            rows[(slice_table_name, p.period_label, run_id)] = {
                "session_id": session_id,
                "run_id": run_id,
                "slice_table_name": slice_table_name,
                "time_column": time_column,
                "period_label": p.period_label,
                "period_start": p.period_start,
                "period_end": p.period_end,
                "row_count": p.row_count,
                "column_sums": p.column_sums or None,
            }
        upsert(
            session,
            TemporalSliceAnalysis,
            list(rows.values()),
            index_elements=["slice_table_name", "period_label", "run_id"],
        )
        return Result.ok(len(rows))

    except Exception as e:
        logger.error("persist_period_sums_failed", error=str(e))
        return Result.fail(f"Failed to persist period sums: {e}")


__all__ = [
    "compute_period_sums",
    "persist_period_sums",
]
