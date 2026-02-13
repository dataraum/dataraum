"""Temporal slice analyzer — drift-only.

Analyzes slices for distribution drift using Jensen-Shannon divergence.
Produces one ColumnDriftResult per categorical column with evidence for
future LLM interpretation.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary
from dataraum.analysis.temporal_slicing.models import (
    CategoryAppearance,
    CategoryDisappearance,
    CategoryShift,
    ColumnDriftResult,
    DriftEvidence,
    TemporalSliceConfig,
    TimeGrain,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table

logger = get_logger(__name__)


def _generate_periods(config: TemporalSliceConfig) -> list[tuple[date, date, str]]:
    """Generate period boundaries based on time grain.

    Returns:
        List of (start_date, end_date, period_label) tuples
    """
    periods = []
    current = config.period_start

    while current < config.period_end:
        if config.time_grain == TimeGrain.DAILY:
            next_date = current + timedelta(days=1)
            label = current.isoformat()
        elif config.time_grain == TimeGrain.WEEKLY:
            next_date = current + timedelta(days=7)
            iso_year, iso_week, _ = current.isocalendar()
            label = f"{iso_year}-W{iso_week:02d}"
        else:  # MONTHLY
            if current.month == 12:
                next_date = date(current.year + 1, 1, 1)
            else:
                next_date = date(current.year, current.month + 1, 1)
            label = f"{current.year}-{current.month:02d}"

        end = min(next_date, config.period_end)
        periods.append((current, end, label))
        current = next_date

    return periods


def _jensen_shannon_divergence(
    p: dict[str, float],
    q: dict[str, float],
) -> float:
    """Compute Jensen-Shannon divergence between two distributions."""
    all_keys = set(p.keys()) | set(q.keys())
    p_vec = [p.get(k, 0.0) for k in all_keys]
    q_vec = [q.get(k, 0.0) for k in all_keys]
    m_vec = [(pi + qi) / 2 for pi, qi in zip(p_vec, q_vec, strict=True)]

    def kl_divergence(a: list[float], b: list[float]) -> float:
        result = 0.0
        for ai, bi in zip(a, b, strict=True):
            if ai > 0 and bi > 0:
                result += ai * math.log(ai / bi)
        return result

    kl_pm = kl_divergence(p_vec, m_vec)
    kl_qm = kl_divergence(q_vec, m_vec)
    return (kl_pm + kl_qm) / 2


def _get_distribution(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    col_name: str,
    time_column: str,
    start: date,
    end: date,
) -> dict[str, float] | None:
    """Get value distribution for a column in a time period."""
    sql = f"""
        SELECT
            "{col_name}" as value,
            COUNT(*) as count
        FROM "{table_name}"
        WHERE CAST("{time_column}" AS DATE) >= ?
          AND CAST("{time_column}" AS DATE) < ?
        GROUP BY "{col_name}"
    """
    results = duckdb_conn.execute(sql, [start, end]).fetchall()
    if not results:
        return None

    total = sum(r[1] for r in results)
    return {str(r[0]) if r[0] is not None else "_NULL_": r[1] / total for r in results}


def _build_drift_evidence(
    per_period: list[tuple[str, float, dict[str, float], dict[str, float]]],
    baseline: dict[str, float],
    threshold: float,
) -> DriftEvidence | None:
    """Build drift evidence from per-period comparisons.

    Args:
        per_period: List of (label, js_div, prev_dist, curr_dist) for each compared period
        baseline: The first period's distribution (used for emerged/vanished)
        threshold: JS divergence threshold for significance
    """
    if not per_period:
        return None

    # Find worst period
    worst_label, worst_js, _, _ = max(per_period, key=lambda x: x[1])

    # Top shifts: largest absolute proportion changes vs baseline
    top_shifts: list[CategoryShift] = []
    for label, js_div, prev_dist, curr_dist in per_period:
        if js_div < threshold:
            continue
        for cat in set(prev_dist.keys()) | set(curr_dist.keys()):
            prev_pct = prev_dist.get(cat, 0.0) * 100
            curr_pct = curr_dist.get(cat, 0.0) * 100
            shift = abs(curr_pct - prev_pct)
            if shift > 5.0:  # Only report shifts > 5pp
                top_shifts.append(
                    CategoryShift(
                        category=cat,
                        baseline_pct=round(prev_pct, 1),
                        period_pct=round(curr_pct, 1),
                        period=label,
                    )
                )

    # Sort by magnitude, keep top 10
    top_shifts.sort(key=lambda s: abs(s.period_pct - s.baseline_pct), reverse=True)
    top_shifts = top_shifts[:10]

    # Emerged categories: in current period but not in baseline
    baseline_cats = set(baseline.keys())
    emerged: list[CategoryAppearance] = []
    for label, _js_div, _, curr_dist in per_period:
        for cat, pct in curr_dist.items():
            if cat not in baseline_cats and pct > 0.01:
                emerged.append(
                    CategoryAppearance(
                        category=cat,
                        period=label,
                        pct=round(pct * 100, 1),
                    )
                )

    # Deduplicate emerged by category (keep first appearance)
    seen_emerged: set[str] = set()
    unique_emerged: list[CategoryAppearance] = []
    for e in emerged:
        if e.category not in seen_emerged:
            seen_emerged.add(e.category)
            unique_emerged.append(e)

    # Vanished categories: in baseline but not in later periods
    vanished: list[CategoryDisappearance] = []
    for label, _, _, curr_dist in per_period:
        for cat in baseline_cats:
            if cat not in curr_dist and baseline.get(cat, 0) > 0.01:
                vanished.append(
                    CategoryDisappearance(
                        category=cat,
                        period=label,
                        last_seen_pct=round(baseline[cat] * 100, 1),
                    )
                )

    # Deduplicate vanished by category (keep first disappearance)
    seen_vanished: set[str] = set()
    unique_vanished: list[CategoryDisappearance] = []
    for v in vanished:
        if v.category not in seen_vanished:
            seen_vanished.add(v.category)
            unique_vanished.append(v)

    # Change points: periods where JS divergence jumps significantly
    change_points: list[str] = []
    if len(per_period) >= 2:
        for i in range(1, len(per_period)):
            prev_js = per_period[i - 1][1]
            curr_js = per_period[i][1]
            # Detect jump: significant increase in divergence
            if curr_js > threshold and (curr_js - prev_js) > threshold:
                change_points.append(per_period[i][0])

    return DriftEvidence(
        worst_period=worst_label,
        worst_js=round(worst_js, 4),
        top_shifts=top_shifts,
        emerged_categories=unique_emerged[:10],
        vanished_categories=unique_vanished[:10],
        change_points=change_points,
    )


def analyze_column_drift(
    slice_table_name: str,
    time_column: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    config: TemporalSliceConfig,
) -> Result[list[ColumnDriftResult]]:
    """Analyze distribution drift for categorical columns in a slice table.

    Args:
        slice_table_name: Name of the slice table in DuckDB
        time_column: Name of the temporal column
        duckdb_conn: DuckDB connection
        session: Database session (for looking up column metadata)
        config: Temporal analysis configuration

    Returns:
        Result containing list of ColumnDriftResult, one per categorical column
    """
    try:
        logger.info(
            "drift_analysis_start",
            table=slice_table_name,
            time_column=time_column,
            grain=config.time_grain.value,
        )

        # Get table metadata
        stmt = select(Table).where(Table.duckdb_path == slice_table_name)
        table = session.execute(stmt).scalar_one_or_none()
        if not table:
            return Result.fail(f"Table not found: {slice_table_name}")

        # Get categorical columns
        col_stmt = select(Column).where(
            Column.table_id == table.table_id,
            Column.resolved_type.in_(["VARCHAR", "TEXT", "STRING"]),
        )
        columns = list(session.execute(col_stmt).scalars().all())

        # Generate periods
        periods = _generate_periods(config)
        if len(periods) < 2:
            return Result.ok([])

        results: list[ColumnDriftResult] = []

        for col in columns:
            col_name = col.column_name
            if col_name == time_column:
                continue

            # Collect distributions per period
            distributions: list[tuple[str, dict[str, float]]] = []
            for start, end, label in periods:
                dist = _get_distribution(
                    duckdb_conn, slice_table_name, col_name, time_column, start, end
                )
                if dist:
                    distributions.append((label, dist))

            if len(distributions) < 2:
                continue

            # Compare consecutive periods and collect per-period JS divergence
            baseline = distributions[0][1]
            js_values: list[float] = []
            per_period: list[tuple[str, float, dict[str, float], dict[str, float]]] = []

            for i in range(1, len(distributions)):
                prev_label, prev_dist = distributions[i - 1]
                curr_label, curr_dist = distributions[i]
                js_div = _jensen_shannon_divergence(prev_dist, curr_dist)
                js_values.append(js_div)
                per_period.append((curr_label, js_div, prev_dist, curr_dist))

            max_js = max(js_values)
            mean_js = sum(js_values) / len(js_values)
            periods_with_drift = sum(1 for js in js_values if js > config.drift_threshold)

            # Build evidence if any drift detected
            evidence = None
            if max_js > config.drift_threshold:
                evidence = _build_drift_evidence(per_period, baseline, config.drift_threshold)

            results.append(
                ColumnDriftResult(
                    column_name=col_name,
                    max_js_divergence=round(max_js, 6),
                    mean_js_divergence=round(mean_js, 6),
                    periods_analyzed=len(js_values),
                    periods_with_drift=periods_with_drift,
                    drift_evidence=evidence,
                )
            )

        logger.info(
            "drift_analysis_complete",
            table=slice_table_name,
            columns_analyzed=len(results),
            columns_with_drift=sum(1 for r in results if r.periods_with_drift > 0),
        )

        return Result.ok(results)

    except Exception as e:
        logger.error("drift_analysis_failed", table=slice_table_name, error=str(e))
        return Result.fail(f"Drift analysis failed: {e}")


def persist_drift_results(
    results: list[ColumnDriftResult],
    slice_table_name: str,
    time_column: str,
    session: Session,
) -> Result[int]:
    """Persist drift analysis results to database.

    Args:
        results: List of ColumnDriftResult from analyze_column_drift
        slice_table_name: Name of the slice table
        time_column: Name of the temporal column
        session: Database session

    Returns:
        Result containing number of records created
    """
    try:
        count = 0
        for result in results:
            evidence_json = None
            if result.drift_evidence:
                evidence_json = result.drift_evidence.model_dump()

            record = ColumnDriftSummary(
                slice_table_name=slice_table_name,
                column_name=result.column_name,
                time_column=time_column,
                max_js_divergence=result.max_js_divergence,
                mean_js_divergence=result.mean_js_divergence,
                periods_analyzed=result.periods_analyzed,
                periods_with_drift=result.periods_with_drift,
                drift_evidence_json=evidence_json,
            )
            session.add(record)
            count += 1

        return Result.ok(count)

    except Exception as e:
        logger.error("persist_drift_failed", error=str(e))
        return Result.fail(f"Failed to persist drift results: {e}")


__all__ = [
    "analyze_column_drift",
    "persist_drift_results",
]
