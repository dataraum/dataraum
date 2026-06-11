"""Temporal slice analyzer — drift + period-level completeness/anomaly.

Analyzes slices for:
- Distribution drift using Jensen-Shannon divergence (per categorical column)
- Period-level completeness: coverage gaps, early cutoffs
- Volume anomalies: spikes, drops, gaps per period
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary, TemporalSliceAnalysis
from dataraum.analysis.temporal_slicing.models import (
    CategoryAppearance,
    CategoryDisappearance,
    CategoryShift,
    ColumnDriftResult,
    CompletenessResult,
    DriftEvidence,
    PeriodAnalysisResult,
    PeriodMetrics,
    TemporalSliceConfig,
    TimeGrain,
    VolumeAnomalyResult,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

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
) -> tuple[dict[str, float], int] | None:
    """Get the value distribution (proportions) + row count for a time period."""
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
    if total <= 0:
        return None
    dist = {str(r[0]) if r[0] is not None else "_NULL_": r[1] / total for r in results}
    return dist, total


# Shared histogram resolution for numeric drift. Quantile (equal-count) bins are
# robust to the heavy tails of financial measures; equal-width bins would dump
# everything into one bucket.
_DRIFT_BINS = 10


def _generalized_drift(
    period_dists: list[dict[str, float]],
    weights: list[float],
) -> float:
    """Drift as the periods' disagreement about the value distribution.

    The size-weighted generalized Jensen–Shannon divergence of the per-period
    distributions from their pooled mixture, normalized by the weight entropy to
    ``[0, 1]``. This is the SAME quantity as the pooling engine's conflict ``C``
    with time periods as the witnesses — the reference is the *pooled*
    distribution, not the previous period. ``0`` = stationary across periods,
    ``1`` = maximal period-disagreement. Catches slow ramps and permanent level
    shifts that consecutive-pair divergence dilutes.
    """
    # Lazy import: entropy/__init__ pulls views → analysis, so a module-level
    # import would cycle. By call time both packages are fully loaded.
    from dataraum.entropy.pooling.pool import jensen_shannon_divergence, shannon_entropy

    if len(period_dists) < 2:
        return 0.0
    total_w = math.fsum(weights)
    if total_w <= 0.0:
        return 0.0
    norm_w = [w / total_w for w in weights]
    keys = sorted(set().union(*(d.keys() for d in period_dists)))
    if not keys:
        return 0.0
    aligned = [[d.get(k, 0.0) for k in keys] for d in period_dists]
    jsd = jensen_shannon_divergence(aligned, norm_w)
    h_w = shannon_entropy(norm_w)
    if h_w <= 1e-12:
        return 0.0
    return min(1.0, jsd / h_w)


def _bin_case(value_expr: str, edges: list[float]) -> str:
    """SQL CASE mapping ``value_expr`` to a bin index in ``[0, len(edges)]``."""
    whens = " ".join(f"WHEN {value_expr} < {edge:.17g} THEN {i}" for i, edge in enumerate(edges))
    return f"CASE {whens} ELSE {len(edges)} END"


def _pooled_quantile_edges(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    col_name: str,
    *,
    n_bins: int = _DRIFT_BINS,
) -> list[float]:
    """Interior quantile edges of the column over the whole slice window.

    These shared edges give every period one comparable histogram support, so a
    period whose values shift reweights the bins and shows as drift. The pooled
    (all-period) quantiles are the reference distribution.
    """
    quantiles = [i / n_bins for i in range(1, n_bins)]
    row = duckdb_conn.execute(
        f'SELECT quantile_cont(TRY_CAST("{col_name}" AS DOUBLE), ?::DOUBLE[]) '
        f'FROM "{table_name}" WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL',
        [quantiles],
    ).fetchone()
    if not row or row[0] is None:
        return []
    return sorted({float(e) for e in row[0] if e is not None})


def _get_binned_distribution(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    col_name: str,
    time_column: str,
    start: date,
    end: date,
    edges: list[float],
) -> tuple[dict[str, float], int] | None:
    """Per-period histogram (proportions + row count) over the shared ``edges``."""
    case = _bin_case("v", edges)
    rows = duckdb_conn.execute(
        f"SELECT {case} AS bin, COUNT(*) AS c FROM "
        f'(SELECT TRY_CAST("{col_name}" AS DOUBLE) AS v FROM "{table_name}" '
        f'WHERE CAST("{time_column}" AS DATE) >= ? AND CAST("{time_column}" AS DATE) < ?) sub '
        f"WHERE v IS NOT NULL GROUP BY bin",
        [start, end],
    ).fetchall()
    if not rows:
        return None
    total = sum(int(c) for _, c in rows)
    if total <= 0:
        return None
    return {str(int(b)): int(c) / total for b, c in rows}, total


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

    # Sort emerged by percentage (most significant first) before truncating
    unique_emerged.sort(key=lambda e: e.pct, reverse=True)
    # Sort vanished by last-seen percentage (most significant first) before truncating
    unique_vanished.sort(key=lambda v: v.last_seen_pct, reverse=True)

    return DriftEvidence(
        worst_period=worst_label,
        worst_js=round(worst_js, 4),
        top_shifts=top_shifts,
        emerged_categories=unique_emerged[:10],
        vanished_categories=unique_vanished[:10],
        change_points=change_points,
    )


def _get_numeric_stats(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    col_name: str,
    time_column: str,
    start: date,
    end: date,
) -> dict[str, float] | None:
    """Get numeric statistics for a column in a time period."""
    sql = f"""
        SELECT
            COUNT(*) AS cnt,
            AVG(CAST("{col_name}" AS DOUBLE)) AS mean_val,
            STDDEV_POP(CAST("{col_name}" AS DOUBLE)) AS stddev_val,
            MEDIAN(CAST("{col_name}" AS DOUBLE)) AS median_val
        FROM "{table_name}"
        WHERE CAST("{time_column}" AS DATE) >= ?
          AND CAST("{time_column}" AS DATE) < ?
          AND "{col_name}" IS NOT NULL
    """
    row = duckdb_conn.execute(sql, [start, end]).fetchone()
    if not row or row[0] == 0:
        return None

    return {
        "count": float(row[0]),
        "mean": float(row[1]) if row[1] is not None else 0.0,
        "stddev": float(row[2]) if row[2] is not None else 0.0,
        "median": float(row[3]) if row[3] is not None else 0.0,
    }


_NUMERIC_TYPES = frozenset(
    {"DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT"}
)


def analyze_column_drift(
    slice_table_name: str,
    time_column: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    config: TemporalSliceConfig,
) -> Result[list[ColumnDriftResult]]:
    """Analyze distribution drift for columns in a slice table.

    Categorical columns (VARCHAR/TEXT/STRING) use Jensen-Shannon divergence.
    Numeric columns (DECIMAL/FLOAT/INTEGER/etc.) use relative mean shift.

    Args:
        slice_table_name: Name of the slice table in DuckDB
        time_column: Name of the temporal column
        duckdb_conn: DuckDB connection
        session: Database session (for looking up column metadata)
        config: Temporal analysis configuration

    Returns:
        Result containing list of ColumnDriftResult, one per analyzed column
    """
    try:
        logger.debug(
            "drift_analysis_start",
            table=slice_table_name,
            time_column=time_column,
            grain=config.time_grain.value,
        )

        # Get table metadata. Post-DAT-341 raw/typed/quarantine share the same
        # bare ``duckdb_path``; slice tables live in the typed schema, so
        # constrain by layer to keep the lookup unique.
        stmt = select(Table).where(
            Table.duckdb_path == slice_table_name,
            Table.layer == "slice",
        )
        table = session.execute(stmt).scalar_one_or_none()
        if not table:
            return Result.fail(f"Table not found: {slice_table_name}")

        # Get categorical columns
        col_stmt = select(Column).where(
            Column.table_id == table.table_id,
            Column.resolved_type.in_(["VARCHAR", "TEXT", "STRING"]),
        )
        categorical_columns = list(session.execute(col_stmt).scalars().all())

        # Get numeric columns
        numeric_col_stmt = select(Column).where(
            Column.table_id == table.table_id,
            Column.resolved_type.in_(list(_NUMERIC_TYPES)),
        )
        numeric_columns = list(session.execute(numeric_col_stmt).scalars().all())

        # Generate periods
        periods = _generate_periods(config)
        if len(periods) < 2:
            return Result.ok([])

        results: list[ColumnDriftResult] = []

        # --- Categorical drift (JS divergence) ---
        for col in categorical_columns:
            col_name = col.column_name
            if col_name == time_column:
                continue

            # Collect distributions (+ row counts) per period
            distributions: list[tuple[str, dict[str, float], int]] = []
            for start, end, label in periods:
                res = _get_distribution(
                    duckdb_conn, slice_table_name, col_name, time_column, start, end
                )
                if res:
                    dist, count = res
                    distributions.append((label, dist, count))

            if len(distributions) < 2:
                continue

            # Compare consecutive periods and collect per-period JS divergence
            # (kept for evidence: worst transition, change points).
            baseline = distributions[0][1]
            js_values: list[float] = []
            per_period: list[tuple[str, float, dict[str, float], dict[str, float]]] = []

            for i in range(1, len(distributions)):
                prev_dist = distributions[i - 1][1]
                curr_label, curr_dist = distributions[i][0], distributions[i][1]
                js_div = _jensen_shannon_divergence(prev_dist, curr_dist)
                js_values.append(js_div)
                per_period.append((curr_label, js_div, prev_dist, curr_dist))

            max_js = max(js_values)
            mean_js = sum(js_values) / len(js_values)
            periods_with_drift = sum(1 for js in js_values if js > config.drift_threshold)

            # Drift score: periods-as-witnesses generalized JSD vs the pooled
            # mixture (size-weighted), normalized to [0, 1].
            drift_divergence = _generalized_drift(
                [d[1] for d in distributions], [float(d[2]) for d in distributions]
            )

            # Build evidence if any drift detected
            evidence = None
            if max_js > config.drift_threshold:
                evidence = _build_drift_evidence(per_period, baseline, config.drift_threshold)

            results.append(
                ColumnDriftResult(
                    column_name=col_name,
                    max_js_divergence=round(max_js, 6),
                    mean_js_divergence=round(mean_js, 6),
                    drift_divergence=round(drift_divergence, 6),
                    periods_analyzed=len(js_values),
                    periods_with_drift=periods_with_drift,
                    drift_evidence=evidence,
                )
            )

        # --- Numeric drift (relative mean shift) ---
        for col in numeric_columns:
            col_name = col.column_name
            if col_name == time_column:
                continue

            stats_list: list[tuple[str, dict[str, float]]] = []
            for start, end, label in periods:
                stats = _get_numeric_stats(
                    duckdb_conn, slice_table_name, col_name, time_column, start, end
                )
                if stats:
                    stats_list.append((label, stats))

            if len(stats_list) < 2:
                continue

            # Drift score: periods-as-witnesses generalized JSD over shared-bin
            # histograms vs the pooled distribution. Captures variance/shape
            # drift the mean-shift proxy below (kept for evidence) misses.
            edges = _pooled_quantile_edges(duckdb_conn, slice_table_name, col_name)
            binned: list[tuple[dict[str, float], int]] = []
            if edges:
                for start, end, _label in periods:
                    res = _get_binned_distribution(
                        duckdb_conn, slice_table_name, col_name, time_column, start, end, edges
                    )
                    if res:
                        binned.append(res)
            drift_divergence = (
                _generalized_drift([b[0] for b in binned], [float(b[1]) for b in binned])
                if len(binned) >= 2
                else 0.0
            )

            # Compute relative mean shift between consecutive periods
            shift_values: list[float] = []
            worst_shift = 0.0
            worst_period = stats_list[0][0]

            for i in range(1, len(stats_list)):
                prev_label, prev_stats = stats_list[i - 1]
                curr_label, curr_stats = stats_list[i]

                prev_mean = prev_stats["mean"]
                curr_mean = curr_stats["mean"]

                # Relative shift: |curr - prev| / max(|prev|, |curr|, 1)
                # Using max of both prevents division-by-zero and handles
                # sign changes gracefully
                denominator = max(abs(prev_mean), abs(curr_mean), 1.0)
                shift = abs(curr_mean - prev_mean) / denominator
                shift_values.append(shift)

                if shift > worst_shift:
                    worst_shift = shift
                    worst_period = curr_label

            max_shift = max(shift_values)
            mean_shift = sum(shift_values) / len(shift_values)

            # Use same drift_threshold semantics: a 10% mean shift is
            # analogous to a 0.1 JS divergence in terms of "something changed"
            periods_with_drift = sum(1 for s in shift_values if s > config.drift_threshold)

            # Map to JS-equivalent scale for consistent detector scoring.
            # A 35% mean shift (our injection) should produce a strong signal.
            # Scale: shift 0.1 → js_equiv 0.1, shift 0.3 → js_equiv 0.3 (1:1)
            # This keeps the detector's existing piecewise scoring working.
            js_equiv_max = min(max_shift, math.log(2))  # cap at ln(2)
            js_equiv_mean = min(mean_shift, math.log(2))

            evidence = None
            if max_shift > config.drift_threshold:
                evidence = DriftEvidence(
                    worst_period=worst_period,
                    worst_js=round(max_shift, 4),
                    top_shifts=[],
                    emerged_categories=[],
                    vanished_categories=[],
                    change_points=[
                        stats_list[i + 1][0]
                        for i, s in enumerate(shift_values)
                        if i > 0
                        and s > config.drift_threshold
                        and shift_values[i - 1] <= config.drift_threshold
                    ],
                )

            results.append(
                ColumnDriftResult(
                    column_name=col_name,
                    max_js_divergence=round(js_equiv_max, 6),
                    mean_js_divergence=round(js_equiv_mean, 6),
                    drift_divergence=round(drift_divergence, 6),
                    periods_analyzed=len(shift_values),
                    periods_with_drift=periods_with_drift,
                    drift_evidence=evidence,
                )
            )

        logger.debug(
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
    *,
    session_id: str,
    run_id: str | None = None,
) -> Result[int]:
    """Persist drift analysis results to database.

    Run-versioned (DAT-448) form-(a) writer (DAT-502): rows are stamped with
    ``run_id`` and UPSERT on ``uq_drift_slice_column_run`` — no run-scoped
    clear. A Temporal success-redelivery (same ``run_id``) converges in
    place; prior runs' rows stay untouched.

    Args:
        results: List of ColumnDriftResult from analyze_column_drift
        slice_table_name: Name of the slice table
        time_column: Name of the temporal column
        session: Database session
        session_id: Investigation session scope.
        run_id: The begin_session run stamped onto the rows.

    Returns:
        Result containing number of records created
    """
    try:
        # Dedup by the unique key, then UPSERT — not session.add. The key
        # ``uq_drift_slice_column_run`` is (slice_table_name, column_name, run_id),
        # which omits ``time_column``, so a column analysed under two time columns
        # shares a key; and a Temporal at-least-once RETRY re-runs this activity. A
        # plain insert violates the constraint in both cases (DAT-447: the teach
        # re-run lane stresses retries). Upsert is idempotent; dedup keeps the last
        # row per key so a single batch can't "affect a row twice" either.
        rows: dict[tuple[str, str, str | None], dict[str, Any]] = {}
        for result in results:
            evidence_json = result.drift_evidence.model_dump() if result.drift_evidence else None
            rows[(slice_table_name, result.column_name, run_id)] = {
                "session_id": session_id,
                "run_id": run_id,
                "slice_table_name": slice_table_name,
                "column_name": result.column_name,
                "time_column": time_column,
                "max_js_divergence": result.max_js_divergence,
                "mean_js_divergence": result.mean_js_divergence,
                "drift_divergence": result.drift_divergence,
                "periods_analyzed": result.periods_analyzed,
                "periods_with_drift": result.periods_with_drift,
                "drift_evidence_json": evidence_json,
            }
        upsert(
            session,
            ColumnDriftSummary,
            list(rows.values()),
            index_elements=["slice_table_name", "column_name", "run_id"],
        )
        return Result.ok(len(rows))

    except Exception as e:
        logger.error("persist_drift_failed", error=str(e))
        return Result.fail(f"Failed to persist drift results: {e}")


def _numeric_columns(duckdb_conn: duckdb.DuckDBPyConnection, slice_table_name: str) -> list[str]:
    """The slice table's numeric columns — the per-period sum targets (DAT-491)."""
    numeric = {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
    }
    try:
        rows = duckdb_conn.execute(f'DESCRIBE "{slice_table_name}"').fetchall()
    except Exception as e:
        # No sums for this slice table → the lineage witness can never fire on
        # this fact; that abstention must be visible, never silent.
        logger.warning("slice_describe_failed", table=slice_table_name, error=str(e))
        return []
    return [r[0] for r in rows if str(r[1]).split("(")[0].upper() in numeric]


def _compute_period_metrics(
    slice_table_name: str,
    time_column: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    periods: list[tuple[date, date, str]],
    numeric_columns: list[str] | None = None,
) -> list[PeriodMetrics]:
    """Compute row counts, day coverage, rolling statistics, and value sums per period.

    Args:
        slice_table_name: Name of the slice table in DuckDB
        time_column: Name of the temporal column
        duckdb_conn: DuckDB connection
        periods: List of (start_date, end_date, period_label) tuples
        numeric_columns: Columns to SUM per period (DAT-491 lineage substrate)

    Returns:
        List of PeriodMetrics, one per period
    """
    metrics: list[PeriodMetrics] = []
    numeric_columns = numeric_columns or []
    sum_parts = "".join(f', SUM("{c}") AS sum_{i}' for i, c in enumerate(numeric_columns))

    for start, end, label in periods:
        expected_days = (end - start).days
        if expected_days <= 0:
            continue

        sql = f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CAST("{time_column}" AS DATE)) AS observed_days
                {sum_parts}
            FROM "{slice_table_name}"
            WHERE CAST("{time_column}" AS DATE) >= ?
              AND CAST("{time_column}" AS DATE) < ?
        """
        row = duckdb_conn.execute(sql, [start, end]).fetchone()
        row_count = row[0] if row else 0
        observed_days = row[1] if row else 0
        column_sums = {
            col: float(row[2 + i])
            for i, col in enumerate(numeric_columns)
            if row and row[2 + i] is not None
        }

        coverage_ratio = observed_days / expected_days if expected_days > 0 else 0.0

        # Last day ratio: volume of last observed day vs average daily volume
        last_day_ratio = 0.0
        if row_count > 0 and observed_days > 0:
            avg_daily = row_count / observed_days
            last_day_sql = f"""
                SELECT COUNT(*) FROM "{slice_table_name}"
                WHERE CAST("{time_column}" AS DATE) = (
                    SELECT MAX(CAST("{time_column}" AS DATE))
                    FROM "{slice_table_name}"
                    WHERE CAST("{time_column}" AS DATE) >= ?
                      AND CAST("{time_column}" AS DATE) < ?
                )
            """
            last_row = duckdb_conn.execute(last_day_sql, [start, end]).fetchone()
            last_day_count = last_row[0] if last_row else 0
            last_day_ratio = last_day_count / avg_daily if avg_daily > 0 else 0.0

        metrics.append(
            PeriodMetrics(
                period_label=label,
                period_start=start,
                period_end=end,
                row_count=row_count,
                column_sums=column_sums,
                expected_days=expected_days,
                observed_days=observed_days,
                coverage_ratio=round(coverage_ratio, 4),
                last_day_ratio=round(last_day_ratio, 4),
            )
        )

    # Compute rolling statistics and z-scores using a trailing window
    # (previous periods only, excluding current — so z-score measures deviation
    # from the baseline established by prior periods)
    baseline_window = 3
    row_counts = [m.row_count for m in metrics]
    for i, m in enumerate(metrics):
        if i >= baseline_window:
            # Trailing window: previous N periods, NOT including current
            window = row_counts[max(0, i - baseline_window) : i]
            rolling_avg = sum(window) / len(window)
            rolling_std = (
                (sum((x - rolling_avg) ** 2 for x in window) / len(window)) ** 0.5
                if len(window) > 1
                else 0.0
            )
            z_score = (m.row_count - rolling_avg) / rolling_std if rolling_std > 0 else 0.0
        else:
            # Not enough history yet
            rolling_avg = float(m.row_count)
            rolling_std = 0.0
            z_score = 0.0

        # Period-over-period change
        pop_change = None
        if i > 0 and row_counts[i - 1] > 0:
            pop_change = round((m.row_count - row_counts[i - 1]) / row_counts[i - 1], 4)

        metrics[i] = m.model_copy(
            update={
                "rolling_avg": round(rolling_avg, 2),
                "rolling_std": round(rolling_std, 2),
                "z_score": round(z_score, 4),
                "period_over_period_change": pop_change,
            }
        )

    return metrics


def _analyze_completeness(
    metrics: list[PeriodMetrics],
    config: TemporalSliceConfig,
) -> list[CompletenessResult]:
    """Evaluate coverage ratios and detect early cutoffs.

    Args:
        metrics: Period metrics from _compute_period_metrics
        config: Configuration with thresholds

    Returns:
        List of CompletenessResult, one per period
    """
    results: list[CompletenessResult] = []

    for m in metrics:
        is_complete = m.coverage_ratio >= config.completeness_threshold
        days_missing_at_end = max(0, m.expected_days - m.observed_days)
        has_early_cutoff = (
            days_missing_at_end > 0 and m.last_day_ratio < config.last_day_ratio_threshold
        )

        results.append(
            CompletenessResult(
                period_label=m.period_label,
                is_complete=is_complete,
                coverage_ratio=m.coverage_ratio,
                has_early_cutoff=has_early_cutoff,
                days_missing_at_end=days_missing_at_end,
            )
        )

    return results


def _detect_volume_anomalies(
    metrics: list[PeriodMetrics],
    config: TemporalSliceConfig,
) -> list[VolumeAnomalyResult]:
    """Detect volume anomalies using z-scores.

    Args:
        metrics: Period metrics with rolling statistics computed
        config: Configuration with volume_zscore_threshold

    Returns:
        List of VolumeAnomalyResult, one per period with anomaly info
    """
    results: list[VolumeAnomalyResult] = []

    for m in metrics:
        z = m.z_score if m.z_score is not None else 0.0
        is_anomaly = abs(z) > config.volume_zscore_threshold

        anomaly_type = None
        if m.row_count == 0:
            is_anomaly = True
            anomaly_type = "gap"
        elif is_anomaly:
            anomaly_type = "spike" if z > 0 else "drop"

        results.append(
            VolumeAnomalyResult(
                period_label=m.period_label,
                is_anomaly=is_anomaly,
                anomaly_type=anomaly_type,
                z_score=z,
                period_over_period_change=m.period_over_period_change,
            )
        )

    return results


def analyze_period_metrics(
    slice_table_name: str,
    time_column: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    config: TemporalSliceConfig,
) -> Result[PeriodAnalysisResult]:
    """Analyze period-level completeness and volume anomalies for a slice table.

    Args:
        slice_table_name: Name of the slice table in DuckDB
        time_column: Name of the temporal column
        duckdb_conn: DuckDB connection
        config: Temporal analysis configuration

    Returns:
        Result containing PeriodAnalysisResult with metrics, completeness, and anomalies
    """
    try:
        logger.debug(
            "period_analysis_start",
            table=slice_table_name,
            time_column=time_column,
            grain=config.time_grain.value,
        )

        periods = _generate_periods(config)
        if not periods:
            return Result.ok(
                PeriodAnalysisResult(
                    slice_table_name=slice_table_name,
                    time_column=time_column,
                    total_periods=0,
                    incomplete_periods=0,
                    anomaly_count=0,
                    period_metrics=[],
                    completeness_results=[],
                    volume_anomalies=[],
                )
            )

        period_metrics = _compute_period_metrics(
            slice_table_name,
            time_column,
            duckdb_conn,
            periods,
            numeric_columns=_numeric_columns(duckdb_conn, slice_table_name),
        )
        completeness = _analyze_completeness(period_metrics, config)
        anomalies = _detect_volume_anomalies(period_metrics, config)

        incomplete_count = sum(1 for c in completeness if not c.is_complete)
        anomaly_count = sum(1 for a in anomalies if a.is_anomaly)

        result = PeriodAnalysisResult(
            slice_table_name=slice_table_name,
            time_column=time_column,
            total_periods=len(periods),
            incomplete_periods=incomplete_count,
            anomaly_count=anomaly_count,
            period_metrics=period_metrics,
            completeness_results=completeness,
            volume_anomalies=anomalies,
        )

        logger.debug(
            "period_analysis_complete",
            table=slice_table_name,
            periods=len(periods),
            incomplete=incomplete_count,
            anomalies=anomaly_count,
        )

        return Result.ok(result)

    except Exception as e:
        logger.error("period_analysis_failed", table=slice_table_name, error=str(e))
        return Result.fail(f"Period analysis failed: {e}")


def persist_period_results(
    result: PeriodAnalysisResult,
    session: Session,
    *,
    session_id: str,
    run_id: str | None = None,
) -> Result[int]:
    """Persist period analysis results to database.

    Run-versioned (DAT-448) form-(a) writer (DAT-502): same discipline as
    :func:`persist_drift_results` — rows dedup in-batch on
    ``uq_tsa_slice_period_run`` (slice_table_name, period_label, run_id), then
    UPSERT. A Temporal success-redelivery (same ``run_id``) converges in place
    (no run-scoped clear); a new run's rows coexist with prior runs'.

    Args:
        result: PeriodAnalysisResult from analyze_period_metrics
        session: Database session
        session_id: Investigation session scope.
        run_id: The begin_session run stamped onto the rows.

    Returns:
        Result containing number of records created
    """
    try:
        # Build lookup maps for completeness and anomaly results
        completeness_map = {c.period_label: c for c in result.completeness_results}
        anomaly_map = {a.period_label: a for a in result.volume_anomalies}

        rows: dict[tuple[str, str, str | None], dict[str, Any]] = {}
        for m in result.period_metrics:
            comp = completeness_map.get(m.period_label)
            anom = anomaly_map.get(m.period_label)

            # Collect issues
            issues: list[dict[str, str]] = []
            if comp and not comp.is_complete:
                issues.append(
                    {"type": "incomplete", "detail": f"coverage={comp.coverage_ratio:.2f}"}
                )
            if comp and comp.has_early_cutoff:
                issues.append(
                    {"type": "early_cutoff", "detail": f"missing_days={comp.days_missing_at_end}"}
                )
            if anom and anom.is_anomaly:
                issues.append(
                    {"type": f"volume_{anom.anomaly_type}", "detail": f"z_score={anom.z_score:.2f}"}
                )

            # PK omitted so the model's Python-side default applies.
            rows[(result.slice_table_name, m.period_label, run_id)] = {
                "session_id": session_id,
                "run_id": run_id,
                "slice_table_name": result.slice_table_name,
                "time_column": result.time_column,
                "period_label": m.period_label,
                "period_start": m.period_start,
                "period_end": m.period_end,
                "row_count": m.row_count,
                "column_sums": m.column_sums or None,
                "expected_days": m.expected_days,
                "observed_days": m.observed_days,
                "coverage_ratio": m.coverage_ratio,
                "is_complete": int(comp.is_complete) if comp else None,
                "has_early_cutoff": int(comp.has_early_cutoff) if comp else None,
                "days_missing_at_end": comp.days_missing_at_end if comp else None,
                "last_day_ratio": m.last_day_ratio,
                "z_score": m.z_score,
                "rolling_avg": m.rolling_avg,
                "rolling_std": m.rolling_std,
                "is_volume_anomaly": int(anom.is_anomaly) if anom else None,
                "anomaly_type": anom.anomaly_type if anom else None,
                "period_over_period_change": m.period_over_period_change,
                "issues_json": issues if issues else None,
            }

        upsert(
            session,
            TemporalSliceAnalysis,
            list(rows.values()),
            index_elements=["slice_table_name", "period_label", "run_id"],
        )
        return Result.ok(len(rows))

    except Exception as e:
        logger.error("persist_period_results_failed", error=str(e))
        return Result.fail(f"Failed to persist period results: {e}")


__all__ = [
    "analyze_column_drift",
    "analyze_period_metrics",
    "persist_drift_results",
    "persist_period_results",
]
