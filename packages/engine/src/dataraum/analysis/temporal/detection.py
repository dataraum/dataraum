"""Basic temporal detection functions.

Detects:
- Time granularity (second, minute, hour, day, week, month, etc.)
- Gaps in time series
- Expected vs actual periods
- Basic completeness

These are foundational functions used by the main processor.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import duckdb

from dataraum.analysis.temporal.models import (
    DATE_TRUNC_GRAINS,
    TemporalCompletenessAnalysis,
    TemporalGapInfo,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result

logger = get_logger(__name__)


def infer_granularity(
    median_gap_seconds: float | None,
    min_gap_seconds: float | None = None,
    max_gap_seconds: float | None = None,
    *,
    config: dict[str, Any],
) -> tuple[str, float]:
    """Infer time granularity from gap statistics.

    Args:
        median_gap_seconds: Median gap between consecutive timestamps in seconds
        min_gap_seconds: Minimum gap (optional, used for confidence)
        max_gap_seconds: Maximum gap (optional, used for confidence)
        config: Temporal config dict (from config/phases/temporal.yaml)

    Returns:
        Tuple of (granularity_name, confidence)
    """
    if median_gap_seconds is None:
        return ("unknown", 0.0)

    cfg = config["granularity"]
    default_confidence = cfg["default_confidence"]
    irregular_confidence = cfg["irregular_confidence"]
    variation_divisor = cfg["variation_divisor"]

    granularities = [(d[0], d[1], d[2]) for d in cfg["definitions"]]

    # Find closest match
    best_match = None
    best_distance = float("inf")

    for name, expected_seconds, tolerance in granularities:
        distance = abs(median_gap_seconds - expected_seconds)
        if distance < tolerance and distance < best_distance:
            best_match = name
            best_distance = distance

    if best_match:
        # Calculate confidence based on consistency
        # Higher confidence if min/max are close to median
        if min_gap_seconds and max_gap_seconds and median_gap_seconds > 0:
            variation = (max_gap_seconds - min_gap_seconds) / median_gap_seconds
            confidence = max(0.5, 1.0 - min(variation / variation_divisor, 0.5))
        else:
            confidence = default_confidence
        return (best_match, confidence)

    return ("irregular", irregular_confidence)


def count_grain_periods(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    granularity: str,
) -> tuple[int, int] | None:
    """Count the present and the expected ``granularity`` buckets of a time column.

    Both halves of the completeness ratio, in ONE unit and from one pass:

    - **actual** — how many grain buckets carry an observation.
    - **expected** — how many grain buckets the ``[min, max]`` window contains, counted
      on the CALENDAR (``date_diff`` between the truncated endpoints, inclusive), not
      by dividing elapsed seconds by a nominal period length.

    The calendar is the whole point. Config's per-granularity seconds are *nominal*
    (a month is "2592000s" = 30 days), so seconds-division silently undercounts real
    calendar buckets: Jan 1 / Feb 1 / Mar 1 spans 59 days, and 59/30 + 1 = 2 "months"
    for 3 actual month buckets — a 1.5 ratio out of a denominator that never had the
    same unit as the numerator. Those nominal seconds are for *inferring* the grain
    (:func:`infer_granularity`), which is all they are accurate enough for; they must
    never be the denominator (DAT-810).

    So ``actual <= expected`` now holds by construction: every distinct bucket in the
    data lies inside the closed window the expected count enumerates.

    Returns:
        ``(actual, expected)``, or ``None`` when ``granularity`` is not a ``date_trunc``
        part (the ``irregular``/``unknown`` sentinels) — no bucket exists to count, so
        it falls loud rather than returning a plausible 0.
    """
    if granularity not in DATE_TRUNC_GRAINS:
        return None

    # `granularity` is a member of the closed DATE_TRUNC_GRAINS set (checked above),
    # never caller input; table/column are internal catalog identifiers.
    bucket = f"date_trunc('{granularity}', \"{column_name}\"::TIMESTAMP)"
    row = duckdb_conn.execute(
        f"""
        SELECT
            COUNT(DISTINCT {bucket}),
            date_diff('{granularity}', MIN({bucket}), MAX({bucket})) + 1
        FROM {table_name}
        WHERE "{column_name}" IS NOT NULL
    """  # noqa: S608
    ).fetchone()

    if not row or row[0] is None or row[1] is None:
        return None
    return int(row[0]), int(row[1])


def analyze_basic_temporal(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    *,
    config: dict[str, Any],
) -> Result[dict[str, Any]]:
    """Analyze basic temporal characteristics of a time column.

    Extracts:
    - Min/max timestamps
    - Distinct count
    - Gap statistics
    - Granularity inference
    - Completeness analysis

    Args:
        duckdb_conn: DuckDB connection
        table_name: DuckDB table name (e.g., 'typed_sales')
        column_name: Column name
        config: Temporal config dict (from config/phases/temporal.yaml)

    Returns:
        Result containing basic temporal analysis dict
    """
    try:
        gap_cfg = config["gaps"]
        stale_mult = config["staleness"]["stale_multiplier"]
        # Get min/max timestamps and count. Cast to TIMESTAMP so a DATE column
        # also yields a datetime (the model + DB column are DateTime-typed).
        # `distinct_count` here is raw distinct INSTANTS — reported as-is, and
        # deliberately NOT the completeness numerator (see below).
        result = duckdb_conn.execute(
            f"""
            SELECT
                MIN("{column_name}"::TIMESTAMP) as min_ts,
                MAX("{column_name}"::TIMESTAMP) as max_ts,
                COUNT(DISTINCT "{column_name}") as distinct_count,
                COUNT(*) as total_count
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
        """
        ).fetchone()

        if not result:
            return Result.fail("No result found")

        min_ts, max_ts, distinct_count, total_count = result

        if not min_ts or not max_ts:
            return Result.fail("No valid timestamps found")

        # Detect granularity by looking at consecutive gaps
        gap_result = duckdb_conn.execute(
            f"""
            WITH ordered_ts AS (
                SELECT DISTINCT "{column_name}" as ts
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
                ORDER BY ts
            ),
            gaps AS (
                SELECT
                    ts,
                    LEAD(ts) OVER (ORDER BY ts) as next_ts,
                    date_diff('second', ts, next_ts) as gap_seconds
                FROM ordered_ts
            )
            SELECT
                percentile_cont(0.5) WITHIN GROUP (ORDER BY gap_seconds) as median_gap_seconds,
                MIN(gap_seconds) as min_gap_seconds,
                MAX(gap_seconds) as max_gap_seconds
            FROM gaps
            WHERE gap_seconds IS NOT NULL
        """
        ).fetchone()

        median_gap, min_gap, max_gap = gap_result if gap_result else (None, None, None)

        # Infer granularity from median gap. This is why the bucket count below needs
        # its own round-trip: the grain is not declared, it is inferred from the gaps,
        # so it isn't known until the query above has run and cannot be folded into it.
        granularity, confidence = infer_granularity(median_gap, min_gap, max_gap, config=config)

        # Both sides of the ratio in ONE unit: calendar grain buckets. `distinct_count`
        # above is raw INSTANTS — a different unit, and dividing it by a period count is
        # what let the ratio exceed 1 (a TIMESTAMP column with sub-grain resolution has
        # more instants than buckets) and get clamped to a false "perfectly complete" 1.0.
        periods = count_grain_periods(duckdb_conn, table_name, column_name, granularity)
        actual_periods, expected_periods = periods if periods else (None, None)

        # No clamp (DAT-810): actual and expected are both calendar bucket counts over
        # the same closed window, so actual <= expected holds by construction and a >1
        # ratio is unreachable. A clamp here could only hide the next unit mismatch —
        # exactly what it did before — so it is gone rather than kept "for safety".
        completeness_ratio = (
            actual_periods / expected_periods
            if actual_periods is not None and expected_periods is not None
            else None
        )

        # Detect significant gaps
        sig_gap_mult = gap_cfg["significant_gap_multiplier"]
        sev_severe_mult = gap_cfg["severity_severe_multiplier"]
        sev_moderate_mult = gap_cfg["severity_moderate_multiplier"]

        gaps = []
        if median_gap:
            gap_threshold = median_gap * sig_gap_mult
            significant_gaps = duckdb_conn.execute(
                f"""
                WITH ordered_ts AS (
                    SELECT DISTINCT "{column_name}" as ts
                    FROM {table_name}
                    WHERE "{column_name}" IS NOT NULL
                    ORDER BY ts
                ),
                gaps AS (
                    SELECT
                        ts as gap_start,
                        LEAD(ts) OVER (ORDER BY ts) as gap_end,
                        date_diff('second', gap_start, gap_end) as gap_seconds
                    FROM ordered_ts
                )
                SELECT gap_start, gap_end, gap_seconds
                FROM gaps
                WHERE gap_seconds > {gap_threshold}
                ORDER BY gap_seconds DESC
            """
            ).fetchall()

            if significant_gaps:
                for gap_start, gap_end, gap_seconds in significant_gaps:
                    if gap_start and gap_end:
                        missing_periods = int(gap_seconds / median_gap) - 1 if median_gap > 0 else 0
                        gap_length_days = gap_seconds / (24 * 3600)
                        # Determine severity based on gap size relative to median
                        if gap_seconds > median_gap * sev_severe_mult:
                            severity = "severe"
                        elif gap_seconds > median_gap * sev_moderate_mult:
                            severity = "moderate"
                        else:
                            severity = "minor"
                        gaps.append(
                            TemporalGapInfo(
                                gap_start=gap_start,
                                gap_end=gap_end,
                                gap_length_days=gap_length_days,
                                missing_periods=missing_periods,
                                severity=severity,
                            )
                        )

        # Calculate span_days
        span_days = (max_ts - min_ts).total_seconds() / (24 * 3600)

        # Build completeness analysis
        largest_gap_days = max((g.gap_length_days for g in gaps), default=None) if gaps else None

        completeness = TemporalCompletenessAnalysis(
            completeness_ratio=completeness_ratio,
            expected_periods=expected_periods,
            actual_periods=actual_periods,
            gap_count=len(gaps),
            largest_gap_days=largest_gap_days,
            gaps=gaps,
        )

        # Staleness: is the freshest observation old relative to the detected
        # cadence? Measured on the DISTINCT-timestamp median gap (robust to
        # duplicate-per-day fact rows — the corrupted row-interval path that
        # scored duplicate-heavy columns "always stale" was deleted in DAT-783).
        # With no median gap (a single distinct timestamp — a repeated as_of/
        # period_end date) there is no cadence to be stale against, so is_stale is
        # False rather than "any age at all" → True.
        if median_gap:
            last_ts = max_ts if max_ts.tzinfo else max_ts.replace(tzinfo=UTC)
            freshness_days = (datetime.now(UTC) - last_ts).total_seconds() / 86400
            is_stale = freshness_days > ((median_gap / 86400) * stale_mult)
        else:
            is_stale = False

        return Result.ok(
            {
                "min_timestamp": min_ts,
                "max_timestamp": max_ts,
                "span_days": span_days,
                "distinct_count": distinct_count,
                "total_count": total_count,
                "granularity": granularity,
                "granularity_confidence": confidence,
                "completeness": completeness,
                "median_gap_seconds": median_gap,
                "is_stale": is_stale,
            }
        )

    except Exception as e:
        return Result.fail(f"Failed to analyze basic temporal: {e}")


__all__ = [
    "infer_granularity",
    "count_grain_periods",
    "analyze_basic_temporal",
]
