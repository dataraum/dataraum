"""Basic temporal detection functions.

Detects:
- Time granularity (second, minute, hour, day, week, month, etc.)
- Gaps in time series
- Expected vs actual periods
- Basic completeness

These are foundational functions used by the main processor.
"""

from datetime import datetime
from typing import Any

import duckdb

from dataraum.analysis.temporal.models import (
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
        config: Temporal config dict (from config/system/temporal.yaml)

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


def calculate_expected_periods(
    min_ts: datetime,
    max_ts: datetime,
    granularity: str,
) -> int:
    """Calculate expected number of periods for a time range.

    Args:
        min_ts: Start of time range
        max_ts: End of time range
        granularity: Detected granularity

    Returns:
        Expected number of periods
    """
    delta = max_ts - min_ts
    total_seconds = delta.total_seconds()

    granularity_seconds = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
        "week": 604800,
        "weekly": 604800,
        "month": 2592000,
        "monthly": 2592000,
        "quarter": 7776000,
        "quarterly": 7776000,
        "year": 31536000,
        "yearly": 31536000,
        "irregular": 1,
        "unknown": 1,
    }

    seconds = granularity_seconds.get(granularity, 86400)
    return int(total_seconds / seconds) + 1


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
        config: Temporal config dict (from config/system/temporal.yaml)

    Returns:
        Result containing basic temporal analysis dict
    """
    try:
        gap_cfg = config["gaps"]
        # Get min/max timestamps and count
        result = duckdb_conn.execute(
            f"""
            SELECT
                MIN("{column_name}") as min_ts,
                MAX("{column_name}") as max_ts,
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

        # Infer granularity from median gap
        granularity, confidence = infer_granularity(median_gap, min_gap, max_gap, config=config)

        # Calculate expected periods based on granularity
        expected_periods = calculate_expected_periods(min_ts, max_ts, granularity)

        # Calculate completeness
        completeness_ratio = distinct_count / expected_periods if expected_periods > 0 else 1.0

        # Detect significant gaps
        sig_gap_mult = gap_cfg["significant_gap_multiplier"]
        sev_severe_mult = gap_cfg["severity_severe_multiplier"]
        sev_moderate_mult = gap_cfg["severity_moderate_multiplier"]
        max_gaps_limit = gap_cfg["max_gaps"]

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
                LIMIT {max_gaps_limit}
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
        completeness_ratio_clamped = min(completeness_ratio, 1.0)
        largest_gap_days = max((g.gap_length_days for g in gaps), default=None) if gaps else None

        completeness = TemporalCompletenessAnalysis(
            completeness_ratio=completeness_ratio_clamped,
            expected_periods=expected_periods,
            actual_periods=distinct_count,
            gap_count=len(gaps),
            largest_gap_days=largest_gap_days,
            gaps=gaps,
        )

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
            }
        )

    except Exception as e:
        return Result.fail(f"Failed to analyze basic temporal: {e}")


__all__ = [
    "infer_granularity",
    "calculate_expected_periods",
    "analyze_basic_temporal",
]
