"""Temporal enrichment for time columns."""

from datetime import datetime

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import (
    ColumnRef,
    Result,
    TemporalEnrichmentResult,
    TemporalGap,
    TemporalProfile,
)
from dataraum_context.storage.models_v2 import Column, Table

# Note: TemporalProfile storage model was replaced by TemporalQualityMetrics in models_v2
# from dataraum_context.storage.models_v2 import TemporalProfile as TemporalProfileModel


async def enrich_temporal(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
) -> Result[TemporalEnrichmentResult]:
    """Extract temporal patterns from time columns.

    Steps:
    1. Find columns with DATE/TIMESTAMP types
    2. Analyze each time column using DuckDB
    3. Detect granularity (day, week, month, etc.)
    4. Find gaps in time series
    5. Store temporal profiles

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        table_ids: List of table IDs to analyze

    Returns:
        Result containing temporal enrichment data
    """
    # Find timestamp columns (based on resolved type)
    stmt = (
        select(Column, Table)
        .join(Table)
        .where(
            Table.table_id.in_(table_ids),
            Column.resolved_type.in_(["DATE", "TIMESTAMP", "TIMESTAMPTZ"]),
        )
    )
    result = await session.execute(stmt)
    timestamp_columns = result.all()

    profiles = []

    for col, table in timestamp_columns:
        # Analyze time column
        temporal_result = await _analyze_time_column(
            duckdb_conn,
            table.table_name,
            table.layer,
            col.column_name,
        )

        if temporal_result.success and temporal_result.value:
            profile = temporal_result.value
            profile.column_id = col.column_id
            profile.column_ref = ColumnRef(
                table_name=table.table_name,
                column_name=col.column_name,
            )
            profiles.append(profile)

            # Store in database
            await _store_temporal_profile(session, profile)

    await session.commit()

    return Result.ok(TemporalEnrichmentResult(profiles=profiles))


async def _analyze_time_column(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    layer: str,
    column_name: str,
) -> Result[TemporalProfile]:
    """Analyze a time column to extract temporal patterns."""
    try:
        # Determine actual table name
        actual_table = f"typed_{table_name}" if layer == "typed" else f"raw_{table_name}"

        # Get min/max timestamps and count
        result = duckdb_conn.execute(
            f"""
            SELECT
                MIN({column_name})::TIMESTAMP as min_ts,
                MAX({column_name})::TIMESTAMP as max_ts,
                COUNT(DISTINCT {column_name}) as distinct_count,
                COUNT(*) as total_count
            FROM {actual_table}
            WHERE {column_name} IS NOT NULL
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
                SELECT DISTINCT {column_name}::TIMESTAMP as ts
                FROM {actual_table}
                WHERE {column_name} IS NOT NULL
                ORDER BY ts
            ),
            gaps AS (
                SELECT
                    ts,
                    LEAD(ts) OVER (ORDER BY ts) as next_ts,
                    epoch(LEAD(ts) OVER (ORDER BY ts) - ts) as gap_seconds
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
        granularity, confidence = _infer_granularity(median_gap, min_gap, max_gap)

        # Calculate expected periods based on granularity
        expected_periods = _calculate_expected_periods(min_ts, max_ts, granularity)

        # Calculate completeness
        completeness_ratio = distinct_count / expected_periods if expected_periods > 0 else 1.0

        # Detect significant gaps (gaps > 2x median)
        gaps = []
        if median_gap:
            gap_threshold = median_gap * 2
            gap_result = duckdb_conn.execute(
                f"""
                WITH ordered_ts AS (
                    SELECT DISTINCT {column_name}::TIMESTAMP as ts
                    FROM {actual_table}
                    WHERE {column_name} IS NOT NULL
                    ORDER BY ts
                ),
                gaps AS (
                    SELECT
                        ts as gap_start,
                        LEAD(ts) OVER (ORDER BY ts) as gap_end,
                        epoch(LEAD(ts) OVER (ORDER BY ts) - ts) as gap_seconds
                    FROM ordered_ts
                )
                SELECT gap_start, gap_end, gap_seconds
                FROM gaps
                WHERE gap_seconds > {gap_threshold}
                ORDER BY gap_seconds DESC
                LIMIT 10
            """
            ).fetchall()

            for gap_start, gap_end, gap_seconds in gap_result:
                if gap_start and gap_end:
                    missing_periods = int(gap_seconds / median_gap) - 1 if median_gap > 0 else 0
                    gaps.append(
                        TemporalGap(
                            start=gap_start,
                            end=gap_end,
                            missing_periods=missing_periods,
                        )
                    )

        # Build temporal profile
        profile = TemporalProfile(
            column_id="",  # Filled by caller
            column_ref=ColumnRef(table_name="", column_name=""),  # Filled by caller
            min_timestamp=min_ts,
            max_timestamp=max_ts,
            detected_granularity=granularity,
            granularity_confidence=confidence,
            expected_periods=expected_periods,
            actual_periods=distinct_count,
            completeness_ratio=min(completeness_ratio, 1.0),
            gap_count=len(gaps),
            gaps=gaps,
            has_seasonality=False,  # Placeholder - can be enhanced later
            seasonality_period=None,
            trend_direction=None,
        )

        return Result.ok(profile)

    except Exception as e:
        return Result.fail(f"Failed to analyze time column: {e}")


def _infer_granularity(
    median_gap: float | None, min_gap: float | None, max_gap: float | None
) -> tuple[str, float]:
    """Infer time granularity from gap statistics.

    Returns:
        Tuple of (granularity_name, confidence)
    """
    if median_gap is None:
        return ("unknown", 0.0)

    # Define expected gaps for each granularity (in seconds)
    granularities = [
        ("second", 1, 0.5),
        ("minute", 60, 5),
        ("hour", 3600, 300),
        ("day", 86400, 3600),
        ("week", 604800, 86400),
        ("month", 2592000, 259200),  # ~30 days
        ("quarter", 7776000, 777600),  # ~90 days
        ("year", 31536000, 3153600),  # ~365 days
    ]

    # Find closest match
    best_match = None
    best_distance = float("inf")

    for name, expected_seconds, tolerance in granularities:
        distance = abs(median_gap - expected_seconds)
        if distance < tolerance and distance < best_distance:
            best_match = name
            best_distance = distance

    if best_match:
        # Calculate confidence based on consistency
        # Higher confidence if min/max are close to median
        if min_gap and max_gap and median_gap > 0:
            variation = (max_gap - min_gap) / median_gap
            confidence = max(0.5, 1.0 - min(variation / 10, 0.5))
        else:
            confidence = 0.7
        return (best_match, confidence)

    return ("irregular", 0.3)


def _calculate_expected_periods(min_ts: datetime, max_ts: datetime, granularity: str) -> int:
    """Calculate expected number of periods for a time range."""
    delta = max_ts - min_ts
    total_seconds = delta.total_seconds()

    granularity_seconds = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
        "week": 604800,
        "month": 2592000,
        "quarter": 7776000,
        "year": 31536000,
        "irregular": 1,
        "unknown": 1,
    }

    seconds = granularity_seconds.get(granularity, 86400)
    return int(total_seconds / seconds) + 1


async def _store_temporal_profile(
    session: AsyncSession,
    profile: TemporalProfile,
) -> None:
    """Store temporal profile in database."""
    # Convert gaps to JSON format
    gaps_json = None
    if profile.gaps:
        gaps_json = {
            "gaps": [
                {
                    "start": gap.start.isoformat(),
                    "end": gap.end.isoformat(),
                    "missing_periods": gap.missing_periods,
                }
                for gap in profile.gaps
            ]
        }

    # TODO: Update to use TemporalQualityMetrics from models_v2
    # The old TemporalProfile model was replaced by TemporalQualityMetrics in the 5-pillar architecture
    # This persistence needs to be reimplemented as part of Phase 4 (Temporal Context)
    #
    # db_profile = TemporalQualityMetrics(
    #     column_id=profile.column_id,
    #     ...  # Map fields to new schema
    # )
    # session.add(db_profile)
