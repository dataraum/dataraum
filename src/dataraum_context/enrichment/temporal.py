"""Temporal enrichment for time columns."""

from datetime import datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import ColumnRef, Result
from dataraum_context.enrichment.models import TemporalEnrichmentResult
from dataraum_context.quality.models import (
    TemporalCompletenessAnalysis,
    TemporalGapInfo,
    TemporalQualityResult,
    TemporalTableSummary,
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
            quality_result = temporal_result.value
            # Fill in metadata fields
            quality_result.metric_id = str(uuid4())
            quality_result.column_id = col.column_id
            quality_result.column_ref = ColumnRef(
                table_name=table.table_name,
                column_name=col.column_name,
            )
            quality_result.column_name = col.column_name
            quality_result.table_name = table.table_name
            profiles.append(quality_result)

            # Store in database
            await _store_temporal_profile(session, quality_result)

    await session.commit()

    # Compute and persist table-level summaries
    processed_tables = set()
    for table_id in table_ids:
        if table_id not in processed_tables:
            summary_result = await compute_table_temporal_summary(table_id, session)
            if summary_result.success and summary_result.value:
                await persist_table_temporal_summary(summary_result.value, session)
            processed_tables.add(table_id)

    await session.commit()

    return Result.ok(TemporalEnrichmentResult(profiles=profiles))


async def _analyze_time_column(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    layer: str,
    column_name: str,
) -> Result[TemporalQualityResult]:
    """Analyze a time column to extract temporal patterns."""
    try:
        # Determine actual table name
        actual_table = f"typed_{table_name}" if layer == "typed" else f"raw_{table_name}"

        # Get min/max timestamps and count
        result = duckdb_conn.execute(
            f"""
            SELECT
                MIN("{column_name}") as min_ts,
                MAX("{column_name}") as max_ts,
                COUNT(DISTINCT "{column_name}") as distinct_count,
                COUNT(*) as total_count
            FROM {actual_table}
            WHERE "{column_name}" IS NOT NULL
        """
        ).fetchone()
        print(result)

        if not result:
            return Result.fail("No result found")

        min_ts, max_ts, distinct_count, total_count = result
        print(min_ts, max_ts, distinct_count, total_count)

        if not min_ts or not max_ts:
            return Result.fail("No valid timestamps found")

        # Detect granularity by looking at consecutive gaps
        gap_result = duckdb_conn.execute(
            f"""
            WITH ordered_ts AS (
                SELECT DISTINCT "{column_name}" as ts
                FROM {actual_table}
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
        granularity, confidence = _infer_granularity(median_gap, min_gap, max_gap)

        # Calculate expected periods based on granularity
        expected_periods = _calculate_expected_periods(min_ts, max_ts, granularity)

        # Calculate completeness
        completeness_ratio = distinct_count / expected_periods if expected_periods > 0 else 1.0

        # Detect significant gaps (gaps > 2x median)
        gaps = []
        if median_gap:
            gap_threshold = median_gap * 2
            significant_gaps = duckdb_conn.execute(
                f"""
                WITH ordered_ts AS (
                    SELECT DISTINCT "{column_name}" as ts
                    FROM {actual_table}
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
                LIMIT 10
            """
            ).fetchall()

            if significant_gaps:
                for gap_start, gap_end, gap_seconds in significant_gaps:
                    if gap_start and gap_end:
                        missing_periods = int(gap_seconds / median_gap) - 1 if median_gap > 0 else 0
                        gap_length_days = gap_seconds / (24 * 3600)  # Convert seconds to days
                        # Determine severity based on gap size relative to median
                        if gap_seconds > median_gap * 10:
                            severity = "severe"
                        elif gap_seconds > median_gap * 5:
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

        # Build temporal quality result (minimal version - full analysis done in quality/temporal.py)
        quality_result = TemporalQualityResult(
            metric_id="",  # Filled by caller
            column_id="",  # Filled by caller
            column_ref=ColumnRef(table_name="", column_name=""),  # Filled by caller
            column_name="",  # Filled by caller
            table_name="",  # Filled by caller
            computed_at=datetime.now(),
            min_timestamp=min_ts,
            max_timestamp=max_ts,
            span_days=span_days,
            detected_granularity=granularity,
            granularity_confidence=confidence,
            completeness=completeness,
            has_issues=completeness_ratio_clamped < 0.9,
        )

        return Result.ok(quality_result)

    except Exception as e:
        print(f"Error analyzing time column {column_name} in table {table_name}: {e}")
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
    result: TemporalQualityResult,
) -> None:
    """Store temporal quality result in database using hybrid storage approach.

    HYBRID STORAGE:
    - Structured fields: Queryable dimensions (timestamps, granularity, completeness, flags)
    - JSONB field: Complete TemporalQualityResult model (zero mapping code)
    """
    from uuid import uuid4

    from dataraum_context.enrichment.db_models import TemporalQualityMetrics

    # Persist using hybrid storage
    db_metric = TemporalQualityMetrics(
        metric_id=result.metric_id if result.metric_id else str(uuid4()),
        column_id=result.column_id,
        computed_at=result.computed_at,
        # STRUCTURED: Queryable dimensions
        min_timestamp=result.min_timestamp,
        max_timestamp=result.max_timestamp,
        detected_granularity=result.detected_granularity,
        completeness_ratio=result.completeness.completeness_ratio if result.completeness else 0.0,
        # Flags for filtering
        has_seasonality=result.seasonality.has_seasonality if result.seasonality else False,
        has_trend=result.trend.has_trend if result.trend else False,
        is_stale=False,  # Will be computed in quality/temporal.py
        # JSONB: Full Pydantic model (zero mapping!)
        temporal_data=result.model_dump(mode="json"),
    )

    session.add(db_metric)


async def compute_table_temporal_summary(
    table_id: str,
    session: AsyncSession,
) -> Result[TemporalTableSummary]:
    """Compute table-level summary of temporal enrichment across all temporal columns.

    This aggregates individual column temporal profiles into a table-level summary
    showing patterns and metrics across all temporal columns in the table.

    Args:
        table_id: Table to analyze
        session: Database session

    Returns:
        Result containing table-level temporal summary
    """
    try:
        from dataraum_context.enrichment.db_models import (
            TemporalQualityMetrics as DBTemporalMetrics,
        )

        # Get table info
        stmt = select(Table).where(Table.table_id == table_id)
        result = await session.execute(stmt)
        table = result.scalar_one_or_none()

        if not table:
            return Result.fail(f"Table {table_id} not found")

        # Get all temporal quality metrics for this table's columns
        metrics_stmt = (
            select(DBTemporalMetrics)
            .join(Column, DBTemporalMetrics.column_id == Column.column_id)
            .where(Column.table_id == table_id)
        )
        metrics_result = await session.execute(metrics_stmt)
        metrics = metrics_result.scalars().all()

        if not metrics:
            return Result.fail(f"No temporal enrichment data found for table {table_id}")

        # Aggregate metrics across columns
        temporal_column_count = len(metrics)
        total_issues = 0
        columns_with_seasonality = 0
        columns_with_trends = 0
        columns_with_change_points = 0
        columns_with_fiscal_alignment = 0
        stalest_column_days = 0
        has_stale_columns = False

        for metric in metrics:
            # Parse temporal_data JSONB
            temporal_data = metric.temporal_data
            if not temporal_data:
                continue

            # Issue count
            if "quality_issues" in temporal_data:
                total_issues += len(temporal_data["quality_issues"])

            # Seasonality
            if "seasonality" in temporal_data and temporal_data["seasonality"]:
                if temporal_data["seasonality"].get("has_seasonality", False):
                    columns_with_seasonality += 1

            # Trends
            if "trend" in temporal_data and temporal_data["trend"]:
                if temporal_data["trend"].get("has_trend", False):
                    columns_with_trends += 1

            # Change points
            if "change_points" in temporal_data:
                if len(temporal_data["change_points"]) > 0:
                    columns_with_change_points += 1

            # Fiscal alignment
            if "fiscal_calendar" in temporal_data and temporal_data["fiscal_calendar"]:
                if temporal_data["fiscal_calendar"].get("fiscal_alignment_detected", False):
                    columns_with_fiscal_alignment += 1

            # Staleness
            if metric.is_stale:
                has_stale_columns = True
            # Track stalest column
            if "update_frequency" in temporal_data and temporal_data["update_frequency"]:
                freshness_days = temporal_data["update_frequency"].get("data_freshness_days", 0)
                if freshness_days > stalest_column_days:
                    stalest_column_days = freshness_days

        summary = TemporalTableSummary(
            table_id=table_id,
            table_name=table.table_name,
            temporal_column_count=temporal_column_count,
            total_issues=total_issues,
            columns_with_seasonality=columns_with_seasonality,
            columns_with_trends=columns_with_trends,
            columns_with_change_points=columns_with_change_points,
            columns_with_fiscal_alignment=columns_with_fiscal_alignment,
            stalest_column_days=int(stalest_column_days) if stalest_column_days > 0 else None,
            has_stale_columns=has_stale_columns,
        )

        return Result.ok(summary)

    except Exception as e:
        return Result.fail(f"Failed to compute table temporal summary: {e}")


async def persist_table_temporal_summary(
    summary: TemporalTableSummary,
    session: AsyncSession,
) -> Result[str]:
    """Persist table-level temporal summary to database.

    Args:
        summary: Computed TemporalTableSummary to persist
        session: Async database session

    Returns:
        Result with table_id on success
    """
    try:
        from dataraum_context.enrichment.db_models import (
            TemporalTableSummaryMetrics as DBTableSummary,
        )

        # Check if record exists (upsert pattern)
        stmt = select(DBTableSummary).where(DBTableSummary.table_id == summary.table_id)
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            # Update existing record
            existing.computed_at = datetime.now()
            existing.temporal_column_count = summary.temporal_column_count
            existing.total_issues = summary.total_issues
            existing.columns_with_seasonality = summary.columns_with_seasonality
            existing.columns_with_trends = summary.columns_with_trends
            existing.columns_with_change_points = summary.columns_with_change_points
            existing.columns_with_fiscal_alignment = summary.columns_with_fiscal_alignment
            existing.stalest_column_days = summary.stalest_column_days
            existing.has_stale_columns = summary.has_stale_columns
            existing.summary_data = summary.model_dump()
        else:
            # Create new record
            db_summary = DBTableSummary(
                table_id=summary.table_id,
                computed_at=datetime.now(),
                temporal_column_count=summary.temporal_column_count,
                total_issues=summary.total_issues,
                columns_with_seasonality=summary.columns_with_seasonality,
                columns_with_trends=summary.columns_with_trends,
                columns_with_change_points=summary.columns_with_change_points,
                columns_with_fiscal_alignment=summary.columns_with_fiscal_alignment,
                stalest_column_days=summary.stalest_column_days,
                has_stale_columns=summary.has_stale_columns,
                summary_data=summary.model_dump(),
            )
            session.add(db_summary)

        await session.commit()
        return Result.ok(summary.table_id)

    except Exception as e:
        await session.rollback()
        return Result.fail(f"Failed to persist table temporal summary: {e}")
