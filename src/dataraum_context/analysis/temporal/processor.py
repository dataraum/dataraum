"""Temporal analysis processor.

Main entry point for temporal profiling, following the same pattern as statistics:
- profile_temporal(table_id, ...): Profile all temporal columns in a table

This analyzes temporal characteristics like:
- Granularity (daily, hourly, etc.)
- Completeness and gaps
- Seasonality patterns
- Trends
- Change points
- Update frequency and staleness

Uses parallel processing for large tables to speed up profiling.
"""

from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum_context.analysis.temporal.db_models import (
    TemporalColumnProfile,
)
from dataraum_context.analysis.temporal.db_models import (
    TemporalTableSummary as DBTemporalTableSummary,
)
from dataraum_context.analysis.temporal.detection import (
    analyze_basic_temporal,
    infer_granularity,
)
from dataraum_context.analysis.temporal.models import (
    TemporalAnalysisResult,
    TemporalProfileResult,
    TemporalQualityIssue,
    TemporalTableSummary,
)
from dataraum_context.analysis.temporal.patterns import (
    analyze_distribution_stability,
    analyze_seasonality,
    analyze_trend,
    analyze_update_frequency,
    detect_change_points,
    detect_fiscal_calendar,
)
from dataraum_context.core.models.base import ColumnRef, Result
from dataraum_context.storage import Column, Table


def _profile_temporal_column_parallel(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    table_duckdb_path: str,
    source_id: str,
    column_id: str,
    column_name: str,
    profiled_at: datetime,
) -> TemporalAnalysisResult | None:
    """Profile a single temporal column in a worker thread.

    Runs in its own thread using a cursor from the shared DuckDB connection.
    DuckDB cursors are thread-safe for read operations.
    Returns TemporalAnalysisResult directly for the main thread to persist.
    """
    cursor = duckdb_conn.cursor()
    try:
        # Load time series
        ts_result = _load_time_series(cursor, table_duckdb_path, column_name)
        if not ts_result.success:
            return None

        time_series = ts_result.unwrap()

        # Basic temporal info
        min_timestamp = time_series.index.min().to_pydatetime()
        max_timestamp = time_series.index.max().to_pydatetime()
        span_days = (max_timestamp - min_timestamp).total_seconds() / 86400

        # Infer granularity
        interval: pd.Series = time_series.index.to_series().diff()
        median_interval = interval.median()

        if isinstance(median_interval, (int, float)):
            interval_seconds = float(median_interval)
        else:
            interval_seconds = median_interval.total_seconds()

        granularity, confidence = infer_granularity(interval_seconds)

        metric_id = str(uuid4())

        # Run pattern analyses
        seasonality_result = analyze_seasonality(time_series)
        seasonality = seasonality_result.value if seasonality_result.success else None

        trend_result = analyze_trend(time_series)
        trend = trend_result.value if trend_result.success else None

        changes_result = detect_change_points(time_series)
        change_points = changes_result.unwrap() if changes_result.success else []

        frequency_result = analyze_update_frequency(time_series)
        update_frequency = frequency_result.value if frequency_result.success else None

        fiscal_result = detect_fiscal_calendar(time_series)
        fiscal_calendar = fiscal_result.value if fiscal_result.success else None

        stability_result = analyze_distribution_stability(time_series)
        distribution_stability = stability_result.value if stability_result.success else None

        # Run basic temporal analysis for completeness
        basic_result = analyze_basic_temporal(cursor, table_duckdb_path, column_name)
        completeness = (
            basic_result.value.get("completeness")
            if basic_result.success and basic_result.value
            else None
        )

        # Detect quality issues
        issues = _detect_quality_issues(
            completeness=completeness,
            update_frequency=update_frequency,
            change_points=change_points,
            distribution_stability=distribution_stability,
            profiled_at=profiled_at,
        )

        # Build result
        column_ref = ColumnRef(
            source_id=source_id,
            table_name=table_name,
            column_name=column_name,
        )

        return TemporalAnalysisResult(
            metric_id=metric_id,
            column_id=column_id,
            column_ref=column_ref,
            column_name=column_name,
            table_name=table_name,
            computed_at=profiled_at,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
            span_days=span_days,
            detected_granularity=granularity,
            granularity_confidence=confidence,
            completeness=completeness,
            seasonality=seasonality,
            trend=trend,
            change_points=change_points,
            update_frequency=update_frequency,
            fiscal_calendar=fiscal_calendar,
            distribution_stability=distribution_stability,
            quality_issues=issues,
            has_issues=len(issues) > 0,
        )
    except Exception:
        return None
    finally:
        cursor.close()


def profile_temporal(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    max_workers: int = 4,
) -> Result[TemporalProfileResult]:
    """Profile temporal columns in a table.

    This is the main entry point for temporal analysis, following the
    same pattern as profile_statistics(). It:
    1. Gets all temporal columns for the table
    2. Analyzes each column for temporal patterns
    3. Stores per-column profiles
    4. Computes and stores table-level summary

    Uses parallel processing for file-based DBs to speed up profiling.

    REQUIRES table.layer == "typed". Raises error otherwise.

    Args:
        table_id: Table ID to profile
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        max_workers: Maximum parallel workers

    Returns:
        Result containing TemporalProfileResult with all column profiles
    """
    try:
        # Get table from metadata
        table = session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        if table.layer != "typed":
            return Result.fail(f"Temporal profiling requires typed tables. Got: {table.layer}")

        # Get all temporal columns for this table
        temporal_types = ["DATE", "TIMESTAMP", "TIMESTAMPTZ"]
        column_stmt = (
            select(Column)
            .where(
                Column.table_id == table.table_id,
                Column.resolved_type.in_(temporal_types),
            )
            .order_by(Column.column_position)
        )
        column_result = session.execute(column_stmt)
        columns = column_result.scalars().all()

        if not columns:
            return Result.ok(
                TemporalProfileResult(
                    column_profiles=[],
                    table_summary=None,
                    duration_seconds=0.0,
                )
            )

        profiled_at = datetime.now(UTC)
        profiles: list[TemporalAnalysisResult] = []
        db_profiles: list[TemporalColumnProfile] = []
        import time

        start_time = time.time()

        # Use parallel processing with cursors from shared connection
        # DuckDB cursors are thread-safe for read operations
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _profile_temporal_column_parallel,
                    duckdb_conn,
                    table.table_name,
                    table.duckdb_path,
                    table.source_id,
                    column.column_id,
                    column.column_name,
                    profiled_at,
                )
                for column in columns
            ]

            for future in futures:
                profile = future.result()
                if profile:
                    profiles.append(profile)

                    # Create DB object (sequential - SQLite writes)
                    db_profile = TemporalColumnProfile(
                        profile_id=profile.metric_id,
                        column_id=profile.column_id,
                        profiled_at=profiled_at,
                        min_timestamp=profile.min_timestamp,
                        max_timestamp=profile.max_timestamp,
                        detected_granularity=profile.detected_granularity,
                        completeness_ratio=(
                            profile.completeness.completeness_ratio
                            if profile.completeness
                            else None
                        ),
                        has_seasonality=profile.seasonality.has_seasonality
                        if profile.seasonality
                        else False,
                        has_trend=profile.trend.has_trend if profile.trend else False,
                        is_stale=profile.update_frequency.is_stale
                        if profile.update_frequency
                        else False,
                        profile_data=profile.model_dump(mode="json"),
                    )
                    db_profiles.append(db_profile)

        # Compute table-level summary
        table_summary = None
        if profiles:
            table_summary = _compute_table_summary(table, profiles, profiled_at)

        # Now add all objects to session at once (avoids autoflush issues)
        for db_profile in db_profiles:
            session.add(db_profile)

        if table_summary:
            _persist_table_summary(table_summary, session)

        # No flush needed - commit happens at session_scope() end
        # The caller (phase/orchestrator) manages the transaction

        duration = time.time() - start_time

        return Result.ok(
            TemporalProfileResult(
                column_profiles=profiles,
                table_summary=table_summary,
                duration_seconds=duration,
            )
        )

    except Exception as e:
        return Result.fail(f"Temporal profiling failed: {e}")


def _profile_temporal_column(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    profiled_at: datetime,
) -> Result[TemporalAnalysisResult]:
    """Profile a single temporal column.

    Args:
        table: Parent table
        column: Column to profile
        duckdb_conn: DuckDB connection
        profiled_at: Timestamp for this profiling run

    Returns:
        Result containing TemporalAnalysisResult
    """
    try:
        # Determine actual table name in DuckDB
        actual_table = table.duckdb_path or f"typed_{table.table_name}"

        # Load time series
        ts_result = _load_time_series(
            duckdb_conn,
            actual_table,
            column.column_name,
        )

        if not ts_result.success:
            return Result.fail(ts_result.error if ts_result.error else "Unknown Error")

        time_series = ts_result.unwrap()

        # Basic temporal info
        min_timestamp = time_series.index.min().to_pydatetime()
        max_timestamp = time_series.index.max().to_pydatetime()
        span_days = (max_timestamp - min_timestamp).total_seconds() / 86400

        # Infer granularity
        interval: pd.Series = time_series.index.to_series().diff()
        median_interval = interval.median()

        if isinstance(median_interval, (int, float)):
            interval_seconds = float(median_interval)
        else:
            interval_seconds = median_interval.total_seconds()

        granularity, confidence = infer_granularity(interval_seconds)

        metric_id = str(uuid4())

        # Run pattern analyses
        seasonality_result = analyze_seasonality(time_series)
        seasonality = seasonality_result.value if seasonality_result.success else None

        trend_result = analyze_trend(time_series)
        trend = trend_result.value if trend_result.success else None

        changes_result = detect_change_points(time_series)
        change_points = changes_result.unwrap() if changes_result.success else []

        frequency_result = analyze_update_frequency(time_series)
        update_frequency = frequency_result.value if frequency_result.success else None

        fiscal_result = detect_fiscal_calendar(time_series)
        fiscal_calendar = fiscal_result.value if fiscal_result.success else None

        stability_result = analyze_distribution_stability(time_series)
        distribution_stability = stability_result.value if stability_result.success else None

        # Run basic temporal analysis for completeness
        basic_result = analyze_basic_temporal(duckdb_conn, actual_table, column.column_name)
        completeness = (
            basic_result.value.get("completeness")
            if basic_result.success and basic_result.value
            else None
        )

        # Detect quality issues
        issues = _detect_quality_issues(
            completeness=completeness,
            update_frequency=update_frequency,
            change_points=change_points,
            distribution_stability=distribution_stability,
            profiled_at=profiled_at,
        )

        # Build result
        column_ref = ColumnRef(
            source_id=table.source_id,
            table_name=table.table_name,
            column_name=column.column_name,
        )

        result_obj = TemporalAnalysisResult(
            metric_id=metric_id,
            column_id=column.column_id,
            column_ref=column_ref,
            column_name=column.column_name,
            table_name=table.table_name,
            computed_at=profiled_at,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
            span_days=span_days,
            detected_granularity=granularity,
            granularity_confidence=confidence,
            completeness=completeness,
            seasonality=seasonality,
            trend=trend,
            change_points=change_points,
            update_frequency=update_frequency,
            fiscal_calendar=fiscal_calendar,
            distribution_stability=distribution_stability,
            quality_issues=issues,
            has_issues=len(issues) > 0,
        )

        return Result.ok(result_obj)

    except Exception as e:
        return Result.fail(f"Failed to profile column {column.column_name}: {e}")


def _load_time_series(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    sample_percent: float = 20.0,
    min_rows: int = 1000,
) -> Result[pd.Series]:
    """Load time series data from DuckDB using Bernoulli sampling.

    Uses DuckDB's Bernoulli sampling to get a random sample distributed across
    the full time range, then orders by timestamp for temporal analysis.

    Args:
        duckdb_conn: DuckDB connection
        table_name: DuckDB table name
        column_name: Column name
        sample_percent: Percentage of rows to sample (default 20%)
        min_rows: Minimum rows to return (uses higher sample if needed)

    Returns:
        Result containing pandas Series indexed by datetime

    TODO: Revisit sampling strategy for temporal analysis:
        - Bernoulli sampling creates artificial gaps that affect:
          - Granularity detection (sees sampled intervals, not true intervals)
          - Completeness analysis (artificial gaps mask real gaps)
          - Seasonality detection (sparse data may miss patterns)
        - Consider stratified sampling (equal samples from time segments)
        - Consider adaptive sampling based on detected granularity
        - For now, use generous 20% sample which balances coverage vs speed
    """
    try:
        # First, get row count to determine if sampling is needed
        count_query = f"""
        SELECT COUNT(*) as cnt
        FROM {table_name}
        WHERE "{column_name}" IS NOT NULL
        """
        count_result = duckdb_conn.execute(count_query).fetchone()
        total_rows = count_result[0] if count_result else 0

        if total_rows == 0:
            return Result.fail("No data found")

        # Adjust sample percentage to ensure minimum rows
        effective_percent = sample_percent
        if total_rows * (sample_percent / 100) < min_rows:
            effective_percent = min(100.0, (min_rows / total_rows) * 100)

        # Use Bernoulli sampling for random distribution across time range
        # Then ORDER BY to restore temporal sequence
        # Note: In DuckDB, WHERE must come before USING SAMPLE
        if effective_percent >= 100.0 or total_rows <= min_rows:
            # No sampling needed for small tables
            query = f"""
            SELECT "{column_name}"::TIMESTAMP as ts
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            ORDER BY "{column_name}"
            """
        else:
            query = f"""
            SELECT "{column_name}"::TIMESTAMP as ts
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            USING SAMPLE {effective_percent:.1f} PERCENT (bernoulli)
            ORDER BY "{column_name}"
            """

        df = duckdb_conn.execute(query).fetchdf()

        if df.empty:
            return Result.fail("No data found after sampling")

        ts = pd.Series(1, index=pd.to_datetime(df["ts"]))
        ts = ts.sort_index()

        return Result.ok(ts)

    except Exception as e:
        return Result.fail(f"Failed to load time series: {e}")


def _detect_quality_issues(
    completeness: object | None,
    update_frequency: object | None,
    change_points: Sequence[object],
    distribution_stability: object | None,
    profiled_at: datetime,
) -> list[TemporalQualityIssue]:
    """Detect quality issues from analysis results."""
    issues = []

    if completeness and hasattr(completeness, "completeness_ratio"):
        if completeness.completeness_ratio < 0.8:
            issues.append(
                TemporalQualityIssue(
                    issue_type="low_completeness",
                    severity="high" if completeness.completeness_ratio < 0.5 else "medium",
                    description=(
                        f"Only {completeness.completeness_ratio:.1%} of expected "
                        "data points present"
                    ),
                    evidence={"completeness_ratio": completeness.completeness_ratio},
                    detected_at=profiled_at,
                )
            )

        if hasattr(completeness, "largest_gap_days") and completeness.largest_gap_days:
            if completeness.largest_gap_days > 30:
                issues.append(
                    TemporalQualityIssue(
                        issue_type="large_gap",
                        severity="high" if completeness.largest_gap_days > 90 else "medium",
                        description=f"Large gap of {completeness.largest_gap_days:.0f} days detected",
                        evidence={"gap_days": completeness.largest_gap_days},
                        detected_at=profiled_at,
                    )
                )

    if update_frequency and hasattr(update_frequency, "is_stale") and update_frequency.is_stale:
        freshness_days = getattr(update_frequency, "data_freshness_days", None)
        if freshness_days is not None:
            issues.append(
                TemporalQualityIssue(
                    issue_type="stale_data",
                    severity="medium",
                    description=f"Data is {freshness_days:.0f} days old",
                    evidence={"freshness_days": freshness_days},
                    detected_at=profiled_at,
                )
            )

    if len(change_points) > 5:
        issues.append(
            TemporalQualityIssue(
                issue_type="many_change_points",
                severity="medium",
                description=f"{len(change_points)} change points detected (unstable pattern)",
                evidence={"change_point_count": len(change_points)},
                detected_at=profiled_at,
            )
        )

    if (
        distribution_stability
        and hasattr(distribution_stability, "stability_score")
        and distribution_stability.stability_score < 0.7
    ):
        issues.append(
            TemporalQualityIssue(
                issue_type="unstable_distribution",
                severity="medium",
                description=(
                    f"Distribution stability score: {distribution_stability.stability_score:.2f}"
                ),
                evidence={"stability_score": distribution_stability.stability_score},
                detected_at=profiled_at,
            )
        )

    return issues


def _compute_table_summary(
    table: Table,
    profiles: list[TemporalAnalysisResult],
    profiled_at: datetime,
) -> TemporalTableSummary:
    """Compute table-level summary from column profiles."""
    total_issues = sum(len(p.quality_issues) for p in profiles)
    columns_with_seasonality = sum(
        1 for p in profiles if p.seasonality and p.seasonality.has_seasonality
    )
    columns_with_trends = sum(1 for p in profiles if p.trend and p.trend.has_trend)
    columns_with_change_points = sum(1 for p in profiles if p.change_points)
    columns_with_fiscal_alignment = sum(
        1 for p in profiles if p.fiscal_calendar and p.fiscal_calendar.fiscal_alignment_detected
    )

    stalest_column_days = 0
    has_stale_columns = False
    for p in profiles:
        if p.update_frequency:
            if p.update_frequency.is_stale:
                has_stale_columns = True
            freshness = p.update_frequency.data_freshness_days
            if freshness is not None and freshness > stalest_column_days:
                stalest_column_days = int(freshness)

    return TemporalTableSummary(
        table_id=table.table_id,
        table_name=table.table_name,
        temporal_column_count=len(profiles),
        total_issues=total_issues,
        columns_with_seasonality=columns_with_seasonality,
        columns_with_trends=columns_with_trends,
        columns_with_change_points=columns_with_change_points,
        columns_with_fiscal_alignment=columns_with_fiscal_alignment,
        stalest_column_days=stalest_column_days if stalest_column_days > 0 else None,
        has_stale_columns=has_stale_columns,
        profiled_at=profiled_at,
    )


def _persist_table_summary(
    summary: TemporalTableSummary,
    session: Session,
) -> None:
    """Persist table-level temporal summary to database."""
    # Check if record exists (upsert pattern)
    stmt = select(DBTemporalTableSummary).where(DBTemporalTableSummary.table_id == summary.table_id)
    result = session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        # Update existing record
        existing.profiled_at = summary.profiled_at or datetime.now(UTC)
        existing.temporal_column_count = summary.temporal_column_count
        existing.total_issues = summary.total_issues
        existing.columns_with_seasonality = summary.columns_with_seasonality
        existing.columns_with_trends = summary.columns_with_trends
        existing.columns_with_change_points = summary.columns_with_change_points
        existing.columns_with_fiscal_alignment = summary.columns_with_fiscal_alignment
        existing.stalest_column_days = summary.stalest_column_days
        existing.has_stale_columns = summary.has_stale_columns
        existing.summary_data = summary.model_dump(mode="json")
    else:
        # Create new record
        db_summary = DBTemporalTableSummary(
            table_id=summary.table_id,
            profiled_at=summary.profiled_at or datetime.now(UTC),
            temporal_column_count=summary.temporal_column_count,
            total_issues=summary.total_issues,
            columns_with_seasonality=summary.columns_with_seasonality,
            columns_with_trends=summary.columns_with_trends,
            columns_with_change_points=summary.columns_with_change_points,
            columns_with_fiscal_alignment=summary.columns_with_fiscal_alignment,
            stalest_column_days=summary.stalest_column_days,
            has_stale_columns=summary.has_stale_columns,
            summary_data=summary.model_dump(mode="json"),
        )
        session.add(db_summary)


__all__ = [
    "profile_temporal",
]
