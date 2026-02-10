"""Quality summary processor.

Orchestrates quality summary analysis: aggregates slice results and
calls the LLM agent to generate summaries per column.

Supports batching multiple columns per LLM call, parallel batch processing,
and skipping existing reports.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import distinct as sql_distinct
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.quality_summary.db_models import (
    ColumnQualityReport,
    ColumnSliceProfile,
    QualitySummaryRun,
)
from dataraum.analysis.quality_summary.models import (
    AggregatedColumnData,
    ColumnQualitySummary,
    QualitySummaryResult,
)
from dataraum.analysis.quality_summary.variance import filter_interesting_columns
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.statistics.db_models import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)
from dataraum.analysis.temporal_slicing.db_models import (
    TemporalDriftAnalysis,
    TemporalSliceAnalysis,
)
from dataraum.core.config import load_yaml_config
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from dataraum.analysis.quality_summary.agent import QualitySummaryAgent
    from dataraum.analysis.quality_summary.variance import SliceVarianceMetrics

logger = get_logger(__name__)

_CONFIG_CACHE: dict[str, object] | None = None


def _load_config() -> dict[str, object]:
    """Load quality_summary config from YAML (cached)."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        _CONFIG_CACHE = load_yaml_config("system/quality_summary.yaml")
    return _CONFIG_CACHE


@dataclass
class BatchResult:
    """Result from processing a single batch."""

    success: bool
    summaries: list[ColumnQualitySummary] = field(default_factory=list)
    error: str | None = None


def _process_batch(
    batch: list[AggregatedColumnData],
    agent: QualitySummaryAgent,
    session_factory: Callable[[], Any],
    source_table_name: str,
    slice_column_name: str,
    total_slices: int,
) -> BatchResult:
    """Process a single batch in a worker thread.

    Each thread gets its own session from the factory for thread safety.

    Args:
        batch: List of column data to process
        agent: Quality summary agent
        session_factory: Factory to create new sessions
        source_table_name: Name of source table
        slice_column_name: Name of slice column
        total_slices: Number of slices

    Returns:
        BatchResult with summaries or error
    """
    try:
        with session_factory() as session:
            batch_result = agent.summarize_columns_batch(
                session=session,
                columns_data=batch,
                source_table_name=source_table_name,
                slice_column_name=slice_column_name,
                total_slices=total_slices,
            )

            if not batch_result.success:
                return BatchResult(success=False, error=batch_result.error)

            return BatchResult(success=True, summaries=batch_result.unwrap())

    except Exception as e:
        return BatchResult(success=False, error=str(e))


def aggregate_slice_results(
    session: Session,
    slice_definition: SliceDefinition,
) -> Result[list[AggregatedColumnData]]:
    """Aggregate analysis results across slices for each column.

    For each column in the source table, collects statistics, quality
    metrics, and semantic info from all slice tables.

    Args:
        session: Database session
        slice_definition: The slice definition to aggregate for

    Returns:
        Result containing list of AggregatedColumnData per column
    """
    try:
        # Get source table and column info
        source_table = session.get(Table, slice_definition.table_id)
        slice_column = session.get(Column, slice_definition.column_id)

        if not source_table or not slice_column:
            return Result.fail("Source table or slice column not found")

        # Get all slice definition column IDs to exclude from analysis
        # These are columns used for slicing - same values in each slice, so redundant
        all_slice_def_cols_stmt = select(sql_distinct(SliceDefinition.column_id)).where(
            SliceDefinition.table_id == source_table.table_id
        )
        all_slice_def_cols_result = session.execute(all_slice_def_cols_stmt)
        slice_definition_column_ids = set(all_slice_def_cols_result.scalars().all())

        # Get all columns from source table, excluding slice columns
        source_cols_stmt = (
            select(Column)
            .where(Column.table_id == source_table.table_id)
            .where(Column.column_id.notin_(slice_definition_column_ids))
            .order_by(Column.column_position)
        )
        source_cols_result = session.execute(source_cols_stmt)
        source_columns = list(source_cols_result.scalars().all())

        # Get all slice tables for this definition
        slice_values = slice_definition.distinct_values or []

        # Find slice tables in database
        slice_tables = []
        for value in slice_values:
            # Generate expected slice table name
            # Note: naming convention matches SlicingAgent (without source table name)
            safe_column = re.sub(r"[^a-zA-Z0-9]", "_", slice_column.column_name)
            safe_column = re.sub(r"_+", "_", safe_column).strip("_").lower()
            safe_value = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
            safe_value = re.sub(r"_+", "_", safe_value).strip("_").lower()
            # Match SlicingAgent fallback for empty values
            if not safe_value:
                safe_value = "unknown"

            slice_table_name = f"slice_{safe_column}_{safe_value}"

            # Find in database
            slice_table_stmt = select(Table).where(
                Table.table_name == slice_table_name,
                Table.layer == "slice",
            )
            slice_table_result = session.execute(slice_table_stmt)
            slice_table = slice_table_result.scalar_one_or_none()

            if slice_table:
                slice_tables.append((slice_table, value))

        if not slice_tables:
            return Result.fail("No slice tables found in database")

        # Build aggregated data for each source column
        aggregated: list[AggregatedColumnData] = []

        for source_col in source_columns:
            agg_data = AggregatedColumnData(
                column_name=source_col.column_name,
                column_id=source_col.column_id,
                source_table_id=source_table.table_id,
                source_table_name=source_table.table_name,
                slice_column_name=slice_column.column_name,
                resolved_type=source_col.resolved_type,
            )

            # Get semantic info from source column (if exists)
            sem_stmt = select(SemanticAnnotation).where(
                SemanticAnnotation.column_id == source_col.column_id
            )
            sem_result = session.execute(sem_stmt)
            sem_ann = sem_result.scalar_one_or_none()
            if sem_ann:
                agg_data.semantic_role = sem_ann.semantic_role
                agg_data.business_name = sem_ann.business_name
                agg_data.business_description = sem_ann.business_description

            # Collect data from each slice
            for slice_table, slice_value in slice_tables:
                # Find the corresponding column in slice table
                slice_col_stmt = select(Column).where(
                    Column.table_id == slice_table.table_id,
                    Column.column_name == source_col.column_name,
                )
                slice_col_result = session.execute(slice_col_stmt)
                slice_col = slice_col_result.scalar_one_or_none()

                if not slice_col:
                    continue

                slice_data: dict[str, Any] = {
                    "slice_name": slice_table.table_name,
                    "slice_value": slice_value,
                    "row_count": slice_table.row_count or 0,
                }

                # Get statistical profile (use limit(1) in case of duplicates)
                profile_stmt = (
                    select(StatisticalProfile)
                    .where(StatisticalProfile.column_id == slice_col.column_id)
                    .limit(1)
                )
                profile_result = session.execute(profile_stmt)
                profile = profile_result.scalar_one_or_none()

                if profile:
                    slice_data["null_count"] = profile.null_count
                    slice_data["null_ratio"] = (
                        profile.null_count / profile.total_count if profile.total_count else None
                    )
                    slice_data["distinct_count"] = profile.distinct_count
                    slice_data["cardinality_ratio"] = profile.cardinality_ratio

                    # Numeric stats from profile_data (nested under numeric_stats)
                    if profile.profile_data:
                        pd = profile.profile_data
                        numeric_stats = pd.get("numeric_stats") or {}
                        slice_data["min_value"] = numeric_stats.get("min_value")
                        slice_data["max_value"] = numeric_stats.get("max_value")
                        slice_data["mean_value"] = numeric_stats.get("mean")
                        slice_data["stddev"] = numeric_stats.get("stddev")

                # Get quality metrics (use limit(1) in case of duplicates)
                quality_stmt = (
                    select(StatisticalQualityMetrics)
                    .where(StatisticalQualityMetrics.column_id == slice_col.column_id)
                    .limit(1)
                )
                quality_result = session.execute(quality_stmt)
                quality = quality_result.scalar_one_or_none()

                if quality:
                    slice_data["benford_compliant"] = quality.benford_compliant
                    slice_data["has_outliers"] = quality.has_outliers
                    slice_data["outlier_ratio"] = quality.iqr_outlier_ratio

                    if quality.quality_data:
                        qd = quality.quality_data
                        benford = qd.get("benford_analysis") or {}
                        slice_data["benford_p_value"] = benford.get("p_value")

                    # Update aggregation counts
                    if quality.benford_compliant is False:
                        agg_data.benford_violation_count += 1
                    if quality.has_outliers:
                        agg_data.outlier_slice_count += 1

                # Update totals
                agg_data.total_rows += slice_data.get("row_count", 0)
                agg_data.total_nulls += slice_data.get("null_count", 0) or 0

                distinct = slice_data.get("distinct_count")
                if distinct is not None:
                    if agg_data.min_distinct is None or distinct < agg_data.min_distinct:
                        agg_data.min_distinct = distinct
                    if agg_data.max_distinct is None or distinct > agg_data.max_distinct:
                        agg_data.max_distinct = distinct

                agg_data.slice_data.append(slice_data)

            aggregated.append(agg_data)

        # Load temporal data from DB and attach to aggregated columns
        slice_table_names = [st.table_name for st, _ in slice_tables]
        if slice_table_names:
            # Query temporal slice analyses for these slice tables
            temporal_stmt = select(TemporalSliceAnalysis).where(
                TemporalSliceAnalysis.slice_table_name.in_(slice_table_names)
            )
            temporal_records = list(session.execute(temporal_stmt).scalars().all())

            # Query temporal drift analyses for these slice tables
            drift_stmt = select(TemporalDriftAnalysis).where(
                TemporalDriftAnalysis.slice_table_name.in_(slice_table_names)
            )
            drift_records = list(session.execute(drift_stmt).scalars().all())

            if temporal_records:
                # Compute summary counts
                incomplete_periods = sum(1 for r in temporal_records if not r.is_complete)
                volume_anomalies = sum(1 for r in temporal_records if r.is_volume_anomaly)

                # Collect issues from all records
                temporal_issues: list[str] = []
                for r in temporal_records:
                    if r.issues_json:
                        temporal_issues.extend(r.issues_json)

                # Build per-period temporal data (compact)
                temporal_data: list[dict[str, Any]] = []
                for r in temporal_records:
                    period_info: dict[str, Any] = {
                        "period_label": r.period_label,
                        "slice_table": r.slice_table_name,
                        "row_count": r.row_count,
                        "coverage_ratio": r.coverage_ratio,
                        "is_complete": bool(r.is_complete),
                        "is_volume_anomaly": bool(r.is_volume_anomaly),
                    }
                    if r.period_over_period_change is not None:
                        period_info["pop_change"] = r.period_over_period_change
                    temporal_data.append(period_info)

                # Build per-column drift count map
                drift_counts: dict[str, int] = {}
                for d in drift_records:
                    if d.has_significant_drift:
                        drift_counts[d.column_name] = drift_counts.get(d.column_name, 0) + 1

                # Attach temporal_context to each aggregated column
                shared_context: dict[str, Any] = {
                    "incomplete_periods": incomplete_periods,
                    "volume_anomalies": volume_anomalies,
                    "temporal_issues": temporal_issues,
                    "temporal_data": temporal_data,
                }
                for agg in aggregated:
                    agg.temporal_context = {
                        **shared_context,
                        "drift_detected_count": drift_counts.get(agg.column_name, 0),
                    }

        return Result.ok(aggregated)

    except Exception as e:
        return Result.fail(f"Failed to aggregate slice results: {e}")


def summarize_quality(
    session: Session,
    agent: QualitySummaryAgent,
    slice_definition: SliceDefinition,
    skip_existing: bool = True,
    session_factory: Callable[[], Any] | None = None,
) -> Result[QualitySummaryResult]:
    """Generate quality summaries for all columns across slices.

    Uses batching to process multiple columns per LLM call for efficiency.
    Skips columns that already have reports (unless skip_existing=False).

    When session_factory is provided and there are multiple batches,
    processes batches in parallel for improved throughput.

    Args:
        session: Database session
        agent: Quality summary agent
        slice_definition: The slice definition to summarize
        skip_existing: Whether to skip columns with existing reports
        session_factory: Optional factory for creating sessions (enables parallel batches)

    Returns:
        Result containing QualitySummaryResult
    """
    started_at = datetime.now(UTC)

    # Get source info
    source_table = session.get(Table, slice_definition.table_id)
    slice_column = session.get(Column, slice_definition.column_id)

    if not source_table or not slice_column:
        return Result.fail("Source table or slice column not found")

    # Create run record
    run = QualitySummaryRun(
        source_table_id=source_table.table_id,
        slice_column_id=slice_column.column_id,
        slice_count=len(slice_definition.distinct_values or []),
        started_at=started_at,
        status="running",
    )
    session.add(run)
    # No flush needed - run_id is client-generated UUID, commit happens at session_scope() end

    try:
        # Aggregate results across slices
        agg_result = aggregate_slice_results(session, slice_definition)
        if not agg_result.success:
            run.status = "failed"
            run.error_message = agg_result.error
            run.completed_at = datetime.now(UTC)
            return Result.fail(agg_result.error or "Aggregation failed")

        aggregated_columns = agg_result.unwrap()

        # Filter out columns with no slice data
        columns_with_data = [c for c in aggregated_columns if c.slice_data]

        # Apply variance-based filtering to reduce LLM noise
        # Only columns with INTERESTING variance patterns go to LLM
        columns_to_process, variance_metrics = filter_interesting_columns(columns_with_data)

        # Log classification summary
        classifications = {name: m.classification.value for name, m in variance_metrics.items()}
        logger.info(
            "variance_classifications",
            total=len(columns_with_data),
            filtered=len(columns_to_process),
            classifications_summary={
                c: sum(1 for v in classifications.values() if v == c)
                for c in ["empty", "constant", "stable", "interesting"]
            },
        )

        # Skip columns with existing reports if requested
        if skip_existing:
            existing_stmt = select(ColumnQualityReport.source_column_id).where(
                ColumnQualityReport.slice_column_id == slice_column.column_id
            )
            existing_result = session.execute(existing_stmt)
            existing_column_ids = set(existing_result.scalars().all())

            columns_to_process = [
                c for c in columns_to_process if c.column_id not in existing_column_ids
            ]

        run.columns_analyzed = len(columns_to_process)

        if not columns_to_process:
            # All columns already have reports or filtered out
            run.reports_generated = 0
            run.status = "completed"
            run.completed_at = datetime.now(UTC)
            run.duration_seconds = (run.completed_at - started_at).total_seconds()
            return Result.ok(
                QualitySummaryResult(
                    source_table_id=source_table.table_id,
                    source_table_name=source_table.table_name,
                    slice_column_name=slice_column.column_name,
                    slice_count=len(slice_definition.distinct_values or []),
                    column_summaries=[],
                    column_classifications=classifications,
                    duration_seconds=run.duration_seconds,
                )
            )

        # Load batch config from YAML
        cfg = _load_config()
        batch_size = cfg.get("batch_size", 10)
        max_batch_workers = cfg.get("max_batch_workers", 4)

        # Create batches
        batches = [
            columns_to_process[i : i + batch_size]
            for i in range(0, len(columns_to_process), batch_size)
        ]

        # Process batches
        column_summaries: list[ColumnQualitySummary] = []
        total_slices = len(slice_definition.distinct_values or [])

        # Use parallel processing if session_factory provided and multiple batches
        if session_factory and len(batches) > 1 and max_batch_workers > 1:
            # Parallel batch processing
            with ThreadPoolExecutor(max_workers=max_batch_workers) as pool:
                futures = {
                    pool.submit(
                        _process_batch,
                        batch,
                        agent,
                        session_factory,
                        source_table.table_name,
                        slice_column.column_name,
                        total_slices,
                    ): batch
                    for batch in batches
                }

                for future in as_completed(futures):
                    batch_result = future.result()
                    if batch_result.success:
                        column_summaries.extend(batch_result.summaries)
        else:
            # Sequential batch processing
            for batch in batches:
                llm_result = agent.summarize_columns_batch(
                    session=session,
                    columns_data=batch,
                    source_table_name=source_table.table_name,
                    slice_column_name=slice_column.column_name,
                    total_slices=total_slices,
                )

                if llm_result.success:
                    column_summaries.extend(llm_result.unwrap())

        # Build column_id lookup
        col_id_map = {c.column_name: c.column_id for c in columns_to_process}

        # Persist reports (always sequential for consistency)
        reports_generated = 0
        for summary in column_summaries:
            col_id = col_id_map.get(summary.column_name)

            report = ColumnQualityReport(
                source_column_id=col_id,
                slice_column_id=slice_column.column_id,
                column_name=summary.column_name,
                source_table_name=summary.source_table_name,
                slice_column_name=summary.slice_column_name,
                slice_count=summary.total_slices,
                overall_quality_score=summary.overall_quality_score,
                quality_grade=summary.quality_grade,
                summary=summary.summary,
                report_data={
                    "key_findings": summary.key_findings,
                    "quality_issues": [qi.model_dump() for qi in summary.quality_issues],
                    "slice_comparisons": [sc.model_dump() for sc in summary.slice_comparisons],
                    "recommendations": summary.recommendations,
                    "slice_values": [sm.slice_value for sm in summary.slice_metrics],
                },
                investigation_views=summary.investigation_views,
            )
            session.add(report)
            reports_generated += 1

        # Update run record
        run.reports_generated = reports_generated
        run.status = "completed"
        run.completed_at = datetime.now(UTC)
        run.duration_seconds = (run.completed_at - started_at).total_seconds()

        # Persist slice profiles from aggregated data
        _save_slice_profiles_from_aggregated(
            session=session,
            aggregated_columns=aggregated_columns,
            slice_definition=slice_definition,
            source_table_name=source_table.table_name,
            variance_metrics=variance_metrics,
        )

        return Result.ok(
            QualitySummaryResult(
                source_table_id=source_table.table_id,
                source_table_name=source_table.table_name,
                slice_column_name=slice_column.column_name,
                slice_count=len(slice_definition.distinct_values or []),
                column_summaries=column_summaries,
                column_classifications=classifications,
                duration_seconds=run.duration_seconds,
            )
        )

    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
        run.completed_at = datetime.now(UTC)
        run.duration_seconds = (run.completed_at - started_at).total_seconds()
        return Result.fail(f"Quality summary failed: {e}")


def _save_slice_profiles_from_aggregated(
    session: Session,
    aggregated_columns: list[AggregatedColumnData],
    slice_definition: SliceDefinition,
    source_table_name: str,
    variance_metrics: dict[str, SliceVarianceMetrics] | None = None,
) -> None:
    """Persist slice profiles from aggregated column data.

    Uses delete + insert strategy: deletes existing profiles for the
    slice definition and inserts fresh data.

    Args:
        session: Database session
        aggregated_columns: List of aggregated column data with slice_data
        slice_definition: The slice definition being processed
        source_table_name: Name of the source table
    """
    # Delete existing profiles for this slice definition via ORM
    # (Using session.delete() instead of session.execute(delete(...)) so the
    # DELETE is batched with the commit lock, avoiding an immediate write
    # transaction that would block parallel phases.)
    existing_stmt = select(ColumnSliceProfile).where(
        ColumnSliceProfile.slice_column_id == slice_definition.column_id
    )
    for existing in session.execute(existing_stmt).scalars().all():
        session.delete(existing)

    # Get slice column name
    slice_column = session.get(Column, slice_definition.column_id)
    slice_column_name = slice_column.column_name if slice_column else "unknown"

    # Load quality scoring config
    cfg = _load_config()
    qs = cfg.get("quality_scoring", {})
    high_null_ratio = qs.get("high_null_ratio", 0.5)
    high_null_penalty = qs.get("high_null_penalty", 0.3)
    moderate_null_ratio = qs.get("moderate_null_ratio", 0.2)
    moderate_null_penalty = qs.get("moderate_null_penalty", 0.1)
    outlier_penalty = qs.get("outlier_penalty", 0.2)
    benford_penalty = qs.get("benford_violation_penalty", 0.1)

    # Insert new profiles from aggregated data
    for col_data in aggregated_columns:
        for slice_info in col_data.slice_data:
            quality_score = 1.0
            has_issues = False
            issue_count = 0

            null_ratio = slice_info.get("null_ratio")
            if null_ratio is not None:
                if null_ratio > high_null_ratio:
                    quality_score -= high_null_penalty
                    issue_count += 1
                    has_issues = True
                elif null_ratio > moderate_null_ratio:
                    quality_score -= moderate_null_penalty
                    issue_count += 1

            if slice_info.get("has_outliers"):
                quality_score -= outlier_penalty
                issue_count += 1
                has_issues = True

            if slice_info.get("benford_compliant") is False:
                quality_score -= benford_penalty
                issue_count += 1
                has_issues = True

            quality_score = max(0.0, min(1.0, quality_score))

            # Build profile_data JSON with extended metrics
            profile_data: dict[str, Any] = {}
            for key in [
                "min_value",
                "max_value",
                "mean_value",
                "stddev",
                "benford_p_value",
                "outlier_ratio",
                "cardinality_ratio",
            ]:
                if slice_info.get(key) is not None:
                    profile_data[key] = slice_info[key]

            profile = ColumnSliceProfile(
                source_column_id=col_data.column_id,
                slice_column_id=slice_definition.column_id,
                source_table_name=source_table_name,
                column_name=col_data.column_name,
                slice_column_name=slice_column_name,
                slice_value=str(slice_info.get("slice_value", "")),
                row_count=slice_info.get("row_count", 0),
                null_ratio=null_ratio,
                distinct_count=slice_info.get("distinct_count"),
                variance_classification=(
                    variance_metrics[col_data.column_name].classification.value
                    if variance_metrics and col_data.column_name in variance_metrics
                    else None
                ),
                quality_score=quality_score,
                has_issues=has_issues,
                issue_count=issue_count,
                profile_data=profile_data if profile_data else None,
            )
            session.add(profile)

    # No explicit flush — all adds are batched and sent during session.commit()
    # inside the commit lock. Flushing here would open a SQLite write transaction
    # outside the lock, blocking parallel phases.


__all__ = [
    "aggregate_slice_results",
    "summarize_quality",
]
