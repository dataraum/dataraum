"""Quality summary processor.

Orchestrates quality summary analysis: aggregates slice results and
calls the LLM agent to generate summaries per column.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.quality_summary.db_models import (
    ColumnQualityReport,
    QualitySummaryRun,
)
from dataraum_context.analysis.quality_summary.models import (
    AggregatedColumnData,
    QualitySummaryResult,
)
from dataraum_context.analysis.semantic.db_models import SemanticAnnotation
from dataraum_context.analysis.slicing.db_models import SliceDefinition
from dataraum_context.analysis.statistics.db_models import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table

if TYPE_CHECKING:
    from dataraum_context.analysis.quality_summary.agent import QualitySummaryAgent


async def aggregate_slice_results(
    session: AsyncSession,
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
        source_table = await session.get(Table, slice_definition.table_id)
        slice_column = await session.get(Column, slice_definition.column_id)

        if not source_table or not slice_column:
            return Result.fail("Source table or slice column not found")

        # Get all columns from source table
        source_cols_stmt = (
            select(Column)
            .where(Column.table_id == source_table.table_id)
            .order_by(Column.column_position)
        )
        source_cols_result = await session.execute(source_cols_stmt)
        source_columns = list(source_cols_result.scalars().all())

        # Get all slice tables for this definition
        slice_values = slice_definition.distinct_values or []

        # Find slice tables in database
        slice_tables = []
        for value in slice_values:
            # Generate expected slice table name
            import re

            safe_source = re.sub(r"[^a-zA-Z0-9]", "_", source_table.table_name)
            safe_source = re.sub(r"_+", "_", safe_source).strip("_").lower()
            safe_column = re.sub(r"[^a-zA-Z0-9]", "_", slice_column.column_name)
            safe_column = re.sub(r"_+", "_", safe_column).strip("_").lower()
            safe_value = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
            safe_value = re.sub(r"_+", "_", safe_value).strip("_").lower()

            slice_table_name = f"slice_{safe_source}_{safe_column}_{safe_value}"

            # Find in database
            slice_table_stmt = select(Table).where(
                Table.table_name == slice_table_name,
                Table.layer == "slice",
            )
            slice_table_result = await session.execute(slice_table_stmt)
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
            sem_result = await session.execute(sem_stmt)
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
                slice_col_result = await session.execute(slice_col_stmt)
                slice_col = slice_col_result.scalar_one_or_none()

                if not slice_col:
                    continue

                slice_data: dict[str, Any] = {
                    "slice_name": slice_table.table_name,
                    "slice_value": slice_value,
                    "row_count": slice_table.row_count or 0,
                }

                # Get statistical profile
                profile_stmt = select(StatisticalProfile).where(
                    StatisticalProfile.column_id == slice_col.column_id
                )
                profile_result = await session.execute(profile_stmt)
                profile = profile_result.scalar_one_or_none()

                if profile:
                    slice_data["null_count"] = profile.null_count
                    slice_data["null_ratio"] = (
                        profile.null_count / profile.total_count if profile.total_count else None
                    )
                    slice_data["distinct_count"] = profile.distinct_count
                    slice_data["cardinality_ratio"] = profile.cardinality_ratio

                    # Numeric stats from profile_data
                    if profile.profile_data:
                        pd = profile.profile_data
                        slice_data["min_value"] = pd.get("min_value")
                        slice_data["max_value"] = pd.get("max_value")
                        slice_data["mean_value"] = pd.get("mean_value")
                        slice_data["stddev"] = pd.get("stddev")

                # Get quality metrics
                quality_stmt = select(StatisticalQualityMetrics).where(
                    StatisticalQualityMetrics.column_id == slice_col.column_id
                )
                quality_result = await session.execute(quality_stmt)
                quality = quality_result.scalar_one_or_none()

                if quality:
                    slice_data["benford_compliant"] = quality.benford_compliant
                    slice_data["has_outliers"] = quality.has_outliers
                    slice_data["outlier_ratio"] = quality.iqr_outlier_ratio

                    if quality.quality_data:
                        qd = quality.quality_data
                        benford = qd.get("benford_analysis", {})
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

        return Result.ok(aggregated)

    except Exception as e:
        return Result.fail(f"Failed to aggregate slice results: {e}")


async def summarize_quality(
    session: AsyncSession,
    agent: QualitySummaryAgent,
    slice_definition: SliceDefinition,
) -> Result[QualitySummaryResult]:
    """Generate quality summaries for all columns across slices.

    Args:
        session: Database session
        agent: Quality summary agent
        slice_definition: The slice definition to summarize

    Returns:
        Result containing QualitySummaryResult
    """
    started_at = datetime.now(UTC)

    # Get source info
    source_table = await session.get(Table, slice_definition.table_id)
    slice_column = await session.get(Column, slice_definition.column_id)

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
    await session.flush()

    try:
        # Aggregate results across slices
        agg_result = await aggregate_slice_results(session, slice_definition)
        if not agg_result.success:
            run.status = "failed"
            run.error_message = agg_result.error
            run.completed_at = datetime.now(UTC)
            await session.commit()
            return Result.fail(agg_result.error or "Aggregation failed")

        aggregated_columns = agg_result.unwrap()
        run.columns_analyzed = len(aggregated_columns)

        # Generate summary for each column
        column_summaries = []
        reports_generated = 0

        for col_data in aggregated_columns:
            # Skip columns with no slice data
            if not col_data.slice_data:
                continue

            # Call LLM for summary
            summary_result = await agent.summarize_column(session, col_data)

            if not summary_result.success:
                # Log but continue with other columns
                continue

            summary = summary_result.unwrap()
            column_summaries.append(summary)

            # Store report in database
            report = ColumnQualityReport(
                source_column_id=col_data.column_id,
                slice_column_id=slice_column.column_id,
                column_name=col_data.column_name,
                source_table_name=col_data.source_table_name,
                slice_column_name=col_data.slice_column_name,
                slice_count=len(col_data.slice_data),
                overall_quality_score=summary.overall_quality_score,
                quality_grade=summary.quality_grade,
                summary=summary.summary,
                report_data={
                    "key_findings": summary.key_findings,
                    "quality_issues": [qi.model_dump() for qi in summary.quality_issues],
                    "slice_comparisons": [sc.model_dump() for sc in summary.slice_comparisons],
                    "recommendations": summary.recommendations,
                    "slice_metrics": [sm.model_dump() for sm in summary.slice_metrics],
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

        await session.commit()

        return Result.ok(
            QualitySummaryResult(
                source_table_id=source_table.table_id,
                source_table_name=source_table.table_name,
                slice_column_name=slice_column.column_name,
                slice_count=len(slice_definition.distinct_values or []),
                column_summaries=column_summaries,
                duration_seconds=run.duration_seconds,
            )
        )

    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
        run.completed_at = datetime.now(UTC)
        run.duration_seconds = (run.completed_at - started_at).total_seconds()
        await session.commit()
        return Result.fail(f"Quality summary failed: {e}")


__all__ = [
    "aggregate_slice_results",
    "summarize_quality",
]
