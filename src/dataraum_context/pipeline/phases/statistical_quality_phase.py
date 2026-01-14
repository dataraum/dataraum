"""Statistical quality phase implementation.

Runs advanced statistical quality checks on typed data:
- Benford's Law compliance (fraud detection for financial data)
- Outlier detection (IQR and Isolation Forest methods)
"""

from __future__ import annotations

from sqlalchemy import func, select

from dataraum_context.analysis.statistics import assess_statistical_quality
from dataraum_context.analysis.statistics.db_models import StatisticalQualityMetrics
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Column, Table


class StatisticalQualityPhase(BasePhase):
    """Statistical quality assessment phase.

    Runs Benford's Law and outlier detection on numeric columns.
    Only processes columns that haven't been assessed yet.
    """

    @property
    def name(self) -> str:
        return "statistical_quality"

    @property
    def description(self) -> str:
        return "Benford's Law and outlier detection"

    @property
    def dependencies(self) -> list[str]:
        return ["statistics"]

    @property
    def outputs(self) -> list[str]:
        return ["quality_metrics"]

    async def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all numeric columns already have quality metrics."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = await ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        # Get all columns for typed tables
        typed_table_ids = [t.table_id for t in typed_tables]
        columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        all_columns = (await ctx.session.execute(columns_stmt)).scalars().all()

        # Filter to numeric columns only
        numeric_columns = [
            c for c in all_columns if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
        ]

        if not numeric_columns:
            return "No numeric columns to assess"

        # Check which already have quality metrics
        assessed_stmt = select(StatisticalQualityMetrics.column_id).distinct()
        assessed_ids = set((await ctx.session.execute(assessed_stmt)).scalars().all())

        numeric_ids = {c.column_id for c in numeric_columns}
        if not (numeric_ids - assessed_ids):
            return "All numeric columns already assessed"

        return None

    async def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run statistical quality assessment on typed tables."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = await ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        # Get all columns for typed tables
        typed_table_ids = [t.table_id for t in typed_tables]
        columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        all_columns = (await ctx.session.execute(columns_stmt)).scalars().all()

        # Check which already have quality metrics
        assessed_stmt = select(StatisticalQualityMetrics.column_id).distinct()
        assessed_ids = set((await ctx.session.execute(assessed_stmt)).scalars().all())

        # Find tables with unassessed numeric columns
        unassessed_tables = []
        for tt in typed_tables:
            table_columns = [c for c in all_columns if c.table_id == tt.table_id]
            numeric_columns = [
                c
                for c in table_columns
                if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
            ]
            if numeric_columns:
                numeric_ids = {c.column_id for c in numeric_columns}
                if numeric_ids - assessed_ids:
                    unassessed_tables.append(tt)

        if not unassessed_tables:
            return PhaseResult.success(
                outputs={"quality_metrics": []},
                records_processed=0,
                records_created=0,
            )

        # Assess each table
        assessed_tables = []
        total_columns_assessed = 0
        benford_violations = 0
        outlier_columns = 0
        warnings = []

        for typed_table in unassessed_tables:
            quality_result = await assess_statistical_quality(
                table_id=typed_table.table_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )

            if not quality_result.success:
                warnings.append(
                    f"Failed to assess {typed_table.table_name}: {quality_result.error}"
                )
                continue

            quality_results = quality_result.unwrap()
            assessed_tables.append(typed_table.table_name)
            total_columns_assessed += len(quality_results)

            # Count findings
            for qr in quality_results:
                if qr.benford_analysis and not qr.benford_analysis.is_compliant:
                    benford_violations += 1
                if qr.outlier_detection and qr.outlier_detection.iqr_outlier_ratio > 0.05:
                    outlier_columns += 1

        if not assessed_tables and warnings:
            return PhaseResult.failed(f"All tables failed assessment: {'; '.join(warnings)}")

        # Get total metrics count
        metrics_count = (
            await ctx.session.execute(select(func.count(StatisticalQualityMetrics.metric_id)))
        ).scalar() or 0

        return PhaseResult.success(
            outputs={
                "quality_metrics": assessed_tables,
                "benford_violations": benford_violations,
                "outlier_columns": outlier_columns,
                "total_metrics": metrics_count,
            },
            records_processed=total_columns_assessed,
            records_created=total_columns_assessed,
            warnings=warnings if warnings else None,
        )
