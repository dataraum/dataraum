"""Statistical quality phase implementation.

Runs advanced statistical quality checks on typed data:
- Benford's Law compliance (fraud detection for financial data)
- Outlier detection (IQR and Isolation Forest methods)
"""

from __future__ import annotations

from collections.abc import Callable
from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from dataraum.analysis.statistics import assess_statistical_quality
from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from dataraum.pipeline.fixes import FixInput, FixResult

logger = get_logger(__name__)


@analysis_phase
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
        return ["column_eligibility"]

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.statistics import quality_db_models

        return [quality_db_models]

    @property
    def post_verification(self) -> list[str]:
        return ["outlier_rate", "benford_compliance"]

    @property
    def fix_handlers(self) -> dict[str, Callable[[FixInput, dict[str, Any]], FixResult]]:
        return {"transform_exclude_outliers": _handle_exclude_outliers}

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all numeric columns already have quality metrics."""

        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return f"No typed tables found for source {ctx.source_id}"

        logger.debug(f"StatQuality: Found {len(typed_tables)} typed tables")

        # Get all columns for typed tables
        typed_table_ids = [t.table_id for t in typed_tables]
        columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        all_columns = (ctx.session.execute(columns_stmt)).scalars().all()

        # Log all column types found
        type_counts: dict[str, int] = {}
        for col in all_columns:
            t = col.resolved_type or "NULL"
            type_counts[t] = type_counts.get(t, 0) + 1
        logger.debug(f"StatQuality: Column types in typed tables: {type_counts}")

        # Filter to numeric columns only
        numeric_types = ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
        numeric_columns = [c for c in all_columns if c.resolved_type in numeric_types]

        if not numeric_columns:
            return f"No numeric columns to assess (types: {numeric_types}, available: {list(type_counts.keys())})"

        logger.debug(f"StatQuality: Found {len(numeric_columns)} numeric columns")

        # Check which already have quality metrics
        assessed_stmt = select(StatisticalQualityMetrics.column_id).distinct()
        assessed_ids = set((ctx.session.execute(assessed_stmt)).scalars().all())

        numeric_ids = {c.column_id for c in numeric_columns}
        if not (numeric_ids - assessed_ids):
            return "All numeric columns already assessed"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run statistical quality assessment on typed tables."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        # Get all columns for typed tables
        typed_table_ids = [t.table_id for t in typed_tables]
        columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        all_columns = (ctx.session.execute(columns_stmt)).scalars().all()

        # Check which already have quality metrics
        assessed_stmt = select(StatisticalQualityMetrics.column_id).distinct()
        assessed_ids = set((ctx.session.execute(assessed_stmt)).scalars().all())

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

        exclude_outlier_columns: set[str] = set(
            ctx.config.get("exclude_outlier_columns", [])
        )

        for typed_table in unassessed_tables:
            quality_result = assess_statistical_quality(
                table_id=typed_table.table_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                exclude_outlier_columns=exclude_outlier_columns,
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
            ctx.session.execute(select(func.count(StatisticalQualityMetrics.metric_id)))
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
            summary=f"{total_columns_assessed} columns assessed, {benford_violations} Benford violations, {outlier_columns} outlier columns",
        )


def _handle_exclude_outliers(fix_input: FixInput, config: dict[str, Any]) -> FixResult:
    """Write exclude_outlier_columns to statistical_quality config.

    Appends each affected column to the exclusion list so that on re-run
    the phase skips outlier detection for those columns.
    """
    from dataraum.pipeline.fixes import ConfigPatch, FixResult

    patches: list[ConfigPatch] = []
    for col in fix_input.affected_columns:
        patches.append(
            ConfigPatch(
                config_path="phases/statistical_quality.yaml",
                operation="append",
                key_path=["exclude_outlier_columns"],
                value=col,
                reason=fix_input.interpretation or f"Exclude outliers for {col}",
            )
        )

    return FixResult(
        config_patches=patches,
        requires_rerun="statistical_quality",
        summary=f"Excluded outlier columns: {', '.join(fix_input.affected_columns)}",
    )
