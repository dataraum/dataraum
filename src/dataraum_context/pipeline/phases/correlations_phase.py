"""Correlations phase implementation.

Analyzes within-table correlations:
- Numeric correlations (Pearson, Spearman)
- Categorical associations (Cramér's V)
- Functional dependencies (A → B)
- Derived columns detection
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select

from dataraum_context.analysis.correlation import analyze_correlations
from dataraum_context.analysis.correlation.db_models import CorrelationAnalysisRun
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Column, Table


class CorrelationsPhase(BasePhase):
    """Within-table correlation analysis phase.

    Analyzes correlations within each typed table to identify
    related columns, functional dependencies, and derived columns.
    """

    @property
    def name(self) -> str:
        return "correlations"

    @property
    def description(self) -> str:
        return "Within-table correlation analysis"

    @property
    def dependencies(self) -> list[str]:
        return ["statistics"]

    @property
    def outputs(self) -> list[str]:
        return ["correlations", "derived_columns"]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all tables already have correlation analysis."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        # Check which tables already have correlation analysis
        analyzed_stmt = select(CorrelationAnalysisRun.target_id).where(
            CorrelationAnalysisRun.target_type == "table"
        )
        analyzed_ids = set((ctx.session.execute(analyzed_stmt)).scalars().all())

        unanalyzed = [t for t in typed_tables if t.table_id not in analyzed_ids]
        if not unanalyzed:
            return "All tables already have correlation analysis"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run within-table correlation analysis."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        # Check which tables already have correlation analysis
        analyzed_stmt = select(CorrelationAnalysisRun.target_id).where(
            CorrelationAnalysisRun.target_type == "table"
        )
        analyzed_ids = set((ctx.session.execute(analyzed_stmt)).scalars().all())

        unanalyzed_tables = [t for t in typed_tables if t.table_id not in analyzed_ids]

        if not unanalyzed_tables:
            return PhaseResult.success(
                outputs={"correlations": [], "derived_columns": []},
                records_processed=0,
                records_created=0,
            )

        # Analyze each table
        analyzed_tables = []
        total_numeric_corr = 0
        total_categorical_assoc = 0
        total_func_deps = 0
        total_derived = 0
        warnings = []

        for typed_table in unanalyzed_tables:
            corr_result = analyze_correlations(
                table_id=typed_table.table_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )

            if not corr_result.success:
                warnings.append(f"Failed to analyze {typed_table.table_name}: {corr_result.error}")
                continue

            result_data = corr_result.unwrap()
            analyzed_tables.append(typed_table.table_name)

            total_numeric_corr += len(result_data.numeric_correlations)
            total_categorical_assoc += len(result_data.categorical_associations)
            total_func_deps += len(result_data.functional_dependencies)
            total_derived += len(result_data.derived_columns)

            # Create analysis run record to track completion
            columns_stmt = select(Column).where(Column.table_id == typed_table.table_id)
            columns = (ctx.session.execute(columns_stmt)).scalars().all()

            run_record = CorrelationAnalysisRun(
                target_id=typed_table.table_id,
                target_type="table",
                rows_analyzed=0,
                columns_analyzed=len(columns),
                started_at=result_data.computed_at,
                completed_at=datetime.now(UTC),
                duration_seconds=result_data.duration_seconds,
            )
            ctx.session.add(run_record)

        ctx.session.commit()

        if not analyzed_tables and warnings:
            return PhaseResult.failed(f"All tables failed analysis: {'; '.join(warnings)}")

        return PhaseResult.success(
            outputs={
                "correlations": analyzed_tables,
                "numeric_correlations": total_numeric_corr,
                "categorical_associations": total_categorical_assoc,
                "functional_dependencies": total_func_deps,
                "derived_columns": total_derived,
            },
            records_processed=len(analyzed_tables),
            records_created=total_numeric_corr
            + total_categorical_assoc
            + total_func_deps
            + total_derived,
            warnings=warnings if warnings else None,
        )
