"""Correlations phase implementation.

Analyzes within-table patterns:
- Derived columns detection (sum, product, ratio, etc.)

Numeric correlations (Pearson, Spearman) are available on-demand via
the correlation processor but are not computed in the pipeline — no
downstream consumer acts on them.
"""

from __future__ import annotations

from sqlalchemy import select

from dataraum.analysis.correlation import analyze_correlations
from dataraum.analysis.correlation.db_models import DerivedColumn
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Table


class CorrelationsPhase(BasePhase):
    """Within-table correlation analysis phase.

    Analyzes correlations within each typed table to identify
    related columns and derived columns.
    """

    @property
    def name(self) -> str:
        return "correlations"

    @property
    def description(self) -> str:
        return "Within-table correlation analysis"

    @property
    def dependencies(self) -> list[str]:
        return ["column_eligibility"]

    @property
    def outputs(self) -> list[str]:
        return ["correlations", "derived_columns"]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all tables already have derived column analysis."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check which tables already have derived column results
        derived_stmt = select(DerivedColumn.table_id.distinct()).where(
            DerivedColumn.table_id.in_(table_ids)
        )
        analyzed_ids = set(ctx.session.execute(derived_stmt).scalars().all())

        unanalyzed = [t for t in typed_tables if t.table_id not in analyzed_ids]
        if not unanalyzed:
            return "All tables already have correlation analysis"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run derived column detection on typed tables."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Check which tables already have derived column results
        derived_stmt = select(DerivedColumn.table_id.distinct()).where(
            DerivedColumn.table_id.in_(table_ids)
        )
        analyzed_ids = set(ctx.session.execute(derived_stmt).scalars().all())

        unanalyzed_tables = [t for t in typed_tables if t.table_id not in analyzed_ids]

        if not unanalyzed_tables:
            return PhaseResult.success(
                outputs={"correlations": [], "derived_columns": []},
                records_processed=0,
                records_created=0,
            )

        # Analyze each table
        analyzed_tables = []
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
            total_derived += len(result_data.derived_columns)

        # Note: commit handled by session_scope() in orchestrator

        if not analyzed_tables and warnings:
            return PhaseResult.failed(f"All tables failed analysis: {'; '.join(warnings)}")

        return PhaseResult.success(
            outputs={
                "correlations": analyzed_tables,
                "derived_columns": total_derived,
            },
            records_processed=len(analyzed_tables),
            records_created=total_derived,
            warnings=warnings if warnings else None,
        )
