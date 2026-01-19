"""Context phase implementation.

Non-LLM phase that builds execution context for graph agents by aggregating
metadata from all analysis modules.
"""

from __future__ import annotations

from sqlalchemy import select

from dataraum_context.graphs.context import build_execution_context
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Table


class ContextPhase(BasePhase):
    """Context building phase for graph execution.

    Aggregates metadata from all analysis modules to build a complete
    context for LLM graph agents. This context includes:
    - Table and column metadata
    - Relationships and graph topology
    - Business cycles and processes
    - Quality issues by severity
    - Entropy summaries
    - Available slices

    Requires: entropy_interpretation, quality_summary phases.
    """

    @property
    def name(self) -> str:
        return "context"

    @property
    def description(self) -> str:
        return "Build execution context for graph agent"

    @property
    def dependencies(self) -> list[str]:
        return ["entropy_interpretation", "quality_summary"]

    @property
    def outputs(self) -> list[str]:
        return ["execution_context"]

    @property
    def is_llm_phase(self) -> bool:
        return False

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only if no typed tables exist."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Build execution context from all analysis modules."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Get optional slice filter from config
        slice_column = ctx.config.get("slice_column")
        slice_value = ctx.config.get("slice_value")

        # Build execution context
        execution_context = build_execution_context(
            session=ctx.session,
            table_ids=table_ids,
            duckdb_conn=ctx.duckdb_conn,
            slice_column=slice_column,
            slice_value=slice_value,
        )

        # Calculate summary stats
        total_columns = sum(len(t.columns) for t in execution_context.tables)
        total_relationships = len(execution_context.relationships)
        total_cycles = len(execution_context.business_cycles)
        total_slices = len(execution_context.available_slices)

        # Quality issues are already counted by severity
        quality_issues_count = execution_context.quality_issues_by_severity

        return PhaseResult.success(
            outputs={
                "tables": len(execution_context.tables),
                "columns": total_columns,
                "relationships": total_relationships,
                "business_cycles": total_cycles,
                "available_slices": total_slices,
                "quality_issues": quality_issues_count,
                "has_field_mappings": execution_context.field_mappings is not None,
                "has_entropy_summary": execution_context.entropy_summary is not None,
            },
            records_processed=len(table_ids),
            records_created=1,  # One context object
        )
