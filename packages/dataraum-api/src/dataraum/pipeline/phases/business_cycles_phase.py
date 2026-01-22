"""Business cycles phase implementation.

Expert LLM agent for detecting business cycles using semantic metadata.
No hardcoded pattern matching - the agent analyzes data structure
and identifies cycles based on entity flows, status columns, and relationships.
"""

from __future__ import annotations

from sqlalchemy import func, select

from dataraum.analysis.cycles import BusinessCycleAgent
from dataraum.analysis.cycles.db_models import BusinessCycleAnalysisRun
from dataraum.llm import create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Table


class BusinessCyclesPhase(BasePhase):
    """Expert LLM agent for business cycle detection.

    Uses semantic metadata as context and provides tools for
    on-demand data exploration to detect business cycles like:
    - Order-to-Cash (revenue cycle)
    - Procure-to-Pay (expense cycle)
    - Accounts Receivable/Payable cycles
    - Inventory cycles

    Requires: semantic, temporal phases.
    """

    @property
    def name(self) -> str:
        return "business_cycles"

    @property
    def description(self) -> str:
        return "Expert LLM cycle detection"

    @property
    def dependencies(self) -> list[str]:
        return ["semantic", "temporal"]

    @property
    def outputs(self) -> list[str]:
        return ["detected_cycles", "business_processes"]

    @property
    def is_llm_phase(self) -> bool:
        return True

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if business cycles have already been detected."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for existing business cycle analysis
        run_stmt = select(func.count(BusinessCycleAnalysisRun.analysis_id)).where(
            BusinessCycleAnalysisRun.table_ids.contains(table_ids)
        )
        run_count = (ctx.session.execute(run_stmt)).scalar() or 0

        if run_count > 0:
            return "Business cycle analysis already run for these tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run business cycle detection using LLM agent."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Initialize LLM infrastructure
        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

        # Create provider
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        # Create business cycle agent (only takes provider, not full LLM infrastructure)
        agent = BusinessCycleAgent(provider=provider)

        # Get optional domain from config (e.g., "financial", "retail", "manufacturing")
        domain = ctx.config.get("domain")

        # Get max tool calls from config
        max_tool_calls = ctx.config.get("max_tool_calls", 10)

        # Run analysis
        analysis_result = agent.analyze(
            session=ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_ids=table_ids,
            max_tool_calls=max_tool_calls,
            domain=domain,
        )

        if not analysis_result.success:
            return PhaseResult.failed(analysis_result.error or "Business cycle analysis failed")

        analysis = analysis_result.unwrap()

        return PhaseResult.success(
            outputs={
                "detected_cycles": len(analysis.cycles),
                "business_processes": analysis.detected_processes,
                "business_summary": analysis.business_summary,
                "data_quality_observations": analysis.data_quality_observations,
                "recommendations": analysis.recommendations,
                "tool_calls_made": len(analysis.tool_calls_made),
                "tables_analyzed": [t.table_name for t in typed_tables],
            },
            records_processed=len(table_ids),
            records_created=len(analysis.cycles),
        )
