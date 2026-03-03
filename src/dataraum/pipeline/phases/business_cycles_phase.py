"""Business cycles phase implementation.

Expert LLM agent for detecting business cycles using semantic metadata.
No hardcoded pattern matching - the agent analyzes data structure
and identifies cycles based on entity flows, status columns, and relationships.
"""

from __future__ import annotations

from types import ModuleType

from sqlalchemy import func, select

from dataraum.analysis.cycles import BusinessCycleAgent
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.db_models import PhaseCheckpoint
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Table


@analysis_phase
class BusinessCyclesPhase(BasePhase):
    """Expert LLM agent for business cycle detection.

    Synthesizes pre-computed pipeline metadata (slice definitions,
    statistical profiles, temporal patterns, enriched views, quality
    signals) into business cycle analysis via a single LLM call.

    Requires: semantic, temporal, enriched_views, slicing, quality_summary.
    """

    @property
    def name(self) -> str:
        return "business_cycles"

    @property
    def description(self) -> str:
        return "Expert LLM cycle detection"

    @property
    def dependencies(self) -> list[str]:
        # Depends on all phases that build_cycle_detection_context reads from
        return [
            "semantic",  # column annotations, table entities
            "temporal",  # temporal column profiles
            "enriched_views",  # pre-joined fact-dimension views
            "slicing",  # categorical dimensions (status columns)
            "quality_summary",  # column quality reports
        ]

    @property
    def outputs(self) -> list[str]:
        return ["detected_cycles", "business_processes"]

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.cycles import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if business cycles have already been detected."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        # Check for existing completed phase checkpoint
        cp_stmt = select(func.count(PhaseCheckpoint.checkpoint_id)).where(
            PhaseCheckpoint.source_id == ctx.source_id,
            PhaseCheckpoint.phase_name == self.name,
            PhaseCheckpoint.status == "completed",
        )
        cp_count = (ctx.session.execute(cp_stmt)).scalar() or 0

        if cp_count > 0:
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

        # Create other components
        renderer = PromptRenderer()

        # Create business cycle agent
        agent = BusinessCycleAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        vertical = ctx.config.get("vertical")
        if not vertical:
            return PhaseResult.failed(
                "No vertical configured. Set 'vertical' in config/phases/business_cycles.yaml."
            )

        # Run analysis
        analysis_result = agent.analyze(
            session=ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_ids=table_ids,
            source_id=ctx.source_id,
            vertical=vertical,
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
                "tables_analyzed": [t.table_name for t in typed_tables],
            },
            records_processed=len(table_ids),
            records_created=len(analysis.cycles),
        )
