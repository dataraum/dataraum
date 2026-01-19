"""Quality summary phase implementation.

LLM-powered quality report generation that aggregates slice results
and generates summaries for each column.
"""

from __future__ import annotations

from sqlalchemy import func, select

from dataraum_context.analysis.quality_summary.agent import QualitySummaryAgent
from dataraum_context.analysis.quality_summary.db_models import QualitySummaryRun
from dataraum_context.analysis.quality_summary.processor import summarize_quality
from dataraum_context.analysis.slicing.db_models import SliceDefinition
from dataraum_context.llm import LLMCache, PromptRenderer, create_provider, load_llm_config
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Table


class QualitySummaryPhase(BasePhase):
    """LLM-powered quality summary phase.

    Aggregates analysis results across slices and generates
    quality summaries per column using LLM.

    Requires: slice_analysis phase.
    """

    @property
    def name(self) -> str:
        return "quality_summary"

    @property
    def description(self) -> str:
        return "LLM quality report generation"

    @property
    def dependencies(self) -> list[str]:
        return ["slice_analysis"]

    @property
    def outputs(self) -> list[str]:
        return ["quality_reports", "quality_grades"]

    @property
    def is_llm_phase(self) -> bool:
        return True

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no slice definitions exist or summaries already generated."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for slice definitions
        slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        slice_defs = (ctx.session.execute(slice_stmt)).scalars().all()

        if not slice_defs:
            return "No slice definitions found"

        # Check for existing quality summary runs
        run_stmt = select(func.count(QualitySummaryRun.run_id)).where(
            QualitySummaryRun.source_table_id.in_(table_ids)
        )
        run_count = (ctx.session.execute(run_stmt)).scalar() or 0

        if run_count >= len(slice_defs):
            return "Quality summaries already generated"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Generate quality summaries using LLM."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Get slice definitions
        slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        slice_definitions = (ctx.session.execute(slice_stmt)).scalars().all()

        if not slice_definitions:
            return PhaseResult.success(
                outputs={
                    "quality_reports": 0,
                    "slice_definitions_processed": 0,
                    "message": "No slice definitions found",
                },
                records_processed=0,
                records_created=0,
            )

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
        cache = LLMCache()
        renderer = PromptRenderer()

        # Create quality summary agent
        agent = QualitySummaryAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
            cache=cache,
        )

        # Get skip_existing setting from config (default True)
        skip_existing = ctx.config.get("skip_existing_summaries", True)

        # Process each slice definition
        total_reports = 0
        total_columns = 0
        errors = []

        for slice_def in slice_definitions:
            summary_result = summarize_quality(
                session=ctx.session,
                agent=agent,
                slice_definition=slice_def,
                skip_existing=skip_existing,
            )

            if not summary_result.success:
                errors.append(f"Slice {slice_def.slice_id}: {summary_result.error}")
                continue

            summary = summary_result.unwrap()
            total_reports += len(summary.column_summaries)
            total_columns += len(summary.column_summaries)

        outputs: dict[str, int | list[str]] = {
            "quality_reports": total_reports,
            "slice_definitions_processed": len(slice_definitions),
            "columns_summarized": total_columns,
        }

        if errors:
            outputs["errors"] = errors

        return PhaseResult.success(
            outputs=outputs,
            records_processed=len(slice_definitions),
            records_created=total_reports,
        )
