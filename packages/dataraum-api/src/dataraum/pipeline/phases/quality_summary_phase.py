"""Quality summary phase implementation.

LLM-powered quality report generation that aggregates slice results
and generates summaries for each column.

Uses parallel processing for slice definitions to improve throughput.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from dataraum.analysis.quality_summary.agent import QualitySummaryAgent
from dataraum.analysis.quality_summary.db_models import QualitySummaryRun
from dataraum.analysis.quality_summary.processor import summarize_quality
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.llm import LLMCache, PromptRenderer, create_provider, load_llm_config
from dataraum.llm.config import LLMConfig
from dataraum.llm.providers.base import LLMProvider
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Table

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


@dataclass
class SliceProcessingResult:
    """Result from processing a single slice definition."""

    slice_id: str
    success: bool
    reports_count: int = 0
    columns_count: int = 0
    error: str | None = None


def _process_slice_definition(
    slice_def: SliceDefinition,
    session_factory: Callable[[], Any],
    config: LLMConfig,
    provider: LLMProvider,
    skip_existing: bool,
) -> SliceProcessingResult:
    """Process a single slice definition in a worker thread.

    Each thread gets its own session from the factory for thread safety.

    Args:
        slice_def: The slice definition to process
        session_factory: Factory to create new sessions
        config: LLM configuration
        provider: LLM provider instance
        skip_existing: Whether to skip existing reports

    Returns:
        SliceProcessingResult with counts or error
    """
    try:
        # Create LLM components (cache and renderer are lightweight)
        cache = LLMCache()
        renderer = PromptRenderer()
        agent = QualitySummaryAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
            cache=cache,
        )

        # Use session factory to get a dedicated session for this thread
        with session_factory() as session:
            summary_result = summarize_quality(
                session=session,
                agent=agent,
                slice_definition=slice_def,
                skip_existing=skip_existing,
                session_factory=session_factory,
            )

            if not summary_result.success:
                return SliceProcessingResult(
                    slice_id=slice_def.slice_id,
                    success=False,
                    error=summary_result.error,
                )

            summary = summary_result.unwrap()
            return SliceProcessingResult(
                slice_id=slice_def.slice_id,
                success=True,
                reports_count=len(summary.column_summaries),
                columns_count=len(summary.column_summaries),
            )

    except Exception as e:
        return SliceProcessingResult(
            slice_id=slice_def.slice_id,
            success=False,
            error=str(e),
        )


class QualitySummaryPhase(BasePhase):
    """LLM-powered quality summary phase.

    Aggregates analysis results across slices and generates
    quality summaries per column using LLM.

    Uses parallel processing for slice definitions when session_factory
    is available, falling back to sequential processing otherwise.

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
        """Generate quality summaries using LLM.

        Uses parallel processing for slice definitions when session_factory
        is available.
        """
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Get slice definitions
        slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        slice_definitions = list((ctx.session.execute(slice_stmt)).scalars().all())

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

        # Get settings from config
        skip_existing = ctx.config.get("skip_existing_summaries", True)
        max_workers = ctx.config.get("quality_summary_workers", 4)

        # Process slice definitions
        total_reports = 0
        total_columns = 0
        errors: list[str] = []

        # Use parallel processing if session_factory is available and multiple slices
        if ctx.session_factory and len(slice_definitions) > 1 and max_workers > 1:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(
                        _process_slice_definition,
                        slice_def,
                        ctx.session_factory,
                        config,
                        provider,
                        skip_existing,
                    ): slice_def
                    for slice_def in slice_definitions
                }

                for future in as_completed(futures):
                    proc_result = future.result()
                    if proc_result.success:
                        total_reports += proc_result.reports_count
                        total_columns += proc_result.columns_count
                    else:
                        errors.append(f"Slice {proc_result.slice_id}: {proc_result.error}")
        else:
            # Sequential processing (fallback)
            cache = LLMCache()
            renderer = PromptRenderer()
            agent = QualitySummaryAgent(
                config=config,
                provider=provider,
                prompt_renderer=renderer,
                cache=cache,
            )

            for slice_def in slice_definitions:
                summary_result = summarize_quality(
                    session=ctx.session,
                    agent=agent,
                    slice_definition=slice_def,
                    skip_existing=skip_existing,
                    session_factory=ctx.session_factory,
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
