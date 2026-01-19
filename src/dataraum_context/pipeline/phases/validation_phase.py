"""Validation phase implementation.

LLM-powered validation checks that interpret table schemas
to identify relevant columns and generate appropriate SQL.
"""

from __future__ import annotations

from sqlalchemy import func, select

from dataraum_context.analysis.validation import ValidationAgent
from dataraum_context.analysis.validation.db_models import ValidationRunRecord
from dataraum_context.llm import LLMCache, PromptRenderer, create_provider, load_llm_config
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Table


class ValidationPhase(BasePhase):
    """LLM-powered validation phase.

    Generates and executes SQL validation checks by passing table schemas
    to the LLM. Can generate cross-table JOINs when validations require
    data from multiple tables.

    Requires: semantic phase.
    """

    @property
    def name(self) -> str:
        return "validation"

    @property
    def description(self) -> str:
        return "LLM-powered validation checks"

    @property
    def dependencies(self) -> list[str]:
        return ["semantic"]

    @property
    def outputs(self) -> list[str]:
        return ["validation_results"]

    @property
    def is_llm_phase(self) -> bool:
        return True

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if validations have already been run for this source."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for existing validation runs
        run_stmt = select(func.count(ValidationRunRecord.run_id)).where(
            ValidationRunRecord.table_ids.contains(table_ids)
        )
        run_count = (ctx.session.execute(run_stmt)).scalar() or 0

        if run_count > 0:
            return "Validation already run for these tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run validation checks using LLM."""
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

        # Check if validation is implicitly enabled (no specific feature flag)
        # Validation uses semantic_analysis settings as baseline

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

        # Create validation agent
        agent = ValidationAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
            cache=cache,
        )

        # Get optional category filter from config
        category = ctx.config.get("validation_category")

        # Run validations
        validation_result = agent.run_validations(
            session=ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_ids=table_ids,
            category=category,
            persist=True,
        )

        if not validation_result.success:
            return PhaseResult.failed(validation_result.error or "Validation failed")

        run_result = validation_result.unwrap()

        return PhaseResult.success(
            outputs={
                "total_checks": run_result.total_checks,
                "passed_checks": run_result.passed_checks,
                "failed_checks": run_result.failed_checks,
                "skipped_checks": run_result.skipped_checks,
                "error_checks": run_result.error_checks,
                "overall_status": run_result.overall_status.value,
                "has_critical_failures": run_result.has_critical_failures,
                "tables_validated": [t.table_name for t in typed_tables],
            },
            records_processed=run_result.total_checks,
            records_created=run_result.total_checks,
        )
