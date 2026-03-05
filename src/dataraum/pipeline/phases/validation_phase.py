"""Validation phase implementation.

LLM-powered validation checks that interpret table schemas
to identify relevant columns and generate appropriate SQL.
"""

from __future__ import annotations

from types import ModuleType

from sqlalchemy import select

from dataraum.analysis.validation import ValidationAgent
from dataraum.analysis.validation.db_models import ValidationResultRecord
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Table


@analysis_phase
class ValidationPhase(BasePhase):
    """LLM-powered validation phase.

    Generates and executes SQL validation checks by passing table schemas
    to the LLM. Can generate cross-table JOINs when validations require
    data from multiple tables.

    Requires: semantic, relationships, enriched_views, slicing.
    """

    @property
    def name(self) -> str:
        return "validation"

    @property
    def description(self) -> str:
        return "LLM-powered validation checks"

    @property
    def dependencies(self) -> list[str]:
        return ["semantic", "relationships", "enriched_views", "slicing"]

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.validation import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if validations have already been run for this source."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return "No typed tables found"

        # Check for existing validation results for this source's tables.
        # table_ids is a JSON array, so we check overlap in Python but only
        # project the minimal columns needed.
        table_id_set = {t.table_id for t in typed_tables}
        rows = ctx.session.execute(
            select(ValidationResultRecord.table_ids)
        ).all()

        for (result_table_ids,) in rows:
            if set(result_table_ids or []) & table_id_set:
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

        # Create validation agent
        agent = ValidationAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        # Vertical is required for loading validation specs
        vertical = ctx.config.get("vertical")
        if not vertical:
            return PhaseResult.failed(
                "No vertical configured. Set 'vertical' in config/phases/validation.yaml."
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
            vertical=vertical,
        )

        if not validation_result.success:
            return PhaseResult.failed(validation_result.error or "Validation failed")

        run_result = validation_result.unwrap()

        # Surface failed validations as warnings for CLI display
        warnings = [
            f"{r.validation_id}: {r.message}" for r in run_result.results if not r.passed
        ]

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
            warnings=warnings,
            summary=f"{run_result.passed_checks} passed, {run_result.failed_checks} failed",
        )
