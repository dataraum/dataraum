"""Catalogue-semantics phase (DAT-823).

The begin_session authoring turn at the catalogue horizon: after
``semantic_per_table`` confirmed the relationships, ``enriched_views`` composed
the fact×dimension views, and ``slicing`` resolved the dimension identities,
one LLM turn authors each table's business reading (UPDATE onto the run's
TableEntity stubs) and every column's ColumnConcept row (meaning +
determination, unit source, derived-formula hypothesis).
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.catalogue.agent import CatalogueSemanticsAgent
from dataraum.analysis.catalogue.processor import author_and_store_catalogue
from dataraum.core.logging import get_logger
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Table

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


@analysis_phase
class CatalogueSemanticsPhase(BasePhase):
    """Catalogue-horizon business-semantics authoring (DAT-823).

    Requires: semantic_per_table (the TableEntity stubs + confirmed
    relationships), enriched_views, slicing. Gated by the same
    ``semantic_analysis`` feature as the per-table tier — it is that feature's
    relocated authoring half, not a new knob.
    """

    @property
    def name(self) -> str:
        return "catalogue_semantics"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.semantic import db_models

        return [db_models]

    def _session_tables(self, ctx: PhaseContext) -> list[Table]:
        """The session's selected tables (DAT-401, source-free).

        Scopes purely by ``ctx.table_ids`` — already validated as typed by
        ``begin_session_select``'s pre-flight, so no ``layer`` filter is
        repeated here (the semantic_per_table pattern).
        """
        if not ctx.table_ids:
            return []
        stmt = select(Table).where(Table.table_id.in_(ctx.table_ids))
        return list(ctx.session.execute(stmt).scalars())

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only on genuine preconditions — never because the run already authored.

        A versioned begin_session re-run MUST re-author (DAT-408); the persist
        is a run-scoped upsert + UPDATE, so a Temporal retry converges instead
        of duplicating.
        """
        if not self._session_tables(ctx):
            return "No typed tables found"
        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        tables = self._session_tables(ctx)
        if not tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")
        table_ids = [t.table_id for t in tables]

        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

        if not config.features.semantic_analysis.enabled:
            return PhaseResult.failed("Semantic analysis is disabled in config.")

        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")
        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        ontology = ctx.config.get("vertical")
        if not ontology:
            return PhaseResult.failed(
                "No vertical configured. Set 'vertical' in config/phases/semantic.yaml."
            )

        agent = CatalogueSemanticsAgent(
            config=config, provider=provider, prompt_renderer=PromptRenderer()
        )
        result = author_and_store_catalogue(
            ctx.session,
            ctx.duckdb_conn,
            agent,
            table_ids,
            ontology,
            run_id=ctx.require_run_id(),
        )
        if not result.success:
            return PhaseResult.failed(result.error or "Catalogue authoring failed")
        stats = result.unwrap()

        return PhaseResult.success(
            outputs={
                **stats.as_output(),
                "tables_analyzed": [t.table_name for t in tables],
            },
            records_processed=len(tables),
            records_created=stats.authored_tables + stats.authored_columns,
            summary=(
                f"{stats.authored_tables} table readings, "
                f"{stats.authored_columns} column concepts "
                f"({stats.ambiguous} ambiguous, {stats.missing} missing)"
            ),
        )
