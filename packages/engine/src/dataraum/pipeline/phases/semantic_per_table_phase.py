"""Per-table semantic synthesis phase (DAT-362 Option B, tier 2 of the split).

Classifies tables (entity type, fact/dimension, grain) and confirms cross-table
relationships, reasoning OVER the already-persisted (post-teach) per-column
annotations from ``semantic_per_column``. It does NOT write column annotations.

This replaces the table half of the retired monolithic ``semantic`` phase.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.relationships.utils import load_relationship_candidates_for_semantic
from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.semantic.processor import synthesize_and_store_tables
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
class SemanticPerTablePhase(BasePhase):
    """Per-table LLM synthesis phase.

    Produces table entity classifications + LLM-confirmed relationships over the
    persisted per-column annotations. Requires: semantic_per_column, relationships.
    """

    @property
    def name(self) -> str:
        return "semantic_per_table"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.semantic import db_models

        return [db_models]

    def _typed_tables(self, ctx: PhaseContext) -> list[Table]:
        """The session's selected tables (DAT-401, source-free).

        Scopes purely by ``ctx.table_ids`` — the begin_session selection, which
        may span sources. The ids are already validated as typed by
        ``begin_session_select``'s pre-flight (the single enforcement point), so
        no ``layer`` filter is repeated here. A source is meaningless past
        add_source, so this phase never reads ``ctx.source_id``
        (feedback-source-dies-at-addsource).
        """
        if not ctx.table_ids:
            return []
        stmt = select(Table).where(Table.table_id.in_(ctx.table_ids))
        return list(ctx.session.execute(stmt).scalars())

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if THIS session already classified every one of its tables."""
        typed_tables = self._typed_tables(ctx)
        if not typed_tables:
            return "No typed tables found"

        # Scoped to this session's own classifications (rows carry session_id):
        # another session's entities over a shared table must not make this
        # session skip classification (DAT-401).
        table_ids = [t.table_id for t in typed_tables]
        entity_table_ids = set(
            ctx.session.execute(
                select(TableEntity.table_id).where(
                    TableEntity.session_id == ctx.require_session_id(),
                    TableEntity.table_id.in_(table_ids),
                )
            )
            .scalars()
            .all()
        )
        if all(tid in entity_table_ids for tid in table_ids):
            return "All tables already classified"
        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        typed_tables = self._typed_tables(ctx)
        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")
        table_ids = [t.table_id for t in typed_tables]

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

        agent = SemanticAgent(config=config, provider=provider, prompt_renderer=PromptRenderer())
        relationship_candidates = load_relationship_candidates_for_semantic(
            session=ctx.session,
            table_ids=table_ids,
            detection_method="candidate",
        )

        result = synthesize_and_store_tables(
            session=ctx.session,
            agent=agent,
            table_ids=table_ids,
            ontology=ontology,
            relationship_candidates=relationship_candidates,
            duckdb_conn=ctx.duckdb_conn,
            session_id=ctx.require_session_id(),
        )
        if not result.success:
            return PhaseResult.failed(result.error or "Table synthesis failed")
        enrichment = result.unwrap()

        entities_count = len(enrichment.entity_detections)
        relationships_count = len(enrichment.relationships)

        previews: list[str] = []
        for ent in enrichment.entity_detections:
            kind = "FACT" if ent.is_fact_table else "DIMENSION" if ent.is_dimension_table else ""
            label = f"{ent.table_name}: {ent.entity_type}"
            if kind:
                label += f" ({kind})"
            previews.append(label)
        for r in enrichment.relationships:
            previews.append(f"{r.from_table}.{r.from_column} → {r.to_table}.{r.to_column}")

        return PhaseResult.success(
            outputs={
                "entities": entities_count,
                "confirmed_relationships": relationships_count,
                "tables_analyzed": [t.table_name for t in typed_tables],
            },
            records_processed=len(typed_tables),
            records_created=entities_count + relationships_count,
            warnings=previews,
            summary=f"{entities_count} entities, {relationships_count} relationships",
        )
