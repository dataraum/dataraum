"""Per-column semantic phase (DAT-362 Option B, tier 1 of the split).

Annotates each column with its semantic role, entity type, business term, and
ontology concept — table-local work that produces the surface in-loop teach
acts on. Runs a capable (balanced) model and PERSISTS its output as
``SemanticAnnotation`` rows; the per-table phase later reads those (post-teach)
rows as read-only context.

This replaces the column half of the retired monolithic ``semantic`` phase.
Cross-table reasoning (entities, relationships) moves to ``semantic_per_table``.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select

from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.semantic.processor import persist_column_annotations
from dataraum.core.logging import get_logger
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer as PromptRendererType
    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


@analysis_phase
class SemanticPerColumnPhase(BasePhase):
    """Per-column LLM annotation phase.

    Annotates columns with semantic roles, entity types, business terms, and
    ontology concept mappings using a capable model, then persists them as
    ``SemanticAnnotation`` rows. Table classification and relationships are the
    job of ``semantic_per_table``.

    Requires: statistics.
    """

    @property
    def name(self) -> str:
        return "semantic_per_column"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.semantic import db_models

        return [db_models]

    def replay_cleanup(self, ctx: PhaseContext, table_ids: list[str]) -> None:
        """Drop the source's LLM annotations so the reduce re-annotates (DAT-343).

        Triggered when a teach (e.g. ``concept_property``) changes the
        ontology this reduce reads. Re-running source-wide is the right
        shape per the user's "widening breadth is good input" framing —
        re-annotation sees the now-bigger source and the new ontology.

        Drops every ``SemanticAnnotation`` whose column belongs to a typed
        table of this source. The next run produces a fresh annotation
        per column. ``table_ids`` is ignored — the reduce is source-wide
        and so is its cleanup (matches the empty ``raw_table_ids`` shape
        the workflow uses for source-tail-only replays).
        """
        del table_ids
        typed_table_ids = self._typed_table_ids(ctx)
        if not typed_table_ids:
            return
        column_ids = list(
            ctx.session.execute(
                select(Column.column_id).where(Column.table_id.in_(typed_table_ids))
            ).scalars()
        )
        if not column_ids:
            return
        ctx.session.execute(
            delete(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids))
        )
        ctx.session.flush()

    def _typed_table_ids(self, ctx: PhaseContext) -> list[str]:
        stmt = select(Table.table_id).where(
            Table.layer == "typed", Table.source_id == ctx.source_id
        )
        return [row[0] for row in ctx.session.execute(stmt)]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if every column in the typed tables already has an LLM annotation.

        Only ``annotation_source == "llm"`` rows count: teach-sourced rows are
        excluded so a re-run after a teach regenerates the LLM annotation (and
        thus the post-teach context the per-table phase reads).
        """
        table_ids = self._typed_table_ids(ctx)
        if not table_ids:
            return "No typed tables found"

        total_columns = (
            ctx.session.execute(
                select(func.count(Column.column_id)).where(Column.table_id.in_(table_ids))
            ).scalar()
            or 0
        )
        if total_columns == 0:
            return "No columns found in typed tables"

        annotated = (
            ctx.session.execute(
                select(func.count(SemanticAnnotation.annotation_id))
                .join(Column)
                .where(
                    Column.table_id.in_(table_ids),
                    SemanticAnnotation.annotation_source == "llm",
                )
            ).scalar()
            or 0
        )
        if annotated >= total_columns:
            return "All columns already have semantic annotations"
        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        table_ids = self._typed_table_ids(ctx)
        if not table_ids:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

        col_config = config.features.column_annotation
        if not col_config or not col_config.enabled:
            return PhaseResult.failed("Column annotation is disabled in config.")

        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")
        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        renderer = PromptRenderer()

        ontology = ctx.config.get("vertical")
        if not ontology:
            return PhaseResult.failed(
                "No vertical configured. Set 'vertical' in config/phases/semantic.yaml."
            )

        # Cold-start ontology induction: _adhoc with no concepts needs an
        # induced ontology before columns can map to concepts. (Moved here from
        # the monolithic phase — concept mapping is now per-column.)
        if ontology == "_adhoc":
            induction_error = self._ensure_adhoc_ontology(
                ctx, config, provider, renderer, table_ids
            )
            if induction_error:
                return PhaseResult.failed(induction_error)

        # Standard-field concepts required by active metric graphs, so the model
        # prioritizes mapping those concepts to actual columns.
        from dataraum.graphs.loader import GraphLoader

        metric_loader = GraphLoader(vertical=ontology)
        metric_loader.load_all()
        required_standard_fields = sorted(metric_loader.get_all_abstract_fields())

        agent = ColumnAnnotationAgent(config=config, provider=provider, prompt_renderer=renderer)
        annotation_result = agent.annotate(
            session=ctx.session,
            table_ids=table_ids,
            ontology=ontology,
            required_standard_fields=required_standard_fields,
        )
        if not annotation_result.success or not annotation_result.value:
            return PhaseResult.failed(f"Column annotation failed: {annotation_result.error}")

        from dataraum.analysis.semantic.ontology import OntologyLoader

        ontology_def = OntologyLoader().load(ontology)
        model_name = provider.get_model_for_tier(col_config.model_tier)

        count = persist_column_annotations(
            ctx.session,
            annotation_result.value,
            table_ids,
            annotated_by=model_name,
            session_id=ctx.require_session_id(),
            ontology_def=ontology_def,
        )

        return PhaseResult.success(
            outputs={"annotations": count, "tables_analyzed": len(table_ids)},
            records_processed=count,
            records_created=count,
            summary=f"{count} column annotations",
        )

    def _ensure_adhoc_ontology(
        self,
        ctx: PhaseContext,
        config: LLMConfig,
        provider: LLMProvider,
        renderer: PromptRendererType,
        table_ids: list[str],
    ) -> str | None:
        """Induce an ``_adhoc`` ontology and persist it as ``concept`` overlay rows.

        DAT-371: the baked-in config root is bind-mounted read-only, so the
        old "write YAML back to ``verticals/_adhoc/ontology.yaml``" path
        crashes on cold start. Induced concepts now land as one
        ``config_overlay`` row per concept (``type="concept"``,
        ``payload={"vertical": "_adhoc", **concept.model_dump()}``,
        workspace-scoped — ``session_id=None``); the layered loader
        materializes them on every subsequent ontology read via
        :func:`dataraum.core.overlay._apply_concept`.

        Returns the error string on induction failure, ``None`` on
        success (including the short-circuit when concepts already exist).
        """
        from dataraum.analysis.semantic.induction import OntologyInductionAgent
        from dataraum.analysis.semantic.ontology import OntologyLoader
        from dataraum.storage import ConfigOverlay

        # Short-circuit: re-read through the layered loader so already-
        # induced concept rows count as "the ontology exists". This is
        # what makes a re-run after a successful induction idempotent.
        existing = OntologyLoader().load("_adhoc")
        if existing is not None and existing.concepts:
            return None

        induction = OntologyInductionAgent(
            config=config, provider=provider, prompt_renderer=renderer
        ).induce(session=ctx.session, table_ids=table_ids)
        if not induction.success:
            return f"Ontology induction failed: {induction.error}"
        if not induction.value or not induction.value.concepts:
            return "Ontology induction returned no concepts."

        for concept in induction.value.concepts:
            ctx.session.add(
                ConfigOverlay(
                    session_id=None,  # workspace-scoped: induction is not per-session
                    type="concept",
                    payload={"vertical": "_adhoc", **concept.model_dump(exclude_none=True)},
                )
            )
        # Commit so the resolver (which leases its own session via
        # session_scope) can see the newly inserted rows on the next
        # load. Postgres READ COMMITTED means a flush alone wouldn't make
        # the rows visible across sessions. Committing here is also a
        # crash-safety win: induction is expensive (LLM call); if a later
        # phase fails, the next run short-circuits this branch via the
        # ``existing.concepts`` check above.
        ctx.session.commit()
        return None
