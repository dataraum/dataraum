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

from sqlalchemy import func, select

from dataraum.analysis.semantic.ontology import OntologyLoader
from dataraum.analysis.semantic.processor import ground_columns
from dataraum.core.logging import get_logger
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

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

    def _typed_table_ids(self, ctx: PhaseContext) -> list[str]:
        stmt = select(Table.table_id).where(
            Table.layer == "typed", Table.source_id == ctx.source_id
        )
        return [row[0] for row in ctx.session.execute(stmt)]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only when there are genuinely no typed columns to annotate.

        Structural early-outs only (DAT-413): "no typed tables" and "no columns".
        A re-run mints a fresh ``run_id`` and must always re-annotate under it, so
        the old "all columns already have an LLM annotation → skip" bail is gone.
        ``ground_columns`` is a pure insert stamping ``run_id`` on each
        ``SemanticAnnotation`` (the ``(column_id, run_id)`` unique constraint lets
        a new run's rows coexist with prior runs'); the promoted head names which
        run is current.
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

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Resolve config, assert the ``_adhoc`` frame exists, then ground.

        Grounding-only (DAT-382): induction has left the engine for the cockpit
        agent tier (the ``frame`` stage writes ``concept`` overlay rows before
        ``add_source`` runs). This phase no longer bootstraps a cold-start
        ontology; it grounds columns against the concepts the frame declared.

        On a cold-start ``_adhoc`` workspace the ontology can only come from
        those frame-written concept rows. If none exist, grounding would map
        every column against an empty concept set — so we FAIL LOUD here rather
        than silently produce concept-less annotations. Journey-layer gating
        (stopping the user before they reach ``add_source``) is DAT-378/356.
        """
        table_ids = self._typed_table_ids(ctx)
        if not table_ids:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

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

        # Cold-start fail-loud (DAT-382, generalized for framed verticals): a
        # vertical whose concepts come only from the cockpit frame stage's
        # overlay rows — `_adhoc` or any framed vertical — grounds against zero
        # concepts (a silent no-op) if frame never ran. Curated builtins like
        # finance ship concepts on disk and pass. Load the configured vertical
        # and refuse when it resolves to no concepts, naming the missing step.
        resolved = OntologyLoader().load(ontology)
        if resolved is None or not resolved.concepts:
            return PhaseResult.failed(
                f"No concepts found for vertical '{ontology}' — grounding requires the "
                "frame stage to declare concepts first (cockpit `frame` writes them as "
                "`concept` overlay rows). Run frame before add_source."
            )

        grounding = ground_columns(
            session=ctx.session,
            config=config,
            provider=provider,
            renderer=renderer,
            table_ids=table_ids,
            ontology=ontology,
            session_id=ctx.require_session_id(),
            run_id=ctx.run_id,
        )
        if not grounding.success:
            return PhaseResult.failed(grounding.error or "Column annotation failed")

        count = grounding.unwrap()
        return PhaseResult.success(
            outputs={"annotations": count, "tables_analyzed": len(table_ids)},
            records_processed=count,
            records_created=count,
            summary=f"{count} column annotations",
        )
