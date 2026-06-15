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
from dataraum.core.vertical import VerticalKind, available_verticals, resolve_vertical
from dataraum.investigation.queries import tables_for_run
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column

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
        """The typed tables this run reduces over — the run's tables.

        The relational scope key is the run, not the source (DAT-421/506): the
        run's tables are the ones ``typing`` linked to ``run_id`` via
        ``run_tables``, and the reduce/readiness layer already keys on that anchor
        (``detect`` + readiness, DAT-410). This phase was the last in the
        add_source spine still filtering ``Table.source_id == ctx.source_id``; it
        now uses the same ``tables_for_run`` key. Source-agnostic for a run whose
        tables span multiple per-object sources.

        Re-run safety: ``typing``'s ``reconcile_typed_table`` reuses the stable
        typed ``Table.table_id`` (no new row on re-type), so the run-tables link is
        a no-op for an already-linked table — the set never widens across teach
        re-runs over the same ``run_id``.
        """
        return tables_for_run(ctx.session, ctx.require_run_id())

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

        # Born-loud on an UNKNOWN vertical (DAT-480): a typo'd or never-framed
        # name would otherwise resolve to zero concepts and look identical to a
        # legitimately-empty framed vertical below. Distinguish it up front and
        # name what DOES exist, so the user fixes the name rather than the data.
        if resolve_vertical(ontology) is VerticalKind.UNKNOWN:
            available = available_verticals()
            return PhaseResult.failed(
                f"Unknown vertical '{ontology}'. Available verticals: "
                f"{', '.join(available) if available else '(none — frame one first)'}. "
                "Frame this vertical (cockpit `frame`) or pick an existing one."
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
            run_id=ctx.require_run_id(),
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
