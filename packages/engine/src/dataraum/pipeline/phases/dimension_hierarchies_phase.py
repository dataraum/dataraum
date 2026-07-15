"""Dimension-hierarchies phase (DAT-761) — stack-v4 FD / drill-down / alias / role discovery.

Session value phase: over each fact's grain-verified enriched view it runs the
DAT-757 gate stack (row-g3 + λ + permutation-BH edges, pair-count aliases with
the disagreement-set role check) across every dimension-like view column —
measures are excluded by their ``semantic_role``, everything else is guarded
by data-grounded checks, not caps. The statistical pass is deterministic and
source-free.

Since DAT-762 the phase additionally carries the class-routed VETO LANE: the
stats decide; deterministic value-evidence routing selects the asserted
structures in the classes the DAT-757 scorecard measured as names-judgeable
(quasi-identifier, free-text determinant, proxy bijection), and a names-only
LLM judge may veto them — vetoed structures are SURFACED
(``needs_confirmation``), never deleted. The judge is built here exactly like
every other phase agent (config + provider, misconfiguration fails the
phase); a judgment call that fails mid-run skips the lane for that view with
the statistical verdicts standing, recorded in the ``veto_lane`` output.

Runs after ``slicing`` and before ``aggregation_lineage`` (the driver tree
consumes the alias groups in DAT-545; role pairs deliberately stay separate
axes). It also folds the user's durable hierarchy/alias teaches into this run
(DAT-537), mirroring the relationship overlay materialization minus
keeper-lift-up + witness — teaches land AFTER the veto lane, so user
assertions are never routed to the judge.
"""

from __future__ import annotations

from types import ModuleType

from sqlalchemy import select

from dataraum.analysis.hierarchies.judge import DimensionIdentityJudge
from dataraum.analysis.hierarchies.processor import discover_dimension_hierarchies
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

logger = get_logger(__name__)


@analysis_phase
class DimensionHierarchiesPhase(BasePhase):
    """Discover drill-down hierarchies, aliases and role pairs over the enriched views."""

    @property
    def name(self) -> str:
        return "dimension_hierarchies"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.hierarchies import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip on genuine preconditions only (a re-run must re-derive, DAT-408)."""
        if not ctx.table_ids:
            return "No tables in session scope"
        has_view = ctx.session.execute(
            select(EnrichedView.view_id)
            .where(
                EnrichedView.fact_table_id.in_(ctx.table_ids),
                EnrichedView.is_grain_verified.is_(True),
            )
            .limit(1)
        ).first()
        if has_view is None:
            return "No grain-verified enriched views — no substrate to relate"
        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        run_id = ctx.require_run_id()

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

        judge = DimensionIdentityJudge(
            config=config, provider=provider, prompt_renderer=PromptRenderer()
        )
        persisted, lane = discover_dimension_hierarchies(
            ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_ids=ctx.table_ids or [],
            run_id=run_id,
            judge=judge,
        )
        return PhaseResult.success(
            outputs={"hierarchies": persisted, "veto_lane": lane.as_output()},
            records_created=persisted,
            summary=(
                f"{persisted} dimension hierarchy/alias/role structure(s) discovered; "
                f"veto lane {lane.status} ({lane.vetoed}/{lane.routed} routed vetoed)"
            ),
        )
