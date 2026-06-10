"""Aggregation-lineage phase (DAT-491) — events→measure rollup discovery.

Session value phase, runs after ``temporal_slice_analysis``: discovery is
deterministic arithmetic over the slice substrate that phase just persisted
(per-(slice value, period) sums on ``TemporalSliceAnalysis.column_sums``),
paired across facts by the shared slice dimensions the slicing agent chose.
No LLM call and no SQL — the LLM judgments this depends on (slice dimensions,
time axes, enriched joins) were all made upstream by the agents that own them.

The phase declares the ``temporal_behavior`` detector in ``pipeline.yaml``, so
the terminal ``session_detect`` re-adjudicates stock/flow with the
data-grounded ``structural_reconciliation`` witness this phase just produced.
"""

from __future__ import annotations

from types import ModuleType

from sqlalchemy import select

from dataraum.analysis.lineage.processor import discover_aggregation_lineage
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

logger = get_logger(__name__)


@analysis_phase
class AggregationLineagePhase(BasePhase):
    """Discover events→measure aggregation lineage over the slice substrate."""

    @property
    def name(self) -> str:
        return "aggregation_lineage"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.lineage import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip on genuine preconditions only (a re-run must re-derive, DAT-408)."""
        if not ctx.table_ids:
            return "No tables in session scope"
        if len(ctx.table_ids) < 2:
            return "Lineage needs a measure table and an event table (≥ 2 tables)"
        has_defs = ctx.session.execute(
            select(SliceDefinition.slice_id)
            .where(
                SliceDefinition.table_id.in_(ctx.table_ids),
                SliceDefinition.run_id == ctx.run_id,
            )
            .limit(1)
        ).first()
        if has_defs is None:
            return "No slice definitions this run (slicing skipped) — no substrate to pair"
        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        session_id = ctx.require_session_id()
        run_id = ctx.require_run_id()
        persisted = discover_aggregation_lineage(
            ctx.session,
            table_ids=ctx.table_ids or [],
            session_id=session_id,
            run_id=run_id,
            period_grain=str(ctx.config.get("time_grain", "monthly")),
        )
        return PhaseResult.success(
            outputs={"reconciled": persisted},
            records_created=persisted,
            summary=f"{persisted} measure column(s) reconciled to event lineage",
        )
