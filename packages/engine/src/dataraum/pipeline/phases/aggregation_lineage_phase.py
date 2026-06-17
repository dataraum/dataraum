"""Aggregation-lineage phase (DAT-491/536) — events→measure rollup discovery.

Session value phase: discovery aggregates inline (one ``GROUP BY dim, period``
over each fact's enriched view, DAT-536) and reconciles the per-(slice value,
period) sums across facts sharing a catalog slice dimension. No LLM call — the
judgments this depends on (slice dimensions, time axes, enriched joins) were all
made upstream by the agents that own them.

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
        run_id = ctx.require_run_id()
        persisted = discover_aggregation_lineage(
            ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_ids=ctx.table_ids or [],
            run_id=run_id,
            period_grain=str(ctx.config.get("time_grain", "monthly")),
        )
        return PhaseResult.success(
            outputs={"reconciled": persisted},
            records_created=persisted,
            summary=f"{persisted} measure column(s) reconciled to event lineage",
        )
