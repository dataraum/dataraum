"""Dimension-hierarchies phase (DAT-537) — g3 FD / drill-down / alias discovery.

Session value phase, deterministic and source-free: over each fact's grain-verified
enriched view it computes the g3 functional-dependency measure across the catalog's
grain-safe slice dimensions (DAT-536), surfacing drill-down hierarchies and 1:1
aliases. No LLM — the judgments it builds on (which columns are slice dimensions,
the enriched joins) were made upstream.

Runs after ``slicing`` and before ``aggregation_lineage`` (it reads the slice
catalog; nothing in the value layer depends on it yet — the answer agent consumes
the hierarchies in DAT-538, the driver tree in DAT-545). It also folds the user's
durable hierarchy/alias teaches into this run (DAT-537), mirroring the relationship
overlay materialization minus keeper-lift-up + witness (g3 is deterministic, so
there is no silent-accept and no detect pool).
"""

from __future__ import annotations

from types import ModuleType

from sqlalchemy import select

from dataraum.analysis.hierarchies.processor import discover_dimension_hierarchies
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

logger = get_logger(__name__)


@analysis_phase
class DimensionHierarchiesPhase(BasePhase):
    """Discover g3 drill-down hierarchies + aliases over the slice substrate."""

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
        has_defs = ctx.session.execute(
            select(SliceDefinition.slice_id)
            .where(
                SliceDefinition.table_id.in_(ctx.table_ids),
                SliceDefinition.run_id == ctx.run_id,
            )
            .limit(1)
        ).first()
        if has_defs is None:
            return "No slice definitions this run (slicing skipped) — no dimensions to relate"
        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        run_id = ctx.require_run_id()
        persisted = discover_dimension_hierarchies(
            ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_ids=ctx.table_ids or [],
            run_id=run_id,
        )
        return PhaseResult.success(
            outputs={"hierarchies": persisted},
            records_created=persisted,
            summary=f"{persisted} dimension hierarchy/alias structure(s) discovered",
        )
