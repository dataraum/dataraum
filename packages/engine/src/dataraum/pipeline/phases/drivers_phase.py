"""Driver-rankings phase (DAT-546) — persist driver discovery per measure.

Session value phase, mirroring ``aggregation_lineage``: source-free, session-scoped,
no LLM call. For each ``semantic_role='measure'`` column on the session's tables it
runs the validated driver-discovery engine (DAT-545/561/563) over the fact's enriched
view and persists the grain-labeled ranking run-versioned. Runs after the value layer
(``slicing`` → ``dimension_hierarchies`` → ``aggregation_lineage`` → ``correlations``)
so its inputs — enriched views, the slice catalog, dimension hierarchies, and
``identity_columns`` — are all present this run.

Persisting (not recomputing on demand) is the pre-computed-context thesis: the answer
agent reads the stored ranking via ``look_drivers``. The genuinely ad-hoc tail (a ratio
a question invents that no measure column backs) is the deferred on-demand path.
"""

from __future__ import annotations

from types import ModuleType

from dataraum.analysis.drivers.persistence import persist_driver_rankings
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

logger = get_logger(__name__)


@analysis_phase
class DriversPhase(BasePhase):
    """Persist per-measure driver rankings over the begin_session substrate (DAT-546)."""

    @property
    def name(self) -> str:
        return "driver_rankings"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.drivers import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only with no session scope; zero measures is a loud success in ``_run``."""
        if not ctx.table_ids:
            return "No tables in session scope"
        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        run_id = ctx.require_run_id()
        persisted = persist_driver_rankings(
            ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_ids=ctx.table_ids or [],
            run_id=run_id,
        )
        return PhaseResult.success(
            outputs={"rankings": persisted},
            records_created=persisted,
            summary=f"{persisted} measure(s) ranked for drivers",
        )
