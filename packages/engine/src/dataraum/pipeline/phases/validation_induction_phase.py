"""Validation induction phase — generate validations over the served graph (DAT-735).

The operating_model stage's PRE-validation step: it serves the promoted graph to an
induction LLM, which proposes typed validation specs; the proposals are
membership-validated against the served context (fabricated references repaired once,
then dropped) and the clean set is persisted as ``source='generated'`` rows. The
downstream ``validation`` phase then declares/binds/executes the seed ``⊕`` generated
``⊕`` teach set uniformly — induction just populates the typed home first.

Placement (spine): ``operating_model_resolve → validation_induction → validation``.
Induction precedes validation because the validation phase reads the rows it writes.

**First-run limitation (recorded for sweep interpretation).** Induction runs BEFORE
the cycles and metrics phases in the spine, so the run-versioned cycles / additivity
it serves come from the PRIOR promoted operating_model head, never this run's. On the
VERY FIRST operating_model run of a workspace no head has promoted yet, so those two
sections are simply ABSENT from the served graph — induction proposes from the
structural substrate alone (concepts + part_of, references, conventions, units, and
the metric DAG, which is declaration-versioned and always present). This is graceful
degradation, not a gap; a re-run, once a head exists, serves the prior model's
cycles/additivity.

**nothing_declared invariant (DAT-845).** This phase reports a ``generated`` count,
NEVER a ``declared`` count — the OperatingModelWorkflow's ``nothing_declared`` gate
keys on the validation/cycles/metrics DECLARED counts only, so zero generated
validations on a thin graph cannot flip a workspace into ``nothing_declared`` (the
generated count and the declared count are different facts).

**Fault isolation.** A degraded induction — the ``induce()`` LLM call's render/parse
failure — logs and succeeds with ``generated=0`` rather than sinking the whole
operating_model run; the seed validations still validate. Only a transient provider
error propagates (it rides the exception to the durable boundary for retry). This
isolation is scoped to the induction turn: a missing LLM config / vertical /
workspace_id is a WIRING failure and still fails the phase loud (as every LLM phase
does), never a degrade.
"""

from __future__ import annotations

from types import ModuleType

from dataraum.analysis.validation.induction import (
    ValidationInductionAgent,
    build_served_context,
)
from dataraum.analysis.validation.validation_store import persist_generated_validations
from dataraum.core.logging import get_logger
from dataraum.lifecycle import BaseRunMap
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

_log = get_logger(__name__)


@analysis_phase
class ValidationInductionPhase(BasePhase):
    """Agentic validation induction over the promoted graph (DAT-735).

    Requires: a promoted begin_session workspace (concepts, edges, references,
    conventions, units, metric DAG) reachable through the run's pinned base heads.
    """

    @property
    def name(self) -> str:
        return "validation_induction"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.validation import db_models

        return [db_models]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Serve the graph → induce validations → persist the clean generated set."""
        table_ids = ctx.table_ids
        vertical = ctx.config.get("vertical")
        # No vertical / no tables ⇒ nothing to induce over. Loud, explicit, generated=0
        # — NEVER a declared count (the nothing_declared gate must not see this phase).
        if not vertical:
            return PhaseResult.success(
                outputs={"outcome": "no_vertical", "generated": 0},
                summary="no vertical — nothing to induce",
            )
        if not table_ids:
            return PhaseResult.success(
                outputs={"outcome": "no_tables", "generated": 0},
                summary="no tables in session scope — nothing to induce",
            )

        raw_base_runs = ctx.config.get("base_runs")
        if raw_base_runs is None:
            return PhaseResult.failed(
                "base_runs missing from the phase config — OperatingModelWorkflow's "
                "resolve activity pins the base-run map before this phase runs "
                "(ADR-0008 in-run mode; no per-phase head resolution)."
            )
        base_runs = BaseRunMap.model_validate(raw_base_runs)
        workspace_id = ctx.config.get("workspace_id")
        if not workspace_id:
            return PhaseResult.failed(
                "workspace_id missing from the phase config — the "
                "run_validation_induction activity threads it (the served graph reads "
                "the workspace's operating-model property graph by schema)."
            )

        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")
        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:  # noqa: BLE001 - provider construction failure is a hard stop
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        agent = ValidationInductionAgent(
            config=config, provider=provider, prompt_renderer=PromptRenderer()
        )

        # Serve the PROMOTED operating_model head (om_run_id=None): this-run cycles /
        # additivity are not written until the later phases, so induction reads the
        # prior model (empty on a first run) — graceful degradation, not a gap.
        served_graph, conventions, membership = build_served_context(
            ctx.session,
            table_ids,
            ctx.duckdb_conn,
            vertical=vertical,
            om_run_id=None,
            catalogue_run_id=base_runs.relationship_run_id,
            workspace_id=workspace_id,
        )

        # A transient ProviderError propagates for retry; a parse/render failure returns
        # Result.fail → degrade (keep the prior generated set), never sink the OM run.
        result = agent.induce(served_graph, conventions, membership)
        if not result.success:
            _log.warning("validation_induction_degraded", reason=result.error)
            return PhaseResult.success(
                outputs={"outcome": "degraded", "generated": 0},
                summary=f"induction degraded ({result.error}) — kept the prior generated set",
            )

        specs = result.unwrap()
        # Supersede-then-insert: a successful induction (even an empty one, on a thin
        # graph) is authoritative — it clears stale generated rows and lands the new set.
        inserted = persist_generated_validations(ctx.session, vertical, specs)

        return PhaseResult.success(
            outputs={"outcome": "induced", "generated": inserted, "proposed": len(specs)},
            records_created=inserted,
            summary=f"induced {len(specs)} validations, {inserted} persisted (source=generated)",
        )
