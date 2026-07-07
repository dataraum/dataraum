"""Business cycles phase — the operating_model stage's second lifecycle family (DAT-455).

Source-free and session-scoped, mirroring the validation phase: operates on
``ctx.table_ids`` (the session's typed tables), never a ``source_id``. The
declared set is the vertical's ``cycles.yaml`` vocabulary ⊕ ``cycle`` overlay
teach rows — one ``cycle`` lifecycle artifact per canonical cycle type. The
engine induces nothing (declares come from the vertical; user declares arrive
via frame-2 teach rows). Each declared cycle flows through the typed artifact
lifecycle:

* **declare** — every loaded cycle type becomes a ``declared`` artifact.
* **bind** (``cycle.bind``) — the LLM grounds the declared vocabulary against
  the workspace in ONE synthesis call (the substrate-generality difference
  from validation: cycles ground + measure together, not per-artifact). A
  declared cycle the synthesis detected (resolving to real tables/columns)
  transitions to ``grounded``; one it did not detect STAYS ``declared`` with
  the reason on the row — visibly impossible, never silently absent.
* **execute** (``cycle.execute``) — a grounded cycle whose completion
  measurement is present (the LLM computed a ``completion_rate`` from the
  status column's value counts) reaches ``executed``. A grounded cycle with no
  derivable completion signal stays ``grounded`` with the reason recorded —
  detected but not measured, never reported as executed.

A re-run supersedes: everything is re-declared and re-flowed under the fresh
``run_id`` (no skip-if-already-ran — the prior run's rows coexist untouched,
and the promoted head names the current run). With no vertical or no declared
cycles the phase succeeds loudly with an explicit ``no_declared_cycles``
outcome.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any

from dataraum.analysis.cycles import BusinessCycleAgent
from dataraum.analysis.cycles.config import get_cycle_types
from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
from dataraum.analysis.cycles.models import DetectedCycle
from dataraum.core.logging import get_logger
from dataraum.lifecycle import BaseRunMap, declare_artifact, transition
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

_log = get_logger(__name__)

# The journey stage this phase runs under — the lifecycle guard authorizes
# cycle.declare/bind/execute for this stage only.
_STAGE = "operating_model"

# DAT-630: a cycle that grounds + measures still reaches ``executed`` only as
# strongly as the agent's honest confidence in the detection. Below this floor
# the executed cycle is FLAGGED — its ``state_reason`` names the weak signal — so
# downstream agents (cycles are signals, not scripture) can weigh it instead of
# treating a thin detection as plainly green. The number still shows; we surface
# the doubt. Mirrors the metric phase's ``_low_confidence_reason`` (same floor).
_LOW_CONFIDENCE_FLOOR = 0.5


@analysis_phase
class BusinessCyclesPhase(BasePhase):
    """LLM cycle detection through the artifact lifecycle (DAT-455).

    Declares the vertical's cycle vocabulary, grounds it against the workspace
    in one synthesis call, and measures completion — each declared cycle moving
    through declare → bind → execute. Mirrors :class:`ValidationPhase`.

    Requires: a begin_session workspace (typed tables, relationships, enriched
    views, slices) reachable through the run's pinned base heads.
    """

    @property
    def name(self) -> str:
        return "business_cycles"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.cycles import db_models
        from dataraum.lifecycle import db_models as lifecycle_db_models

        return [db_models, lifecycle_db_models]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Declare → bind → execute every declared cycle type."""
        table_ids = ctx.table_ids
        if not table_ids:
            return PhaseResult.failed(
                "No tables in session scope — cycle detection operates on the "
                "session's typed table selection (ctx.table_ids)."
            )

        # Declared set: the vertical's cycle vocabulary ⊕ cycle overlay teach
        # rows. No vertical / no declared cycles is a LOUD explicit outcome, not
        # a silent skip (the engine induces nothing now).
        vertical: str | None = ctx.config.get("vertical")
        declared_types = get_cycle_types(vertical) if vertical else {}
        if not vertical or not declared_types:
            outcome = "no_vertical" if not vertical else "no_declared_cycles"
            _log.warning("cycles_nothing_declared", vertical=vertical, outcome=outcome)
            return PhaseResult.success(
                outputs={"outcome": outcome, "declared": 0, "detected_cycles": 0},
                records_processed=0,
                records_created=0,
                summary=f"0 declared cycles ({outcome}) — nothing to ground or measure",
            )

        run_id = ctx.require_run_id()
        # Pinned upstream heads (docs/architecture/persistence.md in-run mode): resolved ONCE by the
        # workflow's pre-flight ``operating_model_resolve`` activity and threaded
        # here through the phase config. The phase performs NO head resolution
        # itself — a missing pin is a wiring bug, fail loud.
        raw_base_runs = ctx.config.get("base_runs")
        if raw_base_runs is None:
            return PhaseResult.failed(
                "base_runs missing from the phase config — OperatingModelWorkflow's "
                "resolve activity pins the base-run map before this phase runs "
                "(docs/architecture/persistence.md in-run mode; no per-phase head resolution)."
            )
        base_runs = BaseRunMap.model_validate(raw_base_runs)

        # Initialize LLM infrastructure
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

        agent = BusinessCycleAgent(
            config=config,
            provider=provider,
            prompt_renderer=PromptRenderer(),
        )

        # declare: every declared cycle type becomes a declared artifact for THIS
        # run — supersession across runs; a success-redelivery RESETS the same
        # run's row to declared (declare-or-reuse, DAT-502).
        artifacts = {}
        for canonical_type, defn in declared_types.items():
            artifacts[canonical_type] = declare_artifact(
                ctx.session,
                artifact_type="cycle",
                artifact_key=canonical_type,
                run_id=run_id,
                stage=_STAGE,
                teaches={
                    "canonical_type": canonical_type,
                    "vertical": vertical,
                    "business_value": defn.get("business_value", "medium"),
                },
            )

        # bind: ONE synthesis call grounds the declared vocabulary against the
        # workspace. A hard synthesis failure (no tool call / LLM error) fails
        # the phase — distinct from a declared cycle that simply did not ground.
        grounding = agent.ground_cycles(
            ctx.session,
            ctx.duckdb_conn,
            table_ids,
            vertical=vertical,
            base_runs=base_runs,
        )
        if not grounding.success or grounding.value is None:
            return PhaseResult.failed(grounding.error or "Cycle grounding failed")
        analysis = grounding.value

        # Index detected cycles by canonical type so each declared artifact can
        # be reconciled against the synthesis. A detected cycle whose canonical
        # type is not in the declared set (the LLM named an off-vocabulary type)
        # is dropped here — declares are the lifecycle's source of truth, so an
        # undeclared detection has no artifact to attach to (logged, not lost
        # silently).
        detected_by_type: dict[str, DetectedCycle] = {}
        for detected in analysis.cycles:
            key = detected.canonical_type
            if key is None:
                # Degenerate LLM output: a detection with no canonical type has no
                # declared artifact to attach to. Drop it, but loudly (DAT-439).
                _log.warning(
                    "cycle_detected_no_canonical_type",
                    cycle_name=detected.cycle_name,
                )
                continue
            if key not in artifacts:
                _log.warning(
                    "cycle_detected_not_declared",
                    canonical_type=key,
                    cycle_name=detected.cycle_name,
                )
                continue
            # First detection per type wins (the LLM emits one cycle per type;
            # a duplicate is unexpected but must not double-persist under the
            # (session, canonical_type, run) UNIQUE).
            detected_by_type.setdefault(key, detected)

        # bind → execute per declared artifact; persist the grounded cycles.
        grounded_against = base_runs.model_dump(mode="json")
        persisted: list[DetectedCycle] = []
        for canonical_type, artifact in artifacts.items():
            cycle = detected_by_type.get(canonical_type)
            if cycle is None:
                # Ungroundable: the synthesis did not detect this declared cycle
                # in the workspace. Stays declared, reason on the row.
                artifact.state_reason = "not detected in this workspace"
                continue
            transition(artifact, operation="bind", stage=_STAGE, grounded_against=grounded_against)
            if cycle.completion_rate is None:
                # Detected but not measured — no derivable completion signal.
                # Stays grounded with the reason; never reported as executed.
                artifact.state_reason = "detected but no completion measurement could be derived"
            else:
                # Executed, but flagged when the detection confidence is thin
                # (DAT-630) — the state_reason carries the doubt downstream.
                reason = _low_confidence_cycle_reason(cycle)
                transition(artifact, operation="execute", stage=_STAGE, state_reason=reason)
                if reason:
                    _log.warning(
                        "cycle_executed_low_confidence",
                        canonical_type=canonical_type,
                        confidence=cycle.confidence,
                    )
            persisted.append(cycle)

        _persist_cycles(ctx.session, persisted, run_id=run_id)

        executed = sum(1 for a in artifacts.values() if a.state == "executed")
        grounded_stuck = sum(1 for a in artifacts.values() if a.state == "grounded")
        declared_stuck = sum(1 for a in artifacts.values() if a.state == "declared")
        low_confidence = sum(
            1 for a in artifacts.values() if a.state == "executed" and a.state_reason is not None
        )

        # Surface detected cycles + data-quality observations as preview lines.
        previews: list[str] = []
        for c in persisted:
            rate = f", {c.completion_rate:.0%} complete" if c.completion_rate is not None else ""
            previews.append(f"{c.cycle_name} ({c.canonical_type}{rate})")
        previews.extend(analysis.data_quality_observations)

        return PhaseResult.success(
            outputs={
                "declared": len(artifacts),
                "executed": executed,
                "low_confidence": low_confidence,
                "stuck_grounded": grounded_stuck,
                "stuck_declared": declared_stuck,
                "detected_cycles": len(persisted),
                "business_processes": analysis.detected_processes,
                "business_summary": analysis.business_summary,
                "data_quality_observations": analysis.data_quality_observations,
                "recommendations": analysis.recommendations,
                "tables_analyzed": analysis.tables_analyzed,
            },
            records_processed=len(table_ids),
            # One DetectedBusinessCycle per grounded cycle + one artifact per declare.
            records_created=len(persisted) + len(artifacts),
            warnings=previews,
            summary=(
                f"{executed}/{len(artifacts)} cycles measured "
                f"({len(persisted)} detected); "
                f"{declared_stuck} ungroundable, {grounded_stuck} detected but unmeasured"
            ),
        )


def _low_confidence_cycle_reason(cycle: DetectedCycle) -> str | None:
    """Reason string if the detection confidence is below the floor, else ``None``.

    The agent records an honest per-cycle ``confidence``; below
    :data:`_LOW_CONFIDENCE_FLOOR` the executed cycle is flagged rather than
    rendered plainly green. ``None`` keeps it unflagged.
    """
    if cycle.confidence >= _LOW_CONFIDENCE_FLOOR:
        return None
    return f"low-confidence detection ({cycle.confidence:.2f} < {_LOW_CONFIDENCE_FLOOR:.2f})"


def _persist_cycles(
    session: Session,
    cycles: list[DetectedCycle],
    *,
    run_id: str,
) -> None:
    """Persist one run-stamped ``DetectedBusinessCycle`` per grounded cycle.

    Form-(a) upsert on ``uq_detected_cycle_run`` (DAT-502): a Temporal
    success-redelivery re-runs the whole phase under the same ``run_id`` and
    re-detects the declared vocabulary — converging in place (the existing
    row keeps its ``cycle_id``; the fresh detection's fields win) instead of
    violating the UNIQUE. ``detected_by_type`` already dedups the batch per
    canonical type.
    """
    rows: list[dict[str, Any]] = [
        {
            "cycle_id": cycle.cycle_id,
            "run_id": run_id,
            "cycle_name": cycle.cycle_name,
            "cycle_type": cycle.cycle_type,
            "canonical_type": cycle.canonical_type,
            "is_known_type": cycle.is_known_type,
            "description": cycle.description,
            "business_value": cycle.business_value,
            "confidence": cycle.confidence,
            "tables_involved": cycle.tables_involved,
            "stages": [s.model_dump() for s in cycle.stages],
            "entity_flows": [ef.model_dump() for ef in cycle.entity_flows],
            "status_table": cycle.status_table,
            "status_column": cycle.status_column,
            "completion_value": cycle.completion_value,
            "total_records": cycle.total_records,
            "completed_cycles": cycle.completed_cycles,
            "completion_rate": cycle.completion_rate,
            "evidence": cycle.evidence,
        }
        for cycle in cycles
    ]
    upsert(
        session,
        DetectedBusinessCycle,
        rows,
        index_elements=["canonical_type", "run_id"],
    )

    _log.debug("detected_cycles_persisted", run_id=run_id, count=len(cycles))
