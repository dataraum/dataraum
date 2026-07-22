"""Promote gate for ``operatingModelWorkflow`` (DAT-845).

A framed vertical that declared ZERO across all three lifecycle families
(validation ⊕ business_cycles ⊕ metrics) has no operating model to seal. The
workflow must NOT flip the ``(catalog, "operating_model")`` head for that run —
an empty promote is indistinguishable downstream from a real one (the cockpit
reads head-presence as "analyzed"). Two facts have to hold:

* **All three declared 0 → refuse, but COMPLETE.** The terminal
  ``operating_model_promote`` activity (the ONLY thing that flips the head) is
  never invoked, and the run completes with the typed ``nothing_declared``
  outcome — not a ``ProgressFailure`` (a re-run can't fix a vertical that
  declares nothing, so failing would only loop). The refusal is loud + queryable:
  ``result.outcome`` + the terminal progress ``phase``.

* **Any family non-zero → promote exactly as today.** The head flip runs once and
  the run completes with ``outcome="promoted"``.

Both drive a real run on a Temporal **dev-server testcontainer** (NOT
``WorkflowEnvironment`` — its time-skipping test-server binary stalls CI, per the
project's Temporal-test convention) with the activities mocked to trivial
deterministic stubs, so no engine/DB is dragged in. Each run's history then feeds
an offline :class:`Replayer` to prove the gate branch is determinism-safe. Real
end-to-end values (live worker, real phases) are covered by compose-smoke.
"""

from __future__ import annotations

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer, Worker

from dataraum.worker.contracts import (
    OperatingModelInput,
    OperatingModelScope,
    PhaseOutcome,
    RunRef,
)
from dataraum.worker.workflows import OperatingModelWorkflow
from tests.integration.worker.conftest import make_sandboxed_runner

_TASK_QUEUE = "dat845-gate-test"
_WORKSPACE_ID = "test"
_WORKFLOW_ID = "operatingmodel-test-dat845"
_VALIDATION_SUMMARY = "validation phase outcome"

# The declared-artifact count each lifecycle family returns — set per test before
# the run so one worker/activity registration serves both scenarios (mirrors the
# module-level-mutable pattern in test_progress_query.py). loadscope keeps this
# module on one xdist worker, so the sequential per-test mutation is race-free.
_declared: dict[str, int] = {"validation": 0, "business_cycles": 0, "metrics": 0}

# Records every terminal head-flip invocation — the ONLY thing that flips the
# (catalog, "operating_model") head. An empty list after a run PROVES the head was
# not promoted (no DB needed at the workflow level).
_promote_calls: list[RunRef] = []


# --- mocked activities: the operating_model spine, deterministic stubs ---------


@activity.defn(name="operating_model_resolve")
async def _resolve(_payload: object) -> OperatingModelScope:
    # Pins the base-run map + table set (ADR-0008); the phases downstream ignore
    # the scope in these stubs. One table is enough to look like a real workspace.
    return OperatingModelScope(relationship_run_id="run-bs", semantic_runs={}, table_ids=["tbl-1"])


@activity.defn(name="validation")
async def _validation(_payload: object) -> PhaseOutcome:
    return PhaseOutcome(
        status="completed", summary=_VALIDATION_SUMMARY, declared=_declared["validation"]
    )


@activity.defn(name="operating_model_detect")
async def _detect(_payload: object) -> PhaseOutcome:
    # Terminal validation-scoring pass (DAT-432); carries no declared signal.
    return PhaseOutcome(status="completed", summary="0 validation detector records")


@activity.defn(name="business_cycles")
async def _cycles(_payload: object) -> PhaseOutcome:
    return PhaseOutcome(status="completed", summary="cycles", declared=_declared["business_cycles"])


@activity.defn(name="metrics")
async def _metrics(_payload: object) -> PhaseOutcome:
    return PhaseOutcome(status="completed", summary="metrics", declared=_declared["metrics"])


@activity.defn(name="operating_model_promote")
async def _promote(run: RunRef) -> PhaseOutcome:
    _promote_calls.append(run)
    return PhaseOutcome(status="completed", summary="promoted 1 operating_model head(s)")


_MOCK_ACTIVITIES = [_resolve, _validation, _detect, _cycles, _metrics, _promote]


def _worker(client: Client) -> Worker:
    return Worker(
        client,
        task_queue=_TASK_QUEUE,
        workflows=[OperatingModelWorkflow],
        activities=_MOCK_ACTIVITIES,
        workflow_runner=make_sandboxed_runner(),
    )


async def _replay(handle: WorkflowHandle) -> None:
    """Offline determinism: replay the captured history, no server."""
    history = await handle.fetch_history()
    replayer = Replayer(
        workflows=[OperatingModelWorkflow],
        data_converter=pydantic_data_converter,
        workflow_runner=make_sandboxed_runner(),
    )
    # replay_workflow raises on any non-determinism — the gate branch + the
    # progress-phase mutations must reconstruct identically.
    await replayer.replay_workflow(history)


@pytest.mark.asyncio
async def test_nothing_declared_refuses_promote_and_completes(temporal_client: Client) -> None:
    """All three families declared 0 → head NOT flipped, run completes nothing_declared."""
    _declared.update(validation=0, business_cycles=0, metrics=0)
    _promote_calls.clear()

    async with _worker(temporal_client):
        handle = await temporal_client.start_workflow(
            OperatingModelWorkflow.run,
            OperatingModelInput(workspace_id=_WORKSPACE_ID, verticals=["finance"]),
            id=f"{_WORKFLOW_ID}-empty",
            task_queue=_TASK_QUEUE,
        )
        result = await handle.result()
        snapshot = await handle.query(OperatingModelWorkflow.get_progress)

    # Refused: the head-flip activity was NEVER invoked, so the (catalog,
    # operating_model) head is untouched — and the run COMPLETED (no raise) with
    # the typed nothing_declared outcome, not a silent green promote.
    assert result.outcome == "nothing_declared"
    assert _promote_calls == [], "operating_model_promote must not run when nothing is declared"
    # validation's own summary still rides through on the gate path (the contract).
    assert result.validation_summary == _VALIDATION_SUMMARY
    # Loud + queryable: the terminal progress phase names the refusal; a refusal is
    # NOT a failure, so no ProgressFailure is stamped.
    assert snapshot.phase == "nothing_declared"
    assert snapshot.failure is None

    await _replay(handle)


@pytest.mark.parametrize(
    ("nonzero_family", "suffix"),
    [("validation", "val"), ("business_cycles", "cyc"), ("metrics", "met")],
)
@pytest.mark.asyncio
async def test_any_family_declared_promotes_as_today(
    temporal_client: Client, nonzero_family: str, suffix: str
) -> None:
    """ANY single family non-zero → promotes as today (spec: 'any of the three').

    Runs the gate with exactly one family declaring (the other two at 0) for each
    of validation / business_cycles / metrics — the condition is symmetric in code,
    but the spec names all three, so each gets its own run.
    """
    _declared.update(validation=0, business_cycles=0, metrics=0)
    _declared[nonzero_family] = 2
    _promote_calls.clear()

    async with _worker(temporal_client):
        handle = await temporal_client.start_workflow(
            OperatingModelWorkflow.run,
            OperatingModelInput(workspace_id=_WORKSPACE_ID, verticals=["finance"]),
            id=f"{_WORKFLOW_ID}-promote-{suffix}",
            task_queue=_TASK_QUEUE,
        )
        result = await handle.result()
        snapshot = await handle.query(OperatingModelWorkflow.get_progress)

    # Something was declared → the head flips exactly once, unchanged from today.
    assert result.outcome == "promoted"
    assert len(_promote_calls) == 1
    assert snapshot.phase == "done"
    assert snapshot.failure is None

    await _replay(handle)
