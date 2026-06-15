"""Parent-level progress query + replay-determinism for ``addSourceWorkflow`` (DAT-406).

The parent workflow serves a read-only ``get_progress`` query returning a
:class:`ProgressSnapshot` (``phase``, ``tables_total``/``tables_completed``, the
per-table ``tables`` steps, and any ``failure``) — the cross-package shape the
cockpit Client polls while the parent is blocked in the fan-out (mirrored TS-side
in DAT-352). Two things have to hold:

* **The snapshot advances.** ``phase`` walks ``import`` →
  ``check_column_limit`` → ``processing_tables`` → ``semantic_per_column`` →
  ``detect`` → ``promote`` → ``done`` and ``tables_completed`` climbs
  monotonically toward ``tables_total`` as each
  child resolves. We drive a real run on a Temporal **dev-server testcontainer**
  (NOT ``WorkflowEnvironment`` — its time-skipping test-server binary stalls CI,
  per the project's Temporal-test convention) with the activities + the child
  workflow mocked to trivial deterministic stubs, so no engine/DB is dragged in.

* **It replays clean.** The ``workflow.as_completed`` fan-out swap and the
  ``self._progress`` phase/counter mutations only change state at points gated
  by awaiting recorded history events, so the offline :class:`Replayer`
  reconstructs the identical run with no non-determinism error. The determinism
  test replays the history captured from the live run above — fully offline, no
  server — which is the project's endorsed determinism path.

Real end-to-end values (live worker, real activities) are covered by
compose-smoke, not here.
"""

from __future__ import annotations

import asyncio

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer, Worker

from dataraum.worker.contracts import (
    AddSourceInput,
    ImportResult,
    PhaseOutcome,
    ProcessTableInput,
    ProgressSnapshot,
    RunScopedInput,
    SourceIdentity,
    TypingResult,
    add_source_workflow_id,
)
from dataraum.worker.workflows import AddSourceWorkflow, ProcessTableWorkflow
from tests.integration.worker.conftest import make_sandboxed_runner

_TASK_QUEUE = "dat406-progress-test"
_RAW_IDS = ["raw-a", "raw-b", "raw-c"]


# --- mocked activities -------------------------------------------------------
#
# Trivial deterministic stubs registered under the production activity names the
# workflows call. They return the right contract shapes and touch no engine/DB,
# so the test exercises ONLY the orchestration + progress bookkeeping. Each child
# sleeps a hair on ``statistics`` so the three children resolve at staggered
# times and the parent's ``as_completed`` loop genuinely advances the counter in
# steps (rather than all three landing in one scheduler tick).


@activity.defn(name="import")
async def _import(_payload: object) -> ImportResult:
    # ``import`` now takes a ``SourcePhaseInput`` (identity + vertical, DAT-506);
    # the stub ignores the payload and just yields the fixed raw-table set.
    return ImportResult(raw_table_ids=list(_RAW_IDS))


# Records every invocation so the test can assert the workflow actually ran the
# gate — without this, dropping the ``execute_activity("check_column_limit", …)``
# call from workflows.py would still pass (the mock merely being registered
# proves nothing).
_check_column_limit_calls: list[RunScopedInput] = []


@activity.defn(name="check_column_limit")
async def _check_column_limit(payload: RunScopedInput) -> PhaseOutcome:
    # Run-scoped column gate (DAT-430), between the import loop and the fan-out.
    # A trivial pass here — the gate's counting/limit logic is exercised in
    # ``tests/unit/worker/test_check_column_limit.py``; this test cares that the
    # orchestration calls it and progress bookkeeping survives it.
    _check_column_limit_calls.append(payload)
    return PhaseOutcome(status="completed")


@activity.defn(name="typing")
async def _typing(payload: ProcessTableInput) -> TypingResult:
    return TypingResult(typed_table_id=f"typed-{payload.raw_table_id}")


@activity.defn(name="statistics")
async def _statistics(_scoped: object) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="column_eligibility")
async def _column_eligibility(_scoped: object) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="statistical_quality")
async def _statistical_quality(_scoped: object) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="temporal")
async def _temporal(_scoped: object) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="semantic_per_column")
async def _semantic_per_column(_payload: object) -> PhaseOutcome:
    # Now a ``SourcePhaseInput`` (identity + vertical, DAT-506).
    return PhaseOutcome(status="completed")


@activity.defn(name="detect")
async def _detect(_identity: SourceIdentity) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="promote_to_latest")
async def _promote_to_latest(_identity: SourceIdentity) -> PhaseOutcome:
    # Terminal head-flip step (DAT-413). A trivial stub here — the parent runs it
    # last, after detect; the orchestration/progress bookkeeping is what's under
    # test, not the snapshot-head write itself (that is exercised in the
    # ConnectionManager-backed phase-activity tests).
    return PhaseOutcome(status="completed")


_MOCK_ACTIVITIES = [
    _import,
    _check_column_limit,
    _typing,
    _statistics,
    _column_eligibility,
    _statistical_quality,
    _temporal,
    _semantic_per_column,
    _detect,
    _promote_to_latest,
]

_IDENTITY = SourceIdentity(workspace_id="test", session_id="sess-dat406")


def _worker(client: Client) -> Worker:
    """A worker serving both workflows + the mocked activities for this test."""
    return Worker(
        client,
        task_queue=_TASK_QUEUE,
        workflows=[AddSourceWorkflow, ProcessTableWorkflow],
        activities=_MOCK_ACTIVITIES,
        workflow_runner=make_sandboxed_runner(),
    )


@pytest.mark.asyncio
async def test_get_progress_advances_and_replays_clean(temporal_client: Client) -> None:
    """``get_progress`` advances through the phases, then the history replays clean.

    Drives a real initial run (3 children) on the dev server, polling the query
    while it runs to prove ``phase`` progresses and ``tables_completed`` climbs
    monotonically to ``tables_total``. Then feeds the completed run's history to
    an offline Replayer to prove the ``as_completed`` swap + the progress
    mutations are determinism-safe — both DAT-406 guarantees in one live run.
    """
    workflow_id = add_source_workflow_id(_IDENTITY.workspace_id, _IDENTITY.session_id)
    _check_column_limit_calls.clear()

    async with _worker(temporal_client):
        handle = await temporal_client.start_workflow(
            AddSourceWorkflow.run,
            AddSourceInput(identity=_IDENTITY, source_ids=["src-dat406"], vertical="finance"),
            id=workflow_id,
            task_queue=_TASK_QUEUE,
        )

        # Poll the query while the run is in flight. We don't try to catch every
        # phase (timing is racy) — we record every snapshot we observe and assert
        # the monotonic/ordering invariants over the trace plus the terminal
        # state. The query answering at all while the parent is mid-fan-out is
        # itself part of what's under test.
        observed: list[ProgressSnapshot] = []
        for _ in range(200):
            observed.append(await handle.query(AddSourceWorkflow.get_progress))
            if observed[-1].phase == "done":
                break
            await asyncio.sleep(0.02)

        result = await handle.result()

    assert len(result.tables) == len(_RAW_IDS)

    # The run-scoped column gate (DAT-430) actually RAN, exactly once, judging
    # the union of the run's raw tables — dropping the ``execute_activity`` call
    # from the workflow body fails here.
    assert len(_check_column_limit_calls) == 1
    assert _check_column_limit_calls[0].table_ids == _RAW_IDS

    # Terminal snapshot: every child counted, fan-out width correct, phase done.
    final = observed[-1]
    assert final.phase == "done"
    assert final.tables_total == len(_RAW_IDS)
    assert final.tables_completed == len(_RAW_IDS)

    # The per-table steps name every fanned-out child and all land "done" on a
    # clean run; a healthy run carries no failure.
    assert {t.raw_table_id for t in final.tables} == set(_RAW_IDS)
    assert all(t.status == "done" for t in final.tables)
    assert final.failure is None

    # tables_completed is monotonic non-decreasing and never overshoots the total.
    completed = [s.tables_completed for s in observed]
    assert completed == sorted(completed), f"tables_completed regressed: {completed}"
    assert all(s.tables_completed <= s.tables_total or s.tables_total == 0 for s in observed)

    # Phase only ever moves forward through the declared order.
    # ``check_column_limit`` is the run-scoped gate between the import loop and
    # the fan-out (DAT-430); ``promote`` is the terminal head-flip step
    # (DAT-413) the parent runs after detect, before done.
    order = [
        "import",
        "check_column_limit",
        "processing_tables",
        "semantic_per_column",
        "detect",
        "promote",
        "done",
    ]
    seen_indices = [order.index(s.phase) for s in observed]
    assert seen_indices == sorted(seen_indices), f"phase regressed: {[s.phase for s in observed]}"

    # --- offline determinism: replay the captured history, no server ---------
    history = await handle.fetch_history()
    replayer = Replayer(
        workflows=[AddSourceWorkflow, ProcessTableWorkflow],
        data_converter=pydantic_data_converter,
        workflow_runner=make_sandboxed_runner(),
    )
    # replay_workflow raises on any non-determinism — the as_completed swap + the
    # phase/counter mutations must reconstruct identically. The parent history is
    # what's under test; the child histories are not fetched (the parent records
    # the child results it awaited, which is all the parent's replay needs).
    await replayer.replay_workflow(history)
