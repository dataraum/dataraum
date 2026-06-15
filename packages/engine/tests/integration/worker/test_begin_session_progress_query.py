"""Progress query + replay-determinism for ``beginSessionWorkflow`` (DAT-435).

The session workflow serves the SAME read-only ``get_progress`` query and
:class:`ProgressSnapshot` shape as ``addSourceWorkflow`` (DAT-406), so the
cockpit polls both through one seam. Three things have to hold:

* **The snapshot advances.** ``phase`` walks the sequential session chain
  (``begin_session_select`` → … → ``session_promote_to_latest`` → ``done``)
  in order, and the fan-out fields stay at their empty defaults (no children).
  Driven on a real Temporal **dev-server testcontainer** (NOT
  ``WorkflowEnvironment`` — project convention) with every activity mocked to
  a trivial deterministic stub, so no engine/DB is dragged in.

* **A failure is stamped.** A stage that raises lands in
  ``snapshot.failure`` with the root-cause message + the phase in flight
  (the same ``_failure_message`` unwrap as add_source), readable off the
  CLOSED run — that is what the cockpit's failure alert renders.

* **It replays clean.** The ``self._progress`` mutations only change state
  between awaited, history-recorded activity completions, so the offline
  :class:`Replayer` reconstructs both the clean and the failed run with no
  non-determinism error.

Real end-to-end values (live worker, real activities) are covered by
compose-smoke, not here.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, Worker

from dataraum.worker.contracts import (
    BeginSessionInput,
    PhaseOutcome,
    ProgressSnapshot,
)
from dataraum.worker.workflows import BeginSessionWorkflow
from tests.integration.worker.conftest import make_sandboxed_runner

_TASK_QUEUE = "dat435-progress-test"
_TABLE_IDS = ["typed-a", "typed-b"]
# Parent workflow ids are cockpit-owned (DAT-506); the test names its own.
_WORKSPACE_ID = "test"

# The sequential session chain in execution order — must match the workflow
# body (workflows.py: select, _SESSION_PHASE_ORDER, the overlay/views stages,
# _SESSION_VALUE_PHASE_ORDER, then the detect/keepers/promote tail). The
# ordering assertion below walks exactly this list.
_PHASE_ORDER = [
    "begin_session_select",
    "relationships",
    "semantic_per_table",
    "session_materialize_overlays",
    "enriched_views",
    "slicing",
    "slicing_view",
    "slice_analysis",
    "temporal_slice_analysis",
    "aggregation_lineage",
    "correlations",
    "session_detect",
    "session_write_keepers",
    "session_promote_to_latest",
]

_StubFn = Callable[[object], Coroutine[object, object, PhaseOutcome]]


def _phase_stub(name: str) -> _StubFn:
    """A trivial deterministic stub registered under one production activity name.

    Returns the right contract shape and touches no engine/DB, so the test
    exercises ONLY the orchestration + progress bookkeeping. The short sleep
    staggers stage completions so the polling loop genuinely observes
    intermediate phases (rather than the whole chain landing in one tick).
    """

    @activity.defn(name=name)
    async def _stub(_payload: object) -> PhaseOutcome:
        await asyncio.sleep(0.01)
        return PhaseOutcome(status="completed")

    return _stub


def _failing_stub(name: str, message: str) -> _StubFn:
    """A stub that fails its stage with a non-retryable phase failure."""

    @activity.defn(name=name)
    async def _stub(_payload: object) -> PhaseOutcome:
        raise ApplicationError(message, type="PhaseFailed", non_retryable=True)

    return _stub


def _worker(client: Client, activities: list[_StubFn]) -> Worker:
    """A worker serving the session workflow + the given activity stubs."""
    return Worker(
        client,
        task_queue=_TASK_QUEUE,
        workflows=[BeginSessionWorkflow],
        activities=activities,
        workflow_runner=make_sandboxed_runner(),
    )


async def _replay(handle_history: object) -> None:
    """Offline determinism: replay a captured history, no server."""
    replayer = Replayer(
        workflows=[BeginSessionWorkflow],
        data_converter=pydantic_data_converter,
        workflow_runner=make_sandboxed_runner(),
    )
    await replayer.replay_workflow(handle_history)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_get_progress_advances_and_replays_clean(temporal_client: Client) -> None:
    """``get_progress`` walks the session chain in order, then replays clean.

    Drives a real run on the dev server, polling the query while it runs. We
    don't try to catch every phase (timing is racy) — we record every snapshot
    we observe and assert the ordering invariants over the trace plus the
    terminal state. The query answering at all while a stage is awaited is
    itself part of what's under test.
    """
    workflow_id = "beginsession-test-dat435-ok"
    stubs = [_phase_stub(name) for name in _PHASE_ORDER]

    async with _worker(temporal_client, stubs):
        handle = await temporal_client.start_workflow(
            BeginSessionWorkflow.run,
            BeginSessionInput(workspace_id=_WORKSPACE_ID, tables=_TABLE_IDS, verticals=["finance"]),
            id=workflow_id,
            task_queue=_TASK_QUEUE,
        )

        observed: list[ProgressSnapshot] = []
        for _ in range(400):
            observed.append(await handle.query(BeginSessionWorkflow.get_progress))
            if observed[-1].phase == "done":
                break
            await asyncio.sleep(0.02)

        result = await handle.result()

    # The result carries the workflow-minted run_id (no session_id, DAT-506).
    assert result.run_id
    assert result.table_ids == _TABLE_IDS

    # Terminal snapshot: chain finished, healthy, and the fan-out fields stayed
    # at their empty defaults (sequential workflow, no children).
    final = observed[-1]
    assert final.phase == "done"
    assert final.failure is None
    assert all(s.tables_total == 0 and s.tables_completed == 0 and s.tables == [] for s in observed)

    # Phase only ever moves forward through the declared session order.
    order = [*_PHASE_ORDER, "done"]
    seen_indices = [order.index(s.phase) for s in observed]
    assert seen_indices == sorted(seen_indices), f"phase regressed: {[s.phase for s in observed]}"

    history = await handle.fetch_history()
    await _replay(history)


@pytest.mark.asyncio
async def test_failure_is_stamped_with_phase_and_replays_clean(
    temporal_client: Client,
) -> None:
    """A failing stage stamps ``failure`` (message + phase), readable off the closed run.

    ``enriched_views`` fails mid-chain; the run ends FAILED and the final
    snapshot — queried AFTER the run closed, which Temporal answers by
    replaying history on the worker — carries the root-cause message and the
    phase in flight. ``table_id`` stays ``None`` (no table-scoped stages in the
    session chain). The failed history must also replay clean: the failure
    stamping in the ``run`` wrapper is a workflow-state mutation like any other.
    """
    workflow_id = "beginsession-test-dat435-fail"
    stubs = [
        _failing_stub(name, "enriched views exploded")
        if name == "enriched_views"
        else _phase_stub(name)
        for name in _PHASE_ORDER
    ]

    async with _worker(temporal_client, stubs):
        handle = await temporal_client.start_workflow(
            BeginSessionWorkflow.run,
            BeginSessionInput(workspace_id=_WORKSPACE_ID, tables=_TABLE_IDS, verticals=["finance"]),
            id=workflow_id,
            task_queue=_TASK_QUEUE,
        )

        with pytest.raises(WorkflowFailureError):
            await handle.result()

        final = await handle.query(BeginSessionWorkflow.get_progress)
        history = await handle.fetch_history()

    assert final.phase == "enriched_views"
    assert final.failure is not None
    assert final.failure.phase == "enriched_views"
    assert "enriched views exploded" in final.failure.message
    assert final.failure.table_id is None

    await _replay(history)
