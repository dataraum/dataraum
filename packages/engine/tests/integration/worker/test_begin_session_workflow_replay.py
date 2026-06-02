"""``BeginSessionWorkflow`` execution + offline-``Replayer`` determinism (DAT-401).

Parity with ``test_progress_query`` for the add_source spine: drive a real run on
a Temporal CLI dev-server testcontainer with the begin_session activities mocked
to trivial deterministic stubs (no engine/DB), so the test exercises ONLY the
orchestration — the sequential ``begin_session_select → relationships →
semantic_per_table`` chain and the replay gating — then feed the captured history
to an offline ``Replayer`` to prove the workflow body is determinism-safe.

Shared dev-server fixtures live in ``conftest.py``. Real end-to-end values (live
worker, real activities + LLM) are covered by compose-smoke, not here.
"""

from __future__ import annotations

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer, Worker

from dataraum.worker.contracts import (
    BeginSessionInput,
    PhaseOutcome,
    ReplayScope,
    SessionIdentity,
    SessionReplayCleanupInput,
    SessionScopedInput,
    begin_session_workflow_id,
)
from dataraum.worker.workflows import BeginSessionWorkflow
from tests.integration.worker.conftest import make_sandboxed_runner

_TASK_QUEUE = "dat401-begin-session-test"

# Ordered log of activity invocations the mocked activities append to, so the
# tests can assert the exact dispatch sequence (initial chain vs replay gating).
# A workflow-determinism-safe side effect: it lives in the ACTIVITY, not the
# workflow body, and the Replayer replays the workflow against recorded activity
# results without re-invoking the activities.
_CALLS: list[str] = []


@activity.defn(name="begin_session_select")
async def _select(_scoped: SessionScopedInput) -> PhaseOutcome:
    _CALLS.append("begin_session_select")
    return PhaseOutcome(status="completed")


@activity.defn(name="relationships")
async def _relationships(_scoped: SessionScopedInput) -> PhaseOutcome:
    _CALLS.append("relationships")
    return PhaseOutcome(status="completed")


@activity.defn(name="semantic_per_table")
async def _semantic_per_table(_scoped: SessionScopedInput) -> PhaseOutcome:
    _CALLS.append("semantic_per_table")
    return PhaseOutcome(status="completed")


@activity.defn(name="session_replay_cleanup_for_phase")
async def _cleanup(payload: SessionReplayCleanupInput) -> PhaseOutcome:
    _CALLS.append(f"cleanup:{payload.phase_name}")
    return PhaseOutcome(status="completed")


_MOCK_ACTIVITIES = [_select, _relationships, _semantic_per_table, _cleanup]


def _worker(client: Client) -> Worker:
    return Worker(
        client,
        task_queue=_TASK_QUEUE,
        workflows=[BeginSessionWorkflow],
        activities=_MOCK_ACTIVITIES,
        workflow_runner=make_sandboxed_runner(),
    )


async def _replay(handle: object) -> None:
    """Offline determinism: replay the captured history with no server."""
    history = await handle.fetch_history()  # type: ignore[attr-defined]
    replayer = Replayer(
        workflows=[BeginSessionWorkflow],
        data_converter=pydantic_data_converter,
        workflow_runner=make_sandboxed_runner(),
    )
    # Raises on any non-determinism — the chain + gating must reconstruct identically.
    await replayer.replay_workflow(history)


@pytest.mark.asyncio
async def test_initial_run_runs_chain_and_replays_clean(temporal_client: Client) -> None:
    """Initial run dispatches select → relationships → semantic_per_table, replays clean."""
    _CALLS.clear()
    identity = SessionIdentity(workspace_id="test", session_id="sess-dat401")
    wf_id = begin_session_workflow_id(identity.workspace_id, identity.session_id)

    async with _worker(temporal_client):
        handle = await temporal_client.start_workflow(
            BeginSessionWorkflow.run,
            BeginSessionInput(identity=identity, tables=["t1", "t2", "t3"]),
            id=wf_id,
            task_queue=_TASK_QUEUE,
        )
        result = await handle.result()

    assert result.session_id == identity.session_id
    assert result.table_ids == ["t1", "t2", "t3"]
    # Sequential chain, no cleanup on the initial run.
    assert _CALLS == ["begin_session_select", "relationships", "semantic_per_table"]

    await _replay(handle)


@pytest.mark.asyncio
async def test_replay_from_semantic_skips_relationships_and_replays_clean(
    temporal_client: Client,
) -> None:
    """A replay from ``semantic_per_table`` skips relationships, cleans then re-runs it.

    Proves the begin_session replay gating in a real run: ``begin_session_select``
    always runs (scope setup); ``relationships`` is before ``from_phase`` so it is
    skipped; ``semantic_per_table`` self-cleans (``session_replay_cleanup_for_phase``)
    then re-runs. History then replays deterministically.
    """
    _CALLS.clear()
    identity = SessionIdentity(workspace_id="test", session_id="sess-dat401-replay")
    wf_id = begin_session_workflow_id(identity.workspace_id, identity.session_id)

    async with _worker(temporal_client):
        handle = await temporal_client.start_workflow(
            BeginSessionWorkflow.run,
            BeginSessionInput(
                identity=identity,
                tables=["t1", "t2"],
                replay=ReplayScope(from_phase="semantic_per_table"),
            ),
            id=wf_id,
            task_queue=_TASK_QUEUE,
        )
        await handle.result()

    assert _CALLS == [
        "begin_session_select",
        "cleanup:semantic_per_table",
        "semantic_per_table",
    ]

    await _replay(handle)
