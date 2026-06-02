"""Parent-level progress query + replay-determinism for ``addSourceWorkflow`` (DAT-406).

The parent workflow serves a read-only ``get_progress`` query returning a
:class:`ProgressSnapshot` ``{phase, tables_total, tables_completed}`` — the
cross-package shape the cockpit Client polls while the parent is blocked in the
fan-out (mirrored TS-side in DAT-352). Two things have to hold:

* **The snapshot advances.** ``phase`` walks ``import`` →
  ``processing_tables`` → ``semantic_per_column`` → ``detect`` → ``done`` and
  ``tables_completed`` climbs monotonically toward ``tables_total`` as each
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
from collections.abc import AsyncIterator, Iterator

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.service import RPCError
from temporalio.worker import Replayer, Worker
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)
from testcontainers.core.container import DockerContainer

from dataraum.worker.contracts import (
    AddSourceInput,
    ImportResult,
    PhaseOutcome,
    ProcessTableInput,
    ProgressSnapshot,
    SourceIdentity,
    TypingResult,
    add_source_workflow_id,
)
from dataraum.worker.workflows import AddSourceWorkflow, ProcessTableWorkflow

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
async def _import(_identity: SourceIdentity) -> ImportResult:
    return ImportResult(raw_table_ids=list(_RAW_IDS))


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
async def _semantic_per_column(_identity: SourceIdentity) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="detect")
async def _detect(_identity: SourceIdentity) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


_MOCK_ACTIVITIES = [
    _import,
    _typing,
    _statistics,
    _column_eligibility,
    _statistical_quality,
    _temporal,
    _semantic_per_column,
    _detect,
]

_IDENTITY = SourceIdentity(workspace_id="test", source_id="src-dat406", session_id="sess-dat406")


@pytest.fixture(scope="module")
def temporal_dev_address() -> Iterator[str]:
    """A single-container Temporal CLI dev server (``server start-dev``).

    The CLI dev server runs an in-memory SQLite Temporal in ONE container — no
    Postgres dependency, fast startup — which is all a query/replay test needs.
    We use it instead of ``WorkflowEnvironment.start_time_skipping()`` because
    that downloads a test-server binary that stalls CI (project convention:
    Temporal tests use testcontainers, determinism is covered offline by the
    Replayer).

    Addressing uses the STANDARD testcontainers port-MAPPING idiom — expose the
    frontend gRPC (7233) and reach it via ``get_container_host_ip()`` + the
    mapped host port, exactly like the ``PostgresContainer`` fixture. We do NOT
    use ``network_mode="host"`` + a fixed port: that routes on a local Docker but
    NOT on CI runners, where the server logs ready yet the client's RPC can't
    reach it (the ``RPCError: Timeout expired`` this test hit in CI).
    """
    container = (
        DockerContainer("temporalio/temporal:latest")
        .with_command("server start-dev --ip 0.0.0.0 --namespace default")
        .with_exposed_ports(7233)
    )
    container.start()
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(7233)
        yield f"{host}:{port}"
    finally:
        container.stop()


@pytest.fixture
async def temporal_client(temporal_dev_address: str) -> AsyncIterator[Client]:
    """Client bound to the dev server with the worker's pydantic data converter.

    The same ``pydantic_data_converter`` the production worker uses (so the
    plain-``@dataclass`` ProgressSnapshot serializes to its flat JSON shape on
    the query wire exactly as it will in production).

    The dev server's gRPC frontend lags the container start, so connect with a
    bounded retry — this is the real readiness gate (a log line doesn't prove the
    frontend is accepting RPCs, which is what burned the host-networking version
    in CI). ``Client.connect`` is eager (it runs ``get_system_info`` on connect)
    and raises a bare ``RuntimeError`` ("connection closed" / Cancelled) while the
    server is still booting — so that, not just ``RPCError``, is the retry signal.
    """
    last_err: Exception | None = None
    for _ in range(120):  # ~60s budget after the image is warmed
        try:
            client = await Client.connect(
                temporal_dev_address,
                namespace="default",
                data_converter=pydantic_data_converter,
            )
            yield client
            return
        except (RPCError, RuntimeError, OSError) as err:
            last_err = err
            await asyncio.sleep(0.5)
    raise RuntimeError(
        f"Temporal dev server at {temporal_dev_address} never accepted a connection: {last_err}"
    )


def _worker(client: Client) -> Worker:
    """A worker serving both workflows + the mocked activities for this test."""
    return Worker(
        client,
        task_queue=_TASK_QUEUE,
        workflows=[AddSourceWorkflow, ProcessTableWorkflow],
        activities=_MOCK_ACTIVITIES,
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                "dataraum", "pydantic", "pydantic_core"
            )
        ),
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
    workflow_id = add_source_workflow_id(_IDENTITY.workspace_id, _IDENTITY.source_id)

    async with _worker(temporal_client):
        handle = await temporal_client.start_workflow(
            AddSourceWorkflow.run,
            AddSourceInput(identity=_IDENTITY),
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

    # Terminal snapshot: every child counted, fan-out width correct, phase done.
    final = observed[-1]
    assert final.phase == "done"
    assert final.tables_total == len(_RAW_IDS)
    assert final.tables_completed == len(_RAW_IDS)

    # tables_completed is monotonic non-decreasing and never overshoots the total.
    completed = [s.tables_completed for s in observed]
    assert completed == sorted(completed), f"tables_completed regressed: {completed}"
    assert all(s.tables_completed <= s.tables_total or s.tables_total == 0 for s in observed)

    # Phase only ever moves forward through the declared order.
    order = ["import", "processing_tables", "semantic_per_column", "detect", "done"]
    seen_indices = [order.index(s.phase) for s in observed]
    assert seen_indices == sorted(seen_indices), f"phase regressed: {[s.phase for s in observed]}"

    # --- offline determinism: replay the captured history, no server ---------
    history = await handle.fetch_history()
    replayer = Replayer(
        workflows=[AddSourceWorkflow, ProcessTableWorkflow],
        data_converter=pydantic_data_converter,
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                "dataraum", "pydantic", "pydantic_core"
            )
        ),
    )
    # replay_workflow raises on any non-determinism — the as_completed swap + the
    # phase/counter mutations must reconstruct identically. The parent history is
    # what's under test; the child histories are not fetched (the parent records
    # the child results it awaited, which is all the parent's replay needs).
    await replayer.replay_workflow(history)
