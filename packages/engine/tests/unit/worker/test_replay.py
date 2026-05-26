"""Crash-replay determinism for addSourceWorkflow (DAT-344, P4).

A Temporal worker that dies mid-run is recovered by **replaying** the workflow's
event history against the workflow code when a worker picks it back up. This
test performs exactly that replay: it feeds a recorded ``addSourceWorkflow``
history (``import`` + ``typing`` both completed) through the ``Replayer`` and
asserts the workflow code replays to the same final state with no
non-determinism.

No live server or activities are needed — the Replayer drives only the workflow
code, using the recorded activity results from the history. The history fixture
was captured from a real run via ``temporal workflow show -o json``; regenerate
it the same way if the workflow's activity sequence changes.
"""

from __future__ import annotations

from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from dataraum.worker.workflows import AddSourceWorkflow

_HISTORY = Path(__file__).parent / "fixtures" / "addsource_history.json"


async def test_addsource_workflow_replays_deterministically() -> None:
    """Replaying a completed addSourceWorkflow history reaches the same final state."""
    history = WorkflowHistory.from_json("addsource-replay", _HISTORY.read_text())

    replayer = Replayer(
        workflows=[AddSourceWorkflow],
        data_converter=pydantic_data_converter,
        # Same passthrough as the worker — the workflow module's package import
        # chain loads duckdb's native ext, which can't be reimported in the
        # sandbox (see worker/main.py).
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                "dataraum", "pydantic", "pydantic_core"
            )
        ),
    )

    # Raises on any non-determinism or replay mismatch; returns cleanly on a
    # faithful replay to the recorded WorkflowExecutionCompleted.
    await replayer.replay_workflow(history)
