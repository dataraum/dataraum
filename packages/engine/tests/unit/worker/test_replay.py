"""Crash-replay determinism for addSourceWorkflow (DAT-344, P4).

A Temporal worker that dies mid-run is recovered by **replaying** the workflow's
event history against the workflow code when a worker picks it back up. This
test performs exactly that replay: it feeds a recorded ``addSourceWorkflow``
history (all seven slice-1 phases completed) through the ``Replayer`` and
asserts the workflow code replays to the same final state with no
non-determinism.

No live server or activities are needed — the Replayer drives only the workflow
code, using the recorded activity results from the history. The fixture was
captured from a one-off ``WorkflowEnvironment`` run with mock activities (a
mock-activity history replays faithfully against the real workflow, since the
Replayer drives only workflow code). To regenerate after a chain change: run
``AddSourceWorkflow`` under ``WorkflowEnvironment.start_time_skipping()`` with
stub activities and write ``handle.fetch_history().to_json_dict()`` here.
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
        #
        # ``coverage`` is test-only: under ``--cov`` on Python 3.14, coverage's
        # default sysmon core lazily imports ``coverage.env`` (which calls
        # ``platform.python_implementation()``) the first time it traces a
        # branch in the workflow. That import lands inside the sandbox and trips
        # RestrictedWorkflowAccessError. Passing it through keeps coverage's own
        # instrumentation out of the sandbox; coverage is not part of the
        # workflow's determinism contract, so the replay check is unaffected.
        # The production worker does not pass it through (no coverage at runtime).
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                "dataraum", "pydantic", "pydantic_core", "coverage"
            )
        ),
    )

    # Raises on any non-determinism or replay mismatch; returns cleanly on a
    # faithful replay to the recorded WorkflowExecutionCompleted.
    await replayer.replay_workflow(history)
