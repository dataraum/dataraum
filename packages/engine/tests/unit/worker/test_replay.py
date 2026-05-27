"""Crash-replay determinism for the per-table workflows (DAT-344, P4; DAT-370).

A Temporal worker that dies mid-run is recovered by **replaying** the workflow's
event history against the workflow code when a worker picks it back up. These
tests perform exactly that replay for both workflows in the per-table topology:

* ``addSourceWorkflow`` — import, fan-out to two ``processTableWorkflow`` children
  via ``asyncio.gather``, then the ``semantic_per_column`` reduce.
* ``processTableWorkflow`` — typing, the four analytics phases, then the
  stage-level ``detect_table``.

No live server or activities are needed — the Replayer drives only workflow code,
using the recorded activity/child-workflow results from the history. The parent
replay needs only ``AddSourceWorkflow`` registered: child workflows appear in the
parent history as start/complete events, not re-executed code.

The fixtures were captured from a one-off ``WorkflowEnvironment`` run with stub
activities (``fixtures/_generate_histories.py``); regenerate them by running that
module after a chain change.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from dataraum.worker.workflows import AddSourceWorkflow, ProcessTableWorkflow

_FIXTURES = Path(__file__).parent / "fixtures"


def _replayer(*workflows: type) -> Replayer:
    return Replayer(
        workflows=list(workflows),
        data_converter=pydantic_data_converter,
        # Same passthrough as the worker — the workflow module's package import
        # chain loads duckdb's native ext, which can't be reimported in the
        # sandbox (see worker/main.py).
        #
        # ``coverage`` is test-only: under ``--cov`` on Python 3.14, coverage's
        # default sysmon core lazily imports ``coverage.env`` (which calls
        # ``platform.python_implementation()``) the first time it traces a branch
        # in the workflow. That import lands inside the sandbox and trips
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


@pytest.mark.parametrize(
    ("workflow", "fixture"),
    [
        (AddSourceWorkflow, "addsource_history.json"),
        (ProcessTableWorkflow, "processtable_history.json"),
    ],
)
async def test_workflow_replays_deterministically(workflow: type, fixture: str) -> None:
    """Replaying a completed history reaches the same final state, no non-determinism."""
    history = WorkflowHistory.from_json("replay", (_FIXTURES / fixture).read_text())
    # Raises on any non-determinism or replay mismatch; returns cleanly on a
    # faithful replay to the recorded WorkflowExecutionCompleted.
    await _replayer(workflow).replay_workflow(history)
