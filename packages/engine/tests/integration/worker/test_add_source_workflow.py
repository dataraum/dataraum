"""addSourceWorkflow end-to-end under Temporal's test environment (DAT-368).

Runs the real :class:`AddSourceWorkflow` against **mock** activities (no DB /
lake / LLM) in a time-skipping ``WorkflowEnvironment``, asserting it drives all
seven slice-1 phases in dependency order and completes. This is the workflow
counterpart to ``test_phase_activity.py`` (which validates the activities
against the real substrate): here the activities are stubbed so the test
isolates the *orchestration* — the chain shape, sequencing, and completion.

The Replayer determinism fixture (``tests/unit/worker/fixtures/
addsource_history.json``, consumed by ``test_replay.py``) was captured from this
exact mock-activity run — Replayer drives only workflow code against recorded
activity results, so a mock-activity history replays faithfully against the real
workflow. Regenerate it the same way if the chain changes:

    uv run python - <<'PY'
    # (see git history of this file for the one-off capture script)
    PY
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from dataraum.worker.contracts import PhaseActivityInput, PhaseActivityResult
from dataraum.worker.workflows import _SLICE1_PHASES, AddSourceWorkflow

pytestmark = pytest.mark.integration

_TASK_QUEUE = "test-add-source-workflow"


def _mock_phase_activity(name: str):  # noqa: ANN202
    """A stub activity registered under ``name`` that returns a completed result."""

    @activity.defn(name=name)
    async def _act(payload: PhaseActivityInput) -> PhaseActivityResult:
        return PhaseActivityResult(phase=name, status="completed", summary=f"mock {name}")

    _act.__name__ = f"mock_{name}"
    return _act


async def test_add_source_workflow_runs_full_slice1_chain() -> None:
    """The workflow drives all seven slice-1 phases in order and completes."""
    mock_activities = [_mock_phase_activity(name) for name in _SLICE1_PHASES]

    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    ) as env:
        async with Worker(
            env.client,
            task_queue=_TASK_QUEUE,
            workflows=[AddSourceWorkflow],
            activities=mock_activities,
            # Same sandbox passthrough as the production worker (worker/main.py):
            # the workflow module's package import chain loads duckdb's native
            # ext, which can't be reimported inside the sandbox.
            workflow_runner=SandboxedWorkflowRunner(
                restrictions=SandboxRestrictions.default.with_passthrough_modules(
                    "dataraum", "pydantic", "pydantic_core"
                )
            ),
        ):
            payload = PhaseActivityInput(
                workspace_id="test",
                source_id=str(uuid4()),
                session_id=str(uuid4()),
            )
            results = await env.client.execute_workflow(
                AddSourceWorkflow.run,
                payload,
                id=f"add-source-{uuid4()}",
                task_queue=_TASK_QUEUE,
            )

    assert [r.phase for r in results] == list(_SLICE1_PHASES)
    assert all(r.status == "completed" for r in results)
