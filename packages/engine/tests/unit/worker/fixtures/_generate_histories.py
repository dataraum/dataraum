"""Regenerate the replay fixtures for the per-table workflows (DAT-370).

One-off generator (NOT a test): runs ``AddSourceWorkflow`` under
``WorkflowEnvironment.start_time_skipping()`` with stub activities, so the import
stub returns two raw ids and the parent fans out two ``ProcessTableWorkflow``
children. Captures the parent history (``addsource_history.json``) and one
child's history (``processtable_history.json``) — the inputs ``test_replay.py``
feeds through the ``Replayer`` to prove determinism offline.

Run from the engine package root:

    uv run python tests/unit/worker/fixtures/_generate_histories.py
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from temporalio import activity
from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from dataraum.worker.contracts import (
    AddSourceInput,
    ImportResult,
    PhaseOutcome,
    ProcessTableInput,
    SourceIdentity,
    TableScopedInput,
    TypingResult,
)
from dataraum.worker.workflows import AddSourceWorkflow, ProcessTableWorkflow

_HERE = Path(__file__).parent
_SOURCE_ID = "gen-src"
_RAW_IDS = ["raw-1", "raw-2"]
_TASK_QUEUE = "history-gen"


@activity.defn(name="import")
async def stub_import(identity: SourceIdentity) -> ImportResult:
    return ImportResult(raw_table_ids=_RAW_IDS)


@activity.defn(name="typing")
async def stub_typing(payload: ProcessTableInput) -> TypingResult:
    return TypingResult(typed_table_id=f"typed-{payload.raw_table_id}")


@activity.defn(name="statistics")
async def stub_statistics(payload: TableScopedInput) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="column_eligibility")
async def stub_column_eligibility(payload: TableScopedInput) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="statistical_quality")
async def stub_statistical_quality(payload: TableScopedInput) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="temporal")
async def stub_temporal(payload: TableScopedInput) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="detect_table")
async def stub_detect_table(payload: TableScopedInput) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="semantic_per_column")
async def stub_semantic_per_column(identity: SourceIdentity) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


@activity.defn(name="detect_source")
async def stub_detect_source(identity: SourceIdentity) -> PhaseOutcome:
    return PhaseOutcome(status="completed")


async def main() -> None:
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    ) as env:
        async with Worker(
            env.client,
            task_queue=_TASK_QUEUE,
            workflows=[AddSourceWorkflow, ProcessTableWorkflow],
            activities=[
                stub_import,
                stub_typing,
                stub_statistics,
                stub_column_eligibility,
                stub_statistical_quality,
                stub_temporal,
                stub_detect_table,
                stub_semantic_per_column,
                stub_detect_source,
            ],
            workflow_runner=SandboxedWorkflowRunner(
                restrictions=SandboxRestrictions.default.with_passthrough_modules(
                    "dataraum", "pydantic", "pydantic_core"
                )
            ),
        ):
            identity = SourceIdentity(
                workspace_id="gen-ws", source_id=_SOURCE_ID, session_id="gen-session"
            )
            handle = await env.client.start_workflow(
                AddSourceWorkflow.run,
                AddSourceInput(identity=identity),
                id=f"addsource-{_SOURCE_ID}",
                task_queue=_TASK_QUEUE,
            )
            await handle.result()

            parent_history = await handle.fetch_history()
            child_handle = env.client.get_workflow_handle(
                f"addsource-{_SOURCE_ID}-table-{_RAW_IDS[0]}"
            )
            child_history = await child_handle.fetch_history()

    _write(_HERE / "addsource_history.json", parent_history)
    _write(_HERE / "processtable_history.json", child_history)
    print("wrote addsource_history.json + processtable_history.json")


def _write(path: Path, history: WorkflowHistory) -> None:
    path.write_text(json.dumps(history.to_json_dict(), indent=2) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
