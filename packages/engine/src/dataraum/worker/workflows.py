"""Temporal workflows (DAT-344) — orchestration, authored in Python.

Runs in Temporal's determinism sandbox, so this module imports ONLY
``temporalio`` + the engine-free :mod:`dataraum.worker.contracts` shapes (pulled
through the sandbox via ``imports_passed_through``). It calls activities by
their registered string names (``import`` / ``typing``) — it never imports the
activity implementations, which would drag the engine into the sandbox.

The activities run on the same worker + task queue (bundled — Temporal's
recommended default; split onto a dedicated activity task queue later if heavy
phases need independent scaling).
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from dataraum.worker.contracts import PhaseActivityInput, PhaseActivityResult

# A deterministic phase failure is raised by the activity as a non-retryable
# ApplicationError of this type; transient failures (e.g. a DuckLake
# optimistic-commit conflict) raise normally and stay retryable.
_RETRY = RetryPolicy(maximum_attempts=5, non_retryable_error_types=["PhaseFailed"])
_TIMEOUT = timedelta(minutes=10)

# The slice-1 table-local chain, in dependency order. Each phase runs once over
# all of the source's tables (per-table fan-out + column batching is E4b-2,
# DAT-370). ``relationships`` + ``semantic_per_table`` are the cross-table
# slice-2 cut — deliberately absent. Activities are called by these registered
# string names; the workflow never imports their implementations.
_SLICE1_PHASES = (
    "import",
    "typing",
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
    "semantic_per_column",
)


@workflow.defn(name="addSourceWorkflow")
class AddSourceWorkflow:
    """Run the full slice-1 table-local pipeline for one source, then complete.

    Drives :data:`_SLICE1_PHASES` sequentially — the order is a valid
    topological sort of the pipeline.yaml dependencies (typing reads import's raw
    tables, statistics reads typed tables, …). Completes after
    ``semantic_per_column``; there is **no** teach wait (the in-loop
    signal/wait + typing replay is DAT-343, built on the ``typing`` activity).
    """

    @workflow.run
    async def run(self, payload: PhaseActivityInput) -> list[PhaseActivityResult]:
        results: list[PhaseActivityResult] = []
        for phase in _SLICE1_PHASES:
            result = await workflow.execute_activity(
                phase,
                payload,
                result_type=PhaseActivityResult,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
            results.append(result)
        return results
