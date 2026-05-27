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


@workflow.defn(name="addSourceWorkflow")
class AddSourceWorkflow:
    """E4a's trivial-but-real workflow: the two table-local de-risk phases.

    Sequential by data dependency — ``typing`` reads the raw tables ``import``
    writes. The full add_source workflow (through semantic_per_column + the
    teach signal) is E4b (DAT-368).
    """

    @workflow.run
    async def run(self, payload: PhaseActivityInput) -> list[PhaseActivityResult]:
        import_result = await workflow.execute_activity(
            "import",
            payload,
            result_type=PhaseActivityResult,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )
        typing_result = await workflow.execute_activity(
            "typing",
            payload,
            result_type=PhaseActivityResult,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )
        return [import_result, typing_result]
