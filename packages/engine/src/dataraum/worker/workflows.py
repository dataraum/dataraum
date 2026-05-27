"""Temporal workflows (DAT-344; per-table fan-out DAT-370) — orchestration in Python.

Runs in Temporal's determinism sandbox, so this module imports ONLY
``temporalio`` + ``asyncio`` + the engine-free :mod:`dataraum.worker.contracts`
shapes (pulled through the sandbox via ``imports_passed_through``). It calls
activities by their registered string names — it never imports the activity
implementations, which would drag the engine into the sandbox.

Topology (DAT-370): the table is the unit of work.

    AddSourceWorkflow(identity)                              [parent]
      import()                  -> raw table ids             (source-level enumerator)
      fan-out via asyncio.gather:
        ProcessTableWorkflow(raw_id) for each raw id         [child, per table]
      semantic_per_column()                                  (source-level reduce)

    ProcessTableWorkflow(raw_table_id)                       [child]
      typing(raw_id) -> typed_id
      statistics -> column_eligibility -> statistical_quality -> temporal   (typed_id)
      detect_table(typed_id)                                 (stage-level detectors)

The child gives per-table history isolation + bounded parent history, and
``typed_id`` is threaded through the child's messages (persisted in history,
replayed verbatim). Detectors run once at the tail of the stage, scoped to the
child's typed table — not per phase (DAT-370).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from dataraum.worker.contracts import (
        AddSourceInput,
        AddSourceResult,
        ImportResult,
        PhaseOutcome,
        ProcessTableInput,
        ProcessTableResult,
        TableScopedInput,
        TypingResult,
    )

# A deterministic phase failure is raised by the activity as a non-retryable
# ApplicationError of this type; transient failures (e.g. a DuckLake
# optimistic-commit conflict) raise normally and stay retryable.
_RETRY = RetryPolicy(maximum_attempts=5, non_retryable_error_types=["PhaseFailed"])
_TIMEOUT = timedelta(minutes=10)

# The table-local analytics phases, in dependency order. ``typing`` precedes
# them (it mints the typed id); ``detect_table`` follows (stage-level detectors).
# The detect step aggregates detectors over ``activity._TABLE_LOCAL_PHASES`` =
# ``("typing", *_ANALYTICS_PHASES)``; ``test_phase_constants.py`` pins the link.
_ANALYTICS_PHASES = (
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
)


@workflow.defn(name="processTableWorkflow")
class ProcessTableWorkflow:
    """Run the table-local chain for one raw table, then complete.

    ``typing`` mints the typed id; the analytics phases run scoped to it; a
    single stage-level ``detect_table`` runs the table-local detectors scoped to
    the same typed table. The typed id travels in the activity results, so it is
    in history and replayed verbatim — never recomputed.
    """

    @workflow.run
    async def run(self, payload: ProcessTableInput) -> ProcessTableResult:
        typing = await workflow.execute_activity(
            "typing",
            payload,
            result_type=TypingResult,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )
        scoped = TableScopedInput(identity=payload.identity, table_id=typing.typed_table_id)
        for phase in _ANALYTICS_PHASES:
            await workflow.execute_activity(
                phase,
                scoped,
                result_type=PhaseOutcome,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
        await workflow.execute_activity(
            "detect_table",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )
        return ProcessTableResult(
            raw_table_id=payload.raw_table_id,
            typed_table_id=typing.typed_table_id,
        )


@workflow.defn(name="addSourceWorkflow")
class AddSourceWorkflow:
    """Import one source, fan out a child workflow per raw table, then reduce.

    ``import`` enumerates the source's raw tables (the table set is unknown until
    it runs); the workflow fans out one :class:`ProcessTableWorkflow` per raw id
    via ``asyncio.gather`` and waits for all to complete; then
    ``semantic_per_column`` runs once as the source-level reduce. The data-
    dependent fan-out is driven off ``import``'s recorded result, so replay is
    deterministic. There is **no** teach wait (that is DAT-343).
    """

    @workflow.run
    async def run(self, payload: AddSourceInput) -> AddSourceResult:
        imported = await workflow.execute_activity(
            "import",
            payload.identity,
            result_type=ImportResult,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Per-table fan-out. Deterministic, collision-free child ids keep replay
        # stable; ParentClosePolicy is TERMINATE (execute_child_workflow's own
        # default) — the parent always gathers its children to completion, so
        # there are no orphans in the happy path. On a permanent child failure
        # gather propagates, the parent ends, and Temporal terminates the
        # in-flight siblings; a parent retry resumes each child idempotently via
        # the phases' should_skip (each phase commits atomically in one
        # session_scope, so there are no partial-write rows to confuse it).
        children = [
            workflow.execute_child_workflow(
                ProcessTableWorkflow.run,
                ProcessTableInput(identity=payload.identity, raw_table_id=raw_id),
                id=f"addsource-{payload.identity.source_id}-table-{raw_id}",
            )
            for raw_id in imported.raw_table_ids
        ]
        tables: list[ProcessTableResult] = list(await asyncio.gather(*children))

        # Source-level reduce: ontology induction is source-global, so it runs
        # once over all tables after the fan-out (moves to the frame workflow
        # later; not a blocker here).
        await workflow.execute_activity(
            "semantic_per_column",
            payload.identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Source-level detect step: run semantic_per_column's declared detectors
        # once after the reduce (the table-local detectors already ran per child
        # in detect_table). Without this the source-level detectors are declared
        # but never execute.
        await workflow.execute_activity(
            "detect_source",
            payload.identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        return AddSourceResult(raw_table_ids=imported.raw_table_ids, tables=tables)
