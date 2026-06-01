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
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from dataraum.worker.contracts import (
        AddSourceInput,
        AddSourceResult,
        ImportResult,
        PhaseOutcome,
        ProcessTableInput,
        ProcessTableResult,
        ReplayCleanupInput,
        ReplayScope,
        SourceIdentity,
        TableScopedInput,
        TypingResult,
        process_table_workflow_id,
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

# Phase order in the parent + child chains; ``_at_or_before(from_phase, p)``
# decides whether to run ``p`` on a replay. Parent stages are the ones the
# parent body invokes (`import` + the source-level reduce); child stages are
# the ones the child body invokes. ``ReplayScope.from_phase`` always names a
# phase listed in one of these tuples — anything else is a programmer error
# on the cockpit side.
_PARENT_PHASE_ORDER = ("import", "semantic_per_column")
_CHILD_PHASE_ORDER = ("typing", *_ANALYTICS_PHASES, "detect_table")


def _at_or_after(phase: str, from_phase: str, order: tuple[str, ...]) -> bool:
    """Return True if ``phase`` runs in a replay starting at ``from_phase``.

    The replay runs ``from_phase`` and everything after it in ``order``;
    everything before is skipped. Either ``phase`` or ``from_phase`` not in
    ``order`` means "this phase doesn't live in the same chain" — return
    False (skip the phase). Pure logic over workflow input, so deterministic.
    """
    if phase not in order or from_phase not in order:
        return False
    return order.index(phase) >= order.index(from_phase)


def _runs_under(phase: str, replay: ReplayScope | None, order: tuple[str, ...]) -> bool:
    """True iff ``phase`` should execute given ``replay`` and its chain ``order``.

    Initial run (``replay is None``) runs every phase. A replay runs only
    the phases at or after ``replay.from_phase`` in ``order``.
    """
    if replay is None:
        return True
    return _at_or_after(phase, replay.from_phase, order)


# All phase names valid as ``ReplayScope.from_phase``. A value outside this
# set is a programmer error on the cockpit side — without this guard a typo
# would silently skip every phase in both chains (since ``_at_or_after``
# returns False for an unknown ``from_phase``) and produce a partial replay
# with no error. ``_PARENT_PHASE_ORDER`` ∪ ``_CHILD_PHASE_ORDER`` is the
# closed set both workflows recognise.
_VALID_REPLAY_PHASES: frozenset[str] = frozenset(_PARENT_PHASE_ORDER) | frozenset(
    _CHILD_PHASE_ORDER
)


def _validate_replay(replay: ReplayScope | None) -> None:
    """Refuse a replay whose ``from_phase`` isn't a known chain phase.

    Pure function over workflow input → deterministic. Raises non-retryable
    so the workflow fails loud on the first attempt; no silent partial replay.
    """
    if replay is None or replay.from_phase in _VALID_REPLAY_PHASES:
        return
    raise ApplicationError(
        f"Unknown replay.from_phase '{replay.from_phase}'. "
        f"Valid values: {sorted(_VALID_REPLAY_PHASES)}",
        type="PhaseFailed",
        non_retryable=True,
    )


async def _maybe_replay_cleanup(
    phase: str,
    replay: ReplayScope | None,
    identity: SourceIdentity,
    table_ids: list[str],
) -> None:
    """Invoke ``replay_cleanup_for_phase`` for every phase that re-runs on a replay.

    Each chain phase owns its outputs and clears them in place before the
    re-run, so its existing ``should_skip`` doesn't bail (DAT-373). Pre-DAT-373
    only the entry phase (``replay.from_phase``) was cleaned and everything
    downstream rode on the entry phase's cascade-delete through the dropped
    typed ``Table``. With stable typed identity that cascade is gone, so each
    phase at-or-after ``from_phase`` cleans its own per-Column rows (scoped to
    ``table_ids``) — owner-scoped, never cross-stage.

    On the initial run (``replay is None``), nothing to clean. The phase order
    used to gate this is the chain ``phase`` belongs to; ``_runs_under`` (which
    the caller already used to decide whether ``phase`` runs) and this share the
    same at-or-after predicate, so cleanup fires exactly for the phases that
    re-execute.
    """
    if replay is None or not _phase_reruns_on_replay(phase, replay):
        return
    await workflow.execute_activity(
        "replay_cleanup_for_phase",
        ReplayCleanupInput(
            identity=identity,
            phase_name=phase,
            table_ids=list(table_ids),
        ),
        result_type=PhaseOutcome,
        start_to_close_timeout=_TIMEOUT,
        retry_policy=_RETRY,
    )


def _phase_reruns_on_replay(phase: str, replay: ReplayScope) -> bool:
    """True iff ``phase`` re-executes under ``replay`` (so it must self-clean).

    A phase re-runs when it is at-or-after ``from_phase`` in whichever chain it
    belongs to. ``semantic_per_column`` is the exception: the parent body always
    re-runs the source-level reduce on ANY replay (the "widening breadth" rule),
    so it must always self-clean too. Pure logic over workflow input →
    deterministic.
    """
    if phase == "semantic_per_column":
        return True
    if phase in _CHILD_PHASE_ORDER:
        return _at_or_after(phase, replay.from_phase, _CHILD_PHASE_ORDER)
    return _at_or_after(phase, replay.from_phase, _PARENT_PHASE_ORDER)


@workflow.defn(name="processTableWorkflow")
class ProcessTableWorkflow:
    """Run the table-local chain for one raw table, then complete.

    Initial run (``payload.replay is None``): ``typing`` mints the typed id,
    the analytics phases run scoped to it, a single stage-level
    ``detect_table`` runs the table-local detectors. The typed id travels in
    the activity results, so it is in history and replayed verbatim.

    Teach replay (``payload.replay`` set; DAT-343): gates each phase on
    ``replay.from_phase``'s position in ``_CHILD_PHASE_ORDER``; if the chain
    starts past ``typing``, the typed id comes from ``lookup_typed_table_id``
    instead of being re-minted. The first activity that runs has its phase
    ``replay_cleanup`` invoked first (via the activity wrapper, see
    ``run_phase``) so the phase's own ``should_skip`` doesn't refuse to
    re-execute.
    """

    @workflow.run
    async def run(self, payload: ProcessTableInput) -> ProcessTableResult:
        replay = payload.replay
        _validate_replay(replay)

        if _runs_under("typing", replay, _CHILD_PHASE_ORDER):
            await _maybe_replay_cleanup("typing", replay, payload.identity, [payload.raw_table_id])
            typing = await workflow.execute_activity(
                "typing",
                payload,
                result_type=TypingResult,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
            typed_table_id = typing.typed_table_id
        else:
            # Replay started past typing — resolve the typed id from substrate
            # so it lands in history (deterministic) and downstream activities
            # see the same scoped input as on an initial run.
            resolved = await workflow.execute_activity(
                "lookup_typed_table_id",
                payload,
                result_type=TypingResult,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
            typed_table_id = resolved.typed_table_id

        scoped = TableScopedInput(identity=payload.identity, table_id=typed_table_id)

        for phase in _ANALYTICS_PHASES:
            if not _runs_under(phase, replay, _CHILD_PHASE_ORDER):
                continue
            # Owner-scoped self-clean before re-run: each analytics phase clears
            # its own per-Column rows for THIS typed table so its ``should_skip``
            # doesn't bail (DAT-373). Scope is the typed id its rows hang off.
            await _maybe_replay_cleanup(phase, replay, payload.identity, [typed_table_id])
            await workflow.execute_activity(
                phase,
                scoped,
                result_type=PhaseOutcome,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )

        if _runs_under("detect_table", replay, _CHILD_PHASE_ORDER):
            await workflow.execute_activity(
                "detect_table",
                scoped,
                result_type=PhaseOutcome,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )

        return ProcessTableResult(
            raw_table_id=payload.raw_table_id,
            typed_table_id=typed_table_id,
        )


@workflow.defn(name="addSourceWorkflow")
class AddSourceWorkflow:
    """Import one source, fan out a child workflow per raw table, then reduce.

    Initial run (``payload.replay is None``): ``import`` enumerates the
    source's raw tables, the workflow fans out one
    :class:`ProcessTableWorkflow` per raw id via ``asyncio.gather`` and waits
    for all to complete, then ``semantic_per_column`` runs once as the
    source-level reduce (followed by ``detect_source`` for its detectors).

    Teach replay (``payload.replay`` set; DAT-343): per-stage gates +
    fan-out scope follow ``replay``:

      * ``import`` runs only if ``replay.from_phase == "import"``;
        otherwise we read the existing raw ids from substrate via
        ``lookup_raw_table_ids`` so the fan-out's data-dependence stays
        recorded in history.
      * Fan-out narrows to ``replay.raw_table_ids`` (None = all tables;
        empty list = no children at all — source-tail-only replays like
        ``concept_property``). Children carry ``replay`` so they gate
        their own activities.
      * ``semantic_per_column`` + ``detect_source`` always re-run on any
        replay (the source-level reduce benefits from the widening data
        breadth — see the DAT-343 refine).

    The data-dependent fan-out is driven off ``imported.raw_table_ids``
    (recorded in history whether ``import`` ran or was looked up), so
    replay stays deterministic.
    """

    @workflow.run
    async def run(self, payload: AddSourceInput) -> AddSourceResult:
        replay = payload.replay
        _validate_replay(replay)

        if _runs_under("import", replay, _PARENT_PHASE_ORDER):
            await _maybe_replay_cleanup("import", replay, payload.identity, [])
            imported = await workflow.execute_activity(
                "import",
                payload.identity,
                result_type=ImportResult,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
        else:
            # Replay past import — look up the existing raw ids so the fan-out
            # is data-dependent in the same shape as the initial run.
            imported = await workflow.execute_activity(
                "lookup_raw_table_ids",
                payload.identity,
                result_type=ImportResult,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )

        # Narrow the fan-out by replay scope: None = every raw, list = those
        # raw ids. An empty list means "no children" — source-tail-only
        # replays (concept_property) re-run the reduce without re-typing.
        target_raw_ids = imported.raw_table_ids
        if replay is not None and replay.raw_table_ids is not None:
            allowed = set(replay.raw_table_ids)
            target_raw_ids = [r for r in target_raw_ids if r in allowed]

        # Replay forwarded to children only when ``from_phase`` is a child
        # chain phase. A parent-stage replay (``from_phase="import"``) has
        # already cleaned the children's substrate state via the import
        # cleanup, so the per-table re-runs against the freshly-imported
        # raw tables must run every child phase from scratch — passing the
        # parent's replay would make ``_runs_under`` return False for every
        # child phase and leave the new raw tables un-typed.
        child_replay = (
            replay if replay is not None and replay.from_phase in _CHILD_PHASE_ORDER else None
        )

        # Deterministic, collision-free child ids keep replay stable. The same
        # id is reused across teach iterations with WorkflowIdReusePolicy.ALLOW_DUPLICATE
        # on the parent — Temporal UI groups iterations naturally. The id encodes
        # workspace_id (DAT-364) so two workspaces sharing a source_id never
        # collide; see process_table_workflow_id for the convention.
        children = [
            workflow.execute_child_workflow(
                ProcessTableWorkflow.run,
                ProcessTableInput(
                    identity=payload.identity,
                    raw_table_id=raw_id,
                    replay=child_replay,
                ),
                id=process_table_workflow_id(
                    payload.identity.workspace_id,
                    payload.identity.source_id,
                    raw_id,
                ),
            )
            for raw_id in target_raw_ids
        ]
        tables: list[ProcessTableResult] = list(await asyncio.gather(*children))

        # Source-level reduce + its detectors: ALWAYS run, on both initial
        # runs and any replay. Per the DAT-343 refine, re-running the reduce
        # over the (possibly widened) source data is strictly better than
        # skipping it. ``detect_source`` is cheap + idempotent
        # (delete-before-insert) so it joins the always-runs set.
        # ``_maybe_replay_cleanup`` is still gated on
        # ``replay.from_phase == "semantic_per_column"`` — it only fires for
        # the concept_property entry case.
        await _maybe_replay_cleanup("semantic_per_column", replay, payload.identity, [])
        await workflow.execute_activity(
            "semantic_per_column",
            payload.identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )
        await workflow.execute_activity(
            "detect_source",
            payload.identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        return AddSourceResult(raw_table_ids=imported.raw_table_ids, tables=tables)
