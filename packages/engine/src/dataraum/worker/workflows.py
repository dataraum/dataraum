"""Temporal workflows (DAT-344; per-table fan-out DAT-370) — orchestration in Python.

Runs in Temporal's determinism sandbox, so this module imports ONLY
``temporalio`` + the engine-free :mod:`dataraum.worker.contracts` shapes (pulled
through the sandbox via ``imports_passed_through``). It calls activities by their
registered string names — it never imports the activity implementations, which
would drag the engine into the sandbox.

Topology (DAT-370): the table is the unit of work.

    AddSourceWorkflow(identity)                              [parent]
      import()                  -> raw table ids             (source-level enumerator)
      fan-out via workflow.as_completed:
        ProcessTableWorkflow(raw_id) for each raw id         [child, per table]
      semantic_per_column()                                  (source-level reduce)
      detect()                                               (single terminal detector pass)

    ProcessTableWorkflow(raw_table_id)                       [child]
      typing(raw_id) -> typed_id
      statistics -> column_eligibility -> statistical_quality -> temporal   (typed_id)

The child gives per-table history isolation + bounded parent history, and
``typed_id`` is threaded through the child's messages (persisted in history,
replayed verbatim). Detectors run once at the very end, source-wide, in the
parent's terminal ``detect`` step — not per phase, not per table (DAT-394:
nothing reads entropy mid-run, so detection has no reason to run before the run
ends; this collapsed the old per-table ``detect_table`` + parent ``detect_source``).
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from dataraum.worker.contracts import (
        AddSourceInput,
        AddSourceResult,
        BeginSessionInput,
        BeginSessionResult,
        ImportResult,
        PhaseOutcome,
        ProcessTableInput,
        ProcessTableResult,
        ProgressSnapshot,
        ReplayCleanupInput,
        ReplayScope,
        SessionIdentity,
        SessionReplayCleanupInput,
        SessionScopedInput,
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
# them (it mints the typed id). Detectors no longer run at the child's tail; the
# single terminal ``detect`` step (parent) runs the union of the detectors these
# phases + ``semantic_per_column`` declare (``activity._DETECTOR_PHASES``);
# ``test_phase_constants.py`` pins that no chain-declared detector is orphaned.
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
# ``detect`` is intentionally absent from both orders: like ``semantic_per_column``
# it ALWAYS re-runs at the parent tail (the workflow body owns that, not
# ``_runs_under``), so it is never a valid ``ReplayScope.from_phase`` entry point.
# Adding it here would make it gatable and break the always-runs invariant.
_PARENT_PHASE_ORDER = ("import", "semantic_per_column")
_CHILD_PHASE_ORDER = ("typing", *_ANALYTICS_PHASES)


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

    Initial run (``payload.replay is None``): ``typing`` mints the typed id and
    the analytics phases run scoped to it. The typed id travels in the activity
    results, so it is in history and replayed verbatim. Detectors do NOT run
    here — they run once, source-wide, in the parent's terminal ``detect`` step
    (DAT-394).

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

        return ProcessTableResult(
            raw_table_id=payload.raw_table_id,
            typed_table_id=typed_table_id,
        )


@workflow.defn(name="addSourceWorkflow")
class AddSourceWorkflow:
    """Import one source, fan out a child workflow per raw table, then reduce.

    Initial run (``payload.replay is None``): ``import`` enumerates the
    source's raw tables, the workflow fans out one
    :class:`ProcessTableWorkflow` per raw id and consumes them with
    :func:`workflow.as_completed` (the deterministic SDK counterpart to
    ``asyncio.as_completed``) so progress can advance as each child resolves,
    then ``semantic_per_column`` runs once as the source-level reduce
    (followed by the terminal ``detect`` step that runs all detectors
    source-wide).

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
      * ``semantic_per_column`` + the terminal ``detect`` always re-run on any
        replay (the source-level reduce benefits from the widening data
        breadth — see the DAT-343 refine; ``detect`` is cheap + idempotent).

    The data-dependent fan-out is driven off ``imported.raw_table_ids``
    (recorded in history whether ``import`` ran or was looked up), so
    replay stays deterministic.

    Progress (DAT-406): the body keeps a :class:`ProgressSnapshot` in
    ``self._progress`` — it advances ``phase`` before each stage and bumps
    ``tables_completed`` as each child resolves. The read-only
    :meth:`get_progress` query returns it; the cockpit Client polls it while
    the parent is blocked in the fan-out (a query answers against current
    state without the workflow having to await). All mutations sit at points
    gated by awaiting recorded history events, so a replay reconstructs the
    identical snapshot — determinism is preserved.
    """

    def __init__(self) -> None:
        # Initialized to the pre-import state so a query that lands before the
        # first stage still returns a well-formed snapshot (never None).
        self._progress = ProgressSnapshot(phase="import", tables_total=0, tables_completed=0)

    @workflow.query
    def get_progress(self) -> ProgressSnapshot:
        """Return the current parent-level progress snapshot (DAT-406).

        Read-only, non-mutating → determinism-safe; Temporal answers it
        against current state even while :meth:`run` is blocked awaiting the
        fan-out. The cockpit Client polls this by workflow id (queried per
        ``run_id``; a ReplayScope replay reuses the id under ALLOW_DUPLICATE
        and resets progress per run). Per-table phase detail is out of scope
        here — the cockpit would query each child by
        ``process_table_workflow_id`` (additive follow-up).
        """
        return self._progress

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

        # The fan-out width is now known (import recorded ``raw_table_ids`` in
        # history → deterministic on replay). Set the progress denominator
        # before any child is awaited so an early query already sees the total.
        self._progress.tables_total = len(target_raw_ids)

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
        # Consume the children with the deterministic ``workflow.as_completed``
        # (NOT ``asyncio.gather``) so ``tables_completed`` advances as each child
        # resolves — a polling query sees real progress mid-fan-out instead of a
        # frozen 0 until the whole batch lands. Each yielded value is a coroutine
        # that resolves to one child's ProcessTableResult; order is not preserved,
        # which AddSourceResult.tables does not rely on (it is a set of raw→typed
        # mappings the reduce/detect read from substrate, not by position). The
        # ``tables_completed`` bump sits after an awaited (history-recorded) child
        # completion, so a replay reconstructs the identical counter.
        self._progress.phase = "processing_tables"
        tables: list[ProcessTableResult] = []
        for child in workflow.as_completed(children):
            tables.append(await child)
            self._progress.tables_completed += 1

        # Source-level reduce + the terminal detector pass: ALWAYS run, on both
        # initial runs and any replay. Per the DAT-343 refine, re-running the
        # reduce over the (possibly widened) source data is strictly better than
        # skipping it. ``detect`` is cheap + idempotent (delete-before-insert) so
        # it joins the always-runs set. ``_maybe_replay_cleanup`` is still gated
        # on ``replay.from_phase == "semantic_per_column"`` — it only fires for
        # the concept_property entry case.
        self._progress.phase = "semantic_per_column"
        await _maybe_replay_cleanup("semantic_per_column", replay, payload.identity, [])
        await workflow.execute_activity(
            "semantic_per_column",
            payload.identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )
        # Single terminal detector pass (DAT-394): runs every wired detector
        # source-wide after the reduce, then persists readiness. Replaces the old
        # per-table detect_table + parent detect_source.
        self._progress.phase = "detect"
        await workflow.execute_activity(
            "detect",
            payload.identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "done"
        return AddSourceResult(raw_table_ids=imported.raw_table_ids, tables=tables)


# --- begin_session (DAT-401) -------------------------------------------------
#
# The session-scoped, source-free analogue of the add_source spine. A
# begin_session run composes a user-selected set of already-typed tables (which
# may span sources) into an analytical session. The work is cross-table
# (relationships are meaningless on one table), so there is NO fan-out — a
# sequential chain over the whole selection. The selection travels as an array
# of typed table ids in the workflow input and is threaded to each activity
# (``SessionScopedInput``); ``begin_session_select`` also persists it to
# ``session_tables`` for provenance + the downstream readiness layer (DAT-408).

# The begin_session chain, in dependency order: structural relationship
# detection, then the LLM table-synthesis that confirms a subset of those
# candidates. ``begin_session_select`` precedes both as the always-run scope
# setup (it is not gatable — re-running its idempotent link is harmless), so it
# is not part of this replay order.
_SESSION_PHASE_ORDER = ("relationships", "semantic_per_table")

# All phase names valid as a begin_session ``ReplayScope.from_phase``. Separate
# from add_source's ``_VALID_REPLAY_PHASES`` — a begin_session replay names a
# begin_session phase, never an ingestion one.
_SESSION_VALID_REPLAY_PHASES: frozenset[str] = frozenset(_SESSION_PHASE_ORDER)


def _validate_session_replay(replay: ReplayScope | None) -> None:
    """Refuse a begin_session replay whose ``from_phase`` isn't a session phase.

    Pure function over workflow input → deterministic. Raises non-retryable so
    the workflow fails loud on the first attempt; no silent partial replay.
    """
    if replay is None or replay.from_phase in _SESSION_VALID_REPLAY_PHASES:
        return
    raise ApplicationError(
        f"Unknown begin_session replay.from_phase '{replay.from_phase}'. "
        f"Valid values: {sorted(_SESSION_VALID_REPLAY_PHASES)}",
        type="PhaseFailed",
        non_retryable=True,
    )


async def _maybe_session_replay_cleanup(
    phase: str,
    replay: ReplayScope | None,
    identity: SessionIdentity,
    table_ids: list[str],
) -> None:
    """Invoke ``session_replay_cleanup_for_phase`` for a phase that re-runs (DAT-401).

    Mirrors :func:`_maybe_replay_cleanup` but source-free: each begin_session
    phase owns its rows and clears them (scoped to ``table_ids``) before the
    re-run so its ``should_skip`` doesn't bail. Fires exactly for the phases the
    caller's ``_runs_under`` gate also runs (the same predicate, used here too);
    nothing to clean on the initial run (``replay is None``).
    """
    if not _runs_under(phase, replay, _SESSION_PHASE_ORDER):
        return
    await workflow.execute_activity(
        "session_replay_cleanup_for_phase",
        SessionReplayCleanupInput(
            identity=identity,
            phase_name=phase,
            table_ids=list(table_ids),
        ),
        result_type=PhaseOutcome,
        start_to_close_timeout=_TIMEOUT,
        retry_policy=_RETRY,
    )


@workflow.defn(name="beginSessionWorkflow")
class BeginSessionWorkflow:
    """Compose a selected set of typed tables into an analytical session (DAT-401).

    Source-free, session-scoped, sequential — the begin_session spine. Runs in
    Temporal's determinism sandbox like the add_source workflows (imports only
    the engine-free contracts).

    Initial run (``payload.replay is None``): ``begin_session_select`` pre-flights
    the selection + links it to the session (``session_tables``), then
    ``relationships`` (structural candidates) → ``semantic_per_table`` (LLM
    classification + confirms a subset) run over the whole selection. NO fan-out
    (the work is cross-table) and NO terminal detect (relationship-granularity
    readiness is DAT-408 / 2.0b).

    Teach replay (``payload.replay`` set; DAT-343 pattern): the same per-phase
    gating + self-clean as add_source — ``_runs_under`` over ``_SESSION_PHASE_ORDER``
    decides which phases re-run, and ``_maybe_session_replay_cleanup`` clears each
    re-running phase's own rows first. ``begin_session_select`` always runs (its
    link merge is idempotent).
    """

    @workflow.run
    async def run(self, payload: BeginSessionInput) -> BeginSessionResult:
        replay = payload.replay
        _validate_session_replay(replay)
        identity = payload.identity
        # The selection is the execution scope, threaded to every activity. It is
        # also what ``begin_session_select`` persists to ``session_tables``.
        scoped = SessionScopedInput(identity=identity, table_ids=payload.tables)

        # Scope setup: pre-flight the selection (reject unknown/non-typed ids) and
        # link it to the session. Always runs — idempotent, and the phases below
        # read the linked set.
        await workflow.execute_activity(
            "begin_session_select",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        for phase in _SESSION_PHASE_ORDER:
            if not _runs_under(phase, replay, _SESSION_PHASE_ORDER):
                continue
            await _maybe_session_replay_cleanup(phase, replay, identity, payload.tables)
            await workflow.execute_activity(
                phase,
                scoped,
                result_type=PhaseOutcome,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )

        return BeginSessionResult(session_id=identity.session_id, table_ids=payload.tables)
