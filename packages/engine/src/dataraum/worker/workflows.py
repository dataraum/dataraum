"""Temporal workflows (DAT-344; per-table fan-out DAT-370) — orchestration in Python.

Runs in Temporal's determinism sandbox, so this module imports ONLY
``temporalio`` + the engine-free :mod:`dataraum.worker.contracts` shapes (pulled
through the sandbox via ``imports_passed_through``). It calls activities by their
registered string names — it never imports the activity implementations, which
would drag the engine into the sandbox.

Topology (DAT-370): the table is the unit of work.

    AddSourceWorkflow(identity)                              [parent]
      import() per source_id    -> raw table ids             (per-source enumerator)
      check_column_limit()                                   (run-scoped cost gate, DAT-430)
      fan-out via workflow.as_completed:
        ProcessTableWorkflow(raw_id) for each raw id         [child, per table]
      semantic_per_column()                                  (session-scoped reduce)
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

with workflow.unsafe.imports_passed_through():
    from dataraum.worker.contracts import (
        AddSourceInput,
        AddSourceResult,
        BeginSessionInput,
        BeginSessionResult,
        ImportResult,
        OperatingModelInput,
        OperatingModelResult,
        OperatingModelScope,
        OperatingModelScopedInput,
        PhaseOutcome,
        ProcessTableInput,
        ProcessTableResult,
        ProgressFailure,
        ProgressSnapshot,
        RunScopedInput,
        SessionScopedInput,
        TableProgress,
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


def _failure_message(err: BaseException) -> str:
    """Unwrap a workflow failure to its root-cause message for the snapshot.

    Temporal wraps a phase failure as ``ActivityError`` →
    ``ApplicationError`` (and a child failure as ``ChildWorkflowError`` → …);
    the useful text is the innermost cause's message — the phase's own
    non-retryable failure string. Walk the ``__cause__`` chain to it; fall back
    to the type name when the root carries no message. Pure over the (replayed,
    history-recorded) exception, so deterministic.
    """
    cause: BaseException = err
    while cause.__cause__ is not None:
        cause = cause.__cause__
    return str(cause) or type(cause).__name__


@workflow.defn(name="processTableWorkflow")
class ProcessTableWorkflow:
    """Run the table-local chain for one raw table, then complete.

    ``typing`` always mints the typed id and the analytics phases run scoped to
    it. The typed id travels in the activity results, so it is in history and
    replayed verbatim. Detectors do NOT run here — they run once, source-wide,
    in the parent's terminal ``detect`` step (DAT-394).

    A teach re-run is now a full re-run of this child against the same raw table
    (DAT-413): there is no partial scoping, so typing always re-mints and every
    analytics phase runs. The new run stamps its own ``run_id`` (carried on the
    inherited identity), so its metadata coexists with the prior run's under the
    widened per-(column, run_id) constraints; the parent's terminal promote step
    flips the head to it.
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
        typed_table_id = typing.typed_table_id

        scoped = TableScopedInput(identity=payload.identity, table_id=typed_table_id)

        for phase in _ANALYTICS_PHASES:
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
    """Import a SET of sources, fan out a child workflow per raw table, then reduce.

    A run ingests 1–N sources (DAT-422): ``import`` runs once per source in the
    input set, enumerating each source's raw tables into one union. The workflow
    fans out one :class:`ProcessTableWorkflow` per raw id and consumes them with
    :func:`workflow.as_completed` (the deterministic SDK counterpart to
    ``asyncio.as_completed``) so progress can advance as each child resolves,
    then ``semantic_per_column`` runs once as the session-scoped reduce (followed
    by the terminal ``detect`` step that runs all detectors over the run's session
    tables, and the terminal ``promote_to_latest`` step that flips the snapshot
    head). Past ``import`` the spine is source-free — it scopes by the session's
    table set, so a run whose tables span per-object sources reduces as one set.

    A teach re-run is now a full re-run (DAT-413): there is no partial replay
    scope. Every execution mints a fresh ``run_id`` and re-derives the pipeline —
    re-types every table and re-reduces (``import`` reuses the source's
    already-loaded raw tables; it does NOT re-load source data). The fresh run's
    metadata coexists with prior runs' under the widened per-(column, run_id)
    constraints, and ``promote_to_latest`` makes this run the current snapshot at
    the end.

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
        fan-out (and on a CLOSED run by replaying history, so the cockpit can
        read the final snapshot — including ``failure`` — off a failed run). The
        cockpit Client polls this by workflow id (a teach re-run reuses the id
        under ALLOW_DUPLICATE and resets progress per run). The snapshot carries
        the per-table ``tables`` steps and any ``failure`` directly, so the
        cockpit needs no per-child queries.
        """
        return self._progress

    @workflow.run
    async def run(self, payload: AddSourceInput) -> AddSourceResult:
        """Run the add_source spine, recording any failure into the snapshot.

        Thin wrapper over :meth:`_run_inner`: on any stage failure it stamps
        ``self._progress.failure`` (root-cause message + the phase in flight)
        before re-raising, so a polling cockpit sees WHY the run ended, not just
        a FAILED status. A failing child has already stamped a table-scoped
        failure (see the fan-out), so the ``is None`` guard preserves it.
        ``except Exception`` deliberately misses ``CancelledError`` (a
        ``BaseException``) so cancellation still propagates clean.
        """
        try:
            return await self._run_inner(payload)
        except Exception as err:
            if self._progress.failure is None:
                self._progress.failure = ProgressFailure(
                    message=_failure_message(err),
                    phase=self._progress.phase,
                )
            raise

    def _mark_table(self, raw_table_id: str, status: str) -> None:
        """Flip one fanned-out table's status in the snapshot (done / failed)."""
        for entry in self._progress.tables:
            if entry.raw_table_id == raw_table_id:
                entry.status = status
                return

    async def _run_inner(self, payload: AddSourceInput) -> AddSourceResult:
        # Mint the snapshot version axis once per execution (DAT-413) and stamp
        # it onto the identity threaded into every activity, so all of this run's
        # metadata rows share one run_id. ``workflow.uuid4`` is the deterministic,
        # replay-safe UUID (NEVER ``uuid.uuid4``). The child workflow inherits the
        # stamped identity via ``ProcessTableInput(identity=identity)``.
        run_id = str(workflow.uuid4())
        base = payload.identity.model_copy(update={"run_id": run_id})

        # A run ingests a SET of objects from 1–N sources (DAT-422). ``import``
        # is the one per-source activity — it loads a source's files into
        # ``lake.raw.*`` — so it runs once per source in ``payload.source_ids``,
        # each scoped to that source's id (``base`` carries the workspace/session/
        # run; only ``source_id`` varies). The cockpit's per-file content-source set
        # is non-empty by construction (Zod ``min(1)``). Sequential, not fanned out:
        # imports write to the shared lake, so serial keeps them off each other's
        # optimistic-commit path and stays determinism-simple. On a teach re-run each
        # import reuses its already-loaded raw tables (``ImportPhase.should_skip``
        # bails on re-load); the re-run re-derives the downstream metadata under the
        # fresh run_id, coexisting with prior runs.
        target_raw_ids: list[str] = []
        for source_id in payload.source_ids:
            imported = await workflow.execute_activity(
                "import",
                base.model_copy(update={"source_id": source_id}),
                result_type=ImportResult,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
            target_raw_ids.extend(imported.raw_table_ids)

        # Past ``import`` the run is source-free (DAT-422): the per-table fan-out
        # and the session-scoped reduce/detect/promote scope by the session's table
        # set (``session_tables``), never a source — so the identity threaded onward
        # drops ``source_id``. ``typing`` links each typed table to the session, so
        # the reduce/detect see the union across every imported source.
        identity = base.model_copy(update={"source_id": None})

        # Run-scoped column gate (DAT-430): ``limits.max_columns`` bounds the
        # RUN's pipeline/LLM cost, so it must judge the union of the run's raw
        # tables — a per-source check stopped bounding anything once a run
        # became a SET of per-file sources. Runs unconditionally before the
        # fan-out (so a run recomposing already-imported sources — every import
        # skipped — is still gated); a breach raises the non-retryable
        # PhaseFailed and the run ends here, before any child does table work.
        # Advance the snapshot first so a gate failure is attributed to THIS
        # stage, not left stamped as "import".
        self._progress.phase = "check_column_limit"
        await workflow.execute_activity(
            "check_column_limit",
            RunScopedInput(identity=identity, table_ids=target_raw_ids),
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # The fan-out width is now known (import recorded ``raw_table_ids`` in
        # history → deterministic on replay). Set the progress denominator + seed
        # the per-table steps as "running" before any child is awaited, so an
        # early query already sees the named steps behind the count. Each flips to
        # "done"/"failed" as its child resolves.
        self._progress.tables_total = len(target_raw_ids)
        self._progress.tables = [
            TableProgress(raw_table_id=raw_id, status="running") for raw_id in target_raw_ids
        ]
        self._progress.phase = "processing_tables"

        # Deterministic, collision-free child ids keep replay stable. The same
        # id is reused across teach iterations with WorkflowIdReusePolicy.ALLOW_DUPLICATE
        # on the parent — Temporal UI groups iterations naturally. The id encodes
        # workspace_id (DAT-364) + the run's session_id (DAT-422) so two workspaces
        # never collide and per-object sources in one run share the parent prefix;
        # see process_table_workflow_id for the convention.
        async def _process(raw_id: str) -> ProcessTableResult:
            # Wrap the child so a failure is attributed to THIS table before it
            # propagates — ``as_completed`` yields in completion order, not input
            # order, so the attribution has to live with the await. The status
            # flip + counter bump sit after the awaited, history-recorded child
            # completion, so a replay reconstructs the identical snapshot.
            try:
                result = await workflow.execute_child_workflow(
                    ProcessTableWorkflow.run,
                    ProcessTableInput(
                        identity=identity,
                        raw_table_id=raw_id,
                    ),
                    id=process_table_workflow_id(
                        identity.workspace_id,
                        identity.session_id,
                        raw_id,
                    ),
                )
            except Exception as err:
                self._mark_table(raw_id, "failed")
                if self._progress.failure is None:
                    self._progress.failure = ProgressFailure(
                        message=_failure_message(err),
                        phase="processing_tables",
                        table_id=raw_id,
                    )
                raise
            self._mark_table(raw_id, "done")
            self._progress.tables_completed += 1
            return result

        # Consume with the deterministic ``workflow.as_completed`` (NOT
        # ``asyncio.gather``) so the per-table flips + ``tables_completed`` land
        # as each child resolves — a polling query sees real progress mid-fan-out
        # instead of a frozen 0 until the whole batch lands. Order is not
        # preserved, which AddSourceResult.tables does not rely on (it is a set of
        # raw→typed mappings the reduce/detect read from substrate, not by
        # position).
        tables: list[ProcessTableResult] = []
        for child in workflow.as_completed([_process(raw_id) for raw_id in target_raw_ids]):
            tables.append(await child)

        # Source-level reduce + the terminal detector pass. The reduce runs once
        # over the run's tables after the fan-out; ``detect`` follows, running
        # every wired detector source-wide.
        self._progress.phase = "semantic_per_column"
        await workflow.execute_activity(
            "semantic_per_column",
            identity,
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
            identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )
        # Terminal promote step (DAT-413): flip the per-(table, stage) snapshot
        # head to this run's run_id. Always runs last, after detect — the run's
        # metadata is now fully written, so the head can name it current.
        # Behavior-preserving in Phase 2 (nothing reads the head yet); Phase 3
        # switches the readers. Not added to begin_session (Slice B).
        self._progress.phase = "promote"
        await workflow.execute_activity(
            "promote_to_latest",
            identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "done"
        return AddSourceResult(raw_table_ids=target_raw_ids, tables=tables)


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
# setup. The body iterates this tuple to execute the chain sequentially.
_SESSION_PHASE_ORDER = ("relationships", "semantic_per_table")

# The value layer (DAT-403), in dependency order, runs AFTER ``enriched_views``:
# slice the fact tables (LLM), narrow each to a slicing view, materialize + profile
# the slices, run drift/period analysis on them, then detect derived columns. All
# scoped by the session's table set (``scoped``), source-free like the spine above.
_SESSION_VALUE_PHASE_ORDER = (
    "slicing",
    "slicing_view",
    "slice_analysis",
    "temporal_slice_analysis",
    "correlations",
)


@workflow.defn(name="beginSessionWorkflow")
class BeginSessionWorkflow:
    """Compose a selected set of typed tables into an analytical session (DAT-401).

    Source-free, session-scoped, sequential — the begin_session spine. Runs in
    Temporal's determinism sandbox like the add_source workflows (imports only
    the engine-free contracts).

    ``begin_session_select`` pre-flights the selection + links it to the session
    (``session_tables``), then ``relationships`` (structural candidates) →
    ``semantic_per_table`` (LLM classification + confirms a subset) run over the
    whole selection, then ``session_materialize_overlays`` (fold the user's durable
    add/keep relationship teaches into this run, DAT-409) → ``enriched_views``
    (grain-preserving fact×dimension views over the defined catalog, DDL versioned
    on the recipe substrate, DAT-415) → ``session_detect`` (relationship-granularity
    readiness) → ``session_write_keepers`` (silent-accept lift-up, DAT-409) →
    ``session_promote_to_latest`` (flip the readiness heads). NO fan-out — the work
    is cross-table.

    The run mints a ``run_id`` (DAT-408) threaded through every activity; a teach
    re-run is a full re-run under a fresh ``run_id`` (candidates re-derive,
    llm/manual/keeper survive, readiness is non-destructive + promoted to the new run).

    Progress (DAT-435): mirrors add_source's DAT-406 pattern — the body keeps a
    :class:`ProgressSnapshot` in ``self._progress`` and advances ``phase``
    before each stage; the read-only :meth:`get_progress` query serves it under
    the SAME query name and snapshot shape as ``AddSourceWorkflow``, so the
    cockpit's existing poll (``getWorkflowProgress``) and ``workflow_status``
    tool report real phases with no contract change. Sequential chain, no
    fan-out → the per-table fields stay at their empty defaults. Every mutation
    sits between awaited, history-recorded activity completions, so a replay
    reconstructs the identical snapshot — determinism is preserved.
    """

    def __init__(self) -> None:
        # Initialized to the pre-flight stage so a query that lands before the
        # first activity still returns a well-formed snapshot (never None). The
        # fan-out fields keep their empty defaults — begin_session has no
        # children, which the cockpit widget reads as "no per-table tally".
        self._progress = ProgressSnapshot(phase="begin_session_select")

    @workflow.query
    def get_progress(self) -> ProgressSnapshot:
        """Return the current progress snapshot (DAT-435).

        Read-only, non-mutating → determinism-safe; Temporal answers it against
        current state even while :meth:`run` is blocked awaiting a stage (and on
        a CLOSED run by replaying history, so the cockpit can read the final
        snapshot — including ``failure`` — off a failed run). Same query name +
        shape as :meth:`AddSourceWorkflow.get_progress`, so the cockpit polls
        both workflows through one seam.
        """
        return self._progress

    @workflow.run
    async def run(self, payload: BeginSessionInput) -> BeginSessionResult:
        """Run the begin_session spine, recording any failure into the snapshot.

        Thin wrapper over :meth:`_run_inner`: on any stage failure it stamps
        ``self._progress.failure`` (root-cause message + the phase in flight)
        before re-raising, so a polling cockpit sees WHY the run ended, not
        just a FAILED status. Unconditional stamp — unlike add_source there is
        no fanned-out child that could have stamped a table-scoped failure
        first. ``except Exception`` deliberately misses ``CancelledError`` (a
        ``BaseException``) so cancellation still propagates clean.
        """
        try:
            return await self._run_inner(payload)
        except Exception as err:
            self._progress.failure = ProgressFailure(
                message=_failure_message(err),
                phase=self._progress.phase,
            )
            raise

    async def _run_inner(self, payload: BeginSessionInput) -> BeginSessionResult:
        # Mint the run's ``run_id`` once (DAT-408), mirroring AddSourceWorkflow, and
        # thread it through every activity so all of this run's metadata shares it
        # and the terminal promote can flip the relationship-readiness heads.
        # ``workflow.uuid4`` is the deterministic, replay-safe source.
        run_id = str(workflow.uuid4())
        identity = payload.identity.model_copy(update={"run_id": run_id})
        # The selection is the execution scope, threaded to every activity. It is
        # also what ``begin_session_select`` persists to ``session_tables``.
        scoped = SessionScopedInput(identity=identity, table_ids=payload.tables)

        # Scope setup: pre-flight the selection (reject unknown/non-typed ids) and
        # link it to the session. Idempotent, and the phases below read the
        # linked set.
        await workflow.execute_activity(
            "begin_session_select",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Advance the snapshot before each stage (DAT-435) so a failure is
        # attributed to the stage in flight and a poll names what is running now.
        for phase in _SESSION_PHASE_ORDER:
            self._progress.phase = phase
            await workflow.execute_activity(
                phase,
                scoped,
                result_type=PhaseOutcome,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )

        # Materialize durable relationship teaches (DAT-409): after the llm pass, fold
        # the user's add/keep overlays into this run as manual/keeper rows (skipping
        # pairs already produced as llm) so the defined catalog detect measures next
        # carries them.
        self._progress.phase = "session_materialize_overlays"
        await workflow.execute_activity(
            "session_materialize_overlays",
            identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Enriched views (DAT-415): build grain-preserving fact×dimension views over
        # the now-complete defined relationship catalog (llm + the just-materialized
        # manual/keeper teaches), versioning each view's DDL on the recipe substrate.
        # Scoped (needs the selection's table_ids); runs before detect so the
        # table-grain readiness it feeds (DAT-402) measures the enriched substrate.
        self._progress.phase = "enriched_views"
        await workflow.execute_activity(
            "enriched_views",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Value layer (DAT-403): slicing → slicing_view → slice_analysis →
        # temporal_slice_analysis → correlations, over the enriched substrate just
        # built. Scoped by the session's table set; feeds the value-layer detectors
        # the terminal ``session_detect`` measures. Each is idempotent on its
        # CREATE-OR-REPLACE / reconcile, so a re-run under a fresh run_id re-derives.
        for phase in _SESSION_VALUE_PHASE_ORDER:
            self._progress.phase = phase
            await workflow.execute_activity(
                phase,
                scoped,
                result_type=PhaseOutcome,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )

        # Terminal relationship detect (DAT-408): runs the relationship detectors
        # over the session's tables, persisting relationship-granularity readiness
        # stamped with ``run_id`` — then promote flips the heads to this run.
        self._progress.phase = "session_detect"
        await workflow.execute_activity(
            "session_detect",
            identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Silent-accept keepers (DAT-409): BEFORE promote, while the head still names
        # the prior run — lift each promoted llm this run didn't reproduce (and the
        # user didn't reject) into a keep overlay for the next run to materialize.
        self._progress.phase = "session_write_keepers"
        await workflow.execute_activity(
            "session_write_keepers",
            identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "session_promote_to_latest"
        await workflow.execute_activity(
            "session_promote_to_latest",
            identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "done"
        return BeginSessionResult(session_id=identity.session_id, table_ids=payload.tables)


@workflow.defn(name="operatingModelWorkflow")
class OperatingModelWorkflow:
    """The journey's third stage — executable knowledge over the workspace (DAT-438).

    Mirrors ``BeginSessionWorkflow``'s shape (source-free, session-scoped,
    sequential, run-versioned) with one structural difference: the input is the
    ``SessionIdentity`` ONLY. begin_session ESTABLISHES the table set; this
    stage operates on the set the session already anchors, so the pre-flight
    ``operating_model_resolve`` re-reads ``session_tables`` AND pins the
    ADR-0008 base-run map (begin_session's promoted detect head + per-table
    semantic heads) once — every downstream read scopes to those pins, no
    per-phase head resolution.

    Spine: resolve → validation → business_cycles (each declare → bind →
    execute through the typed artifact lifecycle) → promote ``(session:{id},
    "operating_model")``. ``business_cycles`` (DAT-455) is the second lifecycle
    family, running after validation so cycle health reads this run's validation
    results; a later slice inserts metrics, and DAT-432 inserts the terminal
    detect (cross_table_consistency) before promote. A re-run is a full re-run
    under a fresh ``run_id`` — declared artifacts, validation results, and
    detected cycles supersede, never mutate (DAT-408).

    Progress (DAT-435 follow-on): the body keeps a :class:`ProgressSnapshot`
    in ``self._progress`` and advances ``phase`` before each stage; the
    read-only :meth:`get_progress` query serves it under the same name + shape
    as the other workflows, so the cockpit polls all of them through one seam.
    """

    def __init__(self) -> None:
        # Initialized to the pre-flight stage so a query that lands before the
        # first activity still returns a well-formed snapshot (never None). The
        # fan-out fields keep their empty defaults — operating_model has no
        # children, which the cockpit widget reads as "no per-table tally".
        self._progress = ProgressSnapshot(phase="operating_model_resolve")

    @workflow.query
    def get_progress(self) -> ProgressSnapshot:
        """Return the current progress snapshot (DAT-435 follow-on).

        Read-only, non-mutating → determinism-safe; same query name + shape as
        :meth:`BeginSessionWorkflow.get_progress`, so the cockpit's progress
        seam covers this workflow without a per-workflow branch.
        """
        return self._progress

    @workflow.run
    async def run(self, payload: OperatingModelInput) -> OperatingModelResult:
        """Run the operating_model spine, recording any failure into the snapshot.

        Thin wrapper over :meth:`_run_inner`, mirroring
        :meth:`BeginSessionWorkflow.run`: on any stage failure it stamps
        ``self._progress.failure`` (root-cause message + the phase in flight)
        before re-raising. ``except Exception`` deliberately misses
        ``CancelledError`` so cancellation still propagates clean.
        """
        try:
            return await self._run_inner(payload)
        except Exception as err:
            self._progress.failure = ProgressFailure(
                message=_failure_message(err),
                phase=self._progress.phase,
            )
            raise

    async def _run_inner(self, payload: OperatingModelInput) -> OperatingModelResult:
        run_id = str(workflow.uuid4())
        identity = payload.identity.model_copy(update={"run_id": run_id})

        # Pre-flight: the session's table set + the run's pinned base heads.
        # Fails loud (no linked tables / unknown session) — nothing to model.
        scope = await workflow.execute_activity(
            "operating_model_resolve",
            identity,
            result_type=OperatingModelScope,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # First lifecycle family (DAT-438): every declared validation (vertical
        # ⊕ teach rows) is declared for this run, bound (SQL vs workspace),
        # executed. Ungroundable specs surface as declared-with-reason.
        scoped = OperatingModelScopedInput(identity=identity, scope=scope)
        self._progress.phase = "validation"
        outcome = await workflow.execute_activity(
            "validation",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Second lifecycle family (DAT-455): the declared cycle vocabulary
        # (vertical ⊕ teach rows) is declared, grounded against the workspace,
        # and measured. Runs AFTER validation so cycle health reads this run's
        # validation results. Ungroundable cycles stay declared-with-reason.
        self._progress.phase = "business_cycles"
        await workflow.execute_activity(
            "business_cycles",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Terminal promote: flip (session:{id}, "operating_model") to this run.
        self._progress.phase = "operating_model_promote"
        await workflow.execute_activity(
            "operating_model_promote",
            identity,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "done"
        return OperatingModelResult(
            session_id=identity.session_id,
            table_ids=scope.table_ids,
            validation_summary=outcome.summary,
        )
