"""Temporal workflows (DAT-344; per-table fan-out DAT-370) — orchestration in Python.

Runs in Temporal's determinism sandbox, so this module imports ONLY
``temporalio`` + the engine-free :mod:`dataraum.worker.contracts` shapes (pulled
through the sandbox via ``imports_passed_through``). It calls activities by their
registered string names — it never imports the activity implementations, which
would drag the engine into the sandbox.

Topology (DAT-370): the table is the unit of work.

    AddSourceWorkflow(workspace_id, sources, verticals)     [parent]
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

Above the analysis workflows sit the two ORCHESTRATION workflows (DAT-708,
ADR-0020 — moved here from the cockpit's TS worker): ``groundingLoopWorkflow``
(onboarding import + one grounding-teach round per execution, replays
tail-called via ``continue_as_new`` with the budget on the input message) and
``sessionCascadeWorkflow`` (begin_session → operating_model), which start the
analysis workflows above as children and bracket each with the cockpit's
run-recording activities, scheduled by name on the cockpit's activity-only
queue. See the section at the bottom of this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from dataraum.worker.contracts import (
        AddSourceInput,
        AddSourceResult,
        AssessAndGroundInput,
        AssessAndGroundResult,
        BeginSessionInput,
        BeginSessionResult,
        GroundingLoopInput,
        ImportInput,
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
        RecordRunInput,
        RunKind,
        RunPhaseInput,
        RunRef,
        RunScopedInput,
        RunStage,
        SessionCascadeInput,
        SessionScopedInput,
        TableProgress,
        TableScopedInput,
        TypingResult,
        cockpit_task_queue_for,
        operating_model_workflow_id,
        process_table_workflow_id,
    )

# A deterministic phase failure is raised by the activity as a non-retryable
# ApplicationError of type ``PhaseFailed``; a transient provider failure raises
# ``TransientPhaseFailure`` (absent here) and stays retryable, as do
# infrastructure failures (e.g. a DuckLake optimistic-commit conflict).
_RETRY = RetryPolicy(maximum_attempts=5, non_retryable_error_types=["PhaseFailed"])

# LLM-calling activities (DAT-503): a transient provider failure (429 / 5xx /
# timeout) is exactly the case Temporal's durable retry exists for. The
# defaults' 100ms initial / 100s cap retries a rate limit far too fast and
# gives up after 5 tries; this policy backs off to a >=60s ceiling and allows
# more attempts so a real upstream outage is ridden out across the LLM's own
# Retry-After windows. ``PhaseFailed`` stays non-retryable (a permanent auth /
# bad-request error must not burn the budget).
_LLM_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=8,
    non_retryable_error_types=["PhaseFailed"],
)
_TIMEOUT = timedelta(minutes=10)

# The ``metrics`` activity heartbeats (DAT-503): a missed pulse within this
# window means the worker died, failing the run over to a retry far sooner than
# the 10-minute ``start_to_close_timeout`` would. Comfortably above the
# activity's 15s pulse cadence so a slow LLM wave never trips it.
_HEARTBEAT_TIMEOUT = timedelta(seconds=60)

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


def _single_vertical(verticals: list[str]) -> str:
    """Resolve the run's single vertical from the workflow input's ``verticals``.

    The wire carries ``verticals: list[str]`` for forward-compat (DAT-506), but a
    workspace realistically has ONE vertical today and the LLM grounding has no
    multi-vertical ontology MERGE. So operate on the resolved single name and fail
    LOUD on a multi-vertical workspace rather than silently using only the first —
    a born-loud guard until multi-vertical grounding lands (DAT-357/479 territory).
    An empty list resolves to the no-vertical default ``"_adhoc"`` (mirrors the
    activity-side ``vertical or "_adhoc"`` coalesce).
    """
    if len(verticals) > 1:
        raise ApplicationError(
            f"multi-vertical grounding not yet supported (got {verticals}); a "
            "workspace must carry exactly one vertical until ontology merge lands.",
            type="PhaseFailed",
            non_retryable=True,
        )
    return verticals[0] if verticals else "_adhoc"


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

        scoped = TableScopedInput(
            run=payload.run, table_id=typed_table_id, vertical=payload.vertical
        )

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
        # Mint the snapshot version axis once per execution (DAT-413) and thread
        # it into every activity via a source-free ``RunRef``, so all of this run's
        # metadata rows share one run_id. ``workflow.uuid4`` is the deterministic,
        # replay-safe UUID (NEVER ``uuid.uuid4``). The child workflow inherits the
        # run ref via ``ProcessTableInput(run=run)``.
        run_id = str(workflow.uuid4())
        run = RunRef(workspace_id=payload.workspace_id, run_id=run_id)
        # The wire carries verticals as a list (forward-compat); resolve the single
        # one (born-loud on multi-vertical until ontology merge lands).
        vertical = _single_vertical(payload.verticals)

        # A run ingests a SET of objects from 1–N sources (DAT-422). ``import``
        # is the one per-source activity — it loads a source's files into
        # ``lake.raw.*`` — so it runs once per source in ``payload.sources``, each
        # scoped to that source's id (the ONLY source id on the wire; everything
        # past import resolves source provenance relationally). The cockpit's
        # per-file content-source set is non-empty by construction (Zod ``min(1)``).
        # Sequential, not fanned out: imports write to the shared lake, so serial
        # keeps them off each other's optimistic-commit path and stays
        # determinism-simple. On a teach re-run each import reuses its
        # already-loaded raw tables (``ImportPhase.should_skip`` bails on re-load);
        # the re-run re-derives the downstream metadata under the fresh run_id,
        # coexisting with prior runs.
        target_raw_ids: list[str] = []
        for source_id in payload.sources:
            imported = await workflow.execute_activity(
                "import",
                ImportInput(run=run, source_id=source_id, vertical=vertical),
                result_type=ImportResult,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
            target_raw_ids.extend(imported.raw_table_ids)

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
            RunScopedInput(run=run, table_ids=target_raw_ids),
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

        # Deterministic, collision-free child ids keep replay stable. The child id
        # is derived from THIS parent's own ``workflow.info().workflow_id``
        # (DAT-506) — the cockpit owns parent-id naming — so the same raw table
        # re-runs under the same child id across teach iterations
        # (WorkflowIdReusePolicy.ALLOW_DUPLICATE on the parent groups them in the
        # Temporal UI), and two parents never collide on a child id. See
        # ``process_table_workflow_id`` for the convention.
        parent_id = workflow.info().workflow_id

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
                        run=run,
                        raw_table_id=raw_id,
                        vertical=vertical,
                    ),
                    id=process_table_workflow_id(parent_id, raw_id),
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
            RunPhaseInput(run=run, vertical=vertical),
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_LLM_RETRY,
        )
        # Single terminal detector pass (DAT-394): runs every wired detector
        # source-wide after the reduce, then persists readiness. Replaces the old
        # per-table detect_table + parent detect_source.
        self._progress.phase = "detect"
        await workflow.execute_activity(
            "detect",
            run,
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
            run,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "done"
        return AddSourceResult(run_id=run_id, raw_table_ids=target_raw_ids, tables=tables)


# --- begin_session (DAT-401) -------------------------------------------------
#
# The session-scoped, source-free analogue of the add_source spine. A
# begin_session run composes a user-selected set of already-typed tables (which
# may span sources) into an analytical session. The work is cross-table
# (relationships are meaningless on one table), so there is NO fan-out — a
# sequential chain over the whole selection. The selection travels as an array
# of typed table ids in the workflow input and is threaded to each activity
# (``SessionScopedInput``); ``begin_session_select`` also persists it to
# ``run_tables`` for provenance + the downstream readiness layer (DAT-408/506).

# The begin_session chain, in dependency order: structural relationship
# detection, then the LLM table-synthesis that confirms a subset of those
# candidates. ``begin_session_select`` precedes all as the always-run scope
# setup. The body iterates this tuple to execute the chain sequentially.
# (aggregation_lineage lives in the VALUE order below — its dependency is the
# slice substrate, not this spine.)
_SESSION_PHASE_ORDER = ("relationships", "semantic_per_table")

# The phase activities that make real Anthropic calls (DAT-503) — they get the
# LLM-shaped ``_LLM_RETRY`` so a transient provider failure is ridden out with
# >=60s backoff instead of the default fast-give-up. Everything else (structural
# detection, deterministic slice arithmetic, promotes) keeps ``_RETRY``. Kept
# beside the chain orders so a new LLM phase can't silently inherit ``_RETRY``.
_LLM_PHASES = frozenset(
    {
        "semantic_per_column",
        "semantic_per_table",
        "slicing",
        "enriched_views",
        "validation",
        "business_cycles",
        "metrics",
    }
)


def _retry_for(phase: str) -> RetryPolicy:
    """Pick the LLM-shaped retry for an LLM-calling phase, else the default."""
    return _LLM_RETRY if phase in _LLM_PHASES else _RETRY


# The value layer (DAT-403/536), in dependency order, runs AFTER ``enriched_views``:
# declare the slice dimensions (LLM → catalog), reconcile events→measure lineage by
# inline aggregation over the enriched views, then detect derived columns. All scoped
# by the session's table set (``scoped``), source-free like the spine above.
_SESSION_VALUE_PHASE_ORDER = (
    "slicing",
    # DAT-537: deterministic g3 FD pass over the slice catalog — drill-down
    # hierarchies + 1:1 aliases (no LLM). Reads slicing's catalog; nothing in the
    # value layer depends on it yet (answer agent consumes it in DAT-538).
    "dimension_hierarchies",
    # DAT-491/536: aggregates each fact's enriched view inline (GROUP BY dim, period)
    # and reconciles the per-period sums across facts sharing a catalog dimension.
    "aggregation_lineage",
    "correlations",
    # NOTE: driver_rankings is NOT here — it runs AFTER session_detect (below) so it
    # reads the POOL-RESOLVED temporal_behavior, not the pre-pool table-agent claim.
    # See the explicit call in the workflow body for why (DAT-543).
)


@workflow.defn(name="beginSessionWorkflow")
class BeginSessionWorkflow:
    """Compose a selected set of typed tables into an analytical session (DAT-401).

    Source-free, session-scoped, sequential — the begin_session spine. Runs in
    Temporal's determinism sandbox like the add_source workflows (imports only
    the engine-free contracts).

    ``begin_session_select`` pre-flights the selection + links it to the run
    (``run_tables``), then ``relationships`` (structural candidates) →
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
        # thread it through every activity via a source-free ``RunRef`` so all of
        # this run's metadata shares it and the terminal promote can flip the
        # relationship-readiness heads. ``workflow.uuid4`` is the deterministic,
        # replay-safe source.
        run_id = str(workflow.uuid4())
        run = RunRef(workspace_id=payload.workspace_id, run_id=run_id)
        vertical = _single_vertical(payload.verticals)
        # The selection is the execution scope, threaded to every activity. It is
        # also what ``begin_session_select`` persists to ``run_tables``.
        scoped = SessionScopedInput(run=run, table_ids=payload.tables, vertical=vertical)

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
                retry_policy=_retry_for(phase),
            )

        # Materialize durable relationship teaches (DAT-409): after the llm pass, fold
        # the user's add/keep overlays into this run as manual/keeper rows (skipping
        # pairs already produced as llm) so the defined catalog detect measures next
        # carries them.
        self._progress.phase = "session_materialize_overlays"
        await workflow.execute_activity(
            "session_materialize_overlays",
            run,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Surrogate mint (DAT-277): AFTER the overlays (user teaches folded in),
        # BEFORE enriched_views — cure each LLM-confirmed composite key at the
        # source by re-materializing both typed tables with the deterministic hash
        # column (DAT-414 recipe substrate) and persisting the ONE single-column
        # surrogate relationship the downstream consumers read. Deterministic, no
        # LLM. With no confirmed composites it is a fast no-op.
        self._progress.phase = "surrogate_mint"
        await workflow.execute_activity(
            "surrogate_mint",
            scoped,
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
            retry_policy=_LLM_RETRY,
        )

        # Value layer (DAT-403/536): slicing → aggregation_lineage → correlations,
        # over the enriched substrate just built. Scoped by the session's table set;
        # feeds the value-layer detectors the terminal ``session_detect`` measures.
        # Each is idempotent on its reconcile, so a re-run under a fresh run_id
        # re-derives.
        for phase in _SESSION_VALUE_PHASE_ORDER:
            self._progress.phase = phase
            await workflow.execute_activity(
                phase,
                scoped,
                result_type=PhaseOutcome,
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_retry_for(phase),
            )

        # Terminal relationship detect (DAT-408): runs the relationship detectors
        # over the session's tables, persisting relationship-granularity readiness
        # stamped with ``run_id`` — then promote flips the heads to this run.
        self._progress.phase = "session_detect"
        await workflow.execute_activity(
            "session_detect",
            run,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # Driver rankings AFTER detect (DAT-543): session_detect resolves the pooled
        # stock/flow verdict onto ColumnConcept.temporal_behavior (the structural
        # witness reconciling event lineage can overturn the table-agent's name-based
        # stock claim). Driver discovery reads that behaviour to pick its target
        # FUNCTION (flow → SUM residual, stock → end-of-period) AND persists the
        # target_type the graph/answer agents read. Run it here so both the ranking
        # STATISTICS and the persisted target_type reflect the resolved verdict, not
        # the pre-pool claim — otherwise a pool-flipped balance ranks against the wrong
        # target and the agent is told "stock, latest-period only" for an additive
        # column. Still pre-promote, so current_driver_rankings sees it. Needs slicing +
        # dimension_hierarchies + enriched_views (all above); declares no detectors, so
        # sitting past the terminal detect orphans nothing.
        self._progress.phase = "driver_rankings"
        await workflow.execute_activity(
            "driver_rankings",
            scoped,
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
            run,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "session_promote_to_latest"
        await workflow.execute_activity(
            "session_promote_to_latest",
            run,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "done"
        return BeginSessionResult(run_id=run_id, table_ids=payload.tables)


@workflow.defn(name="operatingModelWorkflow")
class OperatingModelWorkflow:
    """The journey's third stage — executable knowledge over the workspace (DAT-438).

    Mirrors ``BeginSessionWorkflow``'s shape (source-free, run-versioned,
    sequential) with one structural difference: the input carries no table set.
    begin_session ESTABLISHES the table set; this stage operates on the set the
    workspace catalog already anchors, so the pre-flight
    ``operating_model_resolve`` reads the catalog head's ``run_tables`` AND pins
    the ADR-0008 base-run map (begin_session's promoted detect head + per-table
    semantic heads) AND the table set once — every downstream read scopes to
    those pins, no per-phase head resolution.

    Spine: resolve → validation → business_cycles → metrics (each declare →
    bind/compose → execute through the typed artifact lifecycle) → promote
    ``(catalog, "operating_model")``. ``business_cycles`` (DAT-455) is the
    second family and ``metrics`` (DAT-456) the third, each running after the
    prior so the later families' graph context can read this run's evidence;
    DAT-432 inserts the terminal detect (cross_table_consistency) before promote.
    A promote GATE (DAT-845) sits before that flip: if all three families
    declared ZERO, no operating model exists, so the head is NOT flipped and the
    run completes with the ``nothing_declared`` terminal outcome (see
    :meth:`_run_inner`) — a misconfiguration signal, not a benign variant.
    A re-run is a full re-run under a fresh ``run_id`` — declared artifacts,
    validation results, detected cycles, and composed metrics supersede, never
    mutate (DAT-408).

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
        run = RunRef(workspace_id=payload.workspace_id, run_id=run_id)
        vertical = _single_vertical(payload.verticals)

        # Pre-flight: validate the vertical + pin the run's base heads off the
        # workspace catalog run. Fails loud (unknown vertical / no promoted
        # begin_session catalog run) — nothing to model.
        scope = await workflow.execute_activity(
            "operating_model_resolve",
            RunPhaseInput(run=run, vertical=vertical),
            result_type=OperatingModelScope,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        # First lifecycle family (DAT-438): every declared validation (vertical
        # ⊕ teach rows) is declared for this run, bound (SQL vs workspace),
        # executed. Ungroundable specs surface as declared-with-reason. Every OM
        # phase — validation, detect, cycles, metrics — reads the table set PINNED
        # at resolve (``scope.table_ids``, ADR-0008), not the live catalog head, so
        # a concurrent begin_session promote cannot shift the set mid-run.
        scoped = OperatingModelScopedInput(run=run, scope=scope, vertical=vertical)
        self._progress.phase = "validation"
        validation_outcome = await workflow.execute_activity(
            "validation",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_LLM_RETRY,
        )

        # Terminal-for-evidence detect (DAT-432): score this run's executed
        # validation results (cross_table_consistency → table + column bands)
        # and persist readiness BEFORE the LLM-heavy families. NOTE: the rows
        # become visible to head-resolved readers only at the terminal promote
        # — a cycles/metrics failure still loses the run's visibility (the
        # failed-runs-never-surface invariant), it just doesn't recompute this.
        self._progress.phase = "operating_model_detect"
        await workflow.execute_activity(
            "operating_model_detect",
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
        cycles_outcome = await workflow.execute_activity(
            "business_cycles",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_LLM_RETRY,
        )

        # Third lifecycle family (DAT-456): the declared metric graphs (vertical
        # ⊕ teach rows) are declared, composed against the workspace (inputs
        # resolve to real columns/concepts), and executed (the composed SQL runs
        # cleanly). Runs AFTER business_cycles so the graph context can read this
        # run's cycle + validation evidence. Ungroundable metrics stay
        # declared-with-reason; composed-but-unexecutable stay grounded-with-reason.
        self._progress.phase = "metrics"
        metrics_outcome = await workflow.execute_activity(
            "metrics",
            scoped,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            heartbeat_timeout=_HEARTBEAT_TIMEOUT,
            retry_policy=_LLM_RETRY,
        )

        # Promote gate (DAT-845): a framed vertical that declared ZERO across all
        # three lifecycle families (validation ⊕ business_cycles ⊕ metrics) has no
        # operating model to seal. Refuse to flip the (catalog, "operating_model")
        # head — an empty promote is indistinguishable downstream from a real one
        # (the cockpit reads head-presence as "analyzed"), and a re-run can't fix a
        # vertical that declares nothing, so failing would only loop. COMPLETE
        # instead with the typed ``nothing_declared`` terminal outcome: loud +
        # queryable (result.outcome + the terminal progress phase), a
        # misconfiguration signal the briefing surfaces — never a benign variant.
        # Each phase threads its own declared count on PhaseOutcome; only an
        # explicit 0 from ALL three refuses (a None — no such signal — promotes).
        declared_counts = (
            validation_outcome.declared,
            cycles_outcome.declared,
            metrics_outcome.declared,
        )
        if all(count == 0 for count in declared_counts):
            self._progress.phase = "nothing_declared"
            workflow.logger.warning(
                "operating_model_nothing_declared: no operating model exists — vertical "
                "declares no validations, cycles, or metrics; head NOT promoted "
                "(run_id=%s vertical=%s)",
                run_id,
                vertical,
            )
            return OperatingModelResult(
                run_id=run_id,
                validation_summary=validation_outcome.summary,
                outcome="nothing_declared",
            )

        # Terminal promote: flip (catalog, "operating_model") to this run.
        self._progress.phase = "operating_model_promote"
        await workflow.execute_activity(
            "operating_model_promote",
            run,
            result_type=PhaseOutcome,
            start_to_close_timeout=_TIMEOUT,
            retry_policy=_RETRY,
        )

        self._progress.phase = "done"
        return OperatingModelResult(
            run_id=run_id,
            validation_summary=validation_outcome.summary,
            outcome="promoted",
        )


# --- Orchestration workflows (DAT-708) ----------------------------------------
#
# The two short-lived per-trigger orchestration workflows, ported from the
# cockpit's TS worker (ADR-0020 supersedes ADR-0014): Temporal discourages
# workflow workers outside authentic Node.js, and the cockpit runs under Bun —
# DAT-705 proved workflow-interceptor headers silently never leave its vm
# sandbox. Here they run on the SAME worker and queue as the engine children
# they start, so the children are native Python→Python (no cross-language hop)
# and inherit the parent's task queue. The four cockpit-bound activities
# (cockpit_db run writers + the DAT-551 grounding-teach agent) are scheduled BY
# NAME on the workspace's cockpit activity queue — ``cockpit-<ws>``, derived
# from the input ``workspace_id`` via ``cockpit_task_queue_for`` (DAT-818: one
# cockpit per workspace, so the queue is identity, not config) — same by-string
# idiom as the phase activities above, plus one ``task_queue=`` kwarg. Neither
# workflow narrates: the cockpit's server-side completion-watcher narrates on
# each run row's done edge.

# The cockpit_db run writers each stage is bracketed with — quick local writes,
# so a short timeout; retried because losing the bracket (not the stage) is the
# recoverable failure.
_COCKPIT_WRITE_TIMEOUT = timedelta(minutes=1)
_COCKPIT_WRITE_RETRY = RetryPolicy(maximum_attempts=3)

# The grounding-teach agent (DAT-551) — an LLM tool-loop, so a much longer
# timeout than the cockpit_db writes, and only one retry (a re-run is expensive
# and the loop tolerates a failed round by stopping, never crashing).
_GROUNDING_AGENT_TIMEOUT = timedelta(minutes=10)
_GROUNDING_AGENT_RETRY = RetryPolicy(maximum_attempts=2)

# Default replay budget when the trigger carries none.
_DEFAULT_GROUNDING_ATTEMPTS = 3


@dataclass(frozen=True)
class GroundingStep:
    """What the grounding loop does after one ``assessAndGround`` round.

    ``reason``/``note`` are set only for ``action="surface"`` — why the run
    parks (a human-judgement gap vs an exhausted replay budget) and what to
    tell the human.
    """

    action: Literal["replay", "surface", "done"]
    reason: Literal["judgement", "exhausted"] | None = None
    note: str | None = None


def decide_grounding_step(verdict: AssessAndGroundResult, attempts_remaining: int) -> GroundingStep:
    """Pure decision for one grounding-loop round (DAT-551 P3c).

    Pure over the agent's verdict + the remaining replay budget, so it is
    unit-testable without a Temporal server (``test_grounding_step.py``) — the
    loop's control flow around it is smoke-covered, like the phase chains.

    - Applied teaches AND attempts left → replay (re-run add_source to
      re-measure); a flagged judgement gap is re-evaluated next round on fresh
      readiness, so replay wins while the budget lasts.
    - Applied teaches BUT out of attempts → surface (couldn't converge in
      budget).
    - No teaches applied → nothing mechanical left: surface if a judgement gap
      remains, else done (clean).
    """
    if verdict.appliedCount > 0:
        if attempts_remaining > 0:
            return GroundingStep(action="replay")
        return GroundingStep(action="surface", reason="exhausted", note=verdict.judgementNote)
    if verdict.needsJudgement:
        return GroundingStep(action="surface", reason="judgement", note=verdict.judgementNote)
    return GroundingStep(action="done")


# The type parameter is constrained to the three engine stage results so
# ``_run_stage`` returns the caller's concrete type (the grounding loop reads
# ``AddSourceResult.tables`` off it).
async def _run_stage[StageResultT: (AddSourceResult, BeginSessionResult, OperatingModelResult)](
    *,
    workflow_type: str,
    payload: AddSourceInput | BeginSessionInput | OperatingModelInput,
    result_type: type[StageResultT],
    workflow_id: str,
    workspace_id: str,
    stage: RunStage,
    kind: RunKind,
    conversation_id: str | None,
) -> StageResultT | None:
    """Run one engine stage as a child workflow, bracketed by cockpit_db writes.

    The shared stage runner both orchestration workflows use: start the engine
    child (by its registered type name — the bracket stays monomorphic over the
    three stage types; ``result_type`` reconstructs the concrete result), record
    the run in cockpit_db with the child's REAL execution id (DAT-595 —
    recording post-start under the reused ``addsource-<ws>``-style id keeps
    every run a distinct ``(workflowId, runId)`` row), await it, mark it
    terminal. The bracket writes land on the workspace's own cockpit activity
    queue, derived from ``workspace_id`` (DAT-818). Returns the child's result,
    or None on failure — a failed stage NEVER raises out of the workflow: the
    run is marked failed (if recorded) and the caller stops (a failed stage has
    no clean follow-on).

    ``ParentClosePolicy.ABANDON``: the orchestration workflow finishing (or
    being terminated) must not kill a running engine stage — let it complete
    independently. A grounding REPLAY reuses the same child workflow id; the
    prior execution is already closed by then, so the default id-reuse policy
    (allow-duplicate-when-closed) permits it.

    Recording post-start is orphan-safe HERE: ``recordRun`` is a durable
    activity, so a worker crash replays the workflow and re-runs it (the
    ABANDON'd child keeps going). One residual window: if ``recordRun`` itself
    failed terminally AFTER the child started, the runId is set but no row
    exists — the mark-failed below is a harmless no-op, and the ABANDON'd child
    runs to completion INVISIBLY (the reconcile keys off recorded rows).
    Accepted: that needs a total activity failure after 3 retries, and the
    engine work still completes.
    """
    cockpit_task_queue = cockpit_task_queue_for(workspace_id)
    run_id: str | None = None
    try:
        child = await workflow.start_child_workflow(
            workflow_type,
            payload,
            id=workflow_id,
            result_type=result_type,
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
        )
        run_id = child.first_execution_run_id

        await workflow.execute_activity(
            "recordRun",
            RecordRunInput(
                workspaceId=workspace_id,
                kind=kind,
                stage=stage,
                workflowId=workflow_id,
                runId=run_id,
                conversationId=conversation_id,
            ),
            task_queue=cockpit_task_queue,
            start_to_close_timeout=_COCKPIT_WRITE_TIMEOUT,
            retry_policy=_COCKPIT_WRITE_RETRY,
        )

        # The by-string handle is untyped (Any); ``result_type`` above makes the
        # converter reconstruct the concrete model, so this annotation is true.
        result: StageResultT = await child
        await workflow.execute_activity(
            "markRunStatus",
            args=[workflow_id, run_id, "completed"],
            task_queue=cockpit_task_queue,
            start_to_close_timeout=_COCKPIT_WRITE_TIMEOUT,
            retry_policy=_COCKPIT_WRITE_RETRY,
        )
        return result
    except Exception as err:
        # ``except Exception`` deliberately misses CancelledError (a
        # BaseException) so cancellation still propagates clean.
        workflow.logger.warning(
            "orchestration stage failed: stage=%s workflow_id=%s err=%s",
            stage,
            workflow_id,
            err,
        )
        # Mark failed best-effort, but only if the child started (we have a
        # real runId); a pre-start failure recorded nothing, so there is
        # nothing to mark.
        if run_id is not None:
            try:
                await workflow.execute_activity(
                    "markRunStatus",
                    args=[workflow_id, run_id, "failed"],
                    task_queue=cockpit_task_queue,
                    start_to_close_timeout=_COCKPIT_WRITE_TIMEOUT,
                    retry_policy=_COCKPIT_WRITE_RETRY,
                )
            except Exception as mark_err:
                workflow.logger.warning(
                    "orchestration stage mark-failed write failed: workflow_id=%s err=%s",
                    workflow_id,
                    mark_err,
                )
        return None


@workflow.defn(name="groundingLoopWorkflow")
class GroundingLoopWorkflow:
    """The onboarding import + autonomous grounding-teach loop (DAT-609/708).

    Started by the ``select`` import trigger only, under the per-workspace id
    ``grounding-<ws>`` (a manual replay is a DIRECT engine start — the user is
    doing teach+replay by hand, so it must NOT re-enter this autonomous loop).
    Short-lived, per-trigger: all state rides the start payload — no signals,
    no cross-run state.

    ONE grounding round per execution — re-run, don't loop: the body is
    straight-line (import → assess → decide), and a ``replay`` verdict
    tail-calls the workflow via ``continue_as_new`` with the decremented budget
    on the input message. Temporal carries the loop state durably in the
    payload, each execution's history is one stage + one assess, and the
    replay bound is structural — a round can only recur through the input
    contract, never through workflow-local loop state. A human-judgement gap
    or an exhausted budget parks the run ``awaiting_input`` (the "Needs you"
    inbox) and completes — it NEVER blocks on human input.
    """

    @workflow.run
    async def run(self, payload: GroundingLoopInput) -> None:
        # 1) The import stage. The FIRST execution carries the originating
        #    conversation id (the watcher tracks its progress widget, but
        #    kind="onboarding" ⇒ no chat narration, DAT-597); a continuation
        #    is an INTERNAL autonomous replay and carries conversation_id=None
        #    — it must not fire the watcher's narration (the user already
        #    heard the import landed; the loop's outcome surfaces via the run
        #    monitor / awaiting_input, not N chat messages).
        imported = await _run_stage(
            workflow_type="addSourceWorkflow",
            payload=AddSourceInput(
                workspace_id=payload.workspace_id,
                sources=payload.sources,
                verticals=payload.verticals,
            ),
            result_type=AddSourceResult,
            workflow_id=payload.workflow_id,
            workspace_id=payload.workspace_id,
            stage="add_source",
            kind="onboarding",
            conversation_id=payload.conversation_id,
        )
        if imported is None:
            return  # import failed (already marked) — nothing to ground.

        # 2) One grounding round (the autonomy step). The typed table ids from
        #    THIS execution's import are the readiness scope the agent assesses.
        table_ids = [table.typed_table_id for table in imported.tables]
        if not table_ids:
            return
        attempts_remaining = (
            payload.number_of_attempts
            if payload.number_of_attempts is not None
            else _DEFAULT_GROUNDING_ATTEMPTS
        )
        cockpit_task_queue = cockpit_task_queue_for(payload.workspace_id)

        try:
            verdict = await workflow.execute_activity(
                "assessAndGround",
                AssessAndGroundInput(tableIds=table_ids, attemptsRemaining=attempts_remaining),
                task_queue=cockpit_task_queue,
                result_type=AssessAndGroundResult,
                start_to_close_timeout=_GROUNDING_AGENT_TIMEOUT,
                retry_policy=_GROUNDING_AGENT_RETRY,
            )
        except Exception as err:
            # The assessment died (LLM error after retries) — stop grounding,
            # don't crash. The import itself is already recorded complete.
            # (Misses CancelledError deliberately.)
            workflow.logger.warning(
                "grounding assess failed: workflow_id=%s err=%s",
                payload.workflow_id,
                err,
            )
            return

        step = decide_grounding_step(verdict, attempts_remaining)
        if step.action == "done":
            return
        if step.action == "surface":
            try:
                await workflow.execute_activity(
                    "markRunAwaitingInput",
                    args=[payload.workflow_id, step.note],
                    task_queue=cockpit_task_queue,
                    start_to_close_timeout=_COCKPIT_WRITE_TIMEOUT,
                    retry_policy=_COCKPIT_WRITE_RETRY,
                )
            except Exception:
                # Best-effort park: a lost awaiting_input note must not fail
                # the run's clean exit (the activity itself is best-effort too
                # — it logs, never throws — so this only catches
                # activity-infrastructure failure).
                workflow.logger.warning(
                    "grounding park write failed: workflow_id=%s",
                    payload.workflow_id,
                )
            workflow.logger.info(
                "grounding surfaced for input: workflow_id=%s reason=%s",
                payload.workflow_id,
                step.reason,
            )
            return

        # action == "replay": teaches were applied — re-run add_source to
        # re-measure, as a FRESH execution. The decremented budget rides the
        # input message (Temporal persists it), the null conversation id makes
        # the continuation a non-narrating replay, and the workflow id chain
        # stays `grounding-<ws>` (single-flight holds across continuations —
        # the chain counts as running until the final execution completes).
        workflow.continue_as_new(
            GroundingLoopInput(
                workspace_id=payload.workspace_id,
                workflow_id=payload.workflow_id,
                sources=payload.sources,
                verticals=payload.verticals,
                conversation_id=None,
                number_of_attempts=attempts_remaining - 1,
            )
        )


@workflow.defn(name="sessionCascadeWorkflow")
class SessionCascadeWorkflow:
    """begin_session → (clean) operating_model (DAT-609/708).

    Started under the per-workspace id ``session-<ws>``. Short-lived,
    per-trigger: the cascade is unconditional on a clean begin_session
    (autonomy) — no signals, no breaker. A failed begin_session stops here (no
    cascade); the run is already marked and the watcher narrates the failure.
    """

    @workflow.run
    async def run(self, payload: SessionCascadeInput) -> None:
        began = await _run_stage(
            workflow_type="beginSessionWorkflow",
            payload=BeginSessionInput(
                workspace_id=payload.workspace_id,
                tables=payload.tables,
                verticals=payload.verticals,
            ),
            result_type=BeginSessionResult,
            workflow_id=payload.workflow_id,
            workspace_id=payload.workspace_id,
            stage="begin_session",
            kind="begin_session",
            conversation_id=payload.conversation_id,
        )
        if began is None:
            return

        # Auto-cascade: a clean begin_session advances into operating_model.
        # The OM child id is derived from the workspace (DAT-562 — one per
        # workspace), reusing the same verticals + conversation id.
        # operating_model re-reads the session's table set from the catalog
        # head (DAT-506), so no table set on the wire.
        workflow.logger.info(
            "session cascade → operating_model: workspace_id=%s",
            payload.workspace_id,
        )
        await _run_stage(
            workflow_type="operatingModelWorkflow",
            payload=OperatingModelInput(
                workspace_id=payload.workspace_id,
                verticals=payload.verticals,
            ),
            result_type=OperatingModelResult,
            workflow_id=operating_model_workflow_id(payload.workspace_id),
            workspace_id=payload.workspace_id,
            stage="operating_model",
            kind="begin_session",
            conversation_id=payload.conversation_id,
        )
