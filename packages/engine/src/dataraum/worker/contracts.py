"""Worker I/O contracts (DAT-344, redesigned per-boundary in DAT-370).

Deliberately engine-free: imports nothing but the stdlib + Pydantic. Both the
activity runner (:mod:`dataraum.worker.activity`, which pulls in the whole engine)
and the workflows (:mod:`dataraum.worker.workflows`, which run in Temporal's
determinism sandbox) import these models from here — so the workflow module never
drags SQLAlchemy/DuckDB/the registry into the sandbox.

The shapes are **typed per boundary**, not one uniform envelope: ``import``
discovers raw tables, ``typing`` mints a typed id, the analytics phases are
scoped to a single typed table, and the workflows thread an identity header. The
scheduler-era ``PhaseActivityInput``/``PhaseActivityResult`` god-envelope (one
shape for all phases, with a ``table_ids`` field downstream phases ignored) is
gone — the fan-out (DAT-370) made the per-boundary inputs concrete.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel


@dataclass
class TableProgress:
    """Per-table fan-out status for the add_source progress snapshot (DAT-406+).

    One entry per child ``ProcessTableWorkflow`` so the cockpit can show *which*
    tables are in flight / done / failed, not just an aggregate count.

    Attributes:
        raw_table_id: The engine id of the raw table this child processes. The
            cockpit resolves it to a human table name from the metadata
            ``tables`` table — the engine stays id-only (names live in the DB the
            cockpit reads), so no name lookup leaks into the determinism sandbox.
        status: ``"running"`` once fanned out, ``"done"`` when the child
            resolves, ``"failed"`` if that child errored.
    """

    raw_table_id: str
    status: str


@dataclass
class ProgressFailure:
    """Why an add_source run ended badly — surfaced in the cockpit, not buried.

    A polling cockpit reads this off the snapshot instead of opening the Temporal
    UI for the failure detail.

    Attributes:
        message: The root-cause message — Temporal's Activity/ChildWorkflow
            error chain unwrapped to the phase's own non-retryable failure text.
        phase: The stage in flight when it failed (the snapshot's ``phase``).
        table_id: The raw table whose child failed, when the failure is
            table-scoped; ``None`` for source-level stages (``import`` /
            ``semantic_per_column`` / ``detect``).
    """

    message: str
    phase: str
    table_id: str | None = None


@dataclass
class ProgressSnapshot:
    """Parent-level progress for ``addSourceWorkflow``, served by ``get_progress`` (DAT-406).

    Read-only snapshot the cockpit polls via the Temporal Client's
    ``query`` API while the parent runs (queries answer against current
    state even while ``@workflow.run`` is blocked awaiting the fan-out).
    The workflow body advances ``phase`` before each stage, seeds the
    per-table ``tables`` list once the fan-out set is known, and flips each
    entry (plus ``tables_completed``) as its child resolves.

    Deliberately a plain stdlib ``@dataclass`` (NOT a Pydantic engine
    type): it carries only primitives + nested dataclasses, and the worker's
    ``pydantic_data_converter`` serializes it to the JSON shape the cockpit
    Client (a TS process that cannot import Python types) consumes. That shape
    is the cross-package contract mirrored in
    ``packages/cockpit/src/temporal/types.ts`` — evolve the two in lockstep
    (a field rename/retype here is a cross-PACKAGE change).

    Attributes:
        phase: The stage the parent is currently in. Advances
            ``"import"`` → ``"processing_tables"`` → ``"semantic_per_column"``
            → ``"detect"`` → ``"done"``. A plain string (not an enum) so the
            wire value stays a bare JSON string for the cockpit.
        tables_total: The number of child ``ProcessTableWorkflow``s fanned
            out. ``0`` until ``import`` enumerates the raw tables (or a
            replay narrows the set); set once before the fan-out awaits.
        tables_completed: How many children have resolved so far —
            monotonically increasing toward ``tables_total`` during the
            ``"processing_tables"`` phase.
        tables: One :class:`TableProgress` per fanned-out child — the named
            steps behind the count. Seeded all-``"running"`` at fan-out (after
            ``import`` recorded the ids → deterministic), flipped to
            ``"done"``/``"failed"`` as each child resolves.
        failure: Set when the run ends badly (any stage), so a polling cockpit
            sees the reason without opening the Temporal UI; ``None`` while the
            run is healthy.
    """

    phase: str
    tables_total: int = 0
    tables_completed: int = 0
    tables: list[TableProgress] = field(default_factory=list)
    failure: ProgressFailure | None = None


class SourceIdentity(BaseModel):
    """The identity header the workflows carry into every activity.

    Pure data — IDs the runner uses to reconstruct source identity + phase
    config from the workspace substrate. ``session_id`` is the per-run FK for
    session-scoped rows (the workflow execution processing the source), NOT a
    connection scope. ``workspace_id`` is the (future, DAT-364) routing key the
    runner checks against the worker's bound workspace.
    """

    workspace_id: str
    source_id: str
    session_id: str
    vertical: str | None = None


class SessionIdentity(BaseModel):
    """The identity header ``beginSessionWorkflow`` carries into every activity.

    Source-free by construction (see feedback-source-dies-at-addsource-boundary):
    a session past the add_source boundary composes typed tables that may span
    sources, so "source" is meaningless here. The identity stays small —
    ``workspace_id`` (the DAT-364 routing key the runner checks against the
    worker's bound workspace) + ``session_id`` (the per-run FK + the key that
    resolves the selected table set via ``session_tables``). The session's
    ``vertical`` (frame ontology) is read off the ``InvestigationSession`` row,
    not threaded here — it is session state, not part of the identity.
    """

    workspace_id: str
    session_id: str


class ReplayScope(BaseModel):
    """Scope for a teach-driven replay of ``addSourceWorkflow`` (DAT-343).

    Attached to ``AddSourceInput`` (and propagated to child
    ``ProcessTableInput``) when the cockpit's teach tool starts the
    workflow to re-apply a teach. Optional on the inputs because the
    initial-run call from the cockpit doesn't carry one — every
    pre-replay invocation reads the field as ``None`` and the workflow
    body's gates take the initial-run path.

    Attributes:
        from_phase: Phase name to start the replay at. The workflow runs
            this phase and everything downstream of it; everything before
            is skipped. One of ``"import"``, ``"typing"``, or
            ``"semantic_per_column"`` in slice 1.
        raw_table_ids: Per-child scope filter. ``None`` = all tables (the
            initial-run shape, used by source-wide replays like
            ``null_value``). A non-empty list narrows the parent's fan-out
            to those raw table ids (per-table replays like
            ``type_pattern``). An empty list ``[]`` means "no children" —
            used for source-tail-only replays (``concept_property``) where
            the parent re-runs ``semantic_per_column`` + the terminal
            ``detect`` step without re-typing anything.
    """

    from_phase: str
    raw_table_ids: list[str] | None = None


class PhaseOutcome(BaseModel):
    """Lean per-activity result: outcome + human summary.

    Returned by the activities that don't mint an id (the analytics phases,
    ``semantic_per_column``, the terminal ``detect``). A deterministic phase *failure*
    never travels in here — it is raised as a non-retryable ``ApplicationError``
    by the activity wrapper — so a returned outcome is always ``completed`` or
    ``skipped``. Carries no ``table_ids``/``outputs`` god-fields.
    """

    status: str
    summary: str = ""


class ImportResult(BaseModel):
    """``import`` activity result — the discovered raw table ids (the fan-out source).

    Authoritative whether import ran or was skipped (source already imported):
    the activity reads the source's raw tables from the substrate, so the parent
    always has the ids it needs to fan out.
    """

    raw_table_ids: list[str]


class ProcessTableInput(BaseModel):
    """Input to ``ProcessTableWorkflow`` (and its ``typing`` activity).

    One raw table is the unit of work; the child workflow runs the table-local
    chain over it. ``replay`` (DAT-343) is set when the parent is replaying
    this child for a teach — the child gates which of its activities run.
    """

    identity: SourceIdentity
    raw_table_id: str
    replay: ReplayScope | None = None


class TypingResult(BaseModel):
    """``typing`` activity result — the freshly minted (or resolved) typed id.

    ``typing`` mints a uuid4 typed id ``!=`` the raw id. It travels in this
    result, so it is persisted in Temporal history and replayed verbatim — the
    downstream analytics activities read it from the child workflow, never
    recompute it.

    Also the return shape of ``lookup_typed_table_id`` (DAT-343), which the
    child workflow calls when a teach replay starts past ``typing`` and the
    typed id has to be re-read from substrate.
    """

    typed_table_id: str


class ReplayCleanupInput(BaseModel):
    """Input to the ``replay_cleanup_for_phase`` activity (DAT-343 / DAT-373).

    Carried by the workflow before EVERY phase that re-executes under a teach
    replay (DAT-373), not just the ``from_phase``; the activity invokes that
    phase's owner-scoped ``replay_cleanup`` so its existing ``should_skip``
    doesn't bail on the re-run. Pre-DAT-373 only the entry phase was cleaned and
    downstream phases rode the now-removed typed-Table cascade.

    Attributes:
        identity: source identity header (same as every other phase activity).
        phase_name: which phase's ``replay_cleanup`` to invoke — one of the
            chain phases at-or-after ``replay.from_phase`` (plus the always-rerun
            source-level reduce).
        table_ids: scope passed through to the phase's ``replay_cleanup``.
            For ``typing`` / ``import`` this is the raw table id(s); for the
            table-local analytics phases it is the typed table id their rows
            hang off. An empty list collapses two source-wide shapes onto one
            wire value: (1) ``replay.raw_table_ids is None`` (e.g. ``null_value``
            → ``ImportPhase.replay_cleanup``, which ignores the scope and drops
            everything for the source); and (2) ``replay.raw_table_ids == []``
            (e.g. ``concept_property`` → ``SemanticPerColumnPhase.replay_cleanup``,
            intrinsically source-wide).
    """

    identity: SourceIdentity
    phase_name: str
    table_ids: list[str] = []


class TableScopedInput(BaseModel):
    """Input to the per-table analytics activities — one typed table.

    ``table_id`` is the *typed* table id from :class:`TypingResult`; the phase
    scopes its work to exactly this table.
    """

    identity: SourceIdentity
    table_id: str


class ProcessTableResult(BaseModel):
    """``ProcessTableWorkflow`` result — the raw→typed mapping for one table."""

    raw_table_id: str
    typed_table_id: str


class SessionScopedInput(BaseModel):
    """Input to a begin_session activity — session identity + the typed table set.

    The session-scoped analogue of :class:`TableScopedInput`, but plural: the
    begin_session phases are cross-table (relationships are meaningless on one
    table), so the activity carries the whole selection as an array of typed
    table ids. The array is the execution scope, threaded from the workflow
    input (``begin_session(tables=[…])``) — the same set ``begin_session_select``
    persists to ``session_tables`` for provenance.
    """

    identity: SessionIdentity
    table_ids: list[str]


class SessionReplayCleanupInput(BaseModel):
    """Input to the ``session_replay_cleanup_for_phase`` activity (DAT-401).

    The source-free sibling of :class:`ReplayCleanupInput`: a begin_session
    teach replay clears a phase's own rows (candidate relationships, table
    entities) before the re-run, scoped to ``table_ids``. Mirrors the add_source
    cleanup path but carries a :class:`SessionIdentity` (no ``source_id``).
    """

    identity: SessionIdentity
    phase_name: str
    table_ids: list[str] = []


class BeginSessionInput(BaseModel):
    """Input to ``beginSessionWorkflow`` — session identity + the selected tables.

    Unlike ``add_source`` (whose table set is discovered by ``import``), the
    begin_session table set is the user's explicit selection of already-typed
    tables, so it travels in the input as ``tables`` (an array of typed table
    ids, possibly spanning sources). ``replay`` (DAT-343 pattern) is set when
    re-running after a teach; ``None`` is the initial-run shape.
    """

    identity: SessionIdentity
    tables: list[str]
    replay: ReplayScope | None = None


class BeginSessionResult(BaseModel):
    """``beginSessionWorkflow`` result — the session + the tables it composed."""

    session_id: str
    table_ids: list[str]


class AddSourceInput(BaseModel):
    """Input to ``AddSourceWorkflow`` — source identity + optional replay scope.

    The table set is unknown until ``import`` enumerates it (a source is a dir /
    DB recipe), so the parent carries identity only and discovers tables at run.

    ``replay`` (DAT-343) is set by the cockpit's ``replay`` tool when re-running
    after a teach; ``None`` is the initial-run shape.
    """

    identity: SourceIdentity
    replay: ReplayScope | None = None


class AddSourceResult(BaseModel):
    """``AddSourceWorkflow`` result — the discovered raw tables + per-table outcomes."""

    raw_table_ids: list[str]
    tables: list[ProcessTableResult]


# --- Workflow ID convention (DAT-364) ----------------------------------------
#
# Every Temporal workflow ID encodes the ``workspace_id`` as its first segment.
# Slice 1 runs single-workspace, so the segment is constant today — but threading
# it through now means slice 2+ multi-workspace routing (DAT-357) is a no-op
# rename instead of an audit of every ``start_workflow``/``getHandle`` call site,
# and two workspaces can never collide on a shared ``source_id``. The ``ws_<id>``
# isolation guard in :mod:`dataraum.worker.activity` is the data-side cornerstone;
# this is its workflow-ID counterpart. See the ``durable-execution-lean`` memory.
#
# These helpers live here (the engine-free contracts module the determinism
# sandbox imports through ``imports_passed_through``) so the workflow body can
# build child IDs without dragging the engine into the sandbox. ``workspace_id``
# is a ``str`` (raw UUID with dashes, or the ``"test"`` sentinel) — Temporal
# workflow IDs have no charset restriction, so we keep it verbatim for grep-able
# IDs in the Temporal UI rather than the underscored ``ws_<id>`` schema form.
#
# Parent IDs are owned by the cockpit Client (it starts the workflow); the TS
# side mirrors this convention in ``packages/cockpit/src/temporal/workflow-id.ts``.


def add_source_workflow_id(workspace_id: str, source_id: str) -> str:
    """Workflow ID for the parent ``addSourceWorkflow`` of one source.

    Reused across teach replays of the same source (with
    ``WorkflowIdReusePolicy.ALLOW_DUPLICATE``) so Temporal groups iterations
    under one ID. Mirrored cockpit-side; the cockpit is the caller that starts
    the parent, so this Python helper exists for tests + the child-ID builder.
    """
    return f"addsource-{workspace_id}-{source_id}"


def begin_session_workflow_id(workspace_id: str, session_id: str) -> str:
    """Workflow ID for ``beginSessionWorkflow`` of one session.

    A begin_session run is keyed by its session id (not a source — a session
    spans sources). Reused across teach replays of the same session (with
    ``WorkflowIdReusePolicy.ALLOW_DUPLICATE``) so Temporal groups iterations
    under one ID. The cockpit is the caller that starts the workflow (slice
    2.0c); this Python helper exists for tests + the ID convention.
    """
    return f"beginsession-{workspace_id}-{session_id}"


def process_table_workflow_id(workspace_id: str, source_id: str, raw_table_id: str) -> str:
    """Child ``processTableWorkflow`` ID for one raw table under a source.

    Deterministic + collision-free so replay stays stable: the same raw table
    re-runs under the same child ID across teach iterations. Prefixed with the
    parent's ``addsource-{workspace_id}-{source_id}`` so children are greppable
    under their parent in the Temporal UI (the prefix is a naming convention, not
    a Temporal-native hierarchy), and two workspaces sharing a ``source_id`` get
    distinct child IDs.
    """
    return f"{add_source_workflow_id(workspace_id, source_id)}-table-{raw_table_id}"
