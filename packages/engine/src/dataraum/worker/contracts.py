"""Worker I/O contracts (DAT-344; identity collapsed to ``RunRef`` in DAT-506/426).

Deliberately engine-free: imports nothing but the stdlib + Pydantic. Both the
activity runner (:mod:`dataraum.worker.activity`, which pulls in the whole engine)
and the workflows (:mod:`dataraum.worker.workflows`, which run in Temporal's
determinism sandbox) import these models from here — so the workflow module never
drags SQLAlchemy/DuckDB/the registry into the sandbox.

The wire identity is **source-free and session-free** (DAT-506/426): there is no
``SourceIdentity``/``SessionIdentity`` envelope and no ``session_id``/``source_id``
on the identity. Activities are threaded with a minimal :class:`RunRef`
(``workspace_id`` + the run's ``run_id``). A source id appears on the wire in
exactly ONE place — the ``import`` activity's explicit ``source_id`` argument,
which runs before any ``Table`` row exists and so cannot resolve relationally.
Every phase past import is table-scoped or run-scoped and resolves source
provenance relationally via ``tables.source_id`` (the FK on the row).

The workspace ``verticals`` (frame ontologies, by name) ride on the workflow
INPUT contracts — the driver sources them from ``workspaces`` (cockpit-owned) and
the per-call activities read the resolved name off their phase config.

The shapes are **typed per boundary**, not one uniform envelope: ``import``
discovers raw tables, ``typing`` mints a typed id, the analytics phases are
scoped to a single typed table, the begin_session phases to the whole selection.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel, Field


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
    """Why a workflow run ended badly — surfaced in the cockpit, not buried.

    A polling cockpit reads this off the snapshot instead of opening the Temporal
    UI for the failure detail. Served by both progress-bearing workflows
    (add_source DAT-406, begin_session DAT-435).

    Attributes:
        message: The root-cause message — Temporal's Activity/ChildWorkflow
            error chain unwrapped to the phase's own non-retryable failure text.
        phase: The stage in flight when it failed (the snapshot's ``phase``).
        table_id: The raw table whose child failed, when the failure is
            table-scoped; ``None`` for add_source's run-level stages (``import``
            / ``check_column_limit`` / ``semantic_per_column`` / ``detect`` /
            ``promote``) and ALWAYS ``None`` for begin_session (sequential, no
            table-scoped stages).
    """

    message: str
    phase: str
    table_id: str | None = None


@dataclass
class ProgressSnapshot:
    """Workflow progress served by a ``get_progress`` query (DAT-406, DAT-435).

    One shape for every cockpit-polled workflow — ``addSourceWorkflow`` (the
    original, DAT-406) and ``beginSessionWorkflow`` (DAT-435) serve it from the
    same query name, so the cockpit's poll/types need no per-workflow branch.
    Read-only snapshot the cockpit polls via the Temporal Client's
    ``query`` API while the workflow runs (queries answer against current
    state even while ``@workflow.run`` is blocked awaiting a stage).
    The workflow body advances ``phase`` before each stage; add_source
    additionally seeds the per-table ``tables`` list once the fan-out set is
    known and flips each entry (plus ``tables_completed``) as its child
    resolves — a sequential workflow (begin_session) leaves the fan-out
    fields at their empty defaults.

    Deliberately a plain stdlib ``@dataclass`` (NOT a Pydantic engine
    type): it carries only primitives + nested dataclasses, and the worker's
    ``pydantic_data_converter`` serializes it to the JSON shape the cockpit
    Client (a TS process that cannot import Python types) consumes. That shape
    is the cross-package contract mirrored in
    ``packages/cockpit/src/temporal/types.ts`` — evolve the two in lockstep
    (a field rename/retype here is a cross-PACKAGE change).

    Attributes:
        phase: The stage the workflow is currently in. add_source advances
            ``"import"`` → ``"check_column_limit"`` → ``"processing_tables"``
            → ``"semantic_per_column"`` → ``"detect"`` → ``"promote"`` →
            ``"done"``; begin_session walks its sequential chain
            (``"begin_session_select"`` → … → ``"session_promote_to_latest"``
            → ``"done"`` — the authoritative order is the workflow body). A
            plain string (not an enum) so the wire value stays a bare JSON
            string for the cockpit and a new phase is not a contract change.
        tables_total: The number of child ``ProcessTableWorkflow``s fanned
            out. ``0`` until ``import`` enumerates the raw tables (or a
            replay narrows the set); set once before the fan-out awaits.
            Stays ``0`` for begin_session (sequential, no children).
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


class RunRef(BaseModel):
    """The source-free, session-free run reference threaded into every activity.

    Pure data — the minimal identity an activity needs (DAT-506/426). There is no
    ``session_id`` (sessions live in cockpit_db, never the engine) and no
    ``source_id`` (a run is over a SET of objects spanning 1–N sources; source
    provenance is resolved relationally via ``tables.source_id`` past import).

    ``workspace_id`` is the routing key (and the workflow-id segment the cockpit
    owns). ``run_id`` is the snapshot version axis (DAT-413/408): minted once by
    each workflow's ``run`` (``workflow.uuid4``), threaded into every activity so a
    run's metadata rows share one id, and RETURNED in the workflow result so the
    cockpit can store it and replay by it. The cockpit's initial-run call never
    sets it; the workflow stamps it before the first activity, so a ``None`` at an
    activity that persists run-versioned rows is a caller bug (fail loud).
    """

    workspace_id: str
    run_id: str | None = None


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


class ImportInput(BaseModel):
    """Input to the per-source ``import`` activity — the ONE source-bearing wire shape.

    ``import`` runs once per source in a run (DAT-422): it loads ONE source's
    files into ``lake.raw.*`` before any ``Table`` row exists, so it cannot
    resolve its source relationally — the ``source_id`` is the only source id on
    the whole wire, set per-call by the workflow from ``AddSourceInput.sources``.
    ``vertical`` (by name) rides along so the phase config is built off the
    workflow input.
    """

    run: RunRef
    source_id: str
    vertical: str


class ImportResult(BaseModel):
    """``import`` activity result — the discovered raw table ids (the fan-out source).

    Authoritative whether import ran or was skipped (source already imported):
    the activity reads the source's raw tables from the substrate, so the parent
    always has the ids it needs to fan out.
    """

    raw_table_ids: list[str]


class RunPhaseInput(BaseModel):
    """Input to a run-level add_source phase activity — the run ref + the vertical.

    The source-free run-level add_source activities that build a phase config
    (``semantic_per_column``) carry the workspace ``vertical`` (by name) alongside
    the run ref so the config is built off the workflow input. ``detect`` /
    ``promote_to_latest`` take the bare :class:`RunRef` (they build no LLM config).
    """

    run: RunRef
    vertical: str


class ProcessTableInput(BaseModel):
    """Input to ``ProcessTableWorkflow`` (and its ``typing`` activity).

    One raw table is the unit of work; the child workflow runs the table-local
    chain over it. Source-free: the table resolves its source relationally via
    its row FK. ``vertical`` (by name) rides along so the table-local phases build
    their config off the workflow input.
    """

    run: RunRef
    raw_table_id: str
    vertical: str


class TypingResult(BaseModel):
    """``typing`` activity result — the freshly minted (or resolved) typed id.

    ``typing`` mints a uuid4 typed id ``!=`` the raw id. It travels in this
    result, so it is persisted in Temporal history and replayed verbatim — the
    downstream analytics activities read it from the child workflow, never
    recompute it.
    """

    typed_table_id: str


class TableScopedInput(BaseModel):
    """Input to the per-table analytics activities — one typed table.

    ``table_id`` is the *typed* table id from :class:`TypingResult`; the phase
    scopes its work to exactly this table and resolves its source relationally.
    ``vertical`` (by name) rides along so the phase config is built off the
    workflow input.
    """

    run: RunRef
    table_id: str
    vertical: str


class ProcessTableResult(BaseModel):
    """``ProcessTableWorkflow`` result — the raw→typed mapping for one table."""

    raw_table_id: str
    typed_table_id: str


class RunScopedInput(BaseModel):
    """Input to an add_source run-level gate — run ref + the run's raw table set.

    After the per-source import loop the parent workflow holds the UNION of the
    run's raw table ids, and a run-level gate (``check_column_limit``, DAT-430)
    judges that whole set before the per-table fan-out. Scoping by the explicit id
    union — not by a source (the run has many) — means the gate also fires when
    every import SKIPPED, e.g. a run recomposing already-imported sources into a
    bigger set.
    """

    run: RunRef
    table_ids: list[str]


class SessionScopedInput(BaseModel):
    """Input to a begin_session activity — run ref + the typed table set + vertical.

    The begin_session phases are cross-table (relationships are meaningless on one
    table), so the activity carries the whole selection as an array of typed table
    ids. The array is the execution scope, threaded from the workflow input
    (``begin_session(tables=[…])``) — the same set ``begin_session_select`` anchors
    to ``run_tables`` for provenance. ``vertical`` (by name) rides along so the LLM
    phases build their config off the workflow input.
    """

    run: RunRef
    table_ids: list[str]
    vertical: str


class BeginSessionInput(BaseModel):
    """Input to ``beginSessionWorkflow`` — the table selection + the verticals.

    Unlike ``add_source`` (whose table set is discovered by ``import``), the
    begin_session table set is the user's explicit selection of already-typed
    tables, so it travels in the input as ``tables`` (an array of typed table ids,
    possibly spanning sources). The workspace ``verticals`` (by name) drive the LLM
    table synthesis / relationship reasoning and are sourced by the driver from the
    cockpit-owned workspace record.
    """

    workspace_id: str
    tables: list[str]
    verticals: list[str]


class BeginSessionResult(BaseModel):
    """``beginSessionWorkflow`` result — the run + the tables it composed.

    ``run_id`` is the version axis the cockpit stores + replays by; there is no
    ``session_id`` (sessions live in cockpit_db, DAT-506).
    """

    run_id: str
    table_ids: list[str]


class OperatingModelInput(BaseModel):
    """Input to ``operatingModelWorkflow`` — the workspace + the verticals (DAT-438).

    Unlike begin_session (which ESTABLISHES the table set), operating_model
    operates on the set the workspace catalog already anchors: the pre-flight
    ``operating_model_resolve`` activity reads the catalog head's ``run_tables``.
    The workspace ``verticals`` (by name) drive the declared
    validations/cycles/metrics and are validated born-loud at resolve.
    """

    workspace_id: str
    verticals: list[str]


class OperatingModelScope(BaseModel):
    """``operating_model_resolve``'s output — the pinned base-run map + table set.

    The docs/architecture/persistence.md in-run pin, resolved ONCE at run start: ``relationship_run_id``
    is begin_session's promoted ``(catalog, catalog)`` head; ``semantic_runs``
    the per-table promoted ``(table:{id}, semantic_per_column)`` heads. Wire
    mirror of :class:`dataraum.lifecycle.BaseRunMap` (contracts stay
    engine-free for the workflow sandbox — same hand-mirror discipline as the
    cockpit's ``types.ts``).

    ``table_ids`` is the catalog head's ``run_tables`` PINNED here at resolve
    (docs/architecture/persistence.md): all three OM phase activities read ``payload.scope.table_ids``
    rather than each re-reading the catalog head, so a concurrent begin_session
    promoting a new head mid-run cannot make the three phases see different
    table sets. This is the engine-internal RESOLVE OUTPUT, NOT a wire input —
    :class:`OperatingModelInput` takes no table set; the cockpit never sends one.
    """

    relationship_run_id: str | None = None
    semantic_runs: dict[str, str] = Field(default_factory=dict)
    table_ids: list[str] = Field(default_factory=list)


class OperatingModelScopedInput(BaseModel):
    """Input to an operating_model phase activity — run ref + scope + vertical.

    ``vertical`` (by name) drives the declared validations/cycles/metrics the
    lifecycle families read off their phase config.
    """

    run: RunRef
    scope: OperatingModelScope
    vertical: str


class OperatingModelResult(BaseModel):
    """``operatingModelWorkflow`` result.

    ``run_id`` is the version axis the cockpit stores + replays by (no
    ``session_id`` — sessions live in cockpit_db, DAT-506).
    ``validation_summary`` carries the phase's explicit outcome verbatim —
    including the loud ``no_declared_validations`` case — so the cockpit
    renders what happened without re-deriving it. No ``table_ids``:
    operating_model carries no table set — the phases read the catalog head's
    ``run_tables`` and the cockpit reads the catalog views.
    """

    run_id: str
    validation_summary: str = ""


class AddSourceInput(BaseModel):
    """Input to ``AddSourceWorkflow`` — the workspace + the source set + verticals.

    A run ingests a SET of sources (DAT-422): N per-file content-sources for an
    upload, or one connection source for a database. ``import`` runs once per
    source in ``sources`` (a source is a dir of files / a DB recipe — its raw
    tables are discovered at run), and the per-table fan-out + the run-scoped
    reduce/detect run over the union. The workspace ``verticals`` (by name) drive
    the per-column semantic grounding and are sourced by the driver from the
    cockpit-owned workspace record.
    """

    workspace_id: str
    # The sources this run imports, in order — at least one. The cockpit Client
    # enforces a non-empty set (Zod ``min(1)``).
    sources: list[str]
    verticals: list[str]


class AddSourceResult(BaseModel):
    """``AddSourceWorkflow`` result — the run + the discovered raw tables + outcomes.

    ``run_id`` is the version axis the cockpit stores + replays by (DAT-413).
    """

    run_id: str
    raw_table_ids: list[str]
    tables: list[ProcessTableResult]


# --- Workflow ID convention (DAT-364/506) ------------------------------------
#
# Parent workflow IDs are owned by the cockpit Client (it starts the workflow);
# the engine derives only CHILD ids, from the parent's own
# ``workflow.info().workflow_id`` — never from a payload identity. Keeping the
# child id a pure function of the parent id means the engine needs no
# workspace/session segment of its own, and a child stays greppable under its
# parent in the Temporal UI. This helper lives in the engine-free contracts
# module the determinism sandbox imports through ``imports_passed_through``, so
# the workflow body can build child ids without dragging the engine into the
# sandbox.


def process_table_workflow_id(parent_workflow_id: str, raw_table_id: str) -> str:
    """Child ``processTableWorkflow`` ID for one raw table under a parent run.

    Deterministic + collision-free so replay stays stable: the same raw table
    re-runs under the same child ID across teach iterations. Derived from the
    parent's own workflow id (``workflow.info().workflow_id``) + a
    ``-table-<raw>`` suffix, so children are greppable under their parent in the
    Temporal UI and two parents never collide on a child id. ``raw_table_id`` is
    unique per run, so two per-object sources in the same run never collide.
    """
    return f"{parent_workflow_id}-table-{raw_table_id}"
