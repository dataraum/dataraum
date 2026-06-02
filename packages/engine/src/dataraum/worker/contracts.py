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

from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class ProgressSnapshot:
    """Parent-level progress for ``addSourceWorkflow``, served by ``get_progress`` (DAT-406).

    Read-only snapshot the cockpit polls via the Temporal Client's
    ``query`` API while the parent runs (queries answer against current
    state even while ``@workflow.run`` is blocked awaiting the fan-out).
    The workflow body advances ``phase`` before each stage and bumps
    ``tables_completed`` as each child resolves; ``tables_total`` is set
    once the fan-out set is known.

    Deliberately a plain stdlib ``@dataclass`` (NOT a Pydantic engine
    type): it carries only primitives, and the worker's
    ``pydantic_data_converter`` serializes it to the flat JSON shape
    ``{phase, tables_total, tables_completed}`` that the cockpit Client
    (a TS process that cannot import Python types) consumes. That shape
    is the FROZEN cross-package contract DAT-352 mirrors in
    ``packages/cockpit/src/temporal/types.ts`` — do not change a field
    name/type without re-mirroring it there.

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
    """

    phase: str
    tables_total: int = 0
    tables_completed: int = 0


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


class LinkSessionTablesInput(BaseModel):
    """Input to the ``link_session_tables`` activity (DAT-407).

    Carries the run's identity header + the typed table ids the session
    composes (for ``add_source``, the source's freshly-typed tables). The
    activity upserts one ``session_tables`` row per id so the session's
    source(s) are derivable without the session storing a ``source_id``.
    """

    identity: SourceIdentity
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
