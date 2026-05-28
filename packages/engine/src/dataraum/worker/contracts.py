"""Worker I/O contracts (DAT-344, redesigned per-boundary in DAT-370).

Deliberately engine-free: imports nothing but Pydantic. Both the activity runner
(:mod:`dataraum.worker.activity`, which pulls in the whole engine) and the
workflows (:mod:`dataraum.worker.workflows`, which run in Temporal's determinism
sandbox) import these models from here — so the workflow module never drags
SQLAlchemy/DuckDB/the registry into the sandbox.

The shapes are **typed per boundary**, not one uniform envelope: ``import``
discovers raw tables, ``typing`` mints a typed id, the analytics phases are
scoped to a single typed table, and the workflows thread an identity header. The
scheduler-era ``PhaseActivityInput``/``PhaseActivityResult`` god-envelope (one
shape for all phases, with a ``table_ids`` field downstream phases ignored) is
gone — the fan-out (DAT-370) made the per-boundary inputs concrete.
"""

from __future__ import annotations

from pydantic import BaseModel


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
    workflow to re-apply a teach. Adding this as an optional field on the
    inputs is replay-back-compat (existing histories never carried it,
    and the workflow body's gates default-take the same path as before).

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
            the parent re-runs ``semantic_per_column`` + ``detect_source``
            without re-typing anything.
    """

    from_phase: str
    raw_table_ids: list[str] | None = None


class PhaseOutcome(BaseModel):
    """Lean per-activity result: outcome + human summary.

    Returned by the activities that don't mint an id (the analytics phases,
    ``detect_table``, ``semantic_per_column``). A deterministic phase *failure*
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
    """Input to the ``replay_cleanup_for_phase`` activity (DAT-343).

    Carried by the workflow when it's about to enter the ``from_phase`` of
    a teach replay; the activity invokes the phase's ``replay_cleanup``
    so the phase's existing ``should_skip`` doesn't bail on a re-run.

    Attributes:
        identity: source identity header (same as every other phase activity).
        phase_name: which phase's ``replay_cleanup`` to invoke — must equal
            the workflow's ``replay.from_phase``.
        table_ids: scope passed through to the phase's ``replay_cleanup``;
            empty list = source-wide (matches ``replay.raw_table_ids=None``
            and the source-tail-only ``[]`` shape).
    """

    identity: SourceIdentity
    phase_name: str
    table_ids: list[str] = []


class TableScopedInput(BaseModel):
    """Input to the analytics activities + ``detect_table`` — one typed table.

    ``table_id`` is the *typed* table id from :class:`TypingResult`; the phase
    scopes its work (and the detect step its measurements) to exactly this table.
    """

    identity: SourceIdentity
    table_id: str


class ProcessTableResult(BaseModel):
    """``ProcessTableWorkflow`` result — the raw→typed mapping for one table."""

    raw_table_id: str
    typed_table_id: str


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
