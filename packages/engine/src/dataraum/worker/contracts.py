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
    chain over it.
    """

    identity: SourceIdentity
    raw_table_id: str


class TypingResult(BaseModel):
    """``typing`` activity result — the freshly minted (or resolved) typed id.

    ``typing`` mints a uuid4 typed id ``!=`` the raw id. It travels in this
    result, so it is persisted in Temporal history and replayed verbatim — the
    downstream analytics activities read it from the child workflow, never
    recompute it.
    """

    typed_table_id: str


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
    """Input to ``AddSourceWorkflow`` — just the source identity.

    The table set is unknown until ``import`` enumerates it (a source is a dir /
    DB recipe), so the parent carries identity only and discovers tables at run.
    """

    identity: SourceIdentity


class AddSourceResult(BaseModel):
    """``AddSourceWorkflow`` result — the discovered raw tables + per-table outcomes."""

    raw_table_ids: list[str]
    tables: list[ProcessTableResult]
