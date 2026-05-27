"""Worker I/O contracts (DAT-344) — the Pydantic shapes crossing the Temporal boundary.

Deliberately engine-free: imports nothing but Pydantic. Both the activity runner
(:mod:`dataraum.worker.activity`, which pulls in the whole engine) and the
workflow (:mod:`dataraum.worker.workflows`, which runs in Temporal's
determinism sandbox) import these models from here — so the workflow module
never drags SQLAlchemy/DuckDB/the registry into the sandbox.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class PhaseActivityInput(BaseModel):
    """Pydantic input for a phase activity — IDs only, serialized over Temporal.

    The runner reconstructs everything else (source identity, phase config) from
    these IDs + the workspace substrate, mirroring what ``setup_pipeline``
    assembles in-process today.
    """

    workspace_id: str
    source_id: str
    # Per-run FK for session-scoped rows (e.g. the type_fidelity detector's
    # EntropyObjectRecord). Pure data — NOT a connection scope. In the platform
    # model this is the workflow execution processing the source.
    session_id: str
    vertical: str | None = None
    # Optional table filter (DAT-342). Empty = all of the source's raw tables.
    table_ids: list[str] = Field(default_factory=list)


class PhaseActivityResult(BaseModel):
    """Serializable phase outcome returned across the Temporal boundary."""

    phase: str
    status: str
    summary: str = ""
    records_processed: int = 0
    records_created: int = 0
    outputs: dict[str, object] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None
