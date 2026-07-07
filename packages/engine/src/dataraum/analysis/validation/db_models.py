"""SQLAlchemy models for validation results.

Contains database models for storing validation check results.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import JSON, DateTime, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class ValidationResultRecord(Base):
    """A single validation's grounded SQL for a run — a pure SQL store (docs/architecture/grounding.md).

    Run-versioned (DAT-438): one row per ``(session, validation, run)``. A
    re-run supersedes by writing rows under its fresh ``run_id``; readers scope
    to the promoted ``operating_model`` head (or, in-run, to this run's id),
    never across runs.

    The pass/fail VERDICT is **not** stored — a stored verdict goes stale the
    moment data is re-imported, the SQL does not (DAT-617). Neither are the
    declared judgement params (``severity``/``tolerance``): those live in the
    vertical config, read via the spec reader at every consumer. This record is
    just the durable run-versioned ``sql_used`` (+ the columns it touched); the
    verdict is recomputed on demand by re-running it (``validation/evaluate.py``).
    """

    __tablename__ = "validation_results"
    __table_args__ = (UniqueConstraint("validation_id", "run_id", name="uq_validation_result_run"),)

    result_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    validation_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    # The "table.column" names the generated SQL actually touched (LLM-declared
    # at bind time, DAT-432/L7) — the entropy detector bands these columns when
    # the recomputed verdict is a failure.
    columns_used: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)

    # The grounded SQL (the durable artifact) + when it was bound for this run.
    sql_used: Mapped[str | None] = mapped_column(Text, nullable=True)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = [
    "ValidationResultRecord",
]
