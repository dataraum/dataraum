"""SQLAlchemy models for validation results.

Contains database models for storing validation check results.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class ValidationResultRecord(Base):
    """Record of a single validation's grounded SQL for a run (ADR-0017).

    Run-versioned (DAT-438): one row per ``(session, validation, run)``. A
    re-run supersedes by writing rows under its fresh ``run_id``; readers scope
    to the promoted ``operating_model`` head (or, in-run, to this run's id),
    never across runs.

    The pass/fail VERDICT is **not** stored — a stored verdict goes stale the
    moment data is re-imported, the SQL does not (DAT-617). This record holds the
    run-versioned ``sql_used`` (the durable knowledge) plus the declared
    judgement params the in-run entropy detector needs without a config read
    (``severity``, ``tolerance``). The verdict is recomputed on demand by
    re-running ``sql_used`` (``validation/evaluate.py``).
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

    # Declared judgement params (config-derived, non-stale): the in-run
    # cross_table_consistency detector reads these locally (the entropy/detect
    # layer carries no vertical), the cockpit reads them from its spec reader.
    severity: Mapped[str] = mapped_column(String, nullable=False)
    tolerance: Mapped[float | None] = mapped_column(Float, nullable=True)

    # The grounded SQL (the durable artifact) + when it was bound for this run.
    sql_used: Mapped[str | None] = mapped_column(Text, nullable=True)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = [
    "ValidationResultRecord",
]
