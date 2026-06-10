"""SQLAlchemy models for validation results.

Contains database models for storing validation check results.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class ValidationResultRecord(Base):
    """Record of a single validation check result.

    Run-versioned (DAT-438): one row per ``(session, validation, run)`` —
    the schema axis of the versioned-model consumer contract. A re-run
    supersedes by writing rows under its fresh ``run_id``; readers scope to
    the promoted ``operating_model`` head (or, in-run, to this run's id),
    never read across runs.
    """

    __tablename__ = "validation_results"
    __table_args__ = (
        UniqueConstraint("session_id", "validation_id", "run_id", name="uq_validation_result_run"),
    )

    result_id: Mapped[str] = mapped_column(String, primary_key=True)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    validation_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    # The "table.column" names the generated SQL actually touched (LLM-declared
    # at bind time, DAT-432/L7). Column identity used to die at persistence —
    # without it a failed reconciliation banded only at table grain, never the
    # columns a deliverable metric flows through.
    columns_used: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)

    # Result
    status: Mapped[str] = mapped_column(String, nullable=False)  # passed, failed, skipped, error
    severity: Mapped[str] = mapped_column(String, nullable=False)
    passed: Mapped[bool] = mapped_column(default=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Execution details
    executed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    sql_used: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Results
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


__all__ = [
    "ValidationResultRecord",
]
