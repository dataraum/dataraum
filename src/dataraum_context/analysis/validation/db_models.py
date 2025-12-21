"""SQLAlchemy models for validation results.

Contains database models for storing validation run results.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from dataraum_context.storage import Base


class ValidationRunRecord(Base):
    """Record of a validation run.

    Stores the results of running validations across tables.
    """

    __tablename__ = "validation_runs"
    __table_args__ = {"extend_existing": True}

    run_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    table_name: Mapped[str] = mapped_column(String, nullable=False)

    # Timing
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Summary
    total_checks: Mapped[int] = mapped_column(Integer, default=0)
    passed_checks: Mapped[int] = mapped_column(Integer, default=0)
    failed_checks: Mapped[int] = mapped_column(Integer, default=0)
    skipped_checks: Mapped[int] = mapped_column(Integer, default=0)
    error_checks: Mapped[int] = mapped_column(Integer, default=0)

    overall_status: Mapped[str] = mapped_column(String, default="passed")
    has_critical_failures: Mapped[bool] = mapped_column(default=False)

    # Detailed results (JSON)
    results: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)


class ValidationResultRecord(Base):
    """Record of a single validation check result.

    Stores individual check results for analysis and reporting.
    """

    __tablename__ = "validation_results"
    __table_args__ = {"extend_existing": True}

    result_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    validation_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)

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
    "ValidationRunRecord",
    "ValidationResultRecord",
]
