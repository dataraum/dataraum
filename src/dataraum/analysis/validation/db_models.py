"""SQLAlchemy models for validation results.

Contains database models for storing validation check results.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class ValidationResultRecord(Base):
    """Record of a single validation check result.

    Stores individual check results for analysis and reporting.
    """

    __tablename__ = "validation_results"

    result_id: Mapped[str] = mapped_column(String, primary_key=True)
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
    "ValidationResultRecord",
]
