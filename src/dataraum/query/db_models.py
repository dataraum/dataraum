"""Query execution database models.

SQLAlchemy models for persisting query executions for audit trail.
SQL snippets are stored separately in snippet_models.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class QueryExecutionRecord(Base):
    """Record of a query execution.

    Tracks each time a query is executed for audit trail purposes.
    """

    __tablename__ = "query_executions"

    execution_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    # Source context
    source_id: Mapped[str] = mapped_column(
        ForeignKey("sources.source_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # The question asked
    question: Mapped[str] = mapped_column(Text, nullable=False)

    # Execution details
    sql_executed: Mapped[str] = mapped_column(Text, nullable=False)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Results summary
    success: Mapped[bool] = mapped_column(Integer, nullable=False, default=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Confidence at execution time
    confidence_level: Mapped[str] = mapped_column(String, nullable=False, default="GREEN")
    contract_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Entropy action determined at execution time
    entropy_action: Mapped[str | None] = mapped_column(String, nullable=True)


__all__ = ["QueryExecutionRecord"]
