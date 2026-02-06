"""Column Eligibility Database Models.

SQLAlchemy model for column eligibility decisions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Index, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Source, Table


class ColumnEligibilityRecord(Base):
    """Column eligibility decision with audit trail.

    Records the eligibility evaluation for each column, including:
    - The status (ELIGIBLE, WARN, INELIGIBLE)
    - Which rule triggered the status
    - A snapshot of the metrics at decision time
    """

    __tablename__ = "column_eligibility"

    eligibility_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid4())
    )
    column_id: Mapped[str] = mapped_column(String(36), nullable=False)  # Preserved for audit, no FK
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    source_id: Mapped[str] = mapped_column(
        ForeignKey("sources.source_id", ondelete="CASCADE"), nullable=False
    )

    # Denormalized column metadata (survives column deletion)
    column_name: Mapped[str] = mapped_column(String, nullable=False)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    resolved_type: Mapped[str | None] = mapped_column(String, nullable=True)

    # Decision
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # ELIGIBLE, WARN, INELIGIBLE
    triggered_rule: Mapped[str | None] = mapped_column(String(50))  # Rule ID that matched
    reason: Mapped[str | None] = mapped_column(Text)  # Human-readable reason

    # Snapshot of metrics at decision time (for audit/debugging)
    metrics_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Config version used (for reproducibility)
    config_version: Mapped[str] = mapped_column(String(20), nullable=False)

    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship()
    source: Mapped[Source] = relationship()


# Indexes for common queries
Index("idx_eligibility_column", ColumnEligibilityRecord.column_id)
Index("idx_eligibility_table", ColumnEligibilityRecord.table_id)
Index("idx_eligibility_source", ColumnEligibilityRecord.source_id)
Index("idx_eligibility_status", ColumnEligibilityRecord.status)
