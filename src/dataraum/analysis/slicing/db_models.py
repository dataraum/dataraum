"""SQLAlchemy models for slicing analysis.

Contains database models for slice definitions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column, Table


class SliceDefinition(Base):
    """Stores slice definitions for a table.

    Each record represents a recommended slicing dimension,
    with the column to slice on and the SQL to create slices.
    """

    __tablename__ = "slice_definitions"
    __table_args__ = (
        Index("idx_slice_definitions_table", "table_id"),
        Index("idx_slice_definitions_column", "column_id"),
    )

    slice_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Slice configuration
    slice_priority: Mapped[int] = mapped_column(Integer, nullable=False)
    slice_type: Mapped[str] = mapped_column(String, nullable=False, default="categorical")
    distinct_values: Mapped[list[str] | None] = mapped_column(JSON)
    value_count: Mapped[int | None] = mapped_column(Integer)

    # Analysis reasoning
    reasoning: Mapped[str | None] = mapped_column(Text)
    business_context: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Float)

    # SQL template
    sql_template: Mapped[str | None] = mapped_column(Text)

    # Provenance
    detection_source: Mapped[str] = mapped_column(String, nullable=False, default="llm")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship()
    column: Mapped[Column] = relationship()


__all__ = [
    "SliceDefinition",
]
