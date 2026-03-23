"""SQLAlchemy models for slicing analysis.

Contains database models for slice definitions and slice profiles.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
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
    # Actual column name used for slicing — may differ from columns.column_name when the
    # slice dimension is an enriched FK-prefixed dim col (e.g. "kontonummer_des_gegenkontos__land")
    # while column_id points to the underlying FK column record.
    column_name: Mapped[str | None] = mapped_column(String, nullable=True)

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


class ColumnSliceProfile(Base):
    """Stores quality metrics for a column within a specific slice.

    Each record represents the quality profile of one column in one slice,
    enabling analysis of quality patterns across slices.
    """

    __tablename__ = "column_slice_profiles"
    __table_args__ = (
        Index("idx_slice_profiles_source_column", "source_column_id"),
        Index("idx_slice_profiles_slice_column", "slice_column_id"),
        Index("idx_slice_profiles_lookup", "source_column_id", "slice_column_id", "slice_value"),
    )

    profile_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Reference to source column (in the original typed table)
    source_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Reference to slice definition column
    slice_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Identifiers
    source_table_name: Mapped[str] = mapped_column(String, nullable=False)
    column_name: Mapped[str] = mapped_column(String, nullable=False)
    slice_column_name: Mapped[str] = mapped_column(String, nullable=False)
    slice_value: Mapped[str] = mapped_column(String, nullable=False)

    # Basic metrics
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    null_ratio: Mapped[float | None] = mapped_column(Float)
    distinct_count: Mapped[int | None] = mapped_column(Integer)

    # Quality indicators
    quality_score: Mapped[float | None] = mapped_column(Float)
    has_issues: Mapped[bool] = mapped_column(Integer, nullable=False, default=False)  # SQLite bool
    issue_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Extended metrics stored as JSON for flexibility
    # Contains: min_value, max_value, mean_value, stddev, benford_*, outlier_*, etc.
    profile_data: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    source_column: Mapped[Column] = relationship(foreign_keys=[source_column_id])
    slice_column: Mapped[Column] = relationship(foreign_keys=[slice_column_id])


__all__ = [
    "ColumnSliceProfile",
    "SliceDefinition",
]
