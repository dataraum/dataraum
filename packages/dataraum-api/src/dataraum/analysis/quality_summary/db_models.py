"""SQLAlchemy models for quality summary analysis.

Contains database models for storing quality reports per column.
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


class ColumnQualityReport(Base):
    """Stores quality summary report for a column across slices.

    Each record represents an LLM-generated quality assessment for one
    column, aggregating findings from all slices of that column.
    """

    __tablename__ = "column_quality_reports"
    __table_args__ = (
        Index("idx_quality_reports_source_column", "source_column_id"),
        Index("idx_quality_reports_slice_column", "slice_column_id"),
    )

    report_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Reference to source column (in the original typed table)
    source_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Reference to slice definition column
    slice_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Summary info
    column_name: Mapped[str] = mapped_column(String, nullable=False)
    source_table_name: Mapped[str] = mapped_column(String, nullable=False)
    slice_column_name: Mapped[str] = mapped_column(String, nullable=False)
    slice_count: Mapped[int] = mapped_column(Integer, nullable=False)

    # Quality assessment
    overall_quality_score: Mapped[float] = mapped_column(Float, nullable=False)
    quality_grade: Mapped[str] = mapped_column(String, nullable=False)  # A, B, C, D, F
    summary: Mapped[str] = mapped_column(Text, nullable=False)

    # Detailed findings stored as JSON
    # Contains: key_findings, quality_issues, slice_comparisons, recommendations
    report_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Investigation SQL views (for UI drill-down)
    investigation_views: Mapped[list[dict[str, str]]] = mapped_column(JSON, nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    source_column: Mapped[Column] = relationship(foreign_keys=[source_column_id])
    slice_column: Mapped[Column] = relationship(foreign_keys=[slice_column_id])


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


class QualitySummaryRun(Base):
    """Tracks quality summary analysis runs.

    Records when analysis was performed and summary statistics.
    """

    __tablename__ = "quality_summary_runs"
    __table_args__ = (
        Index("idx_summary_runs_source_table", "source_table_id"),
        Index("idx_summary_runs_slice_column", "slice_column_id"),
    )

    run_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Scope
    source_table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    slice_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    slice_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Results
    columns_analyzed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reports_generated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Timing
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    duration_seconds: Mapped[float | None] = mapped_column(Float)

    # Status
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="running"
    )  # running, completed, failed
    error_message: Mapped[str | None] = mapped_column(Text)

    # Relationships
    source_table: Mapped[Table] = relationship()
    slice_column: Mapped[Column] = relationship()


__all__ = [
    "ColumnQualityReport",
    "ColumnSliceProfile",
    "QualitySummaryRun",
]
