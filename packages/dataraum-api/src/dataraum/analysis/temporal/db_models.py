"""Temporal analysis database models.

SQLAlchemy models for persisting temporal analysis results.
Uses hybrid storage: structured fields for queries + JSONB for full data.

Naming convention (consistent with statistics module):
- TemporalColumnProfile: Per-column temporal analysis (like StatisticalProfile)
- TemporalTableSummary: Per-table aggregated summary
"""

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class TemporalColumnProfile(Base):
    """Per-column temporal analysis profile.

    Similar to StatisticalProfile but for temporal characteristics.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable dimensions (IDs, timestamps, key metrics)
    - JSONB field: Full Pydantic model for flexibility

    This allows:
    - Fast queries on core dimensions
    - Schema flexibility for experimentation
    - Zero mapping code (Pydantic handles serialization)
    """

    __tablename__ = "temporal_column_profiles"

    profile_id: Mapped[str] = mapped_column(String, primary_key=True)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    profiled_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # STRUCTURED: Queryable core dimensions
    min_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    max_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    detected_granularity: Mapped[str] = mapped_column(String, nullable=False)
    completeness_ratio: Mapped[float | None] = mapped_column(Float)

    # Flags for filtering (fast queries)
    has_seasonality: Mapped[bool | None] = mapped_column(Boolean)
    has_trend: Mapped[bool | None] = mapped_column(Boolean)
    is_stale: Mapped[bool | None] = mapped_column(Boolean)

    # JSONB: Full TemporalAnalysisResult model
    profile_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


# Index for efficient column lookups
Index("idx_temporal_profiles_column", TemporalColumnProfile.column_id)


class TemporalTableSummary(Base):
    """Per-table temporal analysis summary.

    Aggregates temporal metrics across all temporal columns in a table.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable aggregates (counts, scores, freshness)
    - JSONB field: Full Pydantic model for flexibility

    This allows dashboards to quickly query:
    - Tables with seasonality patterns
    - Tables with stale data
    - Average temporal quality across tables
    """

    __tablename__ = "temporal_table_summaries"

    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), primary_key=True)
    profiled_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # STRUCTURED: Queryable aggregates
    temporal_column_count: Mapped[int] = mapped_column(Integer, nullable=False)
    total_issues: Mapped[int] = mapped_column(Integer, nullable=False)

    # Pattern counts (for filtering)
    columns_with_seasonality: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    columns_with_trends: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    columns_with_change_points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    columns_with_fiscal_alignment: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Freshness tracking
    stalest_column_days: Mapped[int | None] = mapped_column(Integer)
    has_stale_columns: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # JSONB: Full table summary data
    summary_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


__all__ = [
    "TemporalColumnProfile",
    "TemporalTableSummary",
]
