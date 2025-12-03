"""Temporal context models (Pillar 4) - time-based patterns and quality.

This module stores enhanced temporal analysis including:
- Seasonality strength (quantified)
- Trend breaks (change points)
- Update frequency scoring
- Fiscal calendar alignment
- Distribution stability across time periods
"""

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Mapped, mapped_column

from dataraum_context.storage.models_v2.base import Base


class TemporalQualityMetrics(Base):
    """Temporal quality metrics for a time column.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable dimensions (IDs, timestamps, key metrics)
    - JSONB field: Full Pydantic model for flexibility

    This allows:
    - Fast queries on core dimensions
    - Schema flexibility for experimentation
    - Zero mapping code (Pydantic handles serialization)
    """

    __tablename__ = "temporal_quality_metrics"

    metric_id: Mapped[str] = mapped_column(String, primary_key=True)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # STRUCTURED: Queryable core dimensions
    min_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    max_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    detected_granularity: Mapped[str] = mapped_column(String, nullable=False)
    completeness_ratio: Mapped[float | None] = mapped_column(Float)

    # Flags for filtering (fast queries)
    has_seasonality: Mapped[bool | None] = mapped_column(Boolean)
    has_trend: Mapped[bool | None] = mapped_column(Boolean)
    is_stale: Mapped[bool | None] = mapped_column(Boolean)

    # Overall quality score (for sorting/filtering)
    temporal_quality_score: Mapped[float | None] = mapped_column(Float)

    # JSONB: Full temporal profile + quality data
    # Stores complete Pydantic models: TemporalProfile, quality analysis results
    temporal_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class TemporalTableSummaryMetrics(Base):
    """Table-level temporal quality summary across multiple temporal columns.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable aggregates (counts, scores, freshness)
    - JSONB field: Full TemporalTableSummary Pydantic model

    This allows dashboards to quickly query:
    - Tables with seasonality patterns
    - Tables with stale data
    - Average temporal quality across tables
    """

    __tablename__ = "temporal_table_summary_metrics"

    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), primary_key=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # STRUCTURED: Queryable aggregates
    temporal_column_count: Mapped[int] = mapped_column(nullable=False)
    avg_quality_score: Mapped[float] = mapped_column(Float, nullable=False)
    total_issues: Mapped[int] = mapped_column(nullable=False)

    # Pattern counts (for filtering)
    columns_with_seasonality: Mapped[int] = mapped_column(nullable=False, default=0)
    columns_with_trends: Mapped[int] = mapped_column(nullable=False, default=0)
    columns_with_change_points: Mapped[int] = mapped_column(nullable=False, default=0)
    columns_with_fiscal_alignment: Mapped[int] = mapped_column(nullable=False, default=0)

    # Freshness tracking
    stalest_column_days: Mapped[int | None] = mapped_column()
    has_stale_columns: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # JSONB: Full table summary data
    summary_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


# DEPRECATED: These models are replaced by JSONB storage in TemporalQualityMetrics.temporal_data
#
# SeasonalDecomposition, ChangePoint, DistributionShift data is now stored as JSON
# within the temporal_data field. This provides schema flexibility and reduces
# the number of tables/joins required.
#
# Migration path: These tables can be dropped once all code is updated to use
# the hybrid storage approach.
#
# If you need to query specific change points or distribution shifts,
# use JSON operators or extract the data from temporal_data field.
#
# Example PostgreSQL query:
#   SELECT metric_id, temporal_data->'change_points' as change_points
#   FROM temporal_quality_metrics
#   WHERE temporal_data->>'has_change_points' = 'true'
