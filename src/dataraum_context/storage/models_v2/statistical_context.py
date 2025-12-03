"""Statistical Context Models (Pillar 1).

This pillar provides comprehensive statistical characterization of data:
- Basic statistical profiles (distributions, counts, percentiles)
- Statistical quality metrics (Benford's Law, outliers, distribution stability)

Design notes:
- StatisticalProfile: Core statistical metadata (always computed)
- StatisticalQualityMetrics: Advanced quality assessment (optional, more expensive)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum_context.storage.models_v2.base import Base

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.core import Column


class StatisticalProfile(Base):
    """Statistical profile of a column.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable core dimensions (counts, ratios, flags)
    - JSONB field: Full Pydantic ColumnProfile model for flexibility

    This allows:
    - Fast queries on core metrics (null_ratio, cardinality_ratio)
    - Schema flexibility for experimentation
    - Zero mapping code (Pydantic handles serialization)
    """

    __tablename__ = "statistical_profiles"

    profile_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    profiled_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # STRUCTURED: Queryable core dimensions
    total_count: Mapped[int] = mapped_column(Integer, nullable=False)
    null_count: Mapped[int] = mapped_column(Integer, nullable=False)
    distinct_count: Mapped[int | None] = mapped_column(Integer)
    null_ratio: Mapped[float | None] = mapped_column(Float)
    cardinality_ratio: Mapped[float | None] = mapped_column(Float)

    # Flags for filtering (fast queries)
    is_unique: Mapped[bool | None] = mapped_column(Integer)  # All values unique (potential PK)
    is_numeric: Mapped[bool | None] = mapped_column(Integer)  # Has numeric stats

    # JSONB: Full Pydantic ColumnProfile model
    # Stores: numeric_stats, string_stats, histogram, top_values, detected_patterns
    profile_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="statistical_profiles")


class StatisticalQualityMetrics(Base):
    """Statistical quality assessment for a column.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable quality indicators (flags, scores, key ratios)
    - JSONB field: Full quality analysis results for flexibility

    Advanced quality metrics that may be expensive to compute:
    - Benford's Law compliance (fraud detection for financial amounts)
    - Distribution stability (KS test across time periods)
    - Outlier detection (Isolation Forest, IQR method)
    - Multicollinearity (VIF - requires correlation with other columns)
    """

    __tablename__ = "statistical_quality_metrics"

    metric_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # STRUCTURED: Queryable quality indicators
    quality_score: Mapped[float | None] = mapped_column(Float)  # 0-1 aggregate quality score

    # Flags for filtering (fast queries)
    benford_compliant: Mapped[bool | None] = mapped_column(Integer)
    distribution_stable: Mapped[bool | None] = mapped_column(Integer)
    has_outliers: Mapped[bool | None] = mapped_column(Integer)

    # Key metrics for sorting/filtering
    iqr_outlier_ratio: Mapped[float | None] = mapped_column(Float)
    isolation_forest_anomaly_ratio: Mapped[float | None] = mapped_column(Float)

    # JSONB: Full quality analysis results
    # Stores: Benford analysis, KS test, outlier details, VIF, quality issues
    quality_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="statistical_quality_metrics")


# Indexes for efficient queries
Index(
    "idx_statistical_profiles_column",
    StatisticalProfile.column_id,
    StatisticalProfile.profiled_at.desc(),
)
Index(
    "idx_statistical_quality_column",
    StatisticalQualityMetrics.column_id,
    StatisticalQualityMetrics.computed_at.desc(),
)
