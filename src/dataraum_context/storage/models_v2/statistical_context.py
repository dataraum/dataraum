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
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum_context.storage.models_v2.base import Base

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.core import Column


class StatisticalProfile(Base):
    """Statistical profile of a column.

    This captures basic statistical metadata that's always computed:
    - Counts (total, null, distinct)
    - Distribution (min, max, mean, stddev, percentiles)
    - Top values and histogram
    - Basic quality indicators (null ratio, cardinality)

    Versioned by profiled_at to track changes over time.
    """

    __tablename__ = "statistical_profiles"

    profile_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    profiled_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # ========================================================================
    # Basic Counts
    # ========================================================================
    total_count: Mapped[int] = mapped_column(Integer, nullable=False)
    null_count: Mapped[int] = mapped_column(Integer, nullable=False)
    distinct_count: Mapped[int | None] = mapped_column(Integer)

    # Derived ratios
    null_ratio: Mapped[float | None] = mapped_column(Float)
    cardinality_ratio: Mapped[float | None] = mapped_column(Float)  # distinct/total

    # ========================================================================
    # Numeric Statistics (NULL if column is not numeric)
    # ========================================================================
    min_value: Mapped[float | None] = mapped_column(Float)
    max_value: Mapped[float | None] = mapped_column(Float)
    mean_value: Mapped[float | None] = mapped_column(Float)
    stddev_value: Mapped[float | None] = mapped_column(Float)

    # Distribution shape
    skewness: Mapped[float | None] = mapped_column(Float)
    kurtosis: Mapped[float | None] = mapped_column(Float)
    cv: Mapped[float | None] = mapped_column(Float)  # Coefficient of variation (stddev/mean)

    # Percentiles: {p25, p50, p75, p95, p99}
    percentiles: Mapped[dict | None] = mapped_column(JSON)

    # ========================================================================
    # String Statistics (NULL if column is not string)
    # ========================================================================
    min_length: Mapped[int | None] = mapped_column(Integer)
    max_length: Mapped[int | None] = mapped_column(Integer)
    avg_length: Mapped[float | None] = mapped_column(Float)

    # ========================================================================
    # Distribution Data
    # ========================================================================
    # Histogram: [{bucket_min, bucket_max, count}, ...]
    histogram: Mapped[list | None] = mapped_column(JSON)

    # Top K values: [{value, count, percentage}, ...]
    top_values: Mapped[list | None] = mapped_column(JSON)

    # ========================================================================
    # Entropy (Information Content)
    # ========================================================================
    shannon_entropy: Mapped[float | None] = mapped_column(Float)  # Bits of information
    normalized_entropy: Mapped[float | None] = mapped_column(Float)  # 0-1 scale

    # ========================================================================
    # Uniqueness Metrics
    # ========================================================================
    is_unique: Mapped[bool | None] = mapped_column(Integer)  # All values unique (potential PK)
    duplicate_count: Mapped[int | None] = mapped_column(Integer)  # Number of duplicated values

    # ========================================================================
    # Ordering Metrics
    # ========================================================================
    is_sorted: Mapped[bool | None] = mapped_column(Integer)  # Is column sorted?
    is_monotonic_increasing: Mapped[bool | None] = mapped_column(Integer)
    is_monotonic_decreasing: Mapped[bool | None] = mapped_column(Integer)
    inversions_ratio: Mapped[float | None] = mapped_column(Float)  # Measure of "unsortedness"

    # Relationships
    column: Mapped["Column"] = relationship(back_populates="statistical_profiles")


class StatisticalQualityMetrics(Base):
    """Statistical quality assessment for a column.

    Advanced quality metrics that may be expensive to compute:
    - Benford's Law compliance (fraud detection for financial amounts)
    - Distribution stability (KS test across time periods)
    - Outlier detection (Isolation Forest, IQR method)
    - Multicollinearity (VIF - requires correlation with other columns)

    These metrics are optional and computed based on configuration.
    """

    __tablename__ = "statistical_quality_metrics"

    metric_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # ========================================================================
    # Benford's Law (for financial amounts, counts, etc.)
    # ========================================================================
    benford_chi_square: Mapped[float | None] = mapped_column(Float)
    benford_p_value: Mapped[float | None] = mapped_column(Float)
    benford_compliant: Mapped[bool | None] = mapped_column(Integer)  # p_value > 0.05
    benford_interpretation: Mapped[str | None] = mapped_column(String)

    # First digit distribution: {1: 0.301, 2: 0.176, ...}
    benford_digit_distribution: Mapped[dict | None] = mapped_column(JSON)

    # ========================================================================
    # Distribution Stability (KS test across time periods)
    # ========================================================================
    ks_statistic: Mapped[float | None] = mapped_column(Float)
    ks_p_value: Mapped[float | None] = mapped_column(Float)
    distribution_stable: Mapped[bool | None] = mapped_column(Integer)  # p_value > 0.01

    # Comparison period info
    comparison_period_start: Mapped[datetime | None] = mapped_column(DateTime)
    comparison_period_end: Mapped[datetime | None] = mapped_column(DateTime)

    # ========================================================================
    # Outlier Detection
    # ========================================================================
    # IQR Method
    iqr_outlier_count: Mapped[int | None] = mapped_column(Integer)
    iqr_outlier_ratio: Mapped[float | None] = mapped_column(Float)
    iqr_lower_fence: Mapped[float | None] = mapped_column(Float)
    iqr_upper_fence: Mapped[float | None] = mapped_column(Float)

    # Isolation Forest (ML-based anomaly detection)
    isolation_forest_score: Mapped[float | None] = mapped_column(Float)  # Average anomaly score
    isolation_forest_anomaly_count: Mapped[int | None] = mapped_column(Integer)
    isolation_forest_anomaly_ratio: Mapped[float | None] = mapped_column(Float)

    # Sample outliers for review: [{value, method, score}, ...]
    outlier_samples: Mapped[list | None] = mapped_column(JSON)

    # ========================================================================
    # Multicollinearity (VIF - Variance Inflation Factor)
    # ========================================================================
    vif_score: Mapped[float | None] = mapped_column(Float)  # VIF > 10 indicates multicollinearity
    vif_correlated_columns: Mapped[list | None] = mapped_column(
        JSON
    )  # Column IDs with high correlation

    # ========================================================================
    # Overall Quality Assessment
    # ========================================================================
    quality_score: Mapped[float | None] = mapped_column(Float)  # 0-1 aggregate quality score
    quality_issues: Mapped[list | None] = mapped_column(
        JSON
    )  # [{issue_type, severity, description}, ...]

    # Relationships
    column: Mapped["Column"] = relationship(back_populates="statistical_quality_metrics")


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
