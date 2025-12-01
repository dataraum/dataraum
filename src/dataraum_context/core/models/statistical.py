"""Pydantic models for Statistical Context (Pillar 1).

These models are used for:
- API responses
- Internal data transfer between layers
- Validation of statistical analysis results

Note: These are separate from SQLAlchemy models (storage/models_v2/statistical_context.py)
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# ============================================================================
# Statistical Profile Models
# ============================================================================


class NumericStats(BaseModel):
    """Statistics for numeric columns."""

    min_value: float
    max_value: float
    mean: float
    stddev: float
    skewness: float | None = None
    kurtosis: float | None = None
    cv: float | None = None  # Coefficient of variation
    percentiles: dict[str, float] = Field(default_factory=lambda: {})


class StringStats(BaseModel):
    """Statistics for string columns."""

    min_length: int
    max_length: int
    avg_length: float


class HistogramBucket(BaseModel):
    """A histogram bucket."""

    bucket_min: float | str
    bucket_max: float | str
    count: int


class ValueCount(BaseModel):
    """Value frequency."""

    value: str | int | float | bool
    count: int
    percentage: float


class EntropyStats(BaseModel):
    """Information-theoretic metrics."""

    shannon_entropy: float  # Bits of information
    normalized_entropy: float  # 0-1 scale
    entropy_category: str  # 'low', 'medium', 'high'


class UniquenessStats(BaseModel):
    """Uniqueness and key-worthiness metrics."""

    is_unique: bool  # All values unique
    is_near_unique: bool  # > 99% unique
    duplicate_count: int
    most_duplicated_value: str | int | float | None = None
    most_duplicated_count: int | None = None


class OrderStats(BaseModel):
    """Order and sequence characteristics."""

    is_sorted: bool
    is_monotonic_increasing: bool
    is_monotonic_decreasing: bool
    sort_direction: str | None = None  # 'asc', 'desc', 'unsorted'
    inversions_ratio: float  # Measure of "unsortedness"


class StatisticalProfile(BaseModel):
    """Complete statistical profile for a column.

    This is the Pydantic interface model (for API responses).
    Corresponds to storage.models_v2.statistical_context.StatisticalProfile
    """

    profile_id: str
    column_id: str
    profiled_at: datetime

    # Counts
    total_count: int
    null_count: int
    distinct_count: int | None = None
    null_ratio: float
    cardinality_ratio: float | None = None

    # Type-specific stats
    numeric_stats: NumericStats | None = None
    string_stats: StringStats | None = None

    # Distribution
    histogram: list[HistogramBucket] | None = None
    top_values: list[ValueCount] | None = None

    # Information content
    entropy_stats: EntropyStats | None = None

    # Uniqueness
    uniqueness_stats: UniquenessStats | None = None

    # Ordering
    order_stats: OrderStats | None = None


# ============================================================================
# Statistical Quality Models
# ============================================================================


class BenfordTestResult(BaseModel):
    """Results of Benford's Law test."""

    chi_square: float
    p_value: float
    compliant: bool  # p_value > 0.05
    interpretation: str
    digit_distribution: dict[int, float]  # {1: 0.301, 2: 0.176, ...}


class DistributionStabilityResult(BaseModel):
    """Results of distribution stability test (KS test)."""

    ks_statistic: float
    p_value: float
    stable: bool  # p_value > 0.01
    comparison_period_start: datetime
    comparison_period_end: datetime


class OutlierDetectionResult(BaseModel):
    """Results of outlier detection."""

    method: str  # 'iqr' or 'isolation_forest'
    outlier_count: int
    outlier_ratio: float

    # IQR-specific
    lower_fence: float | None = None
    upper_fence: float | None = None

    # Isolation Forest-specific
    average_anomaly_score: float | None = None

    # Sample outliers
    outlier_samples: list[dict[str, Any]] | None = None  # [{value, score}, ...]


class VIFResult(BaseModel):
    """Variance Inflation Factor result."""

    column_id: str
    vif_score: float
    has_multicollinearity: bool  # VIF > 10
    correlated_columns: list[str] = Field(default_factory=lambda: [])  # Column IDs


class QualityIssue(BaseModel):
    """A quality issue found during analysis."""

    issue_type: str  # 'benford_violation', 'distribution_shift', 'outliers', 'multicollinearity'
    severity: str  # 'critical', 'warning', 'info'
    description: str
    evidence: dict[str, Any] = Field(default_factory=lambda: {})


class StatisticalQualityMetrics(BaseModel):
    """Statistical quality assessment for a column.

    This is the Pydantic interface model (for API responses).
    Corresponds to storage.models_v2.statistical_context.StatisticalQualityMetrics
    """

    metric_id: str
    column_id: str
    computed_at: datetime

    # Quality tests
    benford_test: BenfordTestResult | None = None
    distribution_stability: DistributionStabilityResult | None = None
    outlier_detection: OutlierDetectionResult | None = None
    vif_result: VIFResult | None = None

    # Overall assessment
    quality_score: float | None = None  # 0-1
    quality_issues: list[QualityIssue] = Field(default_factory=lambda: [])


# ============================================================================
# Results and Responses
# ============================================================================


class StatisticalProfilingResult(BaseModel):
    """Result of statistical profiling operation."""

    profiles: list[StatisticalProfile]
    duration_seconds: float


class StatisticalQualityResult(BaseModel):
    """Result of statistical quality assessment operation."""

    metrics: list[StatisticalQualityMetrics]
    duration_seconds: float
