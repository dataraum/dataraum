"""Statistical Profile Models.

Pydantic models for statistical profiling data structures:
- ColumnProfile: Complete statistical profile of a column
- NumericStats: Statistics for numeric columns
- StringStats: Statistics for string columns
- HistogramBucket: Histogram bin
- ValueCount: Frequency count for top values
- DetectedPattern: Pattern detection result (used by schema profiler)
- StatisticsProfileResult: Result of statistics profiling

Statistical Quality Models (moved from quality/models.py in Phase 9A):
- BenfordAnalysis: Benford's Law compliance analysis
- OutlierDetection: Outlier detection results (IQR + Isolation Forest)
- StatisticalQualityResult: Comprehensive statistical quality assessment
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dataraum.core.models.base import ColumnRef


class NumericStats(BaseModel):
    """Statistics for numeric columns."""

    min_value: float
    max_value: float
    mean: float
    stddev: float
    skewness: float | None = None
    kurtosis: float | None = None
    cv: float | None = None  # Coefficient of variation (stddev/mean)
    percentiles: dict[str, float | None] = Field(default_factory=dict)


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
    """A value with its count."""

    value: Any
    count: int
    percentage: float


class DetectedPattern(BaseModel):
    """A detected pattern in column values.

    Used by the schema profiler for pattern detection on raw tables.
    Patterns are stored in SchemaProfileResult.detected_patterns (dict by column name),
    NOT in ColumnProfile which is for statistics stage only.
    """

    name: str
    match_rate: float
    semantic_type: str | None = None


class ColumnProfile(BaseModel):
    """Statistical profile of a column.

    This model is for statistics stage (typed tables) only.
    Pattern detection is done in schema stage and stored separately.
    """

    column_id: str
    column_ref: ColumnRef
    profiled_at: datetime

    total_count: int
    null_count: int
    distinct_count: int

    null_ratio: float
    cardinality_ratio: float

    numeric_stats: NumericStats | None = None
    string_stats: StringStats | None = None

    histogram: list[HistogramBucket] | None = None
    top_values: list[ValueCount] | None = None


class StatisticsProfileResult(BaseModel):
    """Result of statistics profiling (typed stage, all stats).

    Contains all row-based statistics computed on clean typed data.
    Note: correlation_result is handled separately by analysis/correlation module.
    """

    column_profiles: list[ColumnProfile] = Field(default_factory=list)
    duration_seconds: float


# =============================================================================
# Statistical Quality Models (moved from quality/models.py in Phase 9A)
# =============================================================================


class BenfordAnalysis(BaseModel):
    """Benford's Law compliance analysis."""

    chi_square: float
    p_value: float
    is_compliant: bool  # p_value > 0.05
    digit_distribution: dict[str, float]  # {1: 0.301, 2: 0.176, ...}
    interpretation: str


class OutlierDetection(BaseModel):
    """Outlier detection results."""

    # IQR Method
    iqr_lower_fence: float
    iqr_upper_fence: float
    iqr_outlier_count: int
    iqr_outlier_ratio: float

    # Isolation Forest
    isolation_forest_score: float  # Average anomaly score
    isolation_forest_anomaly_count: int
    isolation_forest_anomaly_ratio: float

    # Sample outliers
    outlier_samples: list[dict[str, Any]] = Field(default_factory=list)  # [{value, method, score}]


class StatisticalQualityResult(BaseModel):
    """Comprehensive statistical quality assessment.

    This is the Pydantic source of truth for statistical quality metrics.
    Gets serialized to StatisticalQualityMetrics.quality_data JSONB field.
    """

    column_id: str
    column_ref: ColumnRef

    # Benford's Law (for financial/count columns)
    benford_analysis: BenfordAnalysis | None = None

    # Outlier detection
    outlier_detection: OutlierDetection | None = None

    # Quality issues detected
    quality_issues: list[dict[str, Any]] = Field(
        default_factory=list
    )  # [{issue_type, severity, description}]
