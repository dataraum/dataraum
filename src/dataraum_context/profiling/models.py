"""Profiling layer models.

Defines data structures for statistical profiling and type inference."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dataraum_context.core.models.base import (
    ColumnRef,
    DataType,
    DecisionSource,
)


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
    """A detected pattern in column values."""

    name: str
    match_rate: float
    semantic_type: str | None = None


class TypeCandidate(BaseModel):
    """A candidate type for a column."""

    column_id: str
    column_ref: ColumnRef

    data_type: DataType
    confidence: float
    parse_success_rate: float
    failed_examples: list[str] = Field(default_factory=list)

    detected_pattern: str | None = None
    pattern_match_rate: float | None = None

    detected_unit: str | None = None
    unit_confidence: float | None = None


class ColumnProfile(BaseModel):
    """Statistical profile of a column."""

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
    detected_patterns: list[DetectedPattern] = Field(default_factory=list)


class ProfileResult(BaseModel):
    """Result of profiling operation."""

    profiles: list[ColumnProfile]
    type_candidates: list[TypeCandidate]
    duration_seconds: float


# === Type Resolution Models ===


class TypeDecision(BaseModel):
    """A type decision for a column."""

    column_id: str
    decided_type: DataType
    decision_source: DecisionSource = DecisionSource.AUTO
    decision_reason: str | None = None


class ColumnCastResult(BaseModel):
    """Cast result for a single column."""

    column_id: str
    column_ref: ColumnRef
    source_type: str
    target_type: DataType
    success_count: int
    failure_count: int
    success_rate: float
    failure_samples: list[str] = Field(default_factory=list)


class TypeResolutionResult(BaseModel):
    """Result of type resolution."""

    typed_table_name: str
    quarantine_table_name: str

    total_rows: int
    typed_rows: int
    quarantined_rows: int

    column_results: list[ColumnCastResult]


# === Statistical Quality Models ===


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


class DistributionStability(BaseModel):
    """Distribution stability analysis (KS test)."""

    ks_statistic: float
    ks_p_value: float
    is_stable: bool  # p_value > 0.01
    comparison_period_start: datetime | None = None
    comparison_period_end: datetime | None = None


class VIFResult(BaseModel):
    """Variance Inflation Factor result (multicollinearity detection)."""

    column_id: str
    column_ref: ColumnRef
    vif_score: float
    has_multicollinearity: bool  # VIF > 10
    correlated_columns: list[str] = Field(default_factory=list)  # Column IDs


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

    # Distribution stability
    distribution_stability: DistributionStability | None = None

    # Multicollinearity (VIF)
    vif_score: float | None = None
    vif_correlated_columns: list[str] = Field(default_factory=list)  # Column IDs

    # Overall quality assessment
    quality_score: float  # 0-1 aggregate
    quality_issues: list[dict[str, Any]] = Field(
        default_factory=list
    )  # [{issue_type, severity, description}]


class StatisticalProfilingResult(BaseModel):
    """Result of statistical profiling and quality analysis operation."""

    profiles: list[ColumnProfile] = Field(default_factory=list)
    statistical_quality: list[StatisticalQualityResult] = Field(default_factory=list)
    duration_seconds: float
