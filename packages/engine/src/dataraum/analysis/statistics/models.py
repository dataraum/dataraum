"""Statistical Profile Models.

Pydantic models for statistical profiling data structures:
- ColumnProfile: Complete statistical profile of a column
- NumericStats: Statistics for numeric columns
- StringStats: Statistics for string columns
- HistogramBucket: Histogram bin
- ValueCount: Frequency count for top values
- StatisticsProfileResult: Result of statistics profiling

Statistical Quality Models (moved from quality/models.py in Phase 9A):
- BenfordAnalysis: Benford's Law compliance analysis
- OutlierDetection: Outlier detection results (IQR + Modified Z-Score)
- StatisticalQualityResult: Comprehensive statistical quality assessment
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

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
    mad: float | None = None  # Median Absolute Deviation
    robust_cv: float | None = None  # MAD / |median|, outlier-resistant CV
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


class ColumnProfile(BaseModel):
    """Statistical profile of a column.

    This model is for statistics stage (typed tables) only.
    Pattern detection is done in schema stage and stored separately.
    """

    column_id: str
    column_ref: ColumnRef
    original_name: str | None = None
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


# Benford applicability vocabulary (DAT-843/853): single home — the Pydantic
# Literal below, the ``benford_status`` CHECK (quality_db_models.py) and every
# consumer (LLM flag, entropy detector) read these values.
BENFORD_COMPLIANT = "compliant"
BENFORD_VIOLATING = "violating"
BENFORD_NOT_APPLICABLE = "not_applicable"
BENFORD_STATUSES: tuple[str, ...] = (
    BENFORD_COMPLIANT,
    BENFORD_VIOLATING,
    BENFORD_NOT_APPLICABLE,
)


class BenfordAnalysis(BaseModel):
    """Benford's Law applicability + compliance analysis.

    ``status`` is the typed outcome (DAT-843): 'compliant' / 'violating' are
    measurements; 'not_applicable' means the law is mathematically undefined for
    this column — values confined to under ~one order of magnitude (bounded
    small-integer counts) have scale-determined leading digits, so a chi-square
    verdict would be noise, not evidence. The test statistics exist exactly when
    measured (model-enforced).
    """

    # Literal must spell the values; keep in lockstep with BENFORD_STATUSES above.
    status: Literal["compliant", "violating", "not_applicable"]
    # log10(max|v| / min|v|) over the non-zero values — the applicability gate's
    # measured input, kept for all outcomes.
    magnitude_span_decades: float
    chi_square: float | None
    p_value: float | None
    digit_distribution: dict[str, float] | None  # {1: 0.301, 2: 0.176, ...}
    interpretation: str

    @model_validator(mode="after")
    def _statistics_iff_measured(self) -> BenfordAnalysis:
        measured = self.status != BENFORD_NOT_APPLICABLE
        have = (
            self.chi_square is not None
            and self.p_value is not None
            and self.digit_distribution is not None
        )
        have_none = (
            self.chi_square is None and self.p_value is None and self.digit_distribution is None
        )
        if measured and not have:
            raise ValueError(
                "measured BenfordAnalysis requires chi_square/p_value/digit_distribution"
            )
        if not measured and not have_none:
            raise ValueError("not_applicable BenfordAnalysis must not carry test statistics")
        return self


class OutlierDetection(BaseModel):
    """Outlier detection results."""

    # IQR Method
    iqr_lower_fence: float
    iqr_upper_fence: float
    iqr_outlier_count: int
    iqr_outlier_ratio: float

    # Modified Z-Score (MAD-based)
    zscore_outlier_count: int = 0
    zscore_outlier_ratio: float = 0.0

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
