"""Temporal analysis models.

Consolidated Pydantic models for all temporal analysis:
- Detection: granularity, gaps, completeness
- Patterns: seasonality, trends, change points, fiscal calendar
- Quality: distribution stability, update frequency
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dataraum.core.models.base import ColumnRef

# =============================================================================
# Basic Temporal Detection Models
# =============================================================================


class TemporalGapInfo(BaseModel):
    """Information about a gap in the time series."""

    gap_start: datetime
    gap_end: datetime
    gap_length_days: float
    missing_periods: int
    severity: str  # 'minor', 'moderate', 'severe'


class TemporalCompletenessAnalysis(BaseModel):
    """Temporal completeness analysis."""

    completeness_ratio: float  # 0-1
    expected_periods: int
    actual_periods: int
    gap_count: int
    largest_gap_days: float | None = None
    gaps: list[TemporalGapInfo] = Field(default_factory=list)


# =============================================================================
# Seasonality Models
# =============================================================================


class SeasonalDecompositionResult(BaseModel):
    """Seasonal decomposition results (additive or multiplicative)."""

    seasonal_component: list[float] = Field(default_factory=list)
    trend_component: list[float] = Field(default_factory=list)
    residual_component: list[float] = Field(default_factory=list)
    model_type: str = "additive"  # 'additive' or 'multiplicative'

    # Strength metrics
    seasonality_strength: float | None = None  # 1 - Var(resid) / Var(detrended)
    trend_strength: float | None = None  # 1 - Var(resid) / Var(deseasonalized)
    seasonal_pattern_summary: dict[str, Any] | None = None
    period: int | None = None


class SeasonalityAnalysis(BaseModel):
    """Seasonality detection results."""

    has_seasonality: bool
    strength: float  # 0-1, how strong the seasonal component is
    period: str | None = None  # 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
    period_length: int | None = None  # Number of observations in a period
    peaks: dict[str, int | float] = Field(
        default_factory=dict
    )  # e.g., {"month": 12, "day_of_week": 5}
    model_type: str | None = None  # 'additive' or 'multiplicative'
    decomposition: SeasonalDecompositionResult | None = None


# =============================================================================
# Trend Models
# =============================================================================


class TrendAnalysis(BaseModel):
    """Trend detection results."""

    has_trend: bool
    strength: float  # 0-1
    direction: str  # 'increasing', 'decreasing', 'stable'
    slope: float | None = None
    autocorrelation_lag1: float | None = None


class ChangePointResult(BaseModel):
    """A detected change point in the time series."""

    change_point_id: str
    detected_at: datetime
    index_position: int | None = None

    # Change characteristics
    change_type: str  # 'trend_break', 'level_shift', 'variance_change'
    magnitude: float | None = None
    confidence: float  # 0-1

    # Before/after statistics
    mean_before: float | None = None
    mean_after: float | None = None
    variance_before: float | None = None
    variance_after: float | None = None
    detection_method: str  # 'pelt', 'cusum', etc.


# =============================================================================
# Update Frequency Models
# =============================================================================


class UpdateFrequencyAnalysis(BaseModel):
    """Update frequency and regularity analysis."""

    update_frequency_score: float  # 0-1
    median_interval_seconds: float
    interval_std: float | None = None
    interval_cv: float | None = None

    # Freshness
    last_update: datetime | None = None
    data_freshness_days: float | None = None
    is_stale: bool = False


# =============================================================================
# Fiscal Calendar Models
# =============================================================================


class FiscalCalendarAnalysis(BaseModel):
    """Fiscal calendar alignment analysis."""

    fiscal_alignment_detected: bool
    fiscal_year_end_month: int | None = None  # 1-12
    confidence: float = 0.0  # 0-1

    # Period-end effects
    has_period_end_effects: bool = False
    period_end_spike_ratio: float | None = None
    detected_periods: list[str] = Field(default_factory=list)


# =============================================================================
# Distribution Stability Models
# =============================================================================


class DistributionShiftResult(BaseModel):
    """Distribution shift detected between two periods."""

    shift_id: str

    # Time periods
    period1_start: datetime
    period1_end: datetime
    period2_start: datetime
    period2_end: datetime

    # Test results
    test_statistic: float  # KS statistic
    p_value: float
    is_significant: bool

    # Distribution characteristics
    period1_mean: float | None = None
    period2_mean: float | None = None
    period1_std: float | None = None
    period2_std: float | None = None

    # Interpretation
    shift_direction: str | None = None
    shift_magnitude: float | None = None


class DistributionStabilityAnalysis(BaseModel):
    """Distribution stability across time periods."""

    stability_score: float  # 0-1
    shift_count: int
    shifts: list[DistributionShiftResult] = Field(default_factory=list)

    # Overall statistics
    mean_ks_statistic: float | None = None
    max_ks_statistic: float | None = None


# =============================================================================
# Quality Issue Model
# =============================================================================


class TemporalQualityIssue(BaseModel):
    """A quality issue detected in temporal analysis."""

    issue_type: str  # 'low_completeness', 'large_gap', 'stale_data', etc.
    severity: str  # 'low', 'medium', 'high'
    description: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    detected_at: datetime


# =============================================================================
# Main Result Models
# =============================================================================


class TemporalAnalysisResult(BaseModel):
    """Complete temporal analysis result for a single column.

    This is the per-column result type returned in TemporalProfileResult.column_profiles.
    """

    metric_id: str
    column_id: str
    column_ref: ColumnRef
    column_name: str
    table_name: str
    computed_at: datetime

    # Basic temporal info
    min_timestamp: datetime
    max_timestamp: datetime
    span_days: float
    detected_granularity: str
    granularity_confidence: float

    # Completeness
    completeness: TemporalCompletenessAnalysis | None = None

    # Seasonality
    seasonality: SeasonalityAnalysis | None = None

    # Trend
    trend: TrendAnalysis | None = None
    change_points: list[ChangePointResult] = Field(default_factory=list)

    # Update frequency
    update_frequency: UpdateFrequencyAnalysis | None = None

    # Fiscal calendar
    fiscal_calendar: FiscalCalendarAnalysis | None = None

    # Distribution stability
    distribution_stability: DistributionStabilityAnalysis | None = None

    # Quality issues
    quality_issues: list[TemporalQualityIssue] = Field(default_factory=list)
    has_issues: bool = False


class TemporalTableSummary(BaseModel):
    """Table-level summary of temporal analysis across multiple temporal columns."""

    table_id: str
    table_name: str
    temporal_column_count: int
    total_issues: int

    # Counts of columns with specific patterns
    columns_with_seasonality: int = 0
    columns_with_trends: int = 0
    columns_with_change_points: int = 0
    columns_with_fiscal_alignment: int = 0

    # Overall freshness
    stalest_column_days: int | None = None
    has_stale_columns: bool = False

    # Timestamp
    profiled_at: datetime | None = None


class TemporalProfileResult(BaseModel):
    """Result of temporal profiling for a table.

    This is the main return type for profile_temporal(), following the
    same pattern as StatisticsProfileResult.
    """

    column_profiles: list[TemporalAnalysisResult] = Field(default_factory=list)
    table_summary: TemporalTableSummary | None = None
    duration_seconds: float = 0.0


class TemporalEnrichmentResult(BaseModel):
    """Result of temporal enrichment operation (batch processing).

    DEPRECATED: Use TemporalProfileResult instead.
    """

    profiles: list[TemporalAnalysisResult] = Field(default_factory=list)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Basic detection
    "TemporalGapInfo",
    "TemporalCompletenessAnalysis",
    # Seasonality
    "SeasonalDecompositionResult",
    "SeasonalityAnalysis",
    # Trend
    "TrendAnalysis",
    "ChangePointResult",
    # Update frequency
    "UpdateFrequencyAnalysis",
    # Fiscal calendar
    "FiscalCalendarAnalysis",
    # Distribution stability
    "DistributionShiftResult",
    "DistributionStabilityAnalysis",
    # Quality issues
    "TemporalQualityIssue",
    # Main results
    "TemporalAnalysisResult",
    "TemporalTableSummary",
    "TemporalProfileResult",
    "TemporalEnrichmentResult",  # Deprecated
]
