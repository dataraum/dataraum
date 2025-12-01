"""Pydantic models for Temporal Context (Pillar 4).

These models represent enhanced temporal quality analysis including:
- Seasonality strength (quantified)
- Trend breaks (change points)
- Update frequency scoring
- Fiscal calendar alignment
- Distribution stability across time periods
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# ============================================================================
# Seasonality Models
# ============================================================================


class SeasonalityAnalysis(BaseModel):
    """Seasonality analysis results."""

    has_seasonality: bool
    strength: float  # 0-1, how strong the seasonal component is
    period: str | None = None  # 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
    period_length: int | None = None  # Number of observations in a period
    peaks: dict[str, int | float] = Field(
        default_factory=dict
    )  # e.g., {"month": 12, "day_of_week": 5}
    model_type: str | None = None  # 'additive' or 'multiplicative'


class SeasonalDecompositionResult(BaseModel):
    """Results from seasonal decomposition."""

    decomposition_id: str
    model_type: str  # 'additive' or 'multiplicative'
    period: int | None = None

    # Component variances
    trend_variance: float | None = None
    seasonal_variance: float | None = None
    residual_variance: float | None = None

    # Strength metrics
    seasonality_strength: float | None = None  # 1 - Var(resid) / Var(detrended)
    trend_strength: float | None = None  # 1 - Var(resid) / Var(deseasonalized)

    # Summary statistics
    seasonal_pattern_summary: dict[str, Any] | None = None  # Peak/trough info


# ============================================================================
# Trend Models
# ============================================================================


class TrendAnalysis(BaseModel):
    """Trend analysis results."""

    has_trend: bool
    strength: float  # 0-1, how strong the trend is
    direction: str  # 'increasing', 'decreasing', 'stable'
    slope: float | None = None  # Trend slope (units per time period)
    autocorrelation_lag1: float | None = None  # First-order autocorrelation


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

    # Detection method
    detection_method: str  # 'pelt', 'binseg', 'window', etc.


# ============================================================================
# Update Frequency Models
# ============================================================================


class UpdateFrequencyAnalysis(BaseModel):
    """Update frequency and regularity analysis."""

    update_frequency_score: float  # 0-1, regularity of updates
    median_interval_seconds: float
    interval_std: float | None = None
    interval_cv: float | None = None  # Coefficient of variation

    # Freshness
    last_update: datetime | None = None
    data_freshness_days: float | None = None
    is_stale: bool = False  # Based on expected update frequency


# ============================================================================
# Fiscal Calendar Models
# ============================================================================


class FiscalCalendarAnalysis(BaseModel):
    """Fiscal calendar alignment analysis."""

    fiscal_alignment_detected: bool
    fiscal_year_end_month: int | None = None  # 1-12
    confidence: float = 0.0  # 0-1

    # Period-end effects
    has_period_end_effects: bool = False
    period_end_spike_ratio: float | None = None  # Ratio of end-of-period to average activity
    detected_periods: list[str] = Field(
        default_factory=list
    )  # ['month_end', 'quarter_end', 'year_end']


# ============================================================================
# Distribution Stability Models
# ============================================================================


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
    shift_direction: str | None = None  # 'increase', 'decrease', 'mixed'
    shift_magnitude: float | None = None


class DistributionStabilityAnalysis(BaseModel):
    """Distribution stability across time periods."""

    stability_score: float  # 0-1, higher = more stable
    shift_count: int
    shifts: list[DistributionShiftResult] = Field(default_factory=lambda: [])

    # Overall statistics
    mean_ks_statistic: float | None = None
    max_ks_statistic: float | None = None


# ============================================================================
# Completeness Models
# ============================================================================


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
    gaps: list[TemporalGapInfo] = Field(default_factory=lambda: [])


# ============================================================================
# Quality Issues
# ============================================================================


class TemporalQualityIssue(BaseModel):
    """A temporal quality issue."""

    issue_type: (
        str  # 'large_gap', 'irregular_updates', 'trend_break', 'distribution_shift', 'stale_data'
    )
    severity: str  # 'low', 'medium', 'high'
    description: str
    detected_at: datetime | None = None
    evidence: dict[str, Any] = Field(default_factory=lambda: {})


# ============================================================================
# Main Result Model
# ============================================================================


class TemporalQualityResult(BaseModel):
    """Complete temporal quality analysis result."""

    metric_id: str
    column_id: str
    column_name: str
    table_name: str
    computed_at: datetime

    # Basic temporal info
    min_timestamp: datetime
    max_timestamp: datetime
    span_days: float
    detected_granularity: str
    granularity_confidence: float

    # Seasonality
    seasonality: SeasonalityAnalysis | None = None

    # Trend
    trend: TrendAnalysis | None = None
    change_points: list[ChangePointResult] = Field(default_factory=lambda: [])

    # Update frequency
    update_frequency: UpdateFrequencyAnalysis | None = None

    # Fiscal calendar
    fiscal_calendar: FiscalCalendarAnalysis | None = None

    # Distribution stability
    distribution_stability: DistributionStabilityAnalysis | None = None

    # Completeness
    completeness: TemporalCompletenessAnalysis | None = None

    # Overall quality
    temporal_quality_score: float  # 0-1
    quality_issues: list[TemporalQualityIssue] = Field(default_factory=lambda: [])
    has_issues: bool = False


# ============================================================================
# Summary Model
# ============================================================================


class TemporalQualitySummary(BaseModel):
    """Summary of temporal quality for a table."""

    table_id: str
    table_name: str
    temporal_column_count: int
    avg_quality_score: float
    total_issues: int
    columns_with_seasonality: int
    columns_with_trends: int
    columns_with_change_points: int
    columns_with_fiscal_alignment: int
