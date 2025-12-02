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

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Mapped, mapped_column

from dataraum_context.storage.models_v2.base import Base


class TemporalQualityMetrics(Base):
    """Temporal quality metrics for a time column."""

    __tablename__ = "temporal_quality_metrics"

    metric_id: Mapped[str] = mapped_column(String, primary_key=True)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Basic temporal stats (from existing temporal.py)
    min_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    max_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    span_days: Mapped[float] = mapped_column(Float, nullable=False)
    detected_granularity: Mapped[str] = mapped_column(String, nullable=False)
    granularity_confidence: Mapped[float] = mapped_column(Float, nullable=False)

    # Seasonality (Phase 3)
    has_seasonality: Mapped[bool | None] = mapped_column(Boolean)
    seasonality_strength: Mapped[float | None] = mapped_column(Float)  # 0-1
    seasonality_period: Mapped[str | None] = mapped_column(
        String
    )  # 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
    seasonal_peaks: Mapped[dict[str, Any] | None] = mapped_column(
        JSON
    )  # {"month": 12, "day_of_week": 5}

    # Trend analysis (Phase 3)
    has_trend: Mapped[bool | None] = mapped_column(Boolean)
    trend_strength: Mapped[float | None] = mapped_column(Float)  # 0-1
    trend_direction: Mapped[str | None] = mapped_column(
        String
    )  # 'increasing', 'decreasing', 'stable'
    trend_slope: Mapped[float | None] = mapped_column(Float)
    autocorrelation_lag1: Mapped[float | None] = mapped_column(Float)

    # Change points (Phase 3)
    change_point_count: Mapped[int | None] = mapped_column(Integer)
    change_points: Mapped[dict[str, Any] | None] = mapped_column(
        JSON
    )  # List of detected break points

    # Update frequency (Phase 3)
    update_frequency_score: Mapped[float | None] = mapped_column(Float)  # 0-1, regularity
    median_update_interval_seconds: Mapped[float | None] = mapped_column(Float)
    update_interval_cv: Mapped[float | None] = mapped_column(Float)  # Coefficient of variation
    last_update_timestamp: Mapped[datetime | None] = mapped_column(DateTime)
    data_freshness_days: Mapped[float | None] = mapped_column(Float)

    # Fiscal calendar (Phase 3)
    fiscal_alignment_detected: Mapped[bool | None] = mapped_column(Boolean)
    fiscal_year_end_month: Mapped[int | None] = mapped_column(Integer)  # 1-12
    has_period_end_effects: Mapped[bool | None] = mapped_column(Boolean)
    period_end_spike_ratio: Mapped[float | None] = mapped_column(
        Float
    )  # Ratio of period-end activity

    # Distribution stability (Phase 3)
    distribution_stability_score: Mapped[float | None] = mapped_column(Float)  # 0-1
    distribution_shift_count: Mapped[int | None] = mapped_column(Integer)
    distribution_shifts: Mapped[dict[str, Any] | None] = mapped_column(
        JSON
    )  # KS test results by period

    # Completeness and quality
    completeness_ratio: Mapped[float | None] = mapped_column(Float)  # 0-1
    gap_count: Mapped[int | None] = mapped_column(Integer)
    largest_gap_days: Mapped[float | None] = mapped_column(Float)

    # Staleness indicator
    is_stale: Mapped[bool | None] = mapped_column(
        Boolean
    )  # Derived from freshness vs expected interval

    # Overall temporal quality score
    temporal_quality_score: Mapped[float | None] = mapped_column(Float)  # 0-1
    quality_issues: Mapped[dict[str, Any] | None] = mapped_column(JSON)  # List of detected issues


class SeasonalDecomposition(Base):
    """Seasonal decomposition results for time series."""

    __tablename__ = "seasonal_decomposition"

    decomposition_id: Mapped[str] = mapped_column(String, primary_key=True)
    metric_id: Mapped[str] = mapped_column(
        ForeignKey("temporal_quality_metrics.metric_id"), nullable=False
    )
    computed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Decomposition parameters
    model_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'additive' or 'multiplicative'
    period: Mapped[int | None] = mapped_column(Integer)  # Seasonal period

    # Component statistics
    trend_variance: Mapped[float | None] = mapped_column(Float)
    seasonal_variance: Mapped[float | None] = mapped_column(Float)
    residual_variance: Mapped[float | None] = mapped_column(Float)

    # Strength metrics
    seasonality_strength: Mapped[float | None] = mapped_column(
        Float
    )  # 1 - Var(resid) / Var(detrended)
    trend_strength: Mapped[float | None] = mapped_column(
        Float
    )  # 1 - Var(resid) / Var(deseasonalized)

    # Stored components (sampled or aggregated)
    seasonal_pattern: Mapped[dict[str, Any] | None] = mapped_column(
        JSON
    )  # Seasonal component values
    trend_summary: Mapped[dict[str, Any] | None] = mapped_column(JSON)  # Trend summary stats


class ChangePoint(Base):
    """Detected change points in time series."""

    __tablename__ = "change_points"

    change_point_id: Mapped[str] = mapped_column(String, primary_key=True)
    metric_id: Mapped[str] = mapped_column(
        ForeignKey("temporal_quality_metrics.metric_id"), nullable=False
    )

    # Change point location
    detected_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    index_position: Mapped[int | None] = mapped_column(Integer)  # Position in time series

    # Change characteristics
    change_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'trend_break', 'level_shift', 'variance_change'
    magnitude: Mapped[float | None] = mapped_column(Float)  # Change magnitude
    confidence: Mapped[float | None] = mapped_column(Float)  # Detection confidence

    # Before/after statistics
    mean_before: Mapped[float | None] = mapped_column(Float)
    mean_after: Mapped[float | None] = mapped_column(Float)
    variance_before: Mapped[float | None] = mapped_column(Float)
    variance_after: Mapped[float | None] = mapped_column(Float)

    # Detection method
    detection_method: Mapped[str | None] = mapped_column(String)  # 'pelt', 'binseg', 'window', etc.


class DistributionShift(Base):
    """Distribution shifts detected across time periods."""

    __tablename__ = "distribution_shifts"

    shift_id: Mapped[str] = mapped_column(String, primary_key=True)
    metric_id: Mapped[str] = mapped_column(
        ForeignKey("temporal_quality_metrics.metric_id"), nullable=False
    )

    # Time periods being compared
    period1_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    period1_end: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    period2_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    period2_end: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Statistical test results
    test_statistic: Mapped[float] = mapped_column(Float, nullable=False)  # KS statistic
    p_value: Mapped[float] = mapped_column(Float, nullable=False)
    is_significant: Mapped[bool] = mapped_column(Boolean, nullable=False)

    # Distribution characteristics
    period1_mean: Mapped[float | None] = mapped_column(Float)
    period2_mean: Mapped[float | None] = mapped_column(Float)
    period1_std: Mapped[float | None] = mapped_column(Float)
    period2_std: Mapped[float | None] = mapped_column(Float)

    # Shift interpretation
    shift_direction: Mapped[str | None] = mapped_column(String)  # 'increase', 'decrease', 'mixed'
    shift_magnitude: Mapped[float | None] = mapped_column(Float)  # Normalized change


class UpdateFrequencyHistory(Base):
    """Historical tracking of update frequency patterns."""

    __tablename__ = "update_frequency_history"

    history_id: Mapped[str] = mapped_column(String, primary_key=True)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    measured_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Update metrics
    update_count: Mapped[int] = mapped_column(Integer, nullable=False)
    median_interval_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    interval_std: Mapped[float | None] = mapped_column(Float)
    interval_cv: Mapped[float | None] = mapped_column(Float)  # Coefficient of variation

    # Regularity score
    regularity_score: Mapped[float] = mapped_column(Float, nullable=False)  # 0-1

    # Time range
    period_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    period_end: Mapped[datetime] = mapped_column(DateTime, nullable=False)
