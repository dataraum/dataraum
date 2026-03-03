"""Pydantic models for temporal slice analysis."""

from __future__ import annotations

from datetime import date
from enum import Enum

from pydantic import BaseModel, Field


class TimeGrain(str, Enum):
    """Time granularity for period analysis."""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class TemporalSliceConfig(BaseModel):
    """Configuration for temporal slice analysis."""

    time_column: str
    period_start: date
    period_end: date
    time_grain: TimeGrain = TimeGrain.MONTHLY

    # Thresholds
    drift_threshold: float = 0.1  # JS divergence threshold
    completeness_threshold: float = 0.9  # coverage_ratio for "complete"
    volume_zscore_threshold: float = 2.5  # z-score for anomaly
    last_day_ratio_threshold: float = 0.3  # early cutoff detection


class CategoryShift(BaseModel):
    """A significant change in category proportion."""

    category: str
    baseline_pct: float
    period_pct: float
    period: str


class CategoryAppearance(BaseModel):
    """A category that emerged (wasn't in baseline)."""

    category: str
    period: str
    pct: float


class CategoryDisappearance(BaseModel):
    """A category that vanished (was in baseline, gone in period)."""

    category: str
    period: str
    last_seen_pct: float


class DriftEvidence(BaseModel):
    """Evidence for interpreting what drifted."""

    worst_period: str
    worst_js: float
    top_shifts: list[CategoryShift] = Field(default_factory=list)
    emerged_categories: list[CategoryAppearance] = Field(default_factory=list)
    vanished_categories: list[CategoryDisappearance] = Field(default_factory=list)
    change_points: list[str] = Field(default_factory=list)


class ColumnDriftResult(BaseModel):
    """Result of drift analysis for one column."""

    column_name: str
    max_js_divergence: float
    mean_js_divergence: float
    periods_analyzed: int
    periods_with_drift: int
    drift_evidence: DriftEvidence | None = None


class PeriodMetrics(BaseModel):
    """Per-period volume and day-level metrics."""

    period_label: str
    period_start: date
    period_end: date
    row_count: int
    expected_days: int
    observed_days: int
    coverage_ratio: float
    last_day_ratio: float
    rolling_avg: float | None = None
    rolling_std: float | None = None
    z_score: float | None = None
    period_over_period_change: float | None = None


class CompletenessResult(BaseModel):
    """Completeness assessment for a period."""

    period_label: str
    is_complete: bool
    coverage_ratio: float
    has_early_cutoff: bool
    days_missing_at_end: int


class VolumeAnomalyResult(BaseModel):
    """Volume anomaly detection for a period."""

    period_label: str
    is_anomaly: bool
    anomaly_type: str | None = None  # "spike", "drop", "gap"
    z_score: float
    period_over_period_change: float | None = None


class PeriodAnalysisResult(BaseModel):
    """Combined result of period-level analysis for a slice table."""

    slice_table_name: str
    time_column: str
    total_periods: int
    incomplete_periods: int
    anomaly_count: int
    period_metrics: list[PeriodMetrics]
    completeness_results: list[CompletenessResult]
    volume_anomalies: list[VolumeAnomalyResult]


__all__ = [
    "TimeGrain",
    "TemporalSliceConfig",
    "CategoryShift",
    "CategoryAppearance",
    "CategoryDisappearance",
    "DriftEvidence",
    "ColumnDriftResult",
    "PeriodMetrics",
    "CompletenessResult",
    "VolumeAnomalyResult",
    "PeriodAnalysisResult",
]
