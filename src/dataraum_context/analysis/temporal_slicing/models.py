"""Pydantic models for temporal slice analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
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

    # Baseline window sizes (for rolling averages)
    # Default: 30 days for daily, 5 weeks for weekly, 1 month for monthly
    baseline_periods: int | None = None  # Auto-determined if None

    # Thresholds
    completeness_threshold: float = 0.9  # coverage_ratio threshold
    drift_threshold: float = 0.1  # JS divergence threshold
    volume_zscore_threshold: float = 2.5  # z-score for anomaly
    last_day_ratio_threshold: float = 0.3  # for cutoff detection

    def get_baseline_periods(self) -> int:
        """Get baseline periods, using defaults if not specified."""
        if self.baseline_periods is not None:
            return self.baseline_periods
        return {
            TimeGrain.DAILY: 30,
            TimeGrain.WEEKLY: 5,
            TimeGrain.MONTHLY: 1,
        }[self.time_grain]


class PeriodMetrics(BaseModel):
    """Metrics for a single time period."""

    period_start: date
    period_end: date
    period_label: str  # e.g., "2024-01", "2024-W05"

    # Volume metrics
    row_count: int
    expected_days: int
    observed_days: int
    coverage_ratio: float

    # Day-level metrics for completeness check
    last_day_volume: int | None = None
    avg_day_volume: float | None = None
    last_day_ratio: float | None = None
    max_date_in_period: date | None = None
    days_until_end: int | None = None

    # Rolling statistics (computed across periods)
    volume_rolling_avg: float | None = None
    volume_rolling_std: float | None = None
    z_score: float | None = None
    period_over_period_change: float | None = None


class CompletenessResult(BaseModel):
    """Result of period completeness analysis."""

    period_label: str
    coverage_ratio: float
    is_complete: bool
    days_missing_at_end: int
    has_early_cutoff: bool
    has_volume_dropoff: bool
    last_day_ratio: float | None = None
    issues: list[str] = Field(default_factory=list)


class DistributionDriftResult(BaseModel):
    """Result of distribution drift analysis for a categorical column."""

    column_name: str
    period_label: str
    previous_period_label: str | None = None

    # Drift metrics
    jensen_shannon_divergence: float | None = None
    chi_square_statistic: float | None = None
    chi_square_p_value: float | None = None

    # Category changes
    new_categories: list[str] = Field(default_factory=list)
    missing_categories: list[str] = Field(default_factory=list)

    # Flags
    has_significant_drift: bool = False
    has_category_changes: bool = False
    issues: list[str] = Field(default_factory=list)


class SliceTimeCell(BaseModel):
    """A single cell in the slice × time matrix."""

    slice_value: str
    period_label: str
    row_count: int
    period_over_period_change: float | None = None


class SliceTimeMatrix(BaseModel):
    """Cross-slice temporal comparison matrix."""

    slice_column: str
    periods: list[str]
    slices: list[str]

    # Matrix data: slice_value -> period_label -> metrics
    data: dict[str, dict[str, SliceTimeCell]]

    # Per-slice trends
    slice_trends: dict[str, float]  # slice_value -> trend percentage

    # Global totals per period
    period_totals: dict[str, int]

    # Insights
    hidden_trends: list[str] = Field(default_factory=list)
    compensating_slices: list[tuple[str, str]] = Field(default_factory=list)


class VolumeAnomalyResult(BaseModel):
    """Result of volume anomaly detection."""

    period_label: str
    volume: int
    rolling_avg: float
    rolling_std: float
    z_score: float
    is_anomaly: bool
    anomaly_type: str | None = None  # "spike", "drop", "gap"
    period_over_period_change: float | None = None
    issues: list[str] = Field(default_factory=list)


class TemporalAnalysisResult(BaseModel):
    """Complete result of temporal slice analysis."""

    config: TemporalSliceConfig
    slice_table_name: str
    time_column: str

    # Period-level metrics
    period_metrics: list[PeriodMetrics]

    # Level 1: Completeness
    completeness_results: list[CompletenessResult]

    # Level 2: Distribution drift (per categorical column)
    drift_results: list[DistributionDriftResult]

    # Level 3: Slice × Time matrix
    slice_time_matrix: SliceTimeMatrix | None = None

    # Level 4: Volume anomalies
    volume_anomalies: list[VolumeAnomalyResult]

    # Summary
    total_periods: int
    incomplete_periods: int
    anomaly_count: int
    drift_detected: bool

    # Investigation SQL
    investigation_queries: list[dict[str, str]] = Field(default_factory=list)


@dataclass
class AggregatedTemporalData:
    """Aggregated temporal data for quality summary.

    Used to pass temporal findings to Phase 9 quality summary.
    """

    slice_column_name: str
    time_column: str
    total_periods: int

    # Completeness summary
    incomplete_period_count: int
    avg_coverage_ratio: float
    early_cutoff_count: int

    # Drift summary
    drift_detected_count: int

    # Volume summary
    volume_anomaly_count: int

    # Optional fields with defaults
    max_js_divergence: float | None = None
    max_zscore: float | None = None
    category_change_periods: list[str] = field(default_factory=list)
    gap_periods: list[str] = field(default_factory=list)

    # Slice comparison summary
    declining_slices: list[str] = field(default_factory=list)
    growing_slices: list[str] = field(default_factory=list)
    hidden_trend_insights: list[str] = field(default_factory=list)


@dataclass
class PeriodTopology:
    """Topology metrics for a single time period."""

    period_start: str
    period_end: str
    betti_0: int  # Connected components
    betti_1: int  # Cycles
    betti_2: int  # Voids
    structural_complexity: int
    num_correlations: int  # Edges in correlation graph
    avg_correlation: float
    has_anomalies: bool = False


@dataclass
class TopologyDrift:
    """Detected change in topology between periods."""

    period_from: str
    period_to: str
    metric: str  # betti_0, betti_1, complexity, correlation_density
    value_from: float
    value_to: float
    change_pct: float
    is_significant: bool = False


@dataclass
class TemporalTopologyResult:
    """Result of temporal topology analysis."""

    table_name: str
    time_column: str
    periods_analyzed: int = 0
    period_topologies: list[PeriodTopology] = field(default_factory=list)
    topology_drifts: list[TopologyDrift] = field(default_factory=list)
    trend_direction: str = "stable"  # increasing, decreasing, stable, volatile
    avg_complexity: float = 0.0
    complexity_variance: float = 0.0
    structural_anomaly_periods: list[str] = field(default_factory=list)


__all__ = [
    "TimeGrain",
    "TemporalSliceConfig",
    "PeriodMetrics",
    "CompletenessResult",
    "DistributionDriftResult",
    "SliceTimeCell",
    "SliceTimeMatrix",
    "VolumeAnomalyResult",
    "TemporalAnalysisResult",
    "AggregatedTemporalData",
    "PeriodTopology",
    "TopologyDrift",
    "TemporalTopologyResult",
]
