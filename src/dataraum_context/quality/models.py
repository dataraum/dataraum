"""Quality layer models.

Defines data structures for quality assessment and rules.

This module contains models for:
- Quality rules and scores (original content)
- Temporal quality analysis (seasonality, trends, change points)
- Topological quality analysis (Betti numbers, persistence, cycles)

These models are created by the quality layer during deep analysis,
not during the enrichment phase.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from dataraum_context.core.models.base import ColumnRef, DecisionSource, QualitySeverity


class QualityRule(BaseModel):
    """A quality rule."""

    rule_id: str

    table_name: str
    column_name: str | None = None

    rule_name: str
    rule_type: str
    rule_expression: str
    parameters: dict[str, Any] = Field(default_factory=dict)

    severity: QualitySeverity
    source: DecisionSource
    description: str | None = None


class RuleResult(BaseModel):
    """Result of a single rule execution."""

    rule_id: str
    rule_name: str

    total_records: int
    passed_records: int
    failed_records: int
    pass_rate: float

    failure_samples: list[dict[str, Any]] = Field(default_factory=list)


class QualityScore(BaseModel):
    """Aggregate quality score."""

    scope: str  # 'table' or 'column'
    scope_id: str
    scope_name: str

    completeness: float
    validity: float
    consistency: float
    uniqueness: float
    timeliness: float

    overall: float


class Anomaly(BaseModel):
    """A detected anomaly."""

    table_name: str
    column_name: str | None = None

    anomaly_type: str
    description: str
    severity: QualitySeverity
    evidence: dict[str, Any] = Field(default_factory=dict)


# === Shared Quality Models ===


class QualityIssue(BaseModel):
    """A quality issue detected during analysis."""

    issue_type: str  # 'low_completeness', 'large_gap', 'stale_data', 'irregular_updates'
    severity: str  # 'critical', 'warning', 'info'
    description: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    detected_at: datetime | None = None  # When issue was detected


# === Temporal Quality Models ===


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


class FiscalCalendarAnalysis(BaseModel):
    """Fiscal calendar alignment analysis."""

    fiscal_alignment_detected: bool
    fiscal_year_end_month: int | None = None  # 1-12
    confidence: float = 0.0  # 0-1

    # Period-end effects
    has_period_end_effects: bool = False
    period_end_spike_ratio: float | None = None
    detected_periods: list[str] = Field(default_factory=list)


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


class TemporalQualityResult(BaseModel):
    """Complete temporal quality analysis result.

    This is the Pydantic source of truth for temporal quality metrics.
    Gets serialized to TemporalQualityMetrics.temporal_data JSONB field.
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

    # Completeness
    completeness: TemporalCompletenessAnalysis | None = None

    # Overall quality
    temporal_quality_score: float  # 0-1
    quality_issues: list[QualityIssue] = Field(default_factory=list)
    has_issues: bool = False


class TemporalTableSummary(BaseModel):
    """Table-level summary of temporal quality across multiple temporal columns."""

    table_id: str
    table_name: str
    temporal_column_count: int
    avg_quality_score: float
    total_issues: int

    # Counts of columns with specific patterns
    columns_with_seasonality: int = 0
    columns_with_trends: int = 0
    columns_with_change_points: int = 0
    columns_with_fiscal_alignment: int = 0

    # Overall freshness
    stalest_column_days: int | None = None
    has_stale_columns: bool = False


# === Topological Quality Models ===


class BettiNumbers(BaseModel):
    """Betti numbers from homology analysis."""

    betti_0: int  # Connected components
    betti_1: int  # Cycles / holes
    betti_2: int  # Voids / cavities
    total_complexity: int  # Sum of Betti numbers
    is_connected: bool  # betti_0 == 1
    has_cycles: bool  # betti_1 > 0


class PersistencePoint(BaseModel):
    """A point in a persistence diagram."""

    dimension: int  # 0, 1, or 2
    birth: float
    death: float
    persistence: float  # death - birth


class PersistenceDiagram(BaseModel):
    """Persistence diagram for a specific dimension."""

    dimension: int
    points: list[PersistencePoint]
    max_persistence: float
    num_features: int
    persistent_entropy: float | None = None


class CycleDetection(BaseModel):
    """Detected persistent cycle."""

    cycle_id: str
    dimension: int
    birth: float
    death: float
    persistence: float
    involved_columns: list[str] = Field(default_factory=list)
    cycle_type: str | None = None  # 'money_flow', 'order_fulfillment', etc.
    is_anomalous: bool = False
    anomaly_reason: str | None = None
    first_detected: datetime
    last_seen: datetime


class StabilityAnalysis(BaseModel):
    """Homological stability assessment."""

    bottleneck_distance: float
    is_stable: bool
    stability_threshold: float = 0.1
    stability_level: str  # 'stable', 'minor_changes', 'significant_changes', 'unstable'

    # Change counts
    components_added: int = 0
    components_removed: int = 0
    cycles_added: int = 0
    cycles_removed: int = 0


class TopologicalAnomaly(BaseModel):
    """Detected topological anomaly."""

    anomaly_type: str  # 'unexpected_cycle', 'orphaned_component', 'complexity_spike'
    severity: str  # 'low', 'medium', 'high'
    description: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    affected_tables: list[str] = Field(default_factory=list)
    affected_columns: list[str] = Field(default_factory=list)


class TopologicalQualityResult(BaseModel):
    """Comprehensive topological quality assessment.

    This is the Pydantic source of truth for topological quality metrics.
    Gets serialized to TopologicalQualityMetrics.topology_data JSONB field.
    """

    table_id: str
    table_name: str

    # Betti numbers
    betti_numbers: BettiNumbers

    # Persistence diagrams
    persistence_diagrams: list[PersistenceDiagram] = Field(default_factory=list)

    # Detected cycles
    persistent_cycles: list[CycleDetection] = Field(default_factory=list)

    # Stability
    stability: StabilityAnalysis | None = None

    # Complexity metrics
    structural_complexity: int
    persistent_entropy: float | None = None
    orphaned_components: int
    complexity_trend: str | None = None
    complexity_within_bounds: bool = True

    # Historical complexity context
    complexity_mean: float | None = None
    complexity_std: float | None = None
    complexity_z_score: float | None = None

    # Quality assessment
    quality_score: float
    has_anomalies: bool = False
    anomalies: list[TopologicalAnomaly] = Field(default_factory=list)
    anomalous_cycles: list[CycleDetection] = Field(default_factory=list)
    quality_warnings: list[str] = Field(default_factory=list)
    topology_description: str | None = None


# === Quality Synthesis Models (Pillar 5) ===
# Moved from core/models/quality_synthesis.py for better domain organization


class QualityDimension(str, Enum):
    """Standard data quality dimensions."""

    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    ACCURACY = "accuracy"


class QualitySynthesisSeverity(str, Enum):
    """Severity levels for quality synthesis issues.

    Extended from base QualitySeverity with CRITICAL level.
    """

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class QualitySynthesisIssue(BaseModel):
    """A single quality issue detected during synthesis assessment.

    This is the comprehensive quality issue model used by Pillar 5 synthesis.
    Different from the simpler QualityIssue used by temporal/topological modules.
    """

    issue_id: str = Field(description="Unique identifier for this issue")
    issue_type: str = Field(description="Type of issue (e.g., 'benford_violation', 'large_gap')")
    severity: QualitySynthesisSeverity = Field(description="Severity level")
    dimension: QualityDimension = Field(description="Which quality dimension this affects")

    # Scope
    table_id: str | None = Field(None, description="Table ID if table-level issue")
    column_id: str | None = Field(None, description="Column ID if column-level issue")
    column_name: str | None = Field(None, description="Column name for readability")

    # Description
    description: str = Field(description="Human-readable description of the issue")
    recommendation: str | None = Field(None, description="Suggested remediation")

    # Evidence
    evidence: dict[str, Any] = Field(
        default_factory=dict, description="Supporting data (metrics, examples, etc.)"
    )

    # Source
    source_pillar: int = Field(description="Which pillar detected this (1-5)")
    source_module: str = Field(description="Which module detected this")
    detected_at: datetime = Field(description="When this was detected")


class DimensionScore(BaseModel):
    """Quality score for a single dimension."""

    dimension: QualityDimension
    score: float = Field(ge=0.0, le=1.0, description="Score from 0 (poor) to 1 (excellent)")

    # Contributing factors
    completeness_ratio: float | None = Field(None, ge=0.0, le=1.0)
    null_ratio: float | None = Field(None, ge=0.0, le=1.0)
    parse_success_rate: float | None = Field(None, ge=0.0, le=1.0)
    validation_pass_rate: float | None = Field(None, ge=0.0, le=1.0)

    # Issues affecting this dimension
    issue_count: int = Field(default=0, description="Number of issues in this dimension")
    critical_issues: int = Field(default=0, description="Number of critical issues")

    # Explanation
    explanation: str = Field(description="What this score means")


class ColumnQualityAssessment(BaseModel):
    """Quality assessment for a single column."""

    column_id: str
    column_name: str

    # Dimensional scores
    dimension_scores: list[DimensionScore] = Field(
        description="Scores for each applicable dimension"
    )

    # Overall score (weighted average of dimensions)
    overall_score: float = Field(ge=0.0, le=1.0, description="Overall quality score")

    # Issues specific to this column
    issues: list[QualitySynthesisIssue] = Field(default_factory=list)

    # Contributing pillars
    has_statistical_quality: bool = Field(default=False)
    has_temporal_quality: bool = Field(default=False)
    has_semantic_context: bool = Field(default=False)

    # Computed at
    assessed_at: datetime


class TableQualityAssessment(BaseModel):
    """Quality assessment for a table."""

    table_id: str
    table_name: str

    # Dimensional scores (table-level)
    dimension_scores: list[DimensionScore] = Field(
        description="Scores for each applicable dimension"
    )

    # Overall score
    overall_score: float = Field(ge=0.0, le=1.0, description="Overall table quality score")

    # Column assessments
    column_assessments: list[ColumnQualityAssessment] = Field(default_factory=list)

    # Table-level issues
    issues: list[QualitySynthesisIssue] = Field(default_factory=list)

    # Contributing pillars
    has_statistical_quality: bool = Field(default=False)
    has_topological_quality: bool = Field(default=False)
    has_temporal_quality: bool = Field(default=False)
    has_domain_quality: bool = Field(default=False)

    # Computed at
    assessed_at: datetime


class QualitySynthesisResult(BaseModel):
    """Complete quality synthesis for a table and its columns.

    This is the Pillar 5 output that aggregates quality from all other pillars.
    """

    table_id: str
    table_name: str

    # Table-level assessment
    table_assessment: TableQualityAssessment

    # Summary statistics
    total_columns: int
    columns_assessed: int
    total_issues: int
    critical_issues: int
    warnings: int

    # Issue breakdown by dimension
    issues_by_dimension: dict[str, int] = Field(
        default_factory=dict, description="Count of issues per dimension"
    )

    # Issue breakdown by pillar
    issues_by_pillar: dict[int, int] = Field(
        default_factory=dict,
        description="Count of issues per pillar (1=Statistical, 2=Topological, etc.)",
    )

    # Quality summary (for LLM/human consumption)
    quality_summary: str | None = Field(
        None, description="Natural language summary of quality assessment"
    )

    # Recommendations
    top_recommendations: list[str] = Field(
        default_factory=list, description="Prioritized list of recommendations"
    )

    # Metadata
    synthesis_duration_seconds: float
    synthesized_at: datetime


class DatasetQualitySynthesisResult(BaseModel):
    """Complete dataset-level quality synthesis result.

    Extends table-level quality assessment with cross-table metrics:
    - Cross-table multicollinearity
    - Cross-table consistency violations
    - Dataset-level quality aggregation
    """

    # Scope
    table_ids: list[str]
    table_names: list[str]

    # Table assessments
    table_assessments: list[QualitySynthesisResult] = Field(default_factory=list)

    # Overall metrics
    total_tables: int
    total_columns: int
    columns_assessed: int
    average_table_quality: float = Field(ge=0.0, le=1.0)
    average_column_quality: float = Field(ge=0.0, le=1.0)

    # Issues rollup (including cross-table issues)
    total_issues: int
    critical_issues: int
    warnings: int
    issues_by_dimension: dict[str, int] = Field(default_factory=dict)
    issues_by_pillar: dict[int, int] = Field(default_factory=dict)

    # Cross-table metrics
    has_cross_table_analysis: bool = Field(default=False)
    cross_table_dependencies: int = Field(default=0)
    cross_table_multicollinearity_severity: str | None = None  # "none", "moderate", "severe"
    cross_table_issues: list[QualitySynthesisIssue] = Field(default_factory=list)

    # Dataset-level summary
    dataset_summary: str | None = Field(None)
    top_recommendations: list[str] = Field(default_factory=list)

    # Metadata
    synthesis_duration_seconds: float
    synthesized_at: datetime
