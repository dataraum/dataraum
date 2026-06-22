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

    # Update frequency
    update_frequency: UpdateFrequencyAnalysis | None = None

    # Fiscal calendar
    fiscal_calendar: FiscalCalendarAnalysis | None = None

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


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Basic detection
    "TemporalGapInfo",
    "TemporalCompletenessAnalysis",
    # Update frequency
    "UpdateFrequencyAnalysis",
    # Fiscal calendar
    "FiscalCalendarAnalysis",
    # Quality issues
    "TemporalQualityIssue",
    # Main results
    "TemporalAnalysisResult",
    "TemporalTableSummary",
    "TemporalProfileResult",
]
