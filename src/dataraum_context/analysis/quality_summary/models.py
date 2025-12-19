"""Pydantic models for quality summary analysis.

Contains data structures for aggregated quality metrics across slices
and the LLM-generated quality summaries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field


class SliceMetrics(BaseModel):
    """Quality metrics for a single slice."""

    slice_name: str
    slice_value: str
    row_count: int

    # Statistics
    null_count: int | None = None
    null_ratio: float | None = None
    distinct_count: int | None = None
    cardinality_ratio: float | None = None

    # Numeric stats (if applicable)
    min_value: float | None = None
    max_value: float | None = None
    mean_value: float | None = None
    stddev: float | None = None

    # Quality metrics
    benford_compliant: bool | None = None
    benford_p_value: float | None = None
    has_outliers: bool | None = None
    outlier_ratio: float | None = None


class SliceComparison(BaseModel):
    """Comparison of a metric across slices."""

    metric_name: str
    description: str
    min_value: float | None = None
    max_value: float | None = None
    mean_value: float | None = None
    variance: float | None = None
    outlier_slices: list[str] = Field(default_factory=list)
    notes: str | None = None


class QualityIssue(BaseModel):
    """A quality issue identified in the data."""

    issue_type: str
    severity: str  # low, medium, high, critical
    description: str
    affected_slices: list[str] = Field(default_factory=list)
    sample_values: list[str] | None = None
    investigation_sql: str | None = None


class ColumnQualitySummary(BaseModel):
    """LLM-generated quality summary for a column across slices.

    This is the main output model that will be displayed in the UI.
    """

    column_name: str
    source_table_name: str
    slice_column_name: str
    total_slices: int

    # Overall assessment
    overall_quality_score: float = Field(ge=0.0, le=1.0)
    quality_grade: str  # A, B, C, D, F
    summary: str  # Brief text summary

    # Detailed findings
    key_findings: list[str] = Field(default_factory=list)
    quality_issues: list[QualityIssue] = Field(default_factory=list)
    slice_comparisons: list[SliceComparison] = Field(default_factory=list)

    # Recommendations
    recommendations: list[str] = Field(default_factory=list)

    # Investigation SQL views
    investigation_views: list[dict[str, str]] = Field(default_factory=list)

    # Raw metrics for reference
    slice_metrics: list[SliceMetrics] = Field(default_factory=list)


class QualitySummaryResult(BaseModel):
    """Result from quality summary analysis."""

    source_table_id: str
    source_table_name: str
    slice_column_name: str
    slice_count: int

    column_summaries: list[ColumnQualitySummary]

    # Timing
    duration_seconds: float | None = None


@dataclass
class AggregatedColumnData:
    """Aggregated data for a column across all slices.

    Used internally to collect data before passing to LLM.
    """

    column_name: str
    column_id: str
    source_table_id: str
    source_table_name: str
    slice_column_name: str
    resolved_type: str | None = None

    slice_data: list[dict[str, Any]] = field(default_factory=list)

    # Aggregated statistics across slices
    total_rows: int = 0
    total_nulls: int = 0
    min_distinct: int | None = None
    max_distinct: int | None = None

    # Quality aggregations
    benford_violation_count: int = 0
    outlier_slice_count: int = 0

    # Semantic info (if available)
    semantic_role: str | None = None
    business_name: str | None = None
    business_description: str | None = None


__all__ = [
    "SliceMetrics",
    "SliceComparison",
    "QualityIssue",
    "ColumnQualitySummary",
    "QualitySummaryResult",
    "AggregatedColumnData",
]
