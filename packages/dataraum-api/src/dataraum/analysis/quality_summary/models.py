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

    # Column classifications from variance filtering
    # Maps column_name -> classification (empty, constant, stable, interesting)
    # Only columns classified as "interesting" are sent to LLM for analysis
    column_classifications: dict[str, str] = Field(default_factory=dict)

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


class SliceQualityCell(BaseModel):
    """Quality metrics for a single cell in the slice x column matrix."""

    slice_value: str
    column_name: str
    row_count: int = 0
    null_ratio: float | None = None
    distinct_count: int | None = None
    quality_score: float | None = None  # 0-1 score derived from metrics
    has_issues: bool = False
    issue_count: int = 0


class SliceColumnMatrix(BaseModel):
    """Matrix of slice values x columns with quality metrics.

    Provides a compact view of quality across all slices and columns.
    Structure: rows = slice values, columns = source columns
    """

    source_table_name: str
    slice_column_name: str

    # Axis labels
    slice_values: list[str] = Field(default_factory=list)  # Row labels
    column_names: list[str] = Field(default_factory=list)  # Column labels

    # Matrix data: dict[slice_value, dict[column_name, SliceQualityCell]]
    cells: dict[str, dict[str, SliceQualityCell]] = Field(default_factory=dict)

    # Summary statistics
    total_rows_per_slice: dict[str, int] = Field(default_factory=dict)
    avg_quality_per_column: dict[str, float] = Field(default_factory=dict)

    def get_cell(self, slice_value: str, column_name: str) -> SliceQualityCell | None:
        """Get cell for specific slice value and column."""
        return self.cells.get(slice_value, {}).get(column_name)

    def to_dataframe_dict(self) -> dict[str, Any]:
        """Convert to dict format suitable for pandas DataFrame.

        Returns dict with structure:
        {
            'slice_value': [...],
            'column1_score': [...],
            'column1_nulls': [...],
            'column2_score': [...],
            ...
        }
        """
        result: dict[str, list[Any]] = {"slice_value": []}

        for col_name in self.column_names:
            result[f"{col_name}_score"] = []
            result[f"{col_name}_null_ratio"] = []
            result[f"{col_name}_issues"] = []

        for slice_val in self.slice_values:
            result["slice_value"].append(slice_val)
            for col_name in self.column_names:
                cell = self.get_cell(slice_val, col_name)
                if cell:
                    result[f"{col_name}_score"].append(cell.quality_score)
                    result[f"{col_name}_null_ratio"].append(cell.null_ratio)
                    result[f"{col_name}_issues"].append(cell.issue_count)
                else:
                    result[f"{col_name}_score"].append(None)
                    result[f"{col_name}_null_ratio"].append(None)
                    result[f"{col_name}_issues"].append(None)

        return result


# =============================================================================
# Pydantic models for LLM tool output
# =============================================================================


class QualityIssueOutput(BaseModel):
    """Pydantic model for quality issue in LLM tool output."""

    issue_type: str = Field(
        description="Type of issue (e.g., 'high_null_rate', 'benford_violation')"
    )
    severity: str = Field(description="Severity level: low, medium, high, critical")
    description: str = Field(description="Description of the issue")
    affected_slices: list[str] = Field(
        default_factory=list, description="List of slice names affected by this issue"
    )
    investigation_sql: str | None = Field(
        default=None, description="DuckDB SQL query to investigate this issue"
    )


class SliceComparisonOutput(BaseModel):
    """Pydantic model for slice comparison in LLM tool output."""

    metric_name: str = Field(description="Name of the metric being compared")
    description: str = Field(description="Description of how this metric varies across slices")
    min_value: float | None = Field(default=None, description="Minimum value across slices")
    max_value: float | None = Field(default=None, description="Maximum value across slices")
    outlier_slices: list[str] = Field(
        default_factory=list, description="Slices with unusually high or low values"
    )


class InvestigationViewOutput(BaseModel):
    """Pydantic model for investigation view in LLM tool output."""

    name: str = Field(description="Name for this investigation view")
    description: str = Field(description="What this view helps investigate")
    sql: str = Field(description="DuckDB SQL query for the investigation")


class QualitySummaryOutput(BaseModel):
    """Pydantic model for LLM tool output - single column quality summary.

    Used as a tool definition for structured LLM output via tool use API.
    """

    overall_quality_score: float = Field(
        ge=0.0, le=1.0, description="Overall quality score from 0.0 to 1.0"
    )
    quality_grade: str = Field(description="Quality grade: A, B, C, D, or F")
    summary: str = Field(description="Brief 2-3 sentence summary of the column quality")
    key_findings: list[str] = Field(
        default_factory=list, description="Top 3-5 key observations about patterns across slices"
    )
    quality_issues: list[QualityIssueOutput] = Field(
        default_factory=list, description="Specific quality problems found"
    )
    slice_comparisons: list[SliceComparisonOutput] = Field(
        default_factory=list, description="How metrics vary across slices"
    )
    recommendations: list[str] = Field(
        default_factory=list, description="Actionable steps to improve data quality"
    )
    investigation_views: list[InvestigationViewOutput] = Field(
        default_factory=list, description="SQL queries for drilling into issues"
    )


class ColumnSummaryOutput(BaseModel):
    """Pydantic model for a single column in batch output."""

    column_name: str = Field(description="Exact column name from input")
    overall_quality_score: float = Field(ge=0.0, le=1.0)
    quality_grade: str = Field(description="Quality grade: A, B, C, D, or F")
    summary: str = Field(description="Brief summary of the column quality")
    key_findings: list[str] = Field(default_factory=list)
    quality_issues: list[QualityIssueOutput] = Field(default_factory=list)
    slice_comparisons: list[SliceComparisonOutput] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)


class QualitySummaryBatchOutput(BaseModel):
    """Pydantic model for LLM tool output - batch column quality summary.

    Used as a tool definition for structured LLM output via tool use API.
    """

    columns: list[ColumnSummaryOutput] = Field(
        description="List of quality summaries for each column"
    )


__all__ = [
    "SliceMetrics",
    "SliceComparison",
    "QualityIssue",
    "ColumnQualitySummary",
    "QualitySummaryResult",
    "AggregatedColumnData",
    "SliceQualityCell",
    "SliceColumnMatrix",
    # Tool output models
    "QualityIssueOutput",
    "SliceComparisonOutput",
    "InvestigationViewOutput",
    "QualitySummaryOutput",
    "ColumnSummaryOutput",
    "QualitySummaryBatchOutput",
]
