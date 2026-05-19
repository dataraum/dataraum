"""Pydantic models for slicing analysis.

Contains data structures for slice recommendations and analysis results.
Slices are categorical only - each unique value in a dimension column
creates one slice.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from dataraum.core.models.base import DecisionSource


class SliceRecommendation(BaseModel):
    """A recommended categorical slice dimension.

    Identifies a column suitable for creating data subsets,
    where each unique value in the column becomes a separate slice.
    """

    # Column identification
    table_id: str
    table_name: str
    column_id: str
    column_name: str

    # Slice metadata
    slice_priority: int = Field(description="Priority rank (1 = highest priority slice dimension)")
    distinct_values: list[str] = Field(
        default_factory=list,
        description="List of unique values that will become slices",
    )

    @field_validator("distinct_values", mode="before")
    @classmethod
    def coerce_to_strings(cls, v: Any) -> list[str]:
        """Coerce distinct values to strings (LLM may return ints)."""
        if isinstance(v, list):
            return [str(item) for item in v]
        return []

    value_count: int = Field(description="Number of distinct values (number of slices to create)")

    # Analysis reasoning
    reasoning: str = Field(description="Why this column is a good slicing dimension")
    business_context: str | None = Field(
        default=None,
        description="Business meaning of this dimension (from semantic analysis)",
    )

    # Confidence
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence in this recommendation")

    # SQL for creating slices
    sql_template: str = Field(description="DuckDB SQL template for creating slice tables")


class SliceSQL(BaseModel):
    """Generated SQL for a specific slice."""

    slice_name: str = Field(description="Name for the slice (e.g., 'area_1')")
    slice_value: str = Field(description="The value this slice filters on")
    table_name: str = Field(description="Name for the output table")
    sql_query: str = Field(description="DuckDB SQL to create the slice")


class SlicingAnalysisResult(BaseModel):
    """Result of slicing analysis."""

    # Recommendations ordered by priority
    recommendations: list[SliceRecommendation] = Field(default_factory=list)

    # Generated SQL for all slices
    slice_queries: list[SliceSQL] = Field(default_factory=list)

    # Metadata
    source: DecisionSource = DecisionSource.LLM
    tables_analyzed: int = 0
    columns_considered: int = 0


# =============================================================================
# Pydantic model for LLM tool output
# =============================================================================


class SliceRecommendationOutput(BaseModel):
    """Pydantic model for a slice recommendation in LLM tool output."""

    table_name: str = Field(description="Name of the table containing the column")
    column_name: str = Field(description="Name of the column to slice on")
    priority: int = Field(description="Priority rank (1 = highest priority slice dimension)")
    distinct_values: list[str] = Field(description="List of unique values that will become slices")
    reasoning: str = Field(description="Why this column is a good slicing dimension")
    business_context: str | None = Field(
        default=None, description="Business meaning of this dimension"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence in this recommendation (0.0 to 1.0)"
    )


class SlicingAnalysisOutput(BaseModel):
    """Pydantic model for LLM tool output - slicing analysis.

    Used as a tool definition for structured LLM output via tool use API.
    """

    recommendations: list[SliceRecommendationOutput] = Field(
        default_factory=list,
        description="List of recommended slicing dimensions, ordered by priority",
    )


__all__ = [
    "SliceRecommendation",
    "SliceSQL",
    "SlicingAnalysisResult",
    "SliceRecommendationOutput",
    "SlicingAnalysisOutput",
]
