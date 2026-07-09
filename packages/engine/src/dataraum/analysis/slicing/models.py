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


class SlicingAnalysisResult(BaseModel):
    """Result of slicing analysis."""

    # Recommendations ordered by priority
    recommendations: list[SliceRecommendation] = Field(default_factory=list)

    # Per-table fallback time axis (DAT-491/565): table_name -> column name (own
    # column or an enriched "fk__col" name). The agent judges ONE axis only for
    # tables whose ``time_columns`` came back empty from semantic_per_table;
    # tables that already have axes are inherited untouched.
    time_columns: dict[str, str] = Field(default_factory=dict)

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


class TableTimeColumnOutput(BaseModel):
    """The per-table time-axis judgment (DAT-491)."""

    table_name: str = Field(description="Name of the table")
    column_name: str = Field(
        description=(
            "The table's time axis: the column recording WHEN each row's event "
            "occurred. Either an own column or an enriched 'fk__col' name (a "
            "header date). Only name an axis for a table whose context "
            "'time_columns' is EMPTY; tables that already list axes are kept "
            "as-is."
        )
    )


class SlicingAnalysisOutput(BaseModel):
    """Pydantic model for LLM tool output - slicing analysis.

    Used as a tool definition for structured LLM output via tool use API.
    """

    recommendations: list[SliceRecommendationOutput] = Field(
        default_factory=list,
        description="List of recommended slicing dimensions, ordered by priority",
    )

    time_columns: list[TableTimeColumnOutput] = Field(
        default_factory=list,
        description=(
            "The event-time axis for each analyzed table whose context "
            "'time_columns' is empty. Rule: whenever such a table has an enriched "
            "column flagged is_dimension_time_column, name that column here — it is "
            "the table's event date, joined from its parent/header record (e.g. a "
            "line-item table dated by its parent document via a joined "
            "`<fk>__<date>` column). Skip a table only when "
            "it already lists axes (kept as-is) or has no is_dimension_time_column "
            "candidate at all."
        ),
    )


__all__ = [
    "SliceRecommendation",
    "TableTimeColumnOutput",
    "SlicingAnalysisResult",
    "SliceRecommendationOutput",
    "SlicingAnalysisOutput",
]
