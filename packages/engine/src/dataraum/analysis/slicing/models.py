"""Pydantic models for slicing analysis.

Contains data structures for slice recommendations and analysis results.
Slices are categorical only - each unique value in a dimension column
creates one slice.

Curation contract (DAT-879, replacing the DAT-725 ordinal): a catalog row
carries a MEASURED ``slice_relevance`` (``slicing/relevance.py``) plus the
agent's ABSOLUTE ``slice_interest`` judgment. The ordinal ``slice_priority``
and its ``UNRANKED_SLICE_PRIORITY = 1000`` floor are gone, and so is the
``CURATED_SLICE_BUDGET = 12`` LIMIT the reads used to truncate by. Curated
reads now take the JUDGED rows ordered by measured relevance and REPORT what
they left behind, instead of silently cutting at a constant whose remaining
budget was filled alphabetically (every floor row tied at 1000, so the
tiebreak ``column_name`` decided what an "interesting dimension" was).
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from dataraum.core.models.base import DecisionSource

# The agent's ABSOLUTE interest judgment. Absolute, not a rank: "primary" means
# the same thing on a 3-column table and a 40-column one, which is exactly what
# an ordinal could not do (rank 1 of 3 and rank 1 of 40 are different claims
# wearing the same number). Both values mean the column IS a business breakdown
# axis; they differ in whether a reader reaches for it first. A row the agent
# did not return carries NULL — "not judged", which is not the same as "judged
# uninteresting" and must never be rendered as if it were.
SliceInterest = Literal["primary", "supporting"]
SLICE_INTEREST_VALUES: tuple[str, ...] = ("primary", "supporting")

# Curation sort order for the interest tier. Judged rows come before unjudged
# ones; measured relevance orders WITHIN a tier. This is the whole ordering
# contract — there is no budget constant to read.
SLICE_INTEREST_RANK: dict[str | None, int] = {"primary": 0, "supporting": 1, None: 2}


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
    slice_interest: SliceInterest = Field(
        description="The agent's absolute interest judgment for this dimension"
    )
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

    # The column's measured COUNT(DISTINCT), from the statistical profile —
    # NOT the length of ``distinct_values``, which is a bounded echo (DAT-879).
    # None when the column has no profile to read it from.
    value_count: int | None = Field(
        default=None, description="Measured number of distinct values on this axis"
    )

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
# Pydantic model for the LLM structured output
# =============================================================================


class SliceRecommendationOutput(BaseModel):
    """Pydantic model for a slice recommendation in the LLM structured output."""

    table_name: str = Field(description="Name of the table containing the column")
    column_name: str = Field(description="Name of the column to slice on")
    interest: SliceInterest = Field(
        description=(
            "How a reader would reach for this dimension. "
            '"primary" = a first-choice breakdown of a headline number for this '
            'dataset; "supporting" = a genuine business axis, but one reached for '
            "after the primary ones. Judge each dimension on its own merits — this "
            "is NOT a ranking, so do not spread judgments to fill a distribution, "
            "and do not let a column's position in the list influence it."
        )
    )
    distinct_values: list[str] = Field(description="List of unique values that will become slices")
    reasoning: str = Field(description="Why this column is a good slicing dimension")
    business_context: str = Field(
        description='Business meaning of this dimension; "" when there is none'
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
    """The ``slicing_analysis`` structured output.

    Every field is REQUIRED (DAT-807): not-applicable is a documented empty
    value ("" / []), never an omitted key.
    """

    recommendations: list[SliceRecommendationOutput] = Field(
        description=(
            "The dimensions that are genuine business breakdown axes, each with its "
            "own interest judgment; [] when none qualify. Order carries no meaning — "
            "a column you leave out is not removed from the catalog, it is recorded "
            "as un-judged."
        ),
    )

    time_columns: list[TableTimeColumnOutput] = Field(
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
    "SLICE_INTEREST_RANK",
    "SLICE_INTEREST_VALUES",
    "SliceInterest",
    "SliceRecommendation",
    "TableTimeColumnOutput",
    "SlicingAnalysisResult",
    "SliceRecommendationOutput",
    "SlicingAnalysisOutput",
]
