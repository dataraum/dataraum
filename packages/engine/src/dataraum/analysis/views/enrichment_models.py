"""Pydantic models for LLM-powered enrichment analysis.

Contains tool output models for structured LLM output and internal
result models for processing enrichment recommendations.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from dataraum.analysis.views.builder import DimensionJoin

# =============================================================================
# Tool Output Models - Used as Pydantic tools for LLM structured output
# =============================================================================


class EnrichmentColumnOutput(BaseModel):
    """A column to include from a dimension table."""

    column_name: str = Field(description="Column name from the dimension table")
    enrichment_value: Literal["high", "medium", "low"] = Field(
        description=(
            "'high' = essential dimension (geographic, category codes); "
            "'medium' = useful attribute (name, description); "
            "'low' = supplementary data"
        )
    )
    reasoning: str = Field(description="Why this column adds value to the main dataset")


class DimensionEnrichmentOutput(BaseModel):
    """A recommended dimension table join."""

    dimension_table: str = Field(description="Name of the dimension/lookup table to join")
    join_fact_column: str = Field(description="Column in the main table used for joining")
    join_dimension_column: str = Field(description="Column in the dimension table used for joining")
    dimension_type: Literal["geographic", "category", "reference", "temporal", "other"] = Field(
        description=(
            "Type of dimension being added: "
            "'geographic' = location/region data; "
            "'category' = classification/grouping; "
            "'reference' = lookup/master data; "
            "'temporal' = calendar/time attributes; "
            "'other' = other useful dimensions"
        )
    )
    enrichment_columns: list[EnrichmentColumnOutput] = Field(
        description="Columns from the dimension table to include in the view"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence that this join adds analytical value (0.0-1.0)"
    )
    reasoning: str = Field(description="Why this dimension adds value to the main dataset")


class MainDatasetOutput(BaseModel):
    """A main dataset (fact table) with recommended enrichments."""

    table_name: str = Field(description="Name of the main/fact table")
    is_primary_fact: bool = Field(description="True if this is the primary transactional dataset")
    recommended_enrichments: list[DimensionEnrichmentOutput] = Field(
        default_factory=list, description="Recommended dimension joins to enrich this table"
    )
    skip_reason: str | None = Field(
        default=None, description="If no enrichments recommended, explain why"
    )


class EnrichmentAnalysisOutput(BaseModel):
    """Complete enrichment analysis result.

    Top-level tool output for the analyze_enrichment tool.
    """

    main_datasets: list[MainDatasetOutput] = Field(
        description=(
            "Main datasets (fact tables) with their recommended enrichments. "
            "Include ALL fact tables, even those with no recommended enrichments."
        )
    )
    summary: str = Field(description="Brief summary of the overall enrichment strategy")


# =============================================================================
# Internal Models - Used for storage and processing after LLM output
# =============================================================================


class EnrichmentRecommendation(BaseModel):
    """A processed enrichment recommendation ready for view creation."""

    fact_table_id: str
    fact_table_name: str
    dimension_joins: list[DimensionJoin]
    dimension_type: str
    confidence: float
    reasoning: str
    enrichment_columns: list[str]  # Column names with enrichment values


class EnrichmentAnalysisResult(BaseModel):
    """Result of enrichment analysis operation."""

    recommendations: list[EnrichmentRecommendation] = Field(default_factory=list)
    summary: str = ""
    model_name: str = ""


__all__ = [
    # Tool output models
    "EnrichmentColumnOutput",
    "DimensionEnrichmentOutput",
    "MainDatasetOutput",
    "EnrichmentAnalysisOutput",
    # Internal models
    "EnrichmentRecommendation",
    "EnrichmentAnalysisResult",
]
