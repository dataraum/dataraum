"""Pydantic models for LLM-powered enrichment analysis.

Contains the output models the LLM's structured output is constrained to, and internal
result models for processing enrichment recommendations.

DAT-801: the selection question is neutral — "what related data usefully extends
this fact?" — not "which valuable analytical dimension?". A fact's grain-preserving
FK neighbours are all candidates on the same footing: a classification/lookup table
and the fact's own header (the parent record carrying its event date) are the SAME
mechanism — a column carried across a confirmed key join — so the contract names a
``related_table``, never a ``dimension``, and the relationship role is open text, not
a closed dimension vocabulary that structurally excludes a header.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from dataraum.analysis.views.builder import DimensionJoin

# =============================================================================
# Output Models - the schemas the LLM's structured outputs are constrained to
# =============================================================================


class EnrichmentColumnOutput(BaseModel):
    """A column to include from a related table.

    Per-column ``reasoning`` was deleted in DAT-671: the caller keeps only
    ``column_name``/``enrichment_value``, so the prose was generated once per
    candidate column and dropped unread.
    """

    column_name: str = Field(description="Column name from the related table")
    enrichment_value: Literal["high", "medium", "low"] = Field(
        description=(
            "How useful this column is on the extended fact: "
            "'high' = essential for slicing/filtering/trending; "
            "'medium' = useful attribute (a name, label, or the fact's event date); "
            "'low' = supplementary"
        )
    )


class RelatedTableJoinOutput(BaseModel):
    """A recommended join to a related table that usefully extends the fact.

    The related table may be a classification/lookup, reference/master data, a
    geographic table, OR the fact's own parent record (a header, carrying the event
    date). All are the same mechanism — a column carried across a confirmed
    grain-preserving key join — so none is privileged in this contract.
    """

    related_table: str = Field(description="Name of the related table to join")
    join_fact_column: str = Field(description="Column in the main table used for joining (the FK)")
    join_related_column: str = Field(
        description="Column in the related table used for joining (its key)"
    )
    relationship_role: str = Field(
        description=(
            "What this related data is, in a few words — e.g. 'classification', "
            "'reference/lookup', 'geographic', or 'parent record / header' (the entry, "
            "order, or invoice this row belongs to, carrying its date). Free text; not a "
            "fixed set — describe the relationship, do not force it into a category."
        )
    )
    enrichment_columns: list[EnrichmentColumnOutput] = Field(
        description="Columns from the related table to include in the view"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence that this join adds analytical value (0.0-1.0)"
    )
    reasoning: str = Field(description="Why this related table adds value to the main dataset")


class MainDatasetOutput(BaseModel):
    """A main dataset (fact table) with its recommended extensions.

    ``is_primary_fact`` and ``skip_reason`` were deleted in DAT-671 — both had
    ZERO readers. ``skip_reason`` was the sharper case: the prompt ordered it
    ("include it with skip_reason explaining why") and the caller never looked,
    so a declined table's reason was elicited and discarded every run. An empty
    ``recommended_enrichments`` already says "nothing to extend here".
    """

    table_name: str = Field(description="Name of the main/fact table")
    recommended_enrichments: list[RelatedTableJoinOutput] = Field(
        description="Recommended related-table joins that extend this table; [] when none"
    )


class EnrichmentAnalysisOutput(BaseModel):
    """Complete enrichment analysis result — the ``enrichment_analysis`` output.

    Every field is REQUIRED (DAT-807): not-applicable is a documented empty
    value ("" / []), never an omitted key.

    ``summary`` was deleted in DAT-671: it reached ``EnrichmentAnalysisResult``
    and stopped there — never persisted (no column on ``EnrichedView``), never
    logged, so unreachable from the cockpit, which reads engine metadata only
    through the ``ws_<id>`` schema.
    """

    main_datasets: list[MainDatasetOutput] = Field(
        description=(
            "Main datasets (fact tables) with their recommended extensions. "
            "Include ALL fact tables, even those with no recommended extensions."
        )
    )


# =============================================================================
# Internal Models - Used for storage and processing after LLM output
# =============================================================================


class EnrichmentRecommendation(BaseModel):
    """A processed enrichment recommendation ready for view creation.

    The join SHAPE only. The judge's ``relationship_role`` / ``reasoning`` /
    per-column enrichment ratings and the model name rode here too, purely to be
    copied into ``enriched_views.evidence`` — a column nothing ever read, deleted
    in DAT-671 R6. They are dropped with it rather than left as a second dead
    surface (ADR-0024 d3). The LLM is still ASKED for them: they are load-bearing
    inside :mod:`~dataraum.analysis.views.enrichment_agent` (the per-column
    high/medium rating is exactly what selects ``include_columns``), so the
    output schema and the prompt are unchanged — only this post-LLM carrier is.
    """

    fact_table_id: str
    fact_table_name: str
    dimension_joins: list[DimensionJoin]


class EnrichmentAnalysisResult(BaseModel):
    """Result of enrichment analysis operation."""

    recommendations: list[EnrichmentRecommendation] = Field(default_factory=list)


__all__ = [
    # Structured-output models
    "EnrichmentColumnOutput",
    "RelatedTableJoinOutput",
    "MainDatasetOutput",
    "EnrichmentAnalysisOutput",
    # Internal models
    "EnrichmentRecommendation",
    "EnrichmentAnalysisResult",
]
