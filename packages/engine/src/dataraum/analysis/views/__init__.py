"""Enriched views module.

Creates grain-preserving DuckDB views that join fact tables with their
confirmed dimension tables. These views materialize the semantic understanding
of relationships for downstream consumption (slicing, correlations, etc.).

Uses LLM-powered enrichment analysis to identify which related tables usefully
extend a fact — a classification, a reference/lookup, or the fact's own header —
on equal footing (DAT-801), never a fixed dimension vocabulary.
"""

from dataraum.analysis.views.builder import (
    DimensionJoin,
    EnrichedDimColumn,
    build_enriched_view_sql,
)
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.analysis.views.enrichment_agent import EnrichmentAgent
from dataraum.analysis.views.enrichment_models import (
    EnrichmentAnalysisOutput,
    EnrichmentAnalysisResult,
    EnrichmentColumnOutput,
    EnrichmentRecommendation,
    MainDatasetOutput,
    RelatedTableJoinOutput,
)

__all__ = [
    # Builder
    "DimensionJoin",
    "EnrichedDimColumn",
    "build_enriched_view_sql",
    # DB models
    "EnrichedView",
    # LLM agent
    "EnrichmentAgent",
    # Pydantic models
    "EnrichmentAnalysisOutput",
    "EnrichmentAnalysisResult",
    "EnrichmentColumnOutput",
    "EnrichmentRecommendation",
    "RelatedTableJoinOutput",
    "MainDatasetOutput",
]
