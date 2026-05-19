"""Enriched views module.

Creates grain-preserving DuckDB views that join fact tables with their
confirmed dimension tables. These views materialize the semantic understanding
of relationships for downstream consumption (slicing, correlations, etc.).

Uses LLM-powered enrichment analysis to identify which dimension joins add
valuable analytical dimensions (geographic, category, reference data).
"""

from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.analysis.views.enrichment_agent import EnrichmentAgent
from dataraum.analysis.views.enrichment_models import (
    DimensionEnrichmentOutput,
    EnrichmentAnalysisOutput,
    EnrichmentAnalysisResult,
    EnrichmentColumnOutput,
    EnrichmentRecommendation,
    MainDatasetOutput,
)

__all__ = [
    # Builder
    "DimensionJoin",
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
    "DimensionEnrichmentOutput",
    "MainDatasetOutput",
]
