"""Context Document - unified 5-pillar context for AI consumption.

This module defines ContextDocument which aggregates all 5 analytical pillars:
1. Statistical - From statistical.py models
2. Topological - From topological.py models
3. Semantic - From legacy models.py (SemanticEnrichmentResult)
4. Temporal - From temporal.py models
5. Quality - From quality_synthesis.py models

Design principle: ContextDocument is a thin aggregation layer.
It organizes existing pillar-specific Pydantic models without duplication.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# Import existing Pydantic models from each pillar
from dataraum_context.core.models.correlation import CorrelationAnalysisResult
from dataraum_context.core.models.quality_synthesis import QualitySynthesisResult
from dataraum_context.core.models.statistical import (
    StatisticalProfilingResult,
    StatisticalQualityResult,
)
from dataraum_context.core.models.temporal import TemporalQualitySummary
from dataraum_context.core.models.topological import TopologicalSummary

# For fields that would cause circular imports, we use Any with string annotations
# These will be properly typed when accessed through the main __init__.py


class ContextDocument(BaseModel):
    """Unified context document aggregating all 5 pillars.

    This is the main output of the context engine - a comprehensive document
    giving AI everything needed to understand and query the data.

    The 5 pillars:
    - Statistical: Profiles, distributions, correlations, outliers
    - Topological: TDA features, relationships, graph structure
    - Semantic: Entity types, business terms, relationships
    - Temporal: Time patterns, gaps, seasonality, trends
    - Quality: Synthesized assessment from all pillars

    Each pillar can be None if not yet computed or not applicable.
    """

    # ========================================================================
    # Metadata
    # ========================================================================
    source_id: str
    source_name: str
    generated_at: datetime
    ontology: str  # Applied ontology (e.g., 'financial_reporting')

    # ========================================================================
    # Pillar 1: Statistical Context
    # ========================================================================
    statistical_profiling: StatisticalProfilingResult | None = None
    statistical_quality: StatisticalQualityResult | None = None
    correlation_analysis: CorrelationAnalysisResult | None = None

    # ========================================================================
    # Pillar 2: Topological Context
    # ========================================================================
    topology: Any = None  # TopologyEnrichmentResult (avoid circular import)
    topological_summary: TopologicalSummary | None = None

    # ========================================================================
    # Pillar 3: Semantic Context
    # ========================================================================
    semantic: Any = None  # SemanticEnrichmentResult (avoid circular import)

    # ========================================================================
    # Pillar 4: Temporal Context
    # ========================================================================
    temporal_summary: TemporalQualitySummary | None = None

    # ========================================================================
    # Pillar 5: Quality Context (Synthesized)
    # ========================================================================
    quality: QualitySynthesisResult | None = None

    # ========================================================================
    # Ontology-Specific Content
    # ========================================================================
    relevant_metrics: list[Any] = Field(default_factory=list)  # list[MetricDefinition]
    domain_concepts: list[Any] = Field(default_factory=list)  # list[DomainConcept]

    # ========================================================================
    # LLM-Generated Content (Optional)
    # ========================================================================
    suggested_queries: list[Any] = Field(default_factory=list)  # list[SuggestedQuery]
    ai_summary: str | None = None  # Natural language overview
    key_facts: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    # ========================================================================
    # Provenance
    # ========================================================================
    llm_features_used: list[str] = Field(
        default_factory=list
    )  # e.g., ['semantic_analysis', 'suggested_queries']
    assembly_duration_seconds: float | None = None


__all__ = [
    "ContextDocument",
]
