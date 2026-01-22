"""Semantic analysis module.

LLM-powered semantic analysis with enriched context from prior analysis phases.
The LLM receives analysis results (types, statistics, correlations) and optionally
TDA-detected relationship candidates to confirm/enhance.
"""

from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.db_models import (
    SemanticAnnotation as SemanticAnnotationDB,
)
from dataraum.analysis.semantic.db_models import (
    TableEntity,
)
from dataraum.analysis.semantic.models import (
    EntityDetection,
    Relationship,
    SemanticAnnotation,
    SemanticEnrichmentResult,
)
from dataraum.analysis.semantic.ontology import (
    OntologyConcept,
    OntologyDefinition,
    OntologyLoader,
    OntologyMetric,
    OntologyRule,
    SemanticHint,
)
from dataraum.analysis.semantic.processor import enrich_semantic

__all__ = [
    # Main entry points
    "enrich_semantic",
    "SemanticAgent",
    # Ontology
    "OntologyLoader",
    "OntologyDefinition",
    "OntologyConcept",
    "OntologyMetric",
    "OntologyRule",
    "SemanticHint",
    # Models
    "SemanticAnnotation",
    "EntityDetection",
    "Relationship",
    "SemanticEnrichmentResult",
    # DB Models
    "SemanticAnnotationDB",
    "TableEntity",
]
