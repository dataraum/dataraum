"""Semantic analysis module.

LLM-powered semantic analysis with enriched context from prior analysis phases.
The LLM receives analysis results (types, statistics, correlations) and optionally
TDA-detected relationship candidates to confirm/enhance.
"""

from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent
from dataraum.analysis.semantic.db_models import (
    SemanticAnnotation as SemanticAnnotationDB,
)
from dataraum.analysis.semantic.db_models import (
    TableEntity,
)
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    EntityDetection,
    Relationship,
    SemanticAnnotation,
    SemanticEnrichmentResult,
    TableColumnAnnotation,
)
from dataraum.analysis.semantic.ontology import (
    OntologyConcept,
    OntologyDefinition,
    OntologyLoader,
)
from dataraum.analysis.semantic.processor import (
    persist_column_annotations,
    synthesize_and_store_tables,
)

__all__ = [
    # Main entry points
    "persist_column_annotations",
    "synthesize_and_store_tables",
    "SemanticAgent",
    "ColumnAnnotationAgent",
    # Ontology
    "OntologyLoader",
    "OntologyDefinition",
    "OntologyConcept",
    # Models
    "SemanticAnnotation",
    "EntityDetection",
    "Relationship",
    "SemanticEnrichmentResult",
    "ColumnAnnotationOutput",
    "TableColumnAnnotation",
    # DB Models
    "SemanticAnnotationDB",
    "TableEntity",
]
