"""Relationship detection and evaluation between tables.

Uses value overlap (Jaccard/containment) to detect joinable column pairs.
Enriches candidates with:
- Column uniqueness ratios
- Evaluation metrics (referential integrity, cardinality verification, join quality)
- Graph topology analysis (table roles, schema patterns, cycles)
"""

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.detector import detect_relationships
from dataraum.analysis.relationships.evaluator import (
    evaluate_candidates,
    evaluate_join_candidate,
    evaluate_relationship_candidate,
)
from dataraum.analysis.relationships.finder import find_relationships
from dataraum.analysis.relationships.graph_topology import (
    GraphStructure,
    SchemaCycle,
    TableRole,
    analyze_graph_topology,
    format_graph_structure_for_context,
)
from dataraum.analysis.relationships.joins import find_join_columns
from dataraum.analysis.relationships.models import (
    JoinCandidate,
    RelationshipCandidate,
    RelationshipDetectionResult,
)
from dataraum.analysis.relationships.utils import (
    load_relationship_candidates_for_semantic,
)

__all__ = [
    # Main entry points
    "detect_relationships",
    "find_relationships",
    "evaluate_candidates",
    "analyze_graph_topology",
    # Utilities
    "load_relationship_candidates_for_semantic",
    "format_graph_structure_for_context",
    # Components
    "find_join_columns",
    "evaluate_join_candidate",
    "evaluate_relationship_candidate",
    # Relationship detection models
    "RelationshipCandidate",
    "JoinCandidate",
    "RelationshipDetectionResult",
    # Graph topology models
    "GraphStructure",
    "TableRole",
    "SchemaCycle",
    # DB Models
    "Relationship",
]
