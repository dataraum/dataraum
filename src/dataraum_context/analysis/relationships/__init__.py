"""Relationship detection and evaluation between tables.

Uses value overlap (Jaccard/containment) to detect joinable column pairs.
Enriches candidates with:
- Column uniqueness ratios
- Evaluation metrics (referential integrity, cardinality verification, join quality)
"""

from dataraum_context.analysis.relationships.db_models import Relationship
from dataraum_context.analysis.relationships.detector import detect_relationships
from dataraum_context.analysis.relationships.evaluator import (
    evaluate_candidates,
    evaluate_join_candidate,
    evaluate_relationship_candidate,
)
from dataraum_context.analysis.relationships.finder import find_relationships
from dataraum_context.analysis.relationships.joins import find_join_columns
from dataraum_context.analysis.relationships.models import (
    JoinCandidate,
    RelationshipCandidate,
    RelationshipDetectionResult,
)
from dataraum_context.analysis.relationships.utils import (
    load_relationship_candidates_for_semantic,
)

__all__ = [
    # Main entry points
    "detect_relationships",
    "find_relationships",
    "evaluate_candidates",
    # Utilities
    "load_relationship_candidates_for_semantic",
    # Components
    "find_join_columns",
    "evaluate_join_candidate",
    "evaluate_relationship_candidate",
    # Relationship detection models
    "RelationshipCandidate",
    "JoinCandidate",
    "RelationshipDetectionResult",
    # DB Models
    "Relationship",
]
