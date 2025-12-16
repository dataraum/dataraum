"""Relationship detection between tables.

Combines two signals:
- TDA (Topological Data Analysis): structural similarity via persistence diagrams
- Value overlap: join column detection via Jaccard/containment

Confidence = max(topology_similarity, best_join_confidence)
"""

from dataraum_context.analysis.relationships.detector import detect_relationships
from dataraum_context.analysis.relationships.finder import find_relationships
from dataraum_context.analysis.relationships.joins import find_join_columns
from dataraum_context.analysis.relationships.models import (
    JoinCandidate,
    RelationshipCandidate,
    RelationshipDetectionResult,
)
from dataraum_context.analysis.relationships.topology import (
    compute_persistence,
    persistence_similarity,
)

__all__ = [
    # Main entry points
    "detect_relationships",
    "find_relationships",
    # Components
    "find_join_columns",
    "compute_persistence",
    "persistence_similarity",
    # Models
    "RelationshipCandidate",
    "JoinCandidate",
    "RelationshipDetectionResult",
]
