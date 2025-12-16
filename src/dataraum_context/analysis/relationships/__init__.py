"""Relationship detection between tables.

Combines multiple signals:
- TDA (Topological Data Analysis): structural similarity via persistence diagrams
- Value overlap: join column detection via Jaccard/containment
- Cross-table correlation: multicollinearity analysis across tables

Confidence = max(topology_similarity, best_join_confidence)
"""

from dataraum_context.analysis.relationships.db_models import (
    CrossTableMulticollinearityMetrics,
    Relationship,
)
from dataraum_context.analysis.relationships.detector import detect_relationships
from dataraum_context.analysis.relationships.finder import find_relationships
from dataraum_context.analysis.relationships.joins import find_join_columns
from dataraum_context.analysis.relationships.models import (
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    DependencyGroup,
    EnrichedRelationship,
    JoinCandidate,
    RelationshipCandidate,
    RelationshipDetectionResult,
    SingleRelationshipJoin,
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
    # Relationship detection models
    "RelationshipCandidate",
    "JoinCandidate",
    "RelationshipDetectionResult",
    # Cross-table analysis models
    "EnrichedRelationship",
    "SingleRelationshipJoin",
    "DependencyGroup",
    "CrossTableDependencyGroup",
    "CrossTableMulticollinearityAnalysis",
    # DB Models
    "Relationship",
    "CrossTableMulticollinearityMetrics",
]
