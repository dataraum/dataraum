"""Relationship detection and evaluation models.

Models for:
- JoinCandidate: potential join between columns (with evaluation metrics)
- RelationshipCandidate: candidate relationship between tables (with evaluation metrics)
- RelationshipDetectionResult: detection results

Evaluation metrics are populated by analysis/relationships/evaluator.py.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class JoinCandidate(BaseModel):
    """A potential join between two columns.

    Core metrics:
    - confidence: overall confidence = max(topology_similarity, join_confidence)
    - topology_similarity: column feature similarity (distributional/statistical)
    - join_confidence: value overlap (Jaccard/containment)

    Evaluation metrics (populated by evaluator.py):
    - left_referential_integrity: % of FK values with matching PK
    - right_referential_integrity: % of PK values that are referenced
    - orphan_count: FK values with no matching PK
    - cardinality_verified: whether detected cardinality matches actual
    """

    column1: str
    column2: str
    confidence: float  # max(topology_similarity, join_confidence)
    cardinality: str  # one-to-one, one-to-many, many-to-one

    # Column-level topology similarity (distributional features)
    topology_similarity: float = 0.0
    # Value overlap score (Jaccard/containment)
    join_confidence: float = 0.0

    # Evaluation metrics (populated by evaluator.py)
    left_referential_integrity: float | None = None  # 0-100%
    right_referential_integrity: float | None = None  # 0-100%
    orphan_count: int | None = None
    cardinality_verified: bool | None = None


class RelationshipCandidate(BaseModel):
    """A candidate relationship between two tables.

    The confidence is the best confidence among join_candidates.
    Each JoinCandidate has its own topology_similarity and join_confidence.

    Evaluation metrics (populated by evaluator.py):
    - join_success_rate: % of rows from table1 that match in table2
    - introduces_duplicates: whether join multiplies rows (fan trap)
    """

    table1: str
    table2: str
    confidence: float  # Best confidence among join_candidates
    relationship_type: str

    join_candidates: list[JoinCandidate] = Field(default_factory=list)

    # Evaluation metrics (populated by evaluator.py)
    join_success_rate: float | None = None  # 0-100%
    introduces_duplicates: bool | None = None


class RelationshipDetectionResult(BaseModel):
    """Result of relationship detection."""

    candidates: list[RelationshipCandidate] = Field(default_factory=list)

    total_tables: int = 0
    total_candidates: int = 0
    high_confidence_count: int = 0

    computed_at: datetime | None = None
    duration_seconds: float = 0.0
