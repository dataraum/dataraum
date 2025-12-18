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
    - join_confidence: value overlap score (Jaccard/containment), min 0.3
    - cardinality: detected relationship cardinality
    - left/right_uniqueness: distinct/total ratio for each column

    Evaluation metrics (populated by evaluator.py):
    - left_referential_integrity: % of FK values with matching PK
    - right_referential_integrity: % of PK values that are referenced
    - orphan_count: FK values with no matching PK
    - cardinality_verified: whether detected cardinality matches actual
    """

    column1: str
    column2: str
    join_confidence: float  # Value overlap (Jaccard/containment)
    cardinality: str  # one-to-one, one-to-many, many-to-one, many-to-many

    # Column characteristics (from statistics, not name matching)
    left_uniqueness: float = 0.0  # distinct/total ratio
    right_uniqueness: float = 0.0

    # Evaluation metrics (populated by evaluator.py)
    left_referential_integrity: float | None = None  # 0-100%
    right_referential_integrity: float | None = None  # 0-100%
    orphan_count: int | None = None
    cardinality_verified: bool | None = None


class RelationshipCandidate(BaseModel):
    """A candidate relationship between two tables.

    Evaluation metrics (populated by evaluator.py):
    - join_success_rate: % of rows from table1 that match in table2
    - introduces_duplicates: whether join multiplies rows (fan trap)
    """

    table1: str
    table2: str
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
