"""Relationship detection models.

Models for:
- Relationship candidates (TDA + join detection)

Cross-table models are in analysis/correlation/models.py
Legacy DependencyGroup is in enrichment/cross_table_multicollinearity.py
"""

from datetime import datetime

from pydantic import BaseModel, Field


class JoinCandidate(BaseModel):
    """A potential join between two columns."""

    column1: str
    column2: str
    confidence: float
    cardinality: str  # one-to-one, one-to-many, many-to-one


class RelationshipCandidate(BaseModel):
    """A candidate relationship between two tables."""

    table1: str
    table2: str
    confidence: float
    topology_similarity: float
    relationship_type: str

    join_candidates: list[JoinCandidate] = Field(default_factory=list)


class RelationshipDetectionResult(BaseModel):
    """Result of relationship detection."""

    candidates: list[RelationshipCandidate] = Field(default_factory=list)

    total_tables: int = 0
    total_candidates: int = 0
    high_confidence_count: int = 0

    computed_at: datetime | None = None
    duration_seconds: float = 0.0
