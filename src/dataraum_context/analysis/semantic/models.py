"""Pydantic models for semantic analysis.

Contains data structures for semantic annotations, entity detection,
relationships, and enrichment results.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from dataraum_context.core.models.base import (
    ColumnRef,
    DecisionSource,
    RelationshipType,
    SemanticRole,
)


class SemanticAnnotation(BaseModel):
    """Semantic annotation for a column (LLM-generated or manual)."""

    column_id: str
    column_ref: ColumnRef

    semantic_role: SemanticRole
    entity_type: str | None = None
    business_name: str | None = None
    business_description: str | None = None  # LLM-generated description
    business_domain: str | None = None  # 'finance', 'marketing', 'operations'

    # Ontology mapping - maps to standard field names (e.g., 'accounts_receivable', 'revenue')
    ontology_term: str | None = None

    annotation_source: DecisionSource
    annotated_by: str | None = None  # e.g., 'claude-sonnet-4-20250514' or 'user@example.com'
    confidence: float


class EntityDetection(BaseModel):
    """Entity type detection for a table."""

    table_id: str
    table_name: str

    entity_type: str
    description: str | None = None  # LLM-generated table description
    confidence: float
    evidence: dict[str, Any] = Field(default_factory=dict)

    grain_columns: list[str] = Field(default_factory=list)
    is_fact_table: bool = False
    is_dimension_table: bool = False
    time_column: str | None = None  # Primary time column


class Relationship(BaseModel):
    """A detected relationship between tables."""

    relationship_id: str

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    relationship_type: RelationshipType
    cardinality: str | None = None  # Using Cardinality from base

    confidence: float
    detection_method: str
    evidence: dict[str, Any] = Field(default_factory=dict)

    is_confirmed: bool = False


class SemanticEnrichmentResult(BaseModel):
    """Result of semantic enrichment operation."""

    annotations: list[SemanticAnnotation] = Field(default_factory=list)
    entity_detections: list[EntityDetection] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    source: str = "llm"  # 'llm', 'manual', 'override'


__all__ = [
    "SemanticAnnotation",
    "EntityDetection",
    "Relationship",
    "SemanticEnrichmentResult",
]
