"""Enrichment layer models.

Defines data structures for semantic and topological relationship discovery.

NOTE: Quality analysis models (temporal/topological quality) have been moved to
quality/models.py where they belong. This module now only contains models for
the enrichment discovery phase.
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

# Import quality models that enrichment references
from dataraum_context.quality.models import TemporalQualityResult


class SemanticAnnotation(BaseModel):
    """Semantic annotation for a column (LLM-generated or manual)."""

    column_id: str
    column_ref: ColumnRef

    semantic_role: SemanticRole
    entity_type: str | None = None
    business_name: str | None = None
    business_description: str | None = None  # LLM-generated description

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


class JoinStep(BaseModel):
    """A single step in a join path."""

    from_column: str
    to_table: str
    to_column: str
    confidence: float


class JoinPath(BaseModel):
    """A computed join path between tables."""

    from_table: str
    to_table: str
    steps: list[JoinStep]
    total_confidence: float


# === Enrichment Result Models ===


class SemanticEnrichmentResult(BaseModel):
    """Result of semantic enrichment operation."""

    annotations: list[SemanticAnnotation] = Field(default_factory=list)
    entity_detections: list[EntityDetection] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    source: str = "llm"  # 'llm', 'manual', 'override'


class TopologyEnrichmentResult(BaseModel):
    """Result of topology enrichment operation."""

    relationships: list[Relationship] = Field(default_factory=list)
    join_paths: list[JoinPath] = Field(default_factory=list)


class TemporalEnrichmentResult(BaseModel):
    """Result of temporal enrichment operation."""

    profiles: list[TemporalQualityResult] = Field(default_factory=list)
