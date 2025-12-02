"""Semantic Context Models (Pillar 3).

This pillar provides semantic understanding of data:
- Semantic annotations (roles, entity types, business terms)
- Table entity detection (fact/dimension tables, grain)
- Ontology mapping

Design notes:
- SemanticAnnotation: Column-level semantic metadata
- TableEntity: Table-level entity classification
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum_context.storage.models_v2.base import Base

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.core import Column, Table


class SemanticAnnotation(Base):
    """Semantic annotations for columns.

    Stores LLM-generated or manually-provided semantic metadata
    including business terms, roles, and ontology mappings.
    """

    __tablename__ = "semantic_annotations"
    __table_args__ = (UniqueConstraint("column_id", name="uq_column_semantic_annotation"),)

    annotation_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Classification
    semantic_role: Mapped[str | None] = mapped_column(
        String
    )  # 'identifier', 'measure', 'attribute', 'dimension'
    entity_type: Mapped[str | None] = mapped_column(
        String
    )  # 'customer', 'product', 'transaction', etc.

    # Business terms
    business_name: Mapped[str | None] = mapped_column(String)
    business_description: Mapped[str | None] = mapped_column(Text)
    business_domain: Mapped[str | None] = mapped_column(
        String
    )  # 'finance', 'marketing', 'operations'

    # Ontology mapping
    ontology_term: Mapped[str | None] = mapped_column(String)
    ontology_uri: Mapped[str | None] = mapped_column(String)

    # Provenance
    annotation_source: Mapped[str | None] = mapped_column(
        String
    )  # 'llm', 'manual', 'config_override'
    annotated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    annotated_by: Mapped[str | None] = mapped_column(String)
    confidence: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="semantic_annotation")


class TableEntity(Base):
    """Entity detection at table level.

    Identifies the type of entity represented by the table
    and classifies it as fact/dimension table with grain analysis.
    """

    __tablename__ = "table_entities"

    entity_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    detected_entity_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'customer', 'order', 'product', etc.
    description: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Float)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Grain analysis
    grain_columns: Mapped[dict[str, Any] | None] = mapped_column(
        JSON
    )  # List of column IDs that define grain
    is_fact_table: Mapped[bool | None] = mapped_column(Boolean)
    is_dimension_table: Mapped[bool | None] = mapped_column(Boolean)

    # Provenance
    detection_source: Mapped[str | None] = mapped_column(String)  # 'llm', 'heuristic', 'manual'
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship(back_populates="entity_detections")
