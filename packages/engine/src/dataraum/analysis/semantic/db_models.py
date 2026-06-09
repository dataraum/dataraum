"""SQLAlchemy models for semantic analysis.

Contains database models for semantic annotations and entity detection.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column, Table


class SemanticAnnotation(Base):
    """Semantic annotations for columns.

    Stores LLM-generated or manually-provided semantic metadata
    including business terms, roles, and ontology mappings.
    """

    __tablename__ = "semantic_annotations"
    # One annotation per column PER RUN (DAT-413): the snapshot version axis widens
    # this from ``column_id`` to ``(column_id, run_id)`` so two coexisting runs'
    # rows for the same column don't collide. The promoted head names which run is
    # current; readers head-resolve rather than assume one row per column.
    __table_args__ = (
        UniqueConstraint("column_id", "run_id", name="uq_column_semantic_annotation"),
    )

    annotation_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    # Snapshot version axis (DAT-413): the run that wrote this row. Nullable —
    # additive, behavior-preserving; the head pointer is not consulted yet.
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

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

    # Business concept mapping - maps to standard domain concepts
    # from the active ontology (e.g., 'accounts_receivable', 'revenue', 'fiscal_period')
    business_concept: Mapped[str | None] = mapped_column(String)

    # Temporal behavior from ontology: 'additive' or 'point_in_time'
    temporal_behavior: Mapped[str | None] = mapped_column(String)

    # Independent LLM stock/flow read (ADR-0009 / DAT-445), pooled against the
    # ontology prior above in the temporal_behavior adjudication. The claim
    # ('stock'/'flow'/'unsure') + its confidence are the LLM witness, written by
    # semantic_per_column. ``temporal_behavior_contested`` is written back by the
    # resolved-layer pass (dataraum.entropy.resolve) when the pooled conflict is
    # non-trivial — it flags the resolved temporal_behavior so a downstream SQL
    # agent treats a contested stock with caution. None = no claim / not resolved.
    temporal_behavior_claim: Mapped[str | None] = mapped_column(String)
    temporal_behavior_claim_confidence: Mapped[float | None] = mapped_column(Float)
    temporal_behavior_contested: Mapped[bool | None] = mapped_column(Boolean)

    # Cross-column unit inference: column name that defines the unit for this measure
    # e.g., 'currency_code' for monetary measures. Set by the per-column phase.
    unit_source_column: Mapped[str | None] = mapped_column(String)

    # Resolved null-marker tokens (ADR-0009 / DAT-457): the rejected tokens the
    # null_semantics adjudication resolved to is-null (pooled posterior past
    # threshold). Written by the resolved-layer pass inside the terminal detect
    # (dataraum.entropy.resolve), updating THIS run's annotation. The query agent
    # treats these as NULL in generated SQL. None = not resolved / no rejects.
    null_tokens: Mapped[list[str] | None] = mapped_column(JSON)

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
    # One entity classification per table PER RUN (DAT-408/413). TableEntity is
    # run-versioned and coexists across runs; this constraint (mirroring
    # ``uq_column_semantic_annotation``) makes "one row per ``(table_id, run_id)``"
    # a DB guarantee so the run-scoped readers can trust it. ``run_id`` is nullable
    # (non-run callers/tests) — Postgres/SQLite treat NULLs as distinct, so those
    # rows are unconstrained, which is intentional.
    __table_args__ = (UniqueConstraint("table_id", "run_id", name="uq_table_entity_table_run"),)
    entity_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    # Snapshot version axis (DAT-413): the run that wrote this row. Nullable —
    # additive, behavior-preserving; the head pointer is not consulted yet.
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

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
    time_column: Mapped[str | None] = mapped_column(String, nullable=True)

    # Provenance
    detection_source: Mapped[str | None] = mapped_column(String)  # 'llm', 'heuristic', 'manual'
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship(back_populates="entity_detections")


__all__ = [
    "SemanticAnnotation",
    "TableEntity",
]
