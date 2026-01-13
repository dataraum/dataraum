"""Entropy Layer Database Models.

SQLAlchemy models for persisting entropy measurements and compound risks.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from dataraum_context.storage import Base

# Use JSONB for PostgreSQL, JSON for SQLite
JSON_TYPE = JSONB().with_variant(String, "sqlite")


class EntropyObjectRecord(Base):
    """Persisted entropy measurement.

    Stores individual entropy measurements with their evidence,
    resolution options, and context for both LLM and human consumers.
    """

    __tablename__ = "entropy_objects"

    object_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Identity - what is being measured
    layer: Mapped[str] = mapped_column(
        String, nullable=False
    )  # structural, semantic, value, computational
    dimension: Mapped[str] = mapped_column(String, nullable=False)  # schema, types, units, etc.
    sub_dimension: Mapped[str] = mapped_column(
        String, nullable=False
    )  # naming_clarity, type_fidelity, etc.
    target: Mapped[str] = mapped_column(
        String, nullable=False
    )  # column:{t}.{c}, table:{t}, relationship:{t1}-{t2}

    # Foreign keys to link to source data
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.source_id"))
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))
    column_id: Mapped[str | None] = mapped_column(ForeignKey("columns.column_id"))

    # Measurement
    score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)

    # Evidence (detector-specific)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)

    # Resolution options
    resolution_options: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_TYPE)

    # Context for LLM agents
    llm_context: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)

    # Context for human users
    human_context: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)

    # Metadata
    detector_id: Mapped[str] = mapped_column(String, nullable=False)  # Which detector produced this
    source_analysis_ids: Mapped[list[str] | None] = mapped_column(
        JSON_TYPE
    )  # Links to source analyses

    # Timestamps
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Versioning (for history tracking)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    superseded_by: Mapped[str | None] = mapped_column(String)  # Reference to newer version


# Indexes for common queries
Index("idx_entropy_target", EntropyObjectRecord.target)
Index("idx_entropy_layer_dimension", EntropyObjectRecord.layer, EntropyObjectRecord.dimension)
Index("idx_entropy_table", EntropyObjectRecord.table_id)
Index("idx_entropy_column", EntropyObjectRecord.column_id)
Index("idx_entropy_score", EntropyObjectRecord.score)


class CompoundRiskRecord(Base):
    """Persisted compound risk detection.

    Stores dangerous combinations of entropy dimensions that
    create multiplicative risk.
    """

    __tablename__ = "compound_risks"

    risk_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # What has the risk
    target: Mapped[str] = mapped_column(
        String, nullable=False
    )  # column, table, or relationship reference

    # Foreign keys
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.source_id"))
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))

    # Which dimensions are combined
    dimensions: Mapped[list[str]] = mapped_column(JSON_TYPE, nullable=False)
    dimension_scores: Mapped[dict[str, float]] = mapped_column(JSON_TYPE, nullable=False)

    # Risk assessment
    risk_level: Mapped[str] = mapped_column(String, nullable=False)  # critical, high, medium
    impact: Mapped[str] = mapped_column(String, nullable=False)  # Description of what can go wrong
    multiplier: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    combined_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Mitigation options
    mitigation_options: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_TYPE)

    # Timestamps
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Resolution tracking
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime)
    resolved_by: Mapped[str | None] = mapped_column(String)  # User or system that resolved


Index("idx_compound_risk_target", CompoundRiskRecord.target)
Index("idx_compound_risk_level", CompoundRiskRecord.risk_level)
Index("idx_compound_risk_table", CompoundRiskRecord.table_id)


class EntropySnapshotRecord(Base):
    """Snapshot of entropy state at a point in time.

    Used for tracking entropy trends over time and
    detecting entropy degradation.
    """

    __tablename__ = "entropy_snapshots"

    snapshot_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Scope
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.source_id"))
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))

    # Snapshot time
    snapshot_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Summary statistics
    total_entropy_objects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    high_entropy_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    critical_entropy_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    compound_risk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Per-layer averages
    avg_structural_entropy: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_semantic_entropy: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_value_entropy: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_computational_entropy: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_composite_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Overall readiness
    overall_readiness: Mapped[str] = mapped_column(String, nullable=False, default="investigate")

    # Full snapshot data (for detailed analysis)
    snapshot_data: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)


Index("idx_entropy_snapshot_source", EntropySnapshotRecord.source_id)
Index("idx_entropy_snapshot_table", EntropySnapshotRecord.table_id)
Index("idx_entropy_snapshot_time", EntropySnapshotRecord.snapshot_at)
