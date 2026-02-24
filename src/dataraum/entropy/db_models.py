"""Entropy Layer Database Models.

SQLAlchemy models for persisting entropy measurements:
- EntropyObjectRecord: Individual entropy measurements
- EntropySnapshotRecord: Point-in-time entropy state snapshots

EntropyInterpretationRecord lives in interpretation_db_models.py
(owned by the entropy_interpretation phase).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base

# Use JSONB for PostgreSQL, JSON for SQLite (JSON handles serialization automatically)
JSON_TYPE = JSONB().with_variant(JSON, "sqlite")


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

    # Evidence (detector-specific)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)

    # Resolution options
    resolution_options: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_TYPE)

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



# Indexes for common queries
Index("idx_entropy_target", EntropyObjectRecord.target)
Index("idx_entropy_layer_dimension", EntropyObjectRecord.layer, EntropyObjectRecord.dimension)
Index("idx_entropy_table", EntropyObjectRecord.table_id)
Index("idx_entropy_column", EntropyObjectRecord.column_id)
Index("idx_entropy_score", EntropyObjectRecord.score)


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

    # Overall average entropy
    avg_entropy_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Overall readiness
    overall_readiness: Mapped[str] = mapped_column(String, nullable=False, default="investigate")

    # Full snapshot data (for detailed analysis)
    snapshot_data: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)


Index("idx_entropy_snapshot_source", EntropySnapshotRecord.source_id)
Index("idx_entropy_snapshot_table", EntropySnapshotRecord.table_id)
Index("idx_entropy_snapshot_time", EntropySnapshotRecord.snapshot_at)
