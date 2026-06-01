"""Entropy Layer Database Models.

SQLAlchemy models for persisting entropy measurements:
- EntropyObjectRecord: Individual entropy measurements
- EntropyReadinessRecord: Per-column readiness rollup (DAT-394)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base

# Use JSONB for PostgreSQL, JSON for SQLite (JSON handles serialization automatically)
JSON_TYPE = JSONB().with_variant(JSON, "sqlite")


class EntropyObjectRecord(Base):
    """Persisted entropy measurement.

    Stores individual entropy measurements with their evidence
    and context for both LLM and human consumers.
    """

    __tablename__ = "entropy_objects"

    object_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    # Workspace scope is structural: this row lives in its workspace's Postgres
    # schema. session_id stays NOT NULL but is no longer load-bearing post-DAT-341;
    # entropy/engine.py filters by (source_id, detector_id).
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )

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
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id", ondelete="CASCADE"))
    column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE")
    )

    # Measurement
    score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Evidence (detector-specific)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)

    # Metadata
    detector_id: Mapped[str] = mapped_column(String, nullable=False)  # Which detector produced this
    source_analysis_ids: Mapped[list[str] | None] = mapped_column(
        JSON_TYPE
    )  # Links to source analyses

    # Timestamps
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


# Indexes for common queries
Index("idx_entropy_target", EntropyObjectRecord.target)
Index("idx_entropy_layer_dimension", EntropyObjectRecord.layer, EntropyObjectRecord.dimension)
Index("idx_entropy_table", EntropyObjectRecord.table_id)
Index("idx_entropy_column", EntropyObjectRecord.column_id)
Index("idx_entropy_score", EntropyObjectRecord.score)
Index("idx_entropy_source_detector", EntropyObjectRecord.source_id, EntropyObjectRecord.detector_id)


class EntropyReadinessRecord(Base):
    """Persisted per-column readiness, written by the terminal ``detect`` step (DAT-394).

    The transparent readiness-v2 rollup (``entropy/views/readiness_context.py``)
    rolls detector scores up the network into per-intent readiness. This row is
    its persisted snapshot — one per analyzed column — for the cockpit ``why`` /
    ``look`` tools (read via Drizzle) and as agent context. Self-refreshing: the
    terminal detect step re-runs on every (re-)measure and rewrites these rows
    (delete-before-insert scoped to ``source_id``).

    ``band`` is the collapsed worst-of-intents band the contract gate already
    consumes; ``intents`` carries the per-intent breakdown (query / aggregation /
    reporting) the rollup keeps first-class.
    """

    __tablename__ = "entropy_readiness"

    readiness_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Per-run FK, consistent with EntropyObjectRecord.session_id.
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )

    # Scope — one readiness row per analyzed column. ``source_id`` is the
    # delete-before-insert scope key and is always set, so it is NOT NULL.
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id", ondelete="CASCADE"))
    column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE")
    )

    # Collapsed worst-of-intents band ("ready" / "investigate" / "blocked") — the
    # signal the contract gate consumes — plus the worst per-intent risk behind it.
    band: Mapped[str] = mapped_column(String, nullable=False, default="ready")
    worst_intent_risk: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Per-intent breakdown: [{intent, band, risk, drivers: [{node, state, impact_delta}]}].
    intents: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_TYPE)
    # Column-level ranked drivers (collapsed per-node impact_delta): [{node, state, impact_delta}].
    top_drivers: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_TYPE)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


Index("idx_readiness_source", EntropyReadinessRecord.source_id)
Index("idx_readiness_table", EntropyReadinessRecord.table_id)
Index("idx_readiness_column", EntropyReadinessRecord.column_id)
