"""Entropy Layer Database Models.

SQLAlchemy models for persisting entropy measurements:
- EntropyObjectRecord: Individual entropy measurements
- EntropyReadinessRecord: Per-column readiness rollup (DAT-394)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, String, UniqueConstraint
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
    # entropy/engine.py scopes by (detector_id, table_ids, run_id) — source-free
    # (DAT-408). Source provenance, when needed, is reachable via ``table_id``.
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
    )  # column:{t}.{c}, table:{t}, relationship:{from_col}::{to_col}

    # Foreign keys to link to analyzed data. No ``source_id`` (DAT-408): a
    # measurement is about a table/column/relationship; its source is reachable via
    # ``table_id`` and was never read off this row.
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id", ondelete="CASCADE"))
    column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE")
    )

    # Snapshot version axis (DAT-413): the run that wrote this row. Nullable —
    # additive, behavior-preserving; the head pointer is not consulted yet.
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

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


class EntropyReadinessRecord(Base):
    """Persisted per-column readiness, written by the terminal ``detect`` step (DAT-394).

    The transparent readiness-v2 rollup (``entropy/views/readiness_context.py``)
    rolls detector scores up the network into per-intent readiness. This row is
    its persisted snapshot — one per analyzed column — for the cockpit ``why`` /
    ``look`` tools (read via Drizzle) and as agent context. Self-refreshing: the
    terminal detect step re-runs on every (re-)measure and rewrites these rows
    (delete-before-insert scoped to ``table_id`` — the session's table set, DAT-410).

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

    # Target identity (DAT-408): the single key the cockpit reads (DAT-399 D),
    # carrying ``column:`` / ``relationship:`` / ``table:`` uniformly. For column
    # rows it mirrors the ``(table_id, column_id)`` pair below; for relationship
    # rows it is the only identity (those carry no single column).
    target: Mapped[str] = mapped_column(String, nullable=False)

    # Scope. ``table_id`` is the column-row delete-before-insert scope key (DAT-410);
    # relationship rows carry no ``table_id`` and scope by ``(session_id, target)``.
    # No ``source_id`` (DAT-408): source is reachable via ``table_id`` and was never
    # read off this row.
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id", ondelete="CASCADE"))
    column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE")
    )

    # Snapshot version axis (DAT-413): the run that wrote this row. Nullable —
    # additive, behavior-preserving; the head pointer is not consulted yet.
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

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


Index("idx_readiness_table", EntropyReadinessRecord.table_id)
Index("idx_readiness_column", EntropyReadinessRecord.column_id)
Index("idx_readiness_target", EntropyReadinessRecord.target)


class ClaimWitnessRecord(Base):
    """One witness's opinion on a single canonical claim (ADR-0009, DAT-457).

    The persisted, run-versioned substrate the pooling engine
    (:mod:`dataraum.entropy.pooling`) reads: one row per
    ``(target, claim_field, witness_id)`` holding that witness's probability
    distribution over the claim space plus its measured reliability. The pooled
    ``(conflict, ignorance)`` outcome is an :class:`EntropyObjectRecord`; these
    rows are the provenance behind it — loud, not buried in evidence JSON.

    Adjudication entropy only. The statistical/surprise detectors
    (``null_ratio``/``outlier_rate``/``benford``/``temporal_drift``/
    ``slice_variance``) measure ``D_KL(observed || reference)`` and never write
    here.

    Dual-grain like :class:`EntropyObjectRecord`: written by both detect paths
    (add_source per ``table:{id}``, begin_session per ``session:{id}``), so it
    carries ``session_id`` + ``table_id`` and is classified ``_DUAL_GRAIN`` on
    the promoted-read surface (ADR-0008).
    """

    __tablename__ = "claim_witnesses"
    __table_args__ = (
        UniqueConstraint(
            "target",
            "claim_field",
            "witness_id",
            "run_id",
            name="uq_claim_witness_target_field_witness_run",
        ),
    )

    claim_witness_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Mirrors EntropyObjectRecord scoping: session_id is the via_session_head
    # grain key (NOT NULL), table_id the via_table_head key.
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id", ondelete="CASCADE"))
    column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE")
    )

    # Snapshot version axis (DAT-448): the run that wrote this witness opinion.
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

    # Identity: what is being witnessed, and by whom.
    target: Mapped[str] = mapped_column(String, nullable=False)  # column:{t}.{c}, table:{t}, ...
    claim_field: Mapped[str] = mapped_column(String, nullable=False)  # e.g. unit, temporal_behavior
    witness_id: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g. quarantine_clustering, teach

    # The opinion: a distribution over the canonical claim space, stored
    # label -> probability so the claim space is self-describing.
    distribution: Mapped[dict[str, float] | None] = mapped_column(JSON_TYPE)
    # Measured trust in [0, 1] — the log-linear pooling exponent / evidence weight.
    reliability: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Which measurement produced this row — the delete-before-insert scope key,
    # mirroring EntropyObjectRecord.detector_id.
    detector_id: Mapped[str] = mapped_column(String, nullable=False)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


Index("idx_claim_witness_target", ClaimWitnessRecord.target)
Index("idx_claim_witness_table", ClaimWitnessRecord.table_id)
Index("idx_claim_witness_column", ClaimWitnessRecord.column_id)
