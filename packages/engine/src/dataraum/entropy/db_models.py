"""Entropy Layer Database Models.

SQLAlchemy models for persisting entropy measurements:
- EntropyObjectRecord: Individual entropy measurements
- EntropyReadinessRecord: Per-column readiness rollup (DAT-394)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.entropy.models import (
    ABSTAIN_REASONS,
    COVERAGE_MEASURED,
    COVERAGE_STATES,
    ENTROPY_STATUSES,
    STATUS_MEASURED,
)
from dataraum.storage import Base

# Use JSONB for PostgreSQL, JSON for SQLite (JSON handles serialization automatically)
JSON_TYPE = JSONB().with_variant(JSON, "sqlite")

# Closed-vocabulary CHECK values (DAT-802 enum-standard sweep), derived from the
# single-home enums in ``entropy.dimensions`` — every detector's ``layer`` /
# ``dimension`` / ``sub_dimension`` is already ``isinstance``-asserted against
# these at registration (``detectors/base.py``); this is the DB-enforced backstop
# (the DAT-784 pattern). Sorted for a deterministic CHECK string in the dump.
_LAYER_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in Layer))
_DIMENSION_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in Dimension))
_SUB_DIMENSION_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in SubDimension))


class EntropyObjectRecord(Base):
    """Persisted entropy measurement.

    Stores individual entropy measurements with their evidence
    and context for both LLM and human consumers.
    """

    __tablename__ = "entropy_objects"
    __table_args__ = (
        # Closed-vocabulary enforcement (DAT-802 enum-standard sweep): derived from
        # the Layer/Dimension/SubDimension enums, the single home — every detector
        # is already ``isinstance``-asserted against them at registration
        # (``detectors/base.py``); this is the DB-enforced backstop.
        CheckConstraint(
            "layer IN (" + ", ".join(f"'{v}'" for v in _LAYER_VALUES) + ")", name="layer"
        ),
        CheckConstraint(
            "dimension IN (" + ", ".join(f"'{v}'" for v in _DIMENSION_VALUES) + ")",
            name="dimension",
        ),
        CheckConstraint(
            "sub_dimension IN (" + ", ".join(f"'{v}'" for v in _SUB_DIMENSION_VALUES) + ")",
            name="sub_dimension",
        ),
        # Abstention vocabulary (DAT-853): derived from the single-home constants
        # in ``entropy.models`` (the EntropyObject __post_init__ is the writer-side
        # enforcement; this is the DB backstop). Sorted for a deterministic dump.
        CheckConstraint(
            "status IN (" + ", ".join(f"'{v}'" for v in sorted(ENTROPY_STATUSES)) + ")",
            name="status",
        ),
        CheckConstraint(
            "abstain_reason IS NULL OR abstain_reason IN ("
            + ", ".join(f"'{v}'" for v in sorted(ABSTAIN_REASONS))
            + ")",
            name="abstain_reason",
        ),
        # The pairing: a measured row carries a score and no reason; an abstained
        # row carries a reason and no score — "not measured" can never render as
        # a number.
        CheckConstraint(
            "(status = 'measured' AND score IS NOT NULL AND abstain_reason IS NULL)"
            " OR (status = 'abstained' AND score IS NULL AND abstain_reason IS NOT NULL)",
            name="status_score_reason",
        ),
    )

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
    )  # column:{t}.{c}, table:{t}, relationship:{from_col}::{to_col}

    # Foreign keys to link to analyzed data. No ``source_id`` (DAT-408): a
    # measurement is about a table/column/relationship; its source is reachable via
    # ``table_id`` and was never read off this row.
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))
    column_id: Mapped[str | None] = mapped_column(ForeignKey("columns.column_id"))

    # Snapshot version axis (DAT-413): the run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    # Measurement (DAT-853): ``score`` is NULL exactly when the detector
    # abstained (``status = 'abstained'``); the pairing is CHECK-enforced above.
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False, default=STATUS_MEASURED)
    abstain_reason: Mapped[str | None] = mapped_column(String, nullable=True)

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
    rolls detector scores through the loss tables into per-intent readiness. This row is
    its persisted snapshot — one per analyzed column — for the cockpit ``why`` /
    ``look`` tools (read via Drizzle) and as agent context. Self-refreshing: the
    terminal detect step re-runs on every (re-)measure and rewrites these rows
    (delete-before-insert scoped to ``table_id`` — the session's table set, DAT-410).

    ``band`` is the collapsed worst-of-intents band the contract gate already
    consumes; ``intents`` carries the per-intent breakdown (query / aggregation /
    reporting) the rollup keeps first-class.
    """

    __tablename__ = "entropy_readiness"
    __table_args__ = (
        # Closed-vocabulary enforcement (DAT-802 enum-standard sweep): the 3
        # values ``LossConfig.band()`` (``entropy/loss.py``, the single chokepoint
        # every ``band=`` write reads from) can ever return — hardcoded in its
        # body, not (yet) its own enum; hand-typed inline, matching the
        # ``relationship_type`` precedent.
        CheckConstraint("band IN ('ready', 'investigate', 'blocked')", name="band"),
        # Rollup coverage (DAT-853): the third outcome. The band vocabulary above
        # stays frozen; ``coverage`` distinguishes "measured clean" from "not
        # measured" — derived from the single-home constants in ``entropy.models``.
        CheckConstraint(
            "coverage IN (" + ", ".join(f"'{v}'" for v in sorted(COVERAGE_STATES)) + ")",
            name="coverage",
        ),
    )

    readiness_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Target identity (DAT-408): the single key the cockpit reads (DAT-399 D),
    # carrying ``column:`` / ``relationship:`` / ``table:`` uniformly. For column
    # rows it mirrors the ``(table_id, column_id)`` pair below; for relationship
    # rows it is the only identity (those carry no single column).
    target: Mapped[str] = mapped_column(String, nullable=False)

    # Scope. ``table_id`` is the column-row delete-before-insert scope key (DAT-410);
    # relationship rows carry no ``table_id`` and scope by ``(run_id, target)``.
    # No ``source_id`` (DAT-408): source is reachable via ``table_id`` and was never
    # read off this row.
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))
    column_id: Mapped[str | None] = mapped_column(ForeignKey("columns.column_id"))

    # Snapshot version axis (DAT-413): the run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    # Collapsed worst-of-intents band ("ready" / "investigate" / "blocked") — the
    # signal the contract gate consumes — plus the worst per-intent risk behind it.
    band: Mapped[str] = mapped_column(String, nullable=False, default="ready")
    worst_intent_risk: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Rollup coverage (DAT-853): whether the band rests on actual measurements.
    # Degraded only by GAP-reason abstentions (ABSTAIN_GAP_REASONS — a
    # not_applicable "no such question" does not degrade coverage). 'unmeasured'
    # rows carry band='ready' with zero risk — the band is vacuous and this
    # column says so; previously such targets got NO row (silent green).
    # ``abstentions`` is the self-describing trace: [{detector, reason, intents}].
    coverage: Mapped[str] = mapped_column(String, nullable=False, default=COVERAGE_MEASURED)
    abstentions: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_TYPE)

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
    (``null_ratio``/``benford``) measure ``D_KL(observed || reference)`` and
    never write here.

    Dual-grain like :class:`EntropyObjectRecord`: written by both detect paths
    (add_source per ``table:{id}``, begin_session per the workspace ``catalog``
    head), so it carries ``table_id`` and is classified ``_DUAL_GRAIN`` on the
    promoted-read surface (ADR-0008).
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
    # Mirrors EntropyObjectRecord scoping: table_id is the via_table_head grain key.
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))
    column_id: Mapped[str | None] = mapped_column(ForeignKey("columns.column_id"))

    # Snapshot version axis (DAT-448): the run that wrote this witness opinion.
    run_id: Mapped[str] = mapped_column(String, nullable=False)

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
