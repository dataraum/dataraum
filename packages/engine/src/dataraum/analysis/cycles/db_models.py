"""SQLAlchemy models for business cycle detection.

Contains database models for persisting detected business cycles.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class CycleFamily(Base):
    """The workspace's typed cycle-family declaration — the direction axis (DAT-856).

    A cycle *family* groups direction-typed cycle types that differ ONLY in
    who-owes-whom: e.g. a single ``settlement`` family whose ``incoming`` /
    ``outgoing`` directions each resolve to a declared ``cycle_types`` member
    (accounts_receivable / accounts_payable). Config→DB, the same cut
    :class:`~dataraum.analysis.semantic.db_models.Concept` (DAT-728) and
    :class:`~dataraum.analysis.semantic.db_models.Convention` (DAT-789) took: the
    shipped vertical ``cycles.yaml`` ``cycle_families`` block is the *seed*,
    normalized into typed rows at connect
    (:func:`~dataraum.analysis.cycles.cycle_family_store.ensure_cycle_families_seeded`);
    the cycle judge's DOMAIN KNOWLEDGE serving and the save-time direction
    resolution read these rows, never the YAML — so a *framed* vertical whose
    families exist only as rows would serve identically.

    **The engine never invents a family (the declaration lives in config).**
    ``directions`` maps a declared direction label → the ``cycle_types`` member it
    resolves to (validated against the vocabulary at seed). The direction axis it
    declares is what the output contract's ``direction`` is membership-validated
    against at save (the provenance-contract pattern): a decided direction
    resolves to its member cycle type, while ``undetermined`` is the honest state
    carried on the family itself — never coerced to a direction.

    **Identity contract — NOT run-versioned (the DAT-728/789 pattern).** A family
    is a stable node keyed by ``(vertical, family)``; ``family_id`` is a
    workspace-stable surrogate minted once at seed, NOT a per-run uuid. Edits
    supersede via ``uq_cycle_family_active`` (at most one active row per key) so a
    head-free read is unambiguous. Workspace identity IS the ``ws_<id>`` schema (no
    ``workspace_id`` column); the read surface scopes to the bound
    ``active_vertical`` (``_VERTICAL_SCOPED`` in ``storage/read_views.py``).
    """

    __tablename__ = "cycle_families"
    __table_args__ = (
        # At most one ACTIVE row per (vertical, family); superseded history rows are
        # exempt. The deterministic single-active-row guarantee the head-free reads
        # and the seed's ON CONFLICT DO NOTHING rely on — the same shape as
        # Concept.uq_concept_active / Convention.uq_convention_active.
        Index(
            "uq_cycle_family_active",
            "vertical",
            "family",
            unique=True,
            postgresql_where=text("superseded_at IS NULL"),
            sqlite_where=text("superseded_at IS NULL"),
        ),
        # Lifecycle-source vocabulary (DAT-802, the two-layer standard): the ONLY
        # live writer is 'seed' (``cycle_family_store.ensure_cycle_families_seeded``).
        # No frame/teach writer for cycle families exists yet — a CHECK admitting a
        # value no writer produces is the exact DAT-802 defect, so the set is 'seed'
        # alone until one lands (widening is one line + a re-dump in that PR).
        CheckConstraint("source IS NULL OR source IN ('seed')", name="source"),
    )

    family_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    vertical: Mapped[str] = mapped_column(String, nullable=False)
    # The family's stable identifier within `vertical` (the cycles.yaml key).
    family: Mapped[str] = mapped_column(String, nullable=False)
    # direction label -> the cycle_types member it resolves to (validated against the
    # vocabulary at seed). JSON object; the direction axis the output contract's
    # `direction` is membership-validated against. NOT NULL: a family with no
    # directions is a seed error (born-loud in the store).
    directions: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)

    # Lifecycle: workspace-persistent with supersession (NULL superseded_at = active).
    # Closed vocab: see ck_cycle_families_source — 'seed' is the only live writer.
    source: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)


class DetectedBusinessCycle(Base):
    """A detected business cycle for one operating_model run.

    Stores the details of each detected cycle including its type,
    stages, entity flows, and completion metrics.

    Run-versioned (DAT-455): one row per ``(session, canonical_type, run)`` —
    the schema axis of the versioned-model consumer contract. Source-free past
    the add_source boundary: cycles are detected in operating_model over the
    session's typed tables (``tables_involved``), never scoped to a
    ``source_id``. A re-run supersedes by writing rows under its fresh
    ``run_id``; readers scope to the promoted ``operating_model`` head (or,
    in-run, to this run's id), never across runs.

    Direction axis (DAT-856): a cycle the judge places in a declared
    :class:`CycleFamily` carries the resolved ``family`` + ``direction``. For a
    decided direction the ``canonical_type`` is the family's member (e.g.
    ``accounts_payable``) and ``direction`` its label; when the served evidence
    does NOT decide, ``canonical_type`` is the FAMILY itself (e.g. ``settlement``)
    and ``direction`` is ``'undetermined'`` — the honest detected-but-undirected
    state, distinguishable from both a missed cycle and a directed one, never
    coerced to a direction. A non-family cycle carries neither.
    """

    __tablename__ = "detected_business_cycles"
    __table_args__ = (
        UniqueConstraint("canonical_type", "run_id", name="uq_detected_cycle_run"),
        # Structural invariant (DAT-856, the two-layer standard): family and
        # direction CO-OCCUR — a family cycle always carries a direction (a declared
        # label or the 'undetermined' sentinel), a non-family cycle carries neither.
        # The per-vertical direction VOCABULARY is enforced at save
        # (membership-validated against the loaded family declaration in
        # ``config.resolve_cycle_identity``), not here — the DB pins only what is
        # statically closed, exactly what the vertical-dependent-vocabulary
        # two-layer standard prescribes.
        CheckConstraint(
            "(family IS NULL AND direction IS NULL) "
            "OR (family IS NOT NULL AND direction IS NOT NULL)",
            name="family_direction",
        ),
    )

    cycle_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    # Classification
    cycle_name: Mapped[str] = mapped_column(String, nullable=False)
    cycle_type: Mapped[str] = mapped_column(String, nullable=False)  # Raw LLM output
    canonical_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # The declared cycle vocabulary key — the artifact identity
    is_known_type: Mapped[bool] = mapped_column(Boolean, default=False)  # True if in vocabulary
    # Direction axis (DAT-856). Both NULL for a non-family cycle; both NON-NULL for a
    # family cycle (co-occurrence pinned by ck_detected_business_cycles_family_direction).
    # `family` is open per-vertical vocabulary (no value CHECK); `direction` is a declared
    # label or the 'undetermined' sentinel, membership-validated at save against the
    # family's declaration. NULL on rows written before the axis existed (no backfill).
    family: Mapped[str | None] = mapped_column(String, nullable=True)
    direction: Mapped[str | None] = mapped_column(String, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    business_value: Mapped[str] = mapped_column(String, default="medium")
    confidence: Mapped[float] = mapped_column(Float, default=0.0)

    # Structure
    tables_involved: Mapped[list[str]] = mapped_column(JSON, default=list)
    stages: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    entity_flows: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)

    # Status tracking
    status_table: Mapped[str | None] = mapped_column(String, nullable=True)
    status_column: Mapped[str | None] = mapped_column(String, nullable=True)
    completion_value: Mapped[str | None] = mapped_column(String, nullable=True)

    # Metrics
    total_records: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completed_cycles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_rate: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Evidence
    evidence: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Timestamps
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = [
    "CycleFamily",
    "DetectedBusinessCycle",
]
