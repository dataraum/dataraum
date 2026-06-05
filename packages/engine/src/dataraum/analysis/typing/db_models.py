"""Type inference database models.

SQLAlchemy models for type inference persistence:
- TypeCandidate: Detected type candidates with confidence scores
- TypeDecision: Final type decision (automatic or human override)
- MaterializationRecipe: Versioned typed/quarantine ``CREATE TABLE`` DDL (DAT-414)

These models form the persisted interface between the typing module
and downstream modules (statistics, semantic analysis).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column


class TypeCandidate(Base):
    """Type candidates from value pattern detection.

    Each column may have multiple type candidates with different
    confidence scores based on pattern matching and parsing success.

    Confidence is calculated from:
    - Pattern match rate (regex patterns on VALUES)
    - Parse success rate (TRY_CAST on VALUES)
    - Unit detection confidence (Pint on VALUES)

    Note: Type inference is based ONLY on value analysis,
    NOT on column names (column names are semantically meaningful
    but fragile for type inference).
    """

    __tablename__ = "type_candidates"

    candidate_id: Mapped[str] = mapped_column(
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
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Type candidate
    data_type: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    parse_success_rate: Mapped[float | None] = mapped_column(Float)
    failed_examples: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Pattern info (value-based, NOT column name based)
    detected_pattern: Mapped[str | None] = mapped_column(String)
    pattern_match_rate: Mapped[float | None] = mapped_column(Float)

    # Unit detection (from Pint)
    detected_unit: Mapped[str | None] = mapped_column(String)
    unit_confidence: Mapped[float | None] = mapped_column(Float)

    # Quarantine metrics (set during type resolution)
    quarantine_count: Mapped[int | None] = mapped_column(Integer)
    quarantine_rate: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="type_candidates")


class TypeDecision(Base):
    """Type decisions (human-reviewable).

    Final type decision for a column after inference and optional human review.
    One decision per column.

    Decision sources:
    - 'automatic': System selected best TypeCandidate
    - 'manual': Human override via UI/API
    - 'override': Configuration-based override
    """

    __tablename__ = "type_decisions"
    # One decision per column PER RUN (DAT-413): the snapshot version axis widens
    # this from ``column_id`` to ``(column_id, run_id)`` so two coexisting runs'
    # rows for the same column don't collide. The promoted head names which run is
    # current; readers head-resolve rather than assume one row per column.
    __table_args__ = (UniqueConstraint("column_id", "run_id", name="uq_column_type_decision"),)

    decision_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    # Snapshot version axis (DAT-413): the run that wrote this row. Nullable —
    # additive, behavior-preserving; the head pointer is not consulted yet.
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

    decided_type: Mapped[str] = mapped_column(String, nullable=False)
    decision_source: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'automatic', 'manual', 'override'
    decided_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    decided_by: Mapped[str | None] = mapped_column(String)

    # Audit trail
    previous_type: Mapped[str | None] = mapped_column(String)
    decision_reason: Mapped[str | None] = mapped_column(String)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="type_decisions")


class MaterializationRecipe(Base):
    """Versioned ``CREATE TABLE`` DDL for a physical typed/quarantine artifact (DAT-414).

    The materialization *recipe* — the ``CREATE OR REPLACE TABLE … AS SELECT``
    string typing executes to build a physical DuckDB table — versioned as
    metadata. The DuckDB lake itself stays latest-only; only the DDL string is
    versioned, stamped with the run that emitted it. Re-executing a stored row
    reproduces the artifact's data — the recipe versions the *transformation*,
    not the data, so DDL-written audit columns (the quarantine ``_quarantined_at``
    ``CURRENT_TIMESTAMP``) re-stamp on rebuild — and a reset-to-prior-run replays
    the prior run's stored DDL **without** re-deriving the typing phase.

    Grain ``(table_id, layer, run_id)``: one recipe per produced layer
    (``typed`` / ``quarantine``) per typed Table per run. ``table_id`` is the
    *typed* Table id (stable across re-types, DAT-373) — the physical artifact
    the DDL produces — not the raw input table.

    ``depends_on`` lists the bare DuckDB names this DDL reads from (typed/
    quarantine both read the raw layer), so a rebuild can re-execute a chain in
    dependency order. Single-level for typed/quarantine today; the field
    future-proofs the view-DDL chains in Slice B (DAT-415).
    """

    __tablename__ = "materialization_recipes"
    # One recipe per (typed table, produced layer, run). The run axis lets two
    # coexisting runs' recipes for the same artifact live side by side; the
    # promoted snapshot head names which run is current (DAT-413).
    __table_args__ = (
        UniqueConstraint(
            "table_id", "layer", "run_id", name="uq_materialization_recipe_table_layer_run"
        ),
    )

    recipe_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    # The typed Table whose physical artifact this DDL materializes. CASCADE so a
    # dropped typed Table takes its recipes with it.
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    # Produced lake layer: ``"typed"`` or ``"quarantine"``.
    layer: Mapped[str] = mapped_column(String, nullable=False)
    # Snapshot version axis (DAT-413): the run that emitted this DDL. Nullable to
    # mirror TypeDecision/TypeCandidate (a non-run caller writes a NULL run).
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

    # The fully-qualified DuckDB target the DDL creates
    # (e.g. ``lake.typed."csv__orders"``), captured so a rebuild can verify /
    # reference the artifact without recomposing the name.
    target_fqn: Mapped[str] = mapped_column(String, nullable=False)
    # The exact ``CREATE OR REPLACE TABLE … AS SELECT`` string, re-executed
    # verbatim to rebuild the physical artifact.
    ddl: Mapped[str] = mapped_column(String, nullable=False)
    # Bare DuckDB names this DDL reads from, for dependency-order rebuild.
    depends_on: Mapped[list[str] | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


# Indexes for efficient queries
Index("idx_type_candidates_column", TypeCandidate.column_id)
Index("idx_type_decisions_column", TypeDecision.column_id)
Index("idx_materialization_recipes_table", MaterializationRecipe.table_id)
