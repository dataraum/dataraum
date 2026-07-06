"""SQLAlchemy models for relationship detection.

Contains the Relationship database model for storing detected relationships
between tables (both raw statistical candidates and LLM-confirmed relationships).
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
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column


class Relationship(Base):
    """Detected relationships between columns.

    Represents foreign key relationships or other associations
    detected through value overlap analysis, cardinality analysis,
    or semantic similarity.

    detection_method values (the ``candidate`` / ``not candidate`` split, DAT-408):
    - 'candidate': ephemeral structural candidate, re-derived every run.
    - 'llm': this run's LLM-confirmed relationship.
    - 'manual': user-authored, materialized each run from a teach overlay (DAT-409).
    - 'keeper': silently-accepted llm (a promoted run found it, a later run didn't,
      the user never rejected it) — materialized from a ``keep`` overlay (DAT-409).
    The "defined" catalog the downstream stages read is ``detection_method != 'candidate'``.

    Run-versioned (DAT-408): every row carries the producing ``run_id`` and rows
    coexist across runs (non-destructive; deletes are run_id-scoped, retry-only).
    The durable methods (manual/keeper) are re-materialized into each run from
    overlays, so a single read scoped to the current run sees the whole catalog.
    """

    __tablename__ = "relationships"
    __table_args__ = (
        # Run-grain identity (DAT-408): the catalog is versioned by ``run_id`` like
        # all other metadata, so the unique key includes it — two runs' rows for the
        # same pair+method coexist.
        UniqueConstraint(
            "run_id",
            "from_column_id",
            "to_column_id",
            "detection_method",
            name="uq_relationship_columns_method",
        ),
    )

    relationship_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Snapshot version axis (DAT-408): the run that produced/materialized this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

    # Source side
    from_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    from_column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)

    # Target side
    to_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    to_column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)

    # Classification
    relationship_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'foreign_key', 'semantic_reference', 'derived', 'candidate'
    cardinality: Mapped[str | None] = mapped_column(
        String
    )  # 'one-to-one', 'one-to-many', 'many-to-one', 'many-to-many'

    # Confidence and evidence
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    detection_method: Mapped[str | None] = mapped_column(String)  # 'candidate', 'llm', 'manual'
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Verification (human-in-loop)
    is_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime)
    confirmed_by: Mapped[str | None] = mapped_column(String)

    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    from_column: Mapped[Column] = relationship(
        foreign_keys=[from_column_id], back_populates="relationships_from"
    )
    to_column: Mapped[Column] = relationship(
        foreign_keys=[to_column_id], back_populates="relationships_to"
    )


Index("idx_relationships_from", Relationship.from_table_id)
Index("idx_relationships_to", Relationship.to_table_id)
# Column-level indexes for FK column lookups
Index("idx_relationships_from_column", Relationship.from_column_id)
Index("idx_relationships_to_column", Relationship.to_column_id)
# Composite indexes for table+column filtering
Index(
    "idx_relationships_from_table_column", Relationship.from_table_id, Relationship.from_column_id
)
Index("idx_relationships_to_table_column", Relationship.to_table_id, Relationship.to_column_id)


class SurrogateKeyIntent(Base):
    """The run's composite-key VERDICT record (DAT-277, DAT-697).

    ``semantic_per_table`` writes one row per composite the judge ruled on:
    ``status='confirmed'`` (via ``RelationshipOutput.key_columns`` — persisted
    HERE, never as plain llm relationship rows, so no single-column consumer
    ever joins on a half-key) or ``status='declined'`` (a COMPOSITE-KEY RESCUE
    hint was offered and the judge did not confirm it). The ``surrogate_mint``
    phase reads only the run's confirmed intents; the keeper machinery
    (``materialize.py``) reads both — an adjudicated composite must not be
    silently kept (DAT-697), because silence-as-acceptance requires the system
    to have been silent, and a verdict is not silence.

    Run-versioned like the relationship catalog (DAT-408): rows coexist across
    runs; the mint reads only its own run's intents. ``intent_digest`` is
    deterministic in the component column ids and DIRECTION-NEUTRAL (neither
    the judge's anchor choice nor its from/to orientation is run-stable), so a
    Temporal at-least-once retry upserts the same row instead of duplicating
    it, and the offered-vs-confirmed comparison cannot split one composite
    into two identities.
    """

    __tablename__ = "surrogate_key_intents"
    __table_args__ = (
        UniqueConstraint("run_id", "intent_digest", name="uq_surrogate_intent_run_digest"),
    )

    intent_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    intent_digest: Mapped[str] = mapped_column(String, nullable=False)

    # The judge's ruling: 'confirmed' (mint this composite) or 'declined' (the
    # rescue hint was offered and not confirmed — no relationship in the data).
    status: Mapped[str] = mapped_column(String, nullable=False, default="confirmed")

    from_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    to_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    # Component pairs in CANONICAL order (direction-neutral name key — the
    # anchor holds no positional privilege): [[from_column_id, to_column_id], …].
    # Column ids, not names — the id is the cross-phase-stable identity; the mint
    # resolves physical names from the Column rows when composing the hash DDL.
    column_pairs: Mapped[list[Any]] = mapped_column(JSON, nullable=False)

    # The composite join's measured cardinality (the rescue's collapse proof;
    # never 'many-to-many'). None when no DuckDB connection was available at
    # confirmation time — the mint recomputes on the minted surrogate anyway.
    cardinality: Mapped[str | None] = mapped_column(String)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    reasoning: Mapped[str | None] = mapped_column(String)

    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = ["Relationship", "SurrogateKeyIntent"]
