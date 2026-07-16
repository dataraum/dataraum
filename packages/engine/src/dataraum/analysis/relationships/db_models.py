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
    CheckConstraint,
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
        # Closed-vocabulary enforcement (DAT-772 audit, DAT-782): the values every
        # writer actually produces — ``detector.py`` (structural candidate),
        # ``agent.py``/``processor.py`` (LLM-confirmed, orienting the LLM's
        # foreign_key/hierarchy choice), ``materialize.py`` (manual/keeper overlay
        # materialization, always 'foreign_key'). A producer-side Literal alone
        # (``semantic/models.py``'s ``RelationshipOutput.relationship_type``)
        # already drifted from this once — the CHECK is the DB-enforced backstop.
        CheckConstraint(
            "relationship_type IN ('foreign_key', 'hierarchy', 'candidate')",
            name="relationship_type",
        ),
        # Confirmation-source vocabulary (DAT-776): the DB-enforced backstop for the
        # closed set every writer sets EXPLICITLY via :meth:`oriented_row`. Replaces
        # the inverted ``is_confirmed`` boolean, which the judge-confirm path never
        # set (default False) so every LLM-confirmed FK read as "not confirmed".
        CheckConstraint(
            "confirmation_source IN ('unconfirmed', 'judge', 'user', 'keeper')",
            name="confirmation_source",
        ),
        # Detection-method vocabulary (DAT-802 enum-standard sweep): the closed set
        # every writer produces — ``detector.py`` ('candidate'), ``processor.py``
        # llm-confirm path ('llm'), ``materialize.py`` overlay materialization
        # ('manual' / 'keeper'). Sibling of ``confirmation_source`` in the same
        # table/DAT-776 era that CHECK pass left uncovered.
        CheckConstraint(
            "detection_method IS NULL OR detection_method IN "
            "('candidate', 'llm', 'manual', 'keeper')",
            name="detection_method",
        ),
        # Orientation invariant (DAT-777, upgraded DAT-802): a persisted FK is
        # stored many→one, child→parent — so ``from`` is always the many/fact side
        # every downstream consumer assumes (og_references, the conformed-dim
        # identity resolve, the cockpit fan-out caution). :meth:`oriented_row`
        # FLIPS a ``one-to-many`` row before persisting, so the full closed set a
        # row can ever legally hold is exactly these three (plus NULL) —
        # ``one-to-many`` is now structurally impossible, not merely rejected by a
        # backstop. A mis-oriented row fails loud at flush even on a write path
        # that bypasses the helper.
        CheckConstraint(
            "cardinality IS NULL OR cardinality IN ('one-to-one', 'many-to-one', 'many-to-many')",
            name="cardinality_oriented",
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
    )  # 'foreign_key', 'hierarchy', 'candidate' — see ck_relationships_relationship_type
    cardinality: Mapped[str | None] = mapped_column(
        String
    )  # 'one-to-one', 'one-to-many', 'many-to-one', 'many-to-many'

    # Confidence and evidence
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    detection_method: Mapped[str | None] = mapped_column(String)  # 'candidate', 'llm', 'manual'
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Verification source (DAT-776): WHO/WHAT vouches for this edge. Set EXPLICITLY
    # by every writer via :meth:`oriented_row` (see ``ck_relationships_confirmation_source``):
    #   'unconfirmed' — structural candidate / judge-declined (detector, processor)
    #   'judge'       — this run's LLM judge confirmed it (processor llm path)
    #   'user'        — explicit human teach, add/confirm overlay (materialize manual)
    #   'keeper'      — silently retained: a prior promoted run's judge-confirmed edge
    #                   the user never rejected (materialize keeper, DAT-409) — a
    #                   distinct authority from a user assertion (silence, not a teach).
    # Replaces the ``is_confirmed`` boolean the judge-confirm path never set, so every
    # LLM-confirmed FK read as "not confirmed" (the inversion bug). NOT NULL with a
    # server default for the unconfirmed state.
    confirmation_source: Mapped[str] = mapped_column(
        String, nullable=False, server_default="unconfirmed"
    )

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

    @staticmethod
    def oriented_row(
        *,
        run_id: str | None,
        from_table_id: str,
        from_column_id: str,
        to_table_id: str,
        to_column_id: str,
        relationship_type: str,
        cardinality: str | None,
        confidence: float,
        detection_method: str,
        confirmation_source: str,
        evidence: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """THE single builder for a persisted relationship row (DAT-777).

        All three write paths — detector candidate, LLM judge, overlay
        materialization — build their row through this one helper, so the FK
        orientation invariant is enforced at ONE chokepoint instead of a single
        per-path call the other two forgot. Returns the row dict the writers
        upsert / ``session.add(Relationship(**row))``; the uuid PK is omitted so
        the model's Python-side default applies.

        Orients the endpoints to the many→one, child→parent FK convention every
        downstream consumer assumes (``from`` = the many/fact side). The MEASURED
        cardinality is the signal (DAT-758): ``one-to-many`` means ``from`` is the
        ONE (parent/dim) side, so swap the endpoints — and the directional
        ``left_*``/``right_*`` evidence — to store ``many-to-one``. ``many-to-one``
        is already correct; ``one-to-one`` is orientation-agnostic;
        ``many-to-many``/``None`` cannot be oriented. The DB backstop is
        ``ck_relationships_cardinality_oriented``: a mis-oriented ``one-to-many``
        row fails loud at flush even if a future writer bypasses this helper.
        """
        evidence = dict(evidence) if evidence else {}
        if cardinality == "one-to-many":
            from_table_id, from_column_id, to_table_id, to_column_id = (
                to_table_id,
                to_column_id,
                from_table_id,
                from_column_id,
            )
            cardinality = "many-to-one"
            evidence = _swap_directional_evidence(evidence)
            # A many-to-one child→parent join matches each child to exactly one
            # parent — it never fans out (the one-to-many parent→child join did).
            evidence["introduces_duplicates"] = False
        return {
            "run_id": run_id,
            "from_table_id": from_table_id,
            "from_column_id": from_column_id,
            "to_table_id": to_table_id,
            "to_column_id": to_column_id,
            "relationship_type": relationship_type,
            "cardinality": cardinality,
            "confidence": confidence,
            "detection_method": detection_method,
            "confirmation_source": confirmation_source,
            "evidence": evidence,
        }


def _swap_directional_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    """Exchange every ``left_*``/``right_*`` metric when the FK endpoints flip.

    Referential integrity, uniqueness and any other directional metric are named
    for the FROM/TO endpoints; after the swap the old TO becomes FROM, so the
    pairs exchange. An unpaired ``left_``/``right_`` key still moves — the metric
    follows its endpoint. Only ``left_``/``right_``-PREFIXED keys are covered: an
    unprefixed-but-directional metric (e.g. a bare ``orphan_count``) does NOT swap,
    so a new evidence producer that needs orientation-following must use the prefix.
    """
    swapped: dict[str, Any] = {}
    for key, value in evidence.items():
        if key.startswith("left_"):
            swapped["right_" + key[len("left_") :]] = value
        elif key.startswith("right_"):
            swapped["left_" + key[len("right_") :]] = value
        else:
            swapped[key] = value
    return swapped


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
        # Verdict vocabulary (DAT-802): the only two values ``_confirmed_intent_row``
        # / ``_declined_intent_rows`` (semantic/processor.py) ever write.
        CheckConstraint("status IN ('confirmed', 'declined')", name="status"),
        # Cardinality vocabulary (DAT-802): sourced exclusively from
        # ``compute_composite_cardinality`` (code-computed, never LLM-echoed — the
        # LLM only supplies which columns to include, via ``key_columns``). The
        # confirmed path (``_build_surrogate_intent``) explicitly falls back to no
        # intent when the measurement is ``'many-to-many'`` (never mint a proven
        # fan-out as a key); the declined path only ever carries a
        # ``rescue_fanout_to_composite`` hint, which by construction never returns a
        # many-to-many ``CompositeKey`` either (composite.py's rescue loop). NULL
        # when no DuckDB connection was available at confirmation time.
        CheckConstraint(
            "cardinality IS NULL OR cardinality IN ('one-to-one', 'one-to-many', 'many-to-one')",
            name="cardinality",
        ),
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
