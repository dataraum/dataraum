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

from dataraum.core.models.base import RelationshipType
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

    A judge DECLINE is a SEPARATE FACT from the structural measurement (DAT-824):
    it is recorded as ``judge_verdict='declined'`` ANNOTATED onto the pair's
    existing ``candidate`` row — never a clobbering re-write that would destroy
    the measured value-overlap evidence, and never a new ``detection_method`` that
    would leak into the ``!= 'candidate'`` defined catalog and become a reference.
    A declined candidate stays ``detection_method='candidate'`` (excluded from every
    reference-serving consumer for free) and keeps its measured
    ``confidence``/``evidence`` intact; the judge's reasoning is merged into
    ``evidence['reasoning']``.

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
        # Closed-vocabulary enforcement (DAT-772 audit, DAT-782, DAT-850): the
        # values every writer actually produces — ``detector.py`` (structural
        # candidate), ``agent.py``/``processor.py`` (LLM-claimed foreign_key/
        # hierarchy), ``materialize.py`` (manual/keeper overlay materialization),
        # plus 'conformed_dimension' — never an LLM claim, ASSIGNED by
        # :meth:`oriented_row` when the measured cardinality refutes a reference
        # claim (DAT-850). A producer-side Literal alone (``semantic/models.py``'s
        # ``RelationshipOutput.relationship_type``) already drifted from this
        # once — the CHECK is the DB-enforced backstop.
        CheckConstraint(
            "relationship_type IN ('foreign_key', 'hierarchy', 'conformed_dimension', 'candidate')",
            name="relationship_type",
        ),
        # Edge-kind × cardinality consistency (DAT-850): a reference claim
        # (foreign_key/hierarchy) requires a unique parent side, so a measured
        # 'many-to-many' REFUTES it — that shape is two facts meeting at a shared
        # axis (a conformed-dimension meeting), and before this CHECK it persisted
        # as a plain FK that cycles/validation then consumed as a genuine
        # reference. :meth:`oriented_row` resolves the kind before persisting;
        # this is the backstop for a writer that bypasses the helper. NULL
        # cardinality passes — an unmeasured claim is unknown, not contradicted.
        CheckConstraint(
            "NOT (relationship_type IN ('foreign_key', 'hierarchy') "
            "AND cardinality = 'many-to-many')",
            name="reference_not_many_to_many",
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
        # Judge-verdict vocabulary (DAT-824): a single-column decline is recorded
        # by ANNOTATING the pair's ``candidate`` row rather than clobbering its
        # measured evidence with the LLM's low confidence. ``NULL`` = the judge
        # did not decline this candidate (never adjudicated, or confirmed → see the
        # sibling ``llm`` row); ``'declined'`` = the judge ruled the pair not a real
        # relationship (reasoning kept in ``evidence['reasoning']``). Only ever set
        # on a ``candidate`` row — a confirm becomes a distinct ``llm`` row, so
        # 'confirmed' is deliberately absent from the vocabulary.
        CheckConstraint(
            "judge_verdict IS NULL OR judge_verdict IN ('declined')",
            name="judge_verdict",
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

    # Classification — the edge KIND (DAT-850): is this a reference
    # (foreign_key/hierarchy) or two facts meeting at a shared axis
    # (conformed_dimension)? ONE home: resolved by :meth:`oriented_row` from the
    # claim × the measured cardinality, backstopped by
    # ck_relationships_relationship_type + ck_relationships_reference_not_many_to_many.
    relationship_type: Mapped[str] = mapped_column(String, nullable=False)
    cardinality: Mapped[str | None] = mapped_column(
        String
    )  # 'one-to-one', 'one-to-many', 'many-to-one', 'many-to-many'

    # Confidence and evidence.
    #
    # ``confidence`` is METHOD-DEPENDENT and NOT a cross-method posterior (DAT-839):
    #   - 'llm'      → the judge's existence confidence (0-1) for the pair.
    #   - 'manual'   → the user's assertion (1.0) or the copied llm confidence.
    #   - 'keeper'   → the retained prior-run llm confidence.
    #   - 'candidate'→ the raw VALUE-OVERLAP statistic max(Jaccard, containment),
    #     the SAME number as ``evidence['join_confidence']`` — a measurement, not a
    #     judgement. An unconfirmed candidate reaches 1.0 while a judge-confirmed FK
    #     sits at ~0.95, so candidate and non-candidate values are NOT comparable.
    # Never rank/filter/gate across methods on this column as if it were a posterior;
    # the entropy adjudicator reads the overlap from ``evidence['join_confidence']``
    # and the judge/user confidence from this column PER METHOD, never mixing them.
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    detection_method: Mapped[str | None] = mapped_column(String)  # 'candidate', 'llm', 'manual'
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    # The judge's single-column decline verdict (DAT-824), annotated onto a
    # ``candidate`` row without disturbing its measured evidence. NULL | 'declined'
    # (see ``ck_relationships_judge_verdict``).
    judge_verdict: Mapped[str | None] = mapped_column(String)

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
        is already correct. That normalisation is safe because it only rewrites
        the writer's OWN measured cardinality label into canonical form — it
        decides nothing.

        **``one-to-one`` is NOT re-oriented here.** A previous revision swapped a
        1:1 when forward containment measured less than reverse. Reduce the
        algebra and that condition is ``|from distinct| > |to distinct|`` — a
        distinct-COUNT comparison, not the containment test its comment claimed.
        It is right only when one value set is a clean subset of the other, and
        it INVERTS a correct child→parent emission whenever the child carries
        orphan values: child {1..5} against parent {1,2,3} measures forward 60 /
        reverse 100 and is swapped, with ``cardinality_verified`` True.
        ``joins.py`` deliberately admits such dirty subset FKs and the judge
        prompt deliberately confirms them, so this module manufactures the very
        inputs that rule breaks on. Containment CANNOT tell "child that is a
        clean subset" from "child with orphans" — the two measure identically —
        so declining to swap is the honest behaviour. Removed on that reasoning
        (DAT-725 review), not kept because this corpus happens to hold only the
        shape it gets right.

        A 1:1's direction therefore rests with the JUDGE, which is told the rule
        in ``semantic_per_table``'s orientation section: decide from DEPENDENCE
        (which row cannot exist without the other), with the measured numbers as
        corroboration. Caveat the removal exposes: the detector writes candidate
        rows through this helper, and those already-oriented rows are what the
        judge is later shown — so its ``from`` side is a presentation the judge
        inherits, not a fact it derived.

        Direction only — the judge's EXISTENCE verdict is never touched here.
        ``many-to-many``/``None`` cannot be oriented. The DB backstop is
        ``ck_relationships_cardinality_oriented``: a mis-oriented ``one-to-many``
        row fails loud at flush even if a future writer bypasses this helper.

        **Edge-kind resolution (DAT-850) — the second job of this chokepoint.**
        A reference claim (``foreign_key``/``hierarchy``) requires a unique
        parent side; a measured ``many-to-many`` refutes it — that shape is two
        facts meeting at a shared axis, so the row is persisted as
        ``conformed_dimension`` with the refuted claim kept in
        ``evidence['resolved_from_type']``. Same philosophy as
        ``_resolve_cardinality`` (processor): the measurement, not the LLM's
        guess, decides — and like the orientation rule it only rewrites the
        writer's OWN measured label into an honest kind. The judge's EXISTENCE
        verdict survives (``confirmation_source`` untouched): the edge is real,
        its kind was mislabelled. ``candidate`` rows pass through — a structural
        candidate is a measurement awaiting a claim, not a claim. DB backstop:
        ``ck_relationships_reference_not_many_to_many``.
        """
        evidence = dict(evidence) if evidence else {}
        if cardinality == "many-to-many" and relationship_type in ("foreign_key", "hierarchy"):
            evidence["resolved_from_type"] = relationship_type
            relationship_type = RelationshipType.CONFORMED_DIMENSION.value
        if cardinality == "one-to-many":
            from_table_id, from_column_id, to_table_id, to_column_id = (
                to_table_id,
                to_column_id,
                from_table_id,
                from_column_id,
            )
            cardinality = "many-to-one"
            evidence = swap_directional_evidence(evidence)
            # A many-to-one child→parent join matches each child to exactly one
            # parent — it never fans out (the one-to-many parent→child join did).
            # ``swap_directional_evidence`` dropped the measured-for-the-old-
            # direction flag; this is the answer for the new one.
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


def swap_directional_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    """Re-express a measurement dict for the OPPOSITE join direction.

    THE single implementation of the flip (DAT-725). Every measurement in the
    dict was taken for one ordered pair; reversing the pair changes what each
    one means, so all three classes of directional key are handled here:

    - **Per-side metrics** — ``left_*``/``right_*`` exchange, because the old TO
      becomes the new FROM. An unpaired prefixed key still moves; the metric
      follows its endpoint.
    - **``cardinality``** — ``one-to-many`` ⇄ ``many-to-one``. It reads as
      from-side→to-side, so it inverts with the pair. (``one-to-one`` and
      ``many-to-many`` are symmetric.) A caller that also stores cardinality in
      a COLUMN must keep the two in step; ``Relationship.oriented_row`` does.
    - **``introduces_duplicates``** — the fan-out answer for the measured
      direction only, and NOT recoverable by flipping it: whether a join
      multiplies rows depends on which side is scanned. Dropped, so a caller
      that knows the new direction's answer sets it explicitly and one that
      doesn't carries no claim rather than a reversed one.

    Anything else must be SYMMETRIC — true of the pair regardless of which side
    is named first — and is passed through. That is checked, not assumed: an
    unrecognised bare key raises. This is why. ``evidence`` is a free JSON
    column with no schema, so "directional metrics are prefixed" is a spelling
    convention; twice a writer added a directional metric without the prefix
    (``orphan_count``, ``join_success_rate``, both literally from-side
    measurements) and this function passed them through unchanged, leaving them
    describing a side they were no longer on. Both shipped — ``RI: L=100%`` next
    to ``orphans=8`` on one stored row — and both were invisible until someone
    ran the flip on real data. A silent pass-through cannot tell "symmetric" from
    "misspelled", so the ambiguity is resolved at the one place that knows: add a
    genuinely symmetric key to :data:`_SYMMETRIC_EVIDENCE_KEYS`, or prefix it.
    """
    swapped: dict[str, Any] = {}
    for key, value in evidence.items():
        if key.startswith("left_"):
            swapped["right_" + key[len("left_") :]] = value
        elif key.startswith("right_"):
            swapped["left_" + key[len("right_") :]] = value
        elif key == "introduces_duplicates":
            continue
        elif key == "cardinality":
            swapped[key] = _CARDINALITY_FLIP.get(value, value)
        elif key in _SYMMETRIC_EVIDENCE_KEYS:
            swapped[key] = value
        else:
            raise ValueError(
                f"evidence key {key!r} is neither left_/right_-prefixed nor declared "
                "symmetric, so flipping the pair cannot know what it now means. "
                "Prefix it if it measures one side, or add it to "
                "_SYMMETRIC_EVIDENCE_KEYS if it is true of the pair either way round."
            )
    return swapped


# Keys that mean the same thing whichever side is named first, so a flip leaves
# them alone. Value-overlap statistics are set-symmetric; the rest are provenance
# about the pair or the judge's own prose about it. Adding one here is a claim
# that reversing the endpoints does not change what it says — check that before
# adding, because the whole point of the raise above is that nobody checked
# twice already.
_SYMMETRIC_EVIDENCE_KEYS = frozenset(
    {
        # Value-overlap statistics — computed over the two value SETS.
        "join_confidence",
        "statistical_confidence",
        "algorithm",
        # Whether the measured cardinality was confirmed against the actual
        # join — a property of the check, not of a side.
        "cardinality_verified",
        # The reference claim a measured many-to-many refuted (DAT-850,
        # ``oriented_row``'s edge-kind resolution) — a fact about the pair's
        # adjudication, true either way round.
        "resolved_from_type",
        # Provenance: which writer produced the row, which run measured it,
        # whether the measurement was copied rather than retaken, which teach
        # action created it (add/keep), and which method donated RI evidence.
        "source",
        "measured_run_id",
        "not_remeasured",
        "action",
        "ri_evidence_source",
        # The judge's prose and its composite-key verdict about the pair.
        "reasoning",
        "composite_key_columns",
    }
)


# ``one-to-one``/``many-to-many`` are symmetric — absent by design, not oversight.
_CARDINALITY_FLIP = {"one-to-many": "many-to-one", "many-to-one": "one-to-many"}


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
