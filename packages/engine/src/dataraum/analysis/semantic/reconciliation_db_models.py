"""SQLAlchemy model for an EVALUATED ``reconciles_with`` assertion (DAT-739).

``concept_edges`` records that two computations of one quantity *must tie out*
(:mod:`dataraum.analysis.semantic.reconciles_with` derives the assertion; three
surfaces render it as "must tie out"). Nothing computed whether it actually
holds. This table is where that computation lands: one row per grounding PAIR
per operating_model run, written by the ``metrics`` phase after the edge set for
the run has settled.

**The honest-tolerance posture is structural, not conventional.** No vertical
declares a reconciliation band today — ``concept_edges.tolerance`` is NULL on
every live row — and an evaluation without a declared tolerance cannot yield a
pass or a fail without inventing the number that decides it. So a tolerance-free
evaluation records the OBSERVED DELTA under the verdict
``no_tolerance_declared``: the tie-out is measured and disclosed, and no
judgement is claimed. ``ck_concept_reconciliation_tolerance_verdict`` binds the
two together in the DATABASE — ``no_tolerance_declared`` is legal only when
``tolerance IS NULL``, and ``within_tolerance``/``beyond_tolerance`` only when a
band was actually declared — so no future writer can emit a pass/fail against a
default epsilon. The moment a seed declares a band, the same executor starts
returning the graded verdicts against it; nothing else changes.

``tolerance`` is COPIED onto the row rather than joined at read time because
``concept_edges`` is not run-versioned: a later edit to the declared band would
otherwise silently re-interpret every verdict already recorded against the old
one.

**A pair is the unit of localization.** The row names both groundings
(``left_snippet_id`` / ``right_snippet_id``, each with its relation and its
bound reporting instant) and both numbers, which localizes a disagreement to a
concrete pair of computations. It does NOT elect a culprit: with two angles and
no third opinion, deciding WHICH side is wrong requires an arbitration rule
nobody declared, and inventing one would be the same mistake as inventing a
tolerance. A third grounding, a declared tolerance, or a human resolves it.

**Absence is a statement.** An asserted edge that cannot be evaluated writes an
ABSTAINED row naming the typed reason — never silence. A concept with no
``reconciles_with`` edge writes nothing at all, which is why a single-grounding
concept is untouched by this table rather than gated by it.

Run-versioned exactly like the verdicts it sits beside: the version axis is the
operating_model ``run_id``, current once that run is promoted under the
``(catalog, "operating_model")`` head (``current_concept_reconciliation``). The
``(vertical, from_concept, to_concept, pair_key, run_id)`` UNIQUE is the
run-grain contract's form-(a) upsert key (ADR-0010).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4

from sqlalchemy import CheckConstraint, DateTime, Float, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class ReconciliationStatus(StrEnum):
    """Did the tie-out get computed at all?"""

    #: Both sides executed and produced comparable numbers; the delta is recorded.
    EVALUATED = "evaluated"
    #: The tie-out was NOT computed; ``abstain_reason`` says what stopped it.
    ABSTAINED = "abstained"


class ReconciliationVerdict(StrEnum):
    """What the observed delta means — only ever set on an EVALUATED row."""

    #: A band was declared and the observed relative delta falls inside it.
    WITHIN_TOLERANCE = "within_tolerance"
    #: A band was declared and the observed relative delta exceeds it.
    BEYOND_TOLERANCE = "beyond_tolerance"
    #: No band is declared for this assertion. The delta is measured and
    #: disclosed; no pass/fail is claimed, because none can be without the
    #: number that decides it. This is the normal outcome today.
    NO_TOLERANCE_DECLARED = "no_tolerance_declared"


class ReconciliationAbstainReason(StrEnum):
    """Why a tie-out was not computed — every abstention is one of these."""

    #: Fewer than two groundings to pair. The witness producer asserts a concept
    #: reconciles because a measure's rollup tied out against its events, and the
    #: event side is not a grounding (nothing mints a snippet for it), so that
    #: assertion has no second SQL angle to execute here.
    NO_EVALUABLE_PAIR = "no_evaluable_pair"
    #: The two groundings are bound to DIFFERENT reporting instants (DAT-887).
    #: A stock read at two different moments legitimately differs, so the
    #: difference measures the calendar, not a disagreement. Deliberately not
    #: executed: the only use for the two numbers would be the subtraction this
    #: reason exists to refuse.
    DIFFERENT_REPORTING_INSTANTS = "different_reporting_instants"
    #: The two groundings aggregate differently (a SUM against a COUNT). They
    #: compute different quantities by construction, so there is nothing to tie
    #: out. Differing row RESTRICTIONS are not this case — reaching one quantity
    #: through two different populations is the whole point of a second angle.
    DIFFERENT_AGGREGATIONS = "different_aggregations"
    #: A grounding carries no usable clause parts, so it has no executable form.
    UNRESOLVED_GROUNDING = "unresolved_grounding"
    #: A side failed to execute. The stored statement is re-executed verbatim and
    #: never repaired, so a failure is reported, not worked around.
    EXECUTION_FAILED = "execution_failed"
    #: A side executed and returned NULL — no measured support. There is no
    #: number to compare, and a NULL is never read as a zero.
    NO_VALUE = "no_value"
    #: A side returned a value that is not a quantity (a VARCHAR, a date, a
    #: boolean). Distinct from NO_VALUE: the grounding measured SOMETHING, and
    #: what it measured cannot be reconciled — a louder fact than missing
    #: support, and one that points at the grounding rather than at the data.
    NON_NUMERIC_VALUE = "non_numeric_value"


#: ``pair_key`` value for an assertion-level row that identifies NO pair — the
#: shape an abstention takes when there was nothing to pair in the first place.
#: Mirrors ``metric_axis_additivity``'s ``AXIS_KEY_ALL`` sentinel convention.
PAIR_KEY_UNPAIRED = "*"

_STATUS_VALUES = tuple(sorted(v.value for v in ReconciliationStatus))
_VERDICT_VALUES = tuple(sorted(v.value for v in ReconciliationVerdict))
_ABSTAIN_REASON_VALUES = tuple(sorted(v.value for v in ReconciliationAbstainReason))


def _in_list(column: str, values: tuple[str, ...], *, nullable: bool) -> str:
    """Render a closed-vocabulary CHECK from the one tuple that defines it."""
    rendered = ", ".join(f"'{v}'" for v in values)
    prefix = f"{column} IS NULL OR " if nullable else ""
    return f"{prefix}{column} IN ({rendered})"


class ConceptReconciliation(Base):
    """One evaluated ``reconciles_with`` pair for one operating_model run.

    ``from_concept`` / ``to_concept`` mirror the edge's endpoints and carry its
    grain: equal names are the SELF-LOOP (a concept's own groundings tied out
    against each other, the only shape any live producer writes today), distinct
    names are the declared partner assertion (each side's groundings against the
    other's). Endpoints are concept NAMES within ``vertical`` — the stable key
    ``concept_edges`` itself uses, never ``concept_id``.

    ``pair_key`` is the canonical ``"<snippet>|<snippet>"`` identity of the two
    groundings compared, ordered so the SYMMETRIC assertion produces one stable
    key per pair regardless of which side is read first; ``'*'``
    (:data:`PAIR_KEY_UNPAIRED`) is the assertion-level row written when no pair
    could be formed.
    """

    __tablename__ = "concept_reconciliation"
    __table_args__ = (
        UniqueConstraint(
            "vertical",
            "from_concept",
            "to_concept",
            "pair_key",
            "run_id",
            name="uq_concept_reconciliation_pair",
        ),
        CheckConstraint(_in_list("status", _STATUS_VALUES, nullable=False), name="status"),
        CheckConstraint(_in_list("verdict", _VERDICT_VALUES, nullable=True), name="verdict"),
        CheckConstraint(
            _in_list("abstain_reason", _ABSTAIN_REASON_VALUES, nullable=True),
            name="abstain_reason",
        ),
        # An evaluated row carries the full measurement; an abstained row carries
        # a reason and NO delta. Nothing in between: a half-filled row would read
        # as a tie-out nobody computed.
        CheckConstraint(
            "(status = 'evaluated' AND verdict IS NOT NULL AND abstain_reason IS NULL"
            " AND left_value IS NOT NULL AND right_value IS NOT NULL"
            " AND delta IS NOT NULL AND relative_delta IS NOT NULL)"
            " OR (status = 'abstained' AND verdict IS NULL AND abstain_reason IS NOT NULL"
            " AND delta IS NULL AND relative_delta IS NULL)",
            name="status_verdict_reason",
        ),
        # The honest-tolerance posture, enforced by the database: a graded
        # verdict REQUIRES a declared band, and a tolerance-free evaluation can
        # only ever be the disclosure. No writer can pass/fail against a default.
        CheckConstraint(
            "verdict IS NULL"
            " OR (verdict = 'no_tolerance_declared' AND tolerance IS NULL)"
            " OR (verdict <> 'no_tolerance_declared' AND tolerance IS NOT NULL)",
            name="tolerance_verdict",
        ),
        # A pair row names both of its groundings; the unpaired sentinel names
        # neither. The snippet ids are what make a disagreement localizable.
        CheckConstraint(
            "(pair_key = '*' AND left_snippet_id IS NULL AND right_snippet_id IS NULL)"
            " OR (pair_key <> '*' AND left_snippet_id IS NOT NULL"
            " AND right_snippet_id IS NOT NULL)",
            name="pair_key_snippets",
        ),
    )

    reconciliation_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    #: Snapshot version axis: the operating_model run that evaluated this.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    #: The edge's scope — concept names resolve within one vertical.
    vertical: Mapped[str] = mapped_column(String, nullable=False, index=True)
    from_concept: Mapped[str] = mapped_column(String, nullable=False, index=True)
    to_concept: Mapped[str] = mapped_column(String, nullable=False)
    #: Canonical pair identity, or ``'*'`` when no pair could be formed.
    pair_key: Mapped[str] = mapped_column(String, nullable=False)

    #: The two groundings compared, by ``sql_snippets.snippet_id``. NULL only on
    #: the unpaired sentinel row.
    left_snippet_id: Mapped[str | None] = mapped_column(String)
    right_snippet_id: Mapped[str | None] = mapped_column(String)
    #: The relation each side reads — carried so a disagreement is legible
    #: without resolving the snippet (the GL-side / subledger-side distinction).
    left_relation: Mapped[str | None] = mapped_column(String)
    right_relation: Mapped[str | None] = mapped_column(String)
    #: The reporting instant each side was bound to (DAT-887), when it is a
    #: point-in-time extract. Present on a DIFFERENT_REPORTING_INSTANTS
    #: abstention precisely so the row says WHY it refused to subtract.
    left_as_of: Mapped[str | None] = mapped_column(String)
    right_as_of: Mapped[str | None] = mapped_column(String)

    #: The two executed values. NULL on every abstained row — never zero, which
    #: would assert a measurement nobody made.
    left_value: Mapped[Decimal | None] = mapped_column(Numeric)
    right_value: Mapped[Decimal | None] = mapped_column(Numeric)
    #: ``left_value - right_value``, signed. Kept signed because the direction of
    #: a break is diagnostic (which side is short).
    delta: Mapped[Decimal | None] = mapped_column(Numeric)
    #: ``|delta| / max(|left|, |right|)`` — the quantity a declared tolerance
    #: grades, since a band is declared as a fraction. Defined as 0 when both
    #: sides are 0 (equal values never read as a break), and otherwise always
    #: well-defined because the denominator can only vanish when both are 0.
    relative_delta: Mapped[Decimal | None] = mapped_column(Numeric)
    #: The band this row was graded against, copied from the edge at evaluation
    #: time. NULL means none was declared — see the module note.
    tolerance: Mapped[float | None] = mapped_column(Float)

    status: Mapped[str] = mapped_column(String, nullable=False)
    verdict: Mapped[str | None] = mapped_column(String)
    abstain_reason: Mapped[str | None] = mapped_column(String)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = [
    "PAIR_KEY_UNPAIRED",
    "ConceptReconciliation",
    "ReconciliationAbstainReason",
    "ReconciliationStatus",
    "ReconciliationVerdict",
]
