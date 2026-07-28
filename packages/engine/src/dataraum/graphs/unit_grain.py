"""Compose a metric at UNIT GRAIN, gated by the served additivity verdict (DAT-671 B1).

A metric is a workspace scalar: one number for the whole business. The question a
practitioner actually asks next — *which vendors, which accounts, which customers* —
is one level down from that number, and until now the engine could not express it:
the composers hard-coded a single-row scalar, and ``OutputType.SERIES``/``TABLE``
existed as metadata nothing branched on.

**The grain is a composition, not a second grounding.** The same persisted clause
parts render either way (:func:`~dataraum.graphs.formula_composer.compose_extract_sql`
takes ``group_by``), over the same relation and the same predicates — the validity
scope, the declared row restriction (DAT-838), and any bound reporting instant
(DAT-887) all apply unchanged. The unit-grain rows are therefore a PARTITION of the
very rows the scalar aggregates, which is what lets the parts sum back to it.

**Why the period binding is not re-resolved per entity.** A point-in-time extract is
bound to ONE reporting instant, resolved for the relation from its coverage and the
workspace calendar — nothing in that resolution mentions an entity. Binding each
entity to its own latest period instead would mix instants inside a single column of
numbers, and summing levels across instants is exactly what the TIME verdict
(``semi_additive``, reason ``stock``) refuses. It would also falsify the served
CATEGORICAL verdict, which for a summed stock is ``additive`` — a balance reconciles
across accounts precisely because they are all read AT THE SAME MOMENT. So the
binding resolves once, per relation, and an entity carrying no row at that instant is
ABSENT from the breakdown rather than zero: the data records no level for it.

**The gate is the served verdict — there is no judge here.** Whether a breakdown may
be offered is decided by ``metric_axis_additivity``, computed by W3-a's classifier and
persisted per (target × axis) by the metrics phase. This module READS that row and
maps it; it never re-derives additivity, never inspects a name, and never treats a
missing row as permission. Both local additivity judges were deleted deliberately and
re-introducing one here would silently fork the doctrine.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.graphs.additivity import (
    AbstainReason,
    AdditivityStatus,
    AxisAdditivity,
    AxisKind,
    AxisVerdict,
)
from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, MetricAxisAdditivity

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

#: The slice kinds that can name an ENTITY. A unit-grain breakdown groups by a
#: categorical axis; a numeric/banded axis (DAT-280) is a different feature with its
#: own edges, and is not silently treated as an entity here.
_ENTITY_SLICE_TYPE = "categorical"


@dataclass(frozen=True)
class UnitGrainDecision:
    """Whether a target may be composed at unit grain on one axis, and how.

    ``offered`` False always carries a ``reason``: a breakdown that cannot be gated
    is withheld out loud, never degraded to a silent workspace scalar (a consumer
    would have no way to tell the two apart).

    ``reconciles`` says whether the parts sum to the workspace total. False is a
    perfectly usable breakdown — it is the honest dash on the total row, exactly as
    the drill already renders for a non-reconciling time bucketing — and it is why
    this is not folded into ``offered``.

    ``recompute`` marks the family where the per-entity value must be RECOMPUTED
    from its carriers rather than summed (a ratio, an average, a distinct count):
    per-bucket meaningful, total not.
    """

    axis: str
    offered: bool
    reconciles: bool = False
    recompute: bool = False
    reason: str | None = None

    def __post_init__(self) -> None:
        if not self.offered and not self.reason:
            raise ValueError("a withheld unit grain must name why")
        if not self.offered and (self.reconciles or self.recompute):
            raise ValueError("a withheld unit grain claims nothing about its parts")


def read_categorical_verdict(
    session: Session,
    *,
    target_kind: str,
    target_key: str,
    run_id: str,
) -> AxisAdditivity | None:
    """The served CLASS-level categorical verdict for one drill target, or None.

    Reads the row W3-a's resolver produced and the metrics phase persisted for THIS
    run — the same ``(target_kind, target_key, axis_kind, axis_key, run_id)`` grain
    the cockpit resolves against, so engine and cockpit gate on one fact rather than
    two opinions. Only the class row (``axis_key='*'``) exists for the categorical
    axis today; the resolver refines concrete rows for TIME axes only.

    Returned as :class:`~dataraum.graphs.additivity.AxisAdditivity` — W3-a's own
    verdict vocabulary — rather than the ORM row, so the decision below stays a pure
    function of the served fact and the status/verdict/reason pairing is re-asserted
    on the way out of the database (the table's CHECK constraint states the same
    rule, so a row that fails it here is a corrupted row, and raising beats gating
    on it).

    ``None`` means no verdict was persisted at all. That is NOT permission — see
    :func:`gate_unit_grain`.
    """
    row = session.execute(
        select(MetricAxisAdditivity).where(
            MetricAxisAdditivity.run_id == run_id,
            MetricAxisAdditivity.target_kind == target_kind,
            MetricAxisAdditivity.target_key == target_key,
            MetricAxisAdditivity.axis_kind == AxisKind.CATEGORICAL.value,
            MetricAxisAdditivity.axis_key == AXIS_KEY_ALL,
        )
    ).scalar_one_or_none()
    return None if row is None else served_verdict(row)


def served_verdict(row: MetricAxisAdditivity) -> AxisAdditivity:
    """One persisted verdict row as W3-a's typed verdict."""
    return AxisAdditivity(
        status=AdditivityStatus(row.status),
        verdict=AxisVerdict(row.verdict) if row.verdict else None,
        reason=row.reason,
        abstain_reason=AbstainReason(row.abstain_reason) if row.abstain_reason else None,
    )


def gate_unit_grain(
    axis: str,
    verdict: AxisAdditivity | None,
    carrier_verdicts: dict[str, AxisAdditivity | None] | None = None,
) -> UnitGrainDecision:
    """Map the served verdict onto a unit-grain decision. Total by enumeration.

    The four ``AxisVerdict`` states plus abstention plus no-row are each answered
    explicitly, so a state this function does not know about cannot fall through
    into an accidental offer:

    * **additive** — offer; the parts sum to the total.
    * **non_additive_recompute** — offer, RECOMPUTED per entity from the carriers,
      total does not reconcile. Conditional on every carrier being additive on this
      axis: recomputing a ratio per entity is only honest if its numerator and
      denominator each partition cleanly, which is the same condition the drill's
      time gate applies before offering a per-bucket recompute.
    * **semi_additive** — offer, total does not reconcile. Unreachable from today's
      classifier on a CATEGORICAL axis (both semi-additive reasons, ``stock`` and
      ``snapshot_count``, set ``categorical_additive`` True), but answered rather
      than left to chance.
    * **abstained** — withhold, naming the typed abstain reason.
    * **no row** — withhold. "Not judged" is not "yes".
    """
    if verdict is None:
        return UnitGrainDecision(
            axis=axis,
            offered=False,
            reason=(
                f"no additivity verdict was recorded for this target, so whether it "
                f"breaks down by {axis!r} is unknown — withheld rather than guessed"
            ),
        )
    if verdict.status is AdditivityStatus.ABSTAINED:
        return UnitGrainDecision(
            axis=axis,
            offered=False,
            reason=(
                f"additivity across categories could not be determined "
                f"({verdict.abstain_reason.value if verdict.abstain_reason else '?'}), "
                f"so a per-{axis} breakdown is withheld"
            ),
        )
    if verdict.verdict is AxisVerdict.ADDITIVE:
        return UnitGrainDecision(axis=axis, offered=True, reconciles=True)
    if verdict.verdict is AxisVerdict.NON_ADDITIVE_RECOMPUTE:
        unjudged = sorted(k for k, v in (carrier_verdicts or {}).items() if v is None)
        if unjudged:
            return UnitGrainDecision(
                axis=axis,
                offered=False,
                reason=(
                    f"the value must be recomputed per {axis} from its carriers "
                    f"({verdict.reason}), but {unjudged} carry no additivity verdict — "
                    "recomputing from carriers that may not partition cleanly would "
                    "produce a number with no defensible meaning"
                ),
            )
        not_additive = sorted(
            k
            for k, v in (carrier_verdicts or {}).items()
            if v is not None and v.verdict is not AxisVerdict.ADDITIVE
        )
        if not_additive:
            return UnitGrainDecision(
                axis=axis,
                offered=False,
                reason=(
                    f"the value must be recomputed per {axis} from its carriers "
                    f"({verdict.reason}), but {not_additive} do not themselves sum "
                    "across categories, so the recomputed parts would not mean what "
                    "the total means"
                ),
            )
        return UnitGrainDecision(
            axis=axis,
            offered=True,
            reconciles=False,
            recompute=True,
            reason=f"recomputed per {axis} from its carriers ({verdict.reason}); "
            "the parts do not sum to the total",
        )
    if verdict.verdict is AxisVerdict.SEMI_ADDITIVE:
        return UnitGrainDecision(
            axis=axis,
            offered=True,
            reconciles=False,
            reason=f"per-{axis} values are meaningful but do not sum to the total "
            f"({verdict.reason})",
        )
    return UnitGrainDecision(
        axis=axis,
        offered=False,
        reason=(
            f"unrecognised additivity verdict {verdict.verdict!r} — withheld rather "
            "than interpreted"
        ),
    )


def resolve_entity_axes(session: Session, *, table_id: str, run_id: str) -> list[str]:
    """The served categorical axes of one relation, most interesting first.

    Read from the slice catalog the slicing phase curates — the workspace's own
    statement about which columns are worth grouping by. The entity axis is
    therefore a SERVED fact, never a column picked because its name looks like an
    id: no name is inspected anywhere in this module.

    Ordered by the catalog's own two signals, judgment before measurement:
    ``slice_interest`` ('primary' ahead of 'supporting'), then measured
    ``slice_relevance``. An unmeasured relevance sorts last rather than as zero —
    NULL means unmeasured, which is not a claim that the axis resolves nothing.
    """
    rows = session.execute(
        select(
            SliceDefinition.column_name,
            SliceDefinition.slice_interest,
            SliceDefinition.slice_relevance,
        ).where(
            SliceDefinition.run_id == run_id,
            SliceDefinition.table_id == table_id,
            SliceDefinition.slice_type == _ENTITY_SLICE_TYPE,
            SliceDefinition.column_name.isnot(None),
        )
    ).all()
    ordered = sorted(
        rows,
        key=lambda r: (
            0 if r.slice_interest == "primary" else 1 if r.slice_interest else 2,
            -(r.slice_relevance if r.slice_relevance is not None else -1.0),
        ),
    )
    seen: set[str] = set()
    axes: list[str] = []
    for row in ordered:
        name = str(row.column_name)
        if name not in seen:
            seen.add(name)
            axes.append(name)
    return axes
