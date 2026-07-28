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

from dataraum.analysis.slicing.curation import curated_slices
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
    from decimal import Decimal

    from sqlalchemy.orm import Session

    from dataraum.graphs.models import TransformationGraph

#: The slice kind that can name an ENTITY. ``categorical`` is the only value the
#: catalog's CHECK constraint currently admits, so this filter excludes nothing
#: today — it is future-proofing with a stated intent: when a numeric/banded axis
#: (DAT-280) is added to that vocabulary, a breakdown must not start grouping by
#: it silently. Banding is a different feature with its own edges.
_ENTITY_SLICE_TYPE = "categorical"


# Cost bound on ONE target's breakdown — not a judgment about which entities
# matter, and not a relevance cut. The slice pre-filter admits a categorical
# column up to a near-unique fraction, so a fact of a few million rows can carry
# hundreds of thousands of distinct entities; composed unbounded, one metric
# would write that many rows per run and load them all into Python first. The
# bound applies with an ORDER BY and a DISCLOSED truncation (never a silent cut),
# which is what makes it a cost decision rather than a claim that the tail is
# uninteresting. The n=1 finance corpus has never come close to it — that is
# exactly why the bound must exist before a wider one does.
UNIT_GRAIN_MAX_ENTITIES = 1000


@dataclass(frozen=True)
class UnitGrainRow:
    """One entity's composed value — the shape the phase persists.

    ``value`` is ``None`` when the composition produced NULL for this entity:
    "not computable here" (the FULL OUTER case), never zero. An entity the
    relation carries no row for is not in this list at all — a different
    statement, and the honest one for it.
    """

    entity_value: str
    value: Decimal | None


@dataclass(frozen=True)
class UnitGrainBreakdown:
    """One target's composed breakdown, and whether it is the WHOLE breakdown.

    ``truncated_at`` is ``None`` for a complete partition. When it is set, the
    rows are the first N by entity value out of ``total_entities`` — an ordered,
    reproducible prefix, so re-running names the same entities — and the caller
    MUST disclose it. Rows that are silently a subset would read as the whole
    partition and would not sum to the total the verdict says they should.
    """

    axis: str
    rows: tuple[UnitGrainRow, ...]
    total_entities: int
    truncated_at: int | None = None


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


@dataclass(frozen=True)
class EntityAxes:
    """The served categorical axes of a read, plus what the read left unsaid.

    ``note`` is non-empty when the catalog read has something a consumer must be
    told before trusting the ordering — today, that nothing in it was ever
    JUDGED. It is carried into the withheld reason rather than dropped, because
    presenting an unjudged ordering as a curated one is the worse failure
    (:mod:`dataraum.analysis.slicing.curation` makes the same call).
    """

    axes: tuple[str, ...] = ()
    note: str = ""


def resolve_entity_axes(session: Session, *, table_id: str, run_id: str) -> EntityAxes:
    """The served, JUDGED categorical axes of one relation, most interesting first.

    Read from the slice catalog the slicing phase curates — the workspace's own
    statement about which columns are worth grouping by. The entity axis is
    therefore a SERVED fact, never a column picked because its name looks like an
    id: no name is inspected anywhere in this module.

    Ordering and judged-ness both come from
    :func:`~dataraum.analysis.slicing.curation.curated_slices`, the catalog's own
    curation — not a private ranking here. A third hand-rolled copy of "primary
    before supporting, then measured relevance" would drift from the two that
    already agree, and this one also lacked the NAME tiebreak: on a relevance tie
    the pick was whatever order the rows came back in, so the axis a breakdown was
    persisted against could flip between runs on identical data.

    When NO row carries a judgment the axes are WITHHELD, not served unranked.
    Picking "the most interesting axis" out of a set nothing assessed would
    present a structural ordering as a curated one; the note says so instead.

    The axes are the FACT's own slices, so a breakdown groups by what the fact
    carries — an ``account_id``, not the dimension row's ``name``. Enriched
    ``{fk}__{attr}`` labels are catalogued against the fact too and are reachable
    the same way; a dim-side attribute that the fact's slice inventory does not
    carry is not, so an id-keyed breakdown is the shape a consumer should expect
    to render (and to join a label onto itself).
    """
    rows = list(
        session.execute(
            select(SliceDefinition)
            .where(
                SliceDefinition.run_id == run_id,
                SliceDefinition.table_id == table_id,
                SliceDefinition.slice_type == _ENTITY_SLICE_TYPE,
                SliceDefinition.column_name.isnot(None),
            )
            # A deterministic read BEFORE the sort: `curated_slices` sorts stably,
            # so rows that tie on its whole key keep the order they arrived in —
            # which without this is the database's discretion (the `.limit(1)`
            # no-ORDER-BY class, one layer up).
            .order_by(SliceDefinition.column_name, SliceDefinition.slice_id)
        )
        .scalars()
        .all()
    )
    curated = curated_slices(rows)
    if curated.unjudged_fallback:
        return EntityAxes(note=curated.note)

    seen: set[str] = set()
    axes: list[str] = []
    for row in curated.served:
        name = str(row.column_name)
        if name not in seen:
            seen.add(name)
            axes.append(name)
    return EntityAxes(axes=tuple(axes))


def resolve_metric_entity_axes(
    session: Session, *, graph: TransformationGraph, workspace_id: str, run_id: str
) -> EntityAxes:
    """The categorical axes EVERY grounded carrier of a metric can be grouped by.

    An INTERSECTION, mirroring :func:`~dataraum.graphs.additivity_resolver._common_time_axes`
    for the categorical case: a metric can only be broken down on a column all of
    its carriers carry, or the FULL OUTER join has nothing to join one carrier on.
    Ordered by the first carrier's served ranking (its own judgment-then-measurement
    order), so the choice of axis is the workspace's, not this function's.

    Empty when the metric has no EXTRACT leaves, when any leaf has no healthy
    grounded snippet, when a leaf's relation is outside the current analysis, or
    when the carriers simply share no curated categorical slice. All four are the
    same answer for the caller — there is no per-entity breakdown to offer — and
    the caller discloses that rather than composing something narrower.

    The snippet is looked up on the FULL semantic key including the declared
    ``predicate`` (DAT-838): two extracts that restrict to different rows are
    different measurements, and resolving one to the other's relation would name
    the wrong axes.
    """
    from dataraum.graphs.additivity_resolver import served_relation
    from dataraum.graphs.models import StepType
    from dataraum.query.snippet_library import SnippetLibrary

    library = SnippetLibrary(session)
    shared: list[str] | None = None
    notes: dict[str, None] = {}
    # sorted(): the first carrier fixes the ORDER of the result, so iteration order
    # is part of the output — a dict's insertion order is definition-dependent and
    # a set's is PYTHONHASHSEED-salted.
    for _step_id, step in sorted(graph.steps.items()):
        if step.step_type != StepType.EXTRACT or step.source is None:
            continue
        match = library.find_by_key(
            "extract",
            workspace_id,
            standard_field=step.source.standard_field,
            statement=step.source.statement,
            aggregation=step.aggregation,
            predicate=step.source.predicate,
        )
        if match is None:
            return EntityAxes()
        relations = (match.snippet.parts or {}).get("from") or []
        if not relations:
            return EntityAxes()
        served = served_relation(session, str(relations[0]))
        if served is None:
            return EntityAxes()
        # SliceDefinition rows are keyed on the FACT table (the slicing phase reads
        # the enriched view's columns but writes them against the fact it derives
        # from), while the grounded relation IS the enriched view — a superset of
        # the fact's columns, so every served slice name resolves on it.
        carrier = resolve_entity_axes(session, table_id=served.fact_table_id, run_id=run_id)
        if carrier.note:
            notes[carrier.note] = None  # dict: de-duplicated, order preserved
        if shared is None:
            shared = list(carrier.axes)
        else:
            carried = set(carrier.axes)
            shared = [a for a in shared if a in carried]
        if not shared:
            break
    return EntityAxes(axes=tuple(shared or ()), note=" ".join(notes))
