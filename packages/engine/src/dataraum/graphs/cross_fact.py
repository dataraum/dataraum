"""Resolve the CONFORMED axis two or more facts can be drilled across (DAT-809).

``enriched_views`` is unique per fact by design, so a cross-fact question never
joins views. It composes one subquery per grounding and merges them on a shared
**conformed dimension** — the Kimball drill-across. This module decides whether such
an axis exists and, if so, which LOCAL column realizes it on each carrier's own fact.

**Identity, not names.** The merge key is ``bus_matrix.conformed_group``: the
identity the dimension-hierarchies phase asserted. It is emphatically NOT a column
name match. The pre-existing categorical intersection
(:func:`~dataraum.graphs.unit_grain.resolve_metric_entity_axes`) compares raw column
names across carriers, which is wrong in both directions — two facts both carrying a
``region`` column are not thereby the same region, and one carrying ``account_id``
against another's ``acct`` *is* one axis once the conform judge said so. Names cannot
answer this question; the served group can. ``concept_label`` is display-only for the
same reason (DAT-800): keying on it would split a group whose verdicts drifted labels
and merge two distinct groups that happen to share a generic one.

**Only CONFIRMED conformance may become a join.** A cross-fact merge materially
changes the numbers a practitioner reads, so the bar here is deliberately higher than
:func:`~dataraum.analysis.lineage.processor._shared_dimension_groups`, which answers a
different question (how to GROUP already-computed lineage) and therefore accepts a
structural fallback for a slice whose bus-matrix cell is absent. That fallback is
exactly the silent-join risk here: same-named FK roles conform *structurally*, with no
one having confirmed the underlying relationship. So this module requires, on EVERY
participating fact, a cell that (a) carries a ``conformed_group``, (b) names a
confirmation source other than ``unconfirmed``, and (c) is not flagged
``needs_confirmation``. Anything short of that is a TYPED abstention naming the reason,
never a quieter merge — a false refusal is loud and recoverable, a false join is
neither.

The abstention shape is the DAT-859 template (status enum + closed reason enum + a
frozen dataclass whose ``__post_init__`` enforces the pairing), matching
:class:`~dataraum.graphs.additivity.AxisAdditivity` and
:class:`~dataraum.graphs.unit_grain.UnitGrainDecision`.

This module resolves the AXIS only. Whether the metric may be *rolled up* along it is
the served additivity verdict's business, read through
:func:`~dataraum.graphs.unit_grain.gate_unit_grain` — there is no judge here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import select

from dataraum.analysis.hierarchies.db_models import BusMatrixEntry
from dataraum.analysis.slicing.curation import curated_slices
from dataraum.analysis.slicing.db_models import SliceDefinition

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

    from dataraum.graphs.models import TransformationGraph

logger = structlog.get_logger(__name__)

#: Confirmation sources that may back a cross-fact JOIN. ``unconfirmed`` is excluded:
#: it means nothing asserted the underlying structure, and a detected-but-unconfirmed
#: FK is not a licence to merge two facts' numbers.
CONFIRMED_SOURCES = frozenset({"judge", "keeper", "user"})


class CrossFactStatus(Enum):
    """Whether a conformed drill-across axis was resolved."""

    RESOLVED = "resolved"
    ABSTAINED = "abstained"


class CrossFactAbstain(Enum):
    """Why no cross-fact axis is offered. Closed vocabulary — each is disclosable."""

    #: Every carrier grounds on one fact: not a drill-across (unit grain covers it).
    SINGLE_FACT = "single_fact"
    #: A carrier has no healthy grounded snippet, so its fact is unknown.
    UNRESOLVED_GROUNDING = "unresolved_grounding"
    #: A carrier's relation is not an enriched view of this analysis.
    RELATION_OUTSIDE_ANALYSIS = "relation_outside_analysis"
    #: No conformed group is present on all carriers' facts.
    NO_CONFORMED_DIMENSION = "no_conformed_dimension"
    #: A shared group exists but its conformance is unconfirmed or awaiting review.
    UNCONFIRMED_CONFORMANCE = "unconfirmed_conformance"
    #: The conformed axis names a column the fact's slice catalogue never curated.
    AXIS_UNSLICED = "axis_unsliced"


@dataclass(frozen=True)
class CrossFactAxis:
    """One conformed dimension, and how each fact spells it.

    ``identity`` is the ``conformed_group`` — also used verbatim as the SQL output
    alias, because it is globally unique per group and cannot drift the way a label
    can. ``label`` is the human-readable concept for display and carries no decision.
    """

    identity: str
    label: str
    #: fact_table_id -> the local column on that fact realizing this axis.
    columns: Mapping[str, str]
    #: EXTRACT step_id -> the fact_table_id its grounding reads.
    steps: Mapping[str, str]

    @property
    def facts(self) -> tuple[str, ...]:
        return tuple(sorted(self.columns))

    def step_grain(self) -> dict[str, tuple[tuple[str, str], ...]]:
        """Per-EXTRACT-step ``(local_column, alias)`` keys for the composer.

        Every carrier groups by its OWN column and projects it under the shared
        identity, which is what the formula's FULL OUTER merge then joins on.
        """
        return {
            step_id: ((self.columns[fact], self.identity),)
            for step_id, fact in self.steps.items()
            if fact in self.columns
        }


@dataclass(frozen=True)
class CrossFactDecision:
    """A resolved axis or a typed refusal — never a quieter in-between."""

    status: CrossFactStatus
    axis: CrossFactAxis | None = None
    reason: str | None = None
    abstain_reason: CrossFactAbstain | None = None

    def __post_init__(self) -> None:
        if self.status is CrossFactStatus.RESOLVED:
            if self.axis is None:
                raise ValueError("a resolved cross-fact decision requires an axis")
            if self.abstain_reason is not None:
                raise ValueError("a resolved cross-fact decision must not carry an abstain reason")
        else:
            if self.axis is not None:
                raise ValueError("an abstained cross-fact decision must not carry an axis")
            if self.abstain_reason is None:
                raise ValueError("an abstained cross-fact decision requires a typed reason")
            if not self.reason:
                raise ValueError("an abstained cross-fact decision must name why")


def _abstain(reason: CrossFactAbstain, message: str) -> CrossFactDecision:
    return CrossFactDecision(
        status=CrossFactStatus.ABSTAINED, reason=message, abstain_reason=reason
    )


def carrier_facts(
    session: Session, *, graph: TransformationGraph, workspace_id: str
) -> dict[str, str] | CrossFactDecision:
    """Map each EXTRACT step to the FACT its grounded relation reads.

    The snippet lookup carries the FULL semantic key including the declared predicate
    (DAT-838): two extracts restricting to different rows are different measurements,
    and resolving one to the other's relation would name the wrong fact.
    """
    from dataraum.graphs.additivity_resolver import served_relation
    from dataraum.graphs.models import StepType
    from dataraum.query.snippet_library import SnippetLibrary

    library = SnippetLibrary(session)
    steps: dict[str, str] = {}
    for step_id, step in sorted(graph.steps.items()):
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
            return _abstain(
                CrossFactAbstain.UNRESOLVED_GROUNDING,
                f"carrier {step_id!r} has no healthy grounded snippet, so the fact it "
                "reads is unknown",
            )
        relations = (match.snippet.parts or {}).get("from") or []
        if not relations:
            return _abstain(
                CrossFactAbstain.UNRESOLVED_GROUNDING,
                f"carrier {step_id!r} grounded without a relation (a fall-loud extract)",
            )
        served = served_relation(session, str(relations[0]))
        if served is None:
            return _abstain(
                CrossFactAbstain.RELATION_OUTSIDE_ANALYSIS,
                f"carrier {step_id!r} reads {relations[0]!r}, which is not an enriched "
                "view of this analysis",
            )
        steps[step_id] = served.fact_table_id
    return steps


@dataclass(frozen=True)
class AxisCandidate:
    """One legal merge axis and how each fact spells it."""

    label: str
    #: fact_table_id -> the local column on that fact realizing this axis.
    columns: Mapping[str, str]
    #: The WORKSPACE's own opinion of how interesting this axis is — the best
    #: (lowest) position its column holds in any carrier's curated slice order.
    rank: int


@dataclass(frozen=True)
class AxisSurvey:
    """Every conformed group, sorted into the one bucket that explains its fate.

    ``unsliced`` exists so a confirmed group whose key column the slice catalogue
    never curated can be reported as SUCH. Folding it into "no conformed dimension"
    names the wrong problem and sends someone to confirm a pairing that is already
    confirmed, when the actual fix is to curate the column.
    """

    qualified: dict[str, AxisCandidate]
    #: group -> (fact_table_id, the role/column that resolved to no curated slice)
    unsliced: dict[str, tuple[str, str]]


def _curated_positions(slices: Sequence[SliceDefinition]) -> dict[tuple[str, str], int]:
    """``(fact, column) -> rank`` in the catalog's OWN curated order.

    Delegated to :func:`~dataraum.analysis.slicing.curation.curated_slices` rather
    than re-derived: a third hand-rolled "primary before supporting, then measured
    relevance" would drift from the two that already agree, and this module has no
    business re-ranking what the slicing phase curated. Its documented unjudged
    fallback applies unchanged — and cannot admit an illegal axis, because ranking
    only ORDERS axes the confirmed-conformance gate already accepted.
    """
    by_table: dict[str, list[SliceDefinition]] = {}
    for sd in slices:
        by_table.setdefault(sd.table_id, []).append(sd)
    positions: dict[tuple[str, str], int] = {}
    for table_id, rows in by_table.items():
        for index, row in enumerate(curated_slices(rows).served):
            positions.setdefault((table_id, str(row.column_name)), index)
    return positions


def survey_conformed_axes(
    cells: Sequence[BusMatrixEntry], slices: Sequence[SliceDefinition], facts: set[str]
) -> AxisSurvey:
    """Sort the conformed groups over ``facts`` into qualified vs unsliced.

    Pure, so the gate is testable without a database. Both bus-matrix legs resolve to
    the fact's own physical grouping column:

    - a REFERENCED cell names FK ``roles``; the fact's column is the slice curated for
      that role at the KEY itself (``dimension_attribute IS NULL``), not a dim-side
      attribute the fact does not carry;
    - a FOLDED cell's single ``roles[0]`` IS the fact's grouping column, whose own
      folded slice is the same lens object.

    A group surviving on fewer than every fact is dropped — a merge that silently
    covered only some carriers would answer a narrower question than was asked.
    """
    # (table, fk_role) -> key columns, and (table, column) -> the folded lens exists.
    by_role: dict[tuple[str, str], list[str]] = {}
    folded_lens: set[tuple[str, str]] = set()
    for sd in slices:
        name = sd.column_name
        if not name:
            continue
        if sd.dimension_table_id and sd.fk_role and not sd.dimension_attribute:
            by_role.setdefault((sd.table_id, sd.fk_role), []).append(str(name))
        if not sd.dimension_table_id:
            folded_lens.add((sd.table_id, str(name)))

    positions = _curated_positions(slices)
    # A column the curation dropped is still a LEGAL axis (conformance, not interest,
    # decides legality) — it just sorts after everything the workspace ranked.
    unranked = len(positions) + 1

    groups: dict[str, dict[str, str]] = {}
    labels: dict[str, str] = {}
    ranks: dict[str, int] = {}
    unsliced: dict[str, tuple[str, str]] = {}
    for cell in cells:
        group = cell.conformed_group
        if not group or cell.fact_table_id not in facts:
            continue
        if cell.confirmation_source not in CONFIRMED_SOURCES or cell.needs_confirmation:
            continue
        column: str | None = None
        attempted = ""
        roles = [str(r) for r in (cell.roles or [])]
        if cell.attachment == "referenced":
            attempted = roles[0] if roles else ""
            for role in roles:
                # Deterministic: a role can carry several curated key slices only in
                # pathological catalogues; min() makes the pick reproducible.
                candidates = by_role.get((cell.fact_table_id, role))
                if candidates:
                    column = min(candidates)
                    if len(roles) > 1:
                        # A multi-role group means the judge merged several FK roles
                        # into one axis. Which role the breakdown is actually ABOUT
                        # is then a real choice, so it is recorded rather than left
                        # to be reverse-engineered from the numbers.
                        logger.info(
                            "cross_fact_role_selected",
                            fact_table_id=cell.fact_table_id,
                            conformed_group=group,
                            selected_role=role,
                            available_roles=roles,
                            column=column,
                        )
                    break
        elif cell.attachment == "folded":
            attempted = roles[0] if roles else ""
            if (cell.fact_table_id, attempted) in folded_lens:
                column = attempted
        if column is None:
            logger.info(
                "cross_fact_axis_unsliced",
                fact_table_id=cell.fact_table_id,
                conformed_group=group,
                attachment=cell.attachment,
                column=attempted,
            )
            unsliced.setdefault(group, (cell.fact_table_id, attempted))
            continue
        groups.setdefault(group, {})[cell.fact_table_id] = column
        labels.setdefault(group, cell.concept_label)
        rank = positions.get((cell.fact_table_id, column), unranked)
        ranks[group] = min(ranks.get(group, unranked), rank)

    qualified = {
        group: AxisCandidate(label=labels[group], columns=columns, rank=ranks[group])
        for group, columns in groups.items()
        if set(columns) == facts
    }
    # A group that qualified is not ALSO an unsliced complaint: one fact's cell may
    # have failed to resolve while another duplicate cell for the same group did.
    return AxisSurvey(
        qualified=qualified,
        unsliced={g: v for g, v in unsliced.items() if g not in qualified},
    )


def resolve_cross_fact_axis(
    session: Session, *, graph: TransformationGraph, workspace_id: str, run_id: str
) -> CrossFactDecision:
    """The conformed axis this metric's carriers may be drilled across, or why not.

    ``run_id`` is the CATALOGUE run whose bus matrix and slice inventory to read
    (ADR-0010: every consumer reads run-scoped, never latest-wins).

    When several conformed groups qualify, the pick is the lowest identity — a total
    order over globally-unique group signatures, so the axis a breakdown is composed
    against is reproducible across runs on identical data rather than scan-dependent.
    """
    resolved = carrier_facts(session, graph=graph, workspace_id=workspace_id)
    if isinstance(resolved, CrossFactDecision):
        return resolved
    steps = resolved
    facts = set(steps.values())
    if len(facts) < 2:
        return _abstain(
            CrossFactAbstain.SINGLE_FACT,
            "every carrier grounds on one fact — there is nothing to drill across "
            "(a single-fact breakdown is unit grain, not a merge)",
        )

    cells = list(
        session.execute(
            select(BusMatrixEntry)
            .where(
                BusMatrixEntry.run_id == run_id,
                BusMatrixEntry.fact_table_id.in_(sorted(facts)),
            )
            .order_by(BusMatrixEntry.signature)
        )
        .scalars()
        .all()
    )
    if not cells:
        return _abstain(
            CrossFactAbstain.NO_CONFORMED_DIMENSION,
            f"no bus-matrix cells recorded for {sorted(facts)} in run {run_id!r}",
        )
    slices = list(
        session.execute(
            select(SliceDefinition)
            .where(
                SliceDefinition.run_id == run_id,
                SliceDefinition.table_id.in_(sorted(facts)),
            )
            .order_by(SliceDefinition.column_name, SliceDefinition.slice_id)
        )
        .scalars()
        .all()
    )

    survey = survey_conformed_axes(cells, slices, facts)
    if not survey.qualified:
        # Three different problems with three different fixes. Reporting any of them
        # as another sends someone to do the wrong thing, so each gets its own reason.
        if survey.unsliced:
            group, (fact, column) = min(survey.unsliced.items())
            named = f" ({column!r})" if column else ""
            return _abstain(
                CrossFactAbstain.AXIS_UNSLICED,
                f"the dimension conformed as {group!r} IS confirmed, but the column"
                f"{named} realizing it on fact {fact!r} is not in that fact's curated "
                "slice inventory — the breakdown would have nothing to group by. "
                "Curating that column enables the drill-across; the pairing itself "
                "needs no further confirmation",
            )
        unconfirmed = {
            c.conformed_group
            for c in cells
            if c.conformed_group
            and (c.confirmation_source not in CONFIRMED_SOURCES or c.needs_confirmation)
        }
        if unconfirmed:
            return _abstain(
                CrossFactAbstain.UNCONFIRMED_CONFORMANCE,
                f"{sorted(facts)} share a candidate dimension, but its conformance is "
                "unconfirmed or awaiting review — confirming the pairing would enable "
                "the drill-across; merging on it now would assert an identity nobody did",
            )
        return _abstain(
            CrossFactAbstain.NO_CONFORMED_DIMENSION,
            f"{sorted(facts)} share no confirmed conformed dimension, so there is no "
            "legal merge key — comparing them would require a fact-to-fact join",
        )

    # The WORKSPACE picks the axis, not this function: order by the catalog's own
    # curation and use the identity only as the final total-order tiebreak. The
    # identity is a uuid-bearing signature, so sorting on it alone would have made
    # "which axis is a metric broken down by" an accident of uuid4 — and it is part
    # of the persisted upsert key, so that accident would be durable.
    identity = min(survey.qualified, key=lambda g: (survey.qualified[g].rank, g))
    candidate = survey.qualified[identity]
    return CrossFactDecision(
        status=CrossFactStatus.RESOLVED,
        axis=CrossFactAxis(
            identity=identity,
            label=candidate.label,
            columns=candidate.columns,
            steps=steps,
        ),
    )
