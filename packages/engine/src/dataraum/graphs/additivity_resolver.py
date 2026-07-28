"""Resolve a graph's per-(target x axis) additivity verdicts (DAT-857/868).

Bridges the pure classifier (:mod:`dataraum.graphs.additivity`) to the workspace
catalog. For each of a metric's EXTRACT leaves it reads the grounded snippet's
``select_expr`` (the aggregate seam), resolves each aggregated SERVED column to
stock/flow (the reconciled aggregation-lineage witness, falling back to the
ontology prior — DAT-812 via ``source_column_id``, so a DIM/header-column measure
resolves too) and the fact's periodic-snapshot grain, classifies, and rolls the
per-extract atoms up through the DAG.

Nothing here returns "no answer". Every leaf resolves to a classification or a
TYPED ABSTENTION, and abstention is per-leaf: one unreadable extract no longer
blanks the verdicts of its perfectly-grounded siblings (the DAT-868 universe
defect). The caller persists a row either way, so a consumer can always tell
"non-additive" from "not judged".

Run-scoping: the enriched view is latest-only (one row per fact, keyed by name),
while stock/flow (``column_concepts``, ``measure_aggregation_lineage``) and the
fact grain (``table_entities``) are catalog-grain artifacts read at the pinned
begin_session ``catalogue_run_id``, exactly as the drivers phase pins them, so a
Temporal redelivery can never pick an arbitrary run's behaviour. The temporal
CADENCE is column-grain instead, so it is read through its head-resolving read
view rather than the base table (see :func:`_time_axes`).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, select, text
from sqlalchemy.exc import SQLAlchemyError

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.lineage.models import PATTERN_CUMULATIVE, PATTERN_PER_PERIOD
from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity, TableRole
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.graphs.additivity import (
    FLOW,
    POINT_IN_TIME,
    AbstainReason,
    AdditivityStatus,
    AxisAdditivity,
    AxisKind,
    abstained,
    axis_additivity,
    classify_extract,
    most_restrictive,
    parse_aggregate_calls,
    roll_up_metric,
    select_expr_is_ratio,
)
from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, BUCKET_GRAINS
from dataraum.graphs.models import StepType
from dataraum.query.snippet_library import SnippetLibrary
from dataraum.storage import Column

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy import Row
    from sqlalchemy.orm import Session

    from dataraum.graphs.additivity import AxisClass
    from dataraum.graphs.models import GraphStep, TransformationGraph


@dataclass(frozen=True)
class TimeAxis:
    """A time column the relation can be bucketed on, and its observed cadence.

    ``bucket_grain`` is the finest ladder rung (``og_period_grain``, DAT-730) the
    data actually supports — ``None`` when the cadence is irregular/unknown, which
    is a missing fact, not a licence to bucket at any grain.
    """

    column: str
    bucket_grain: str | None


@dataclass(frozen=True)
class ExtractResolution:
    """One EXTRACT leaf: its classification, or the typed reason there is none.

    Independent per extract (DAT-868): one unresolvable leaf abstains ITSELF and
    leaves its siblings classified. The metric roll-up still abstains when a leaf
    it actually depends on is missing — the difference is that a measure node for
    a perfectly-grounded sibling no longer vanishes with it.
    """

    step_id: str
    standard_field: str | None
    axis_class: AxisClass | None = None
    abstain_reason: AbstainReason | None = None
    time_axes: tuple[TimeAxis, ...] = ()


def resolve_extract_classes(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    graph: TransformationGraph,
    workspace_id: str,
    catalogue_run_id: str,
    read_schema: str | None = None,
) -> list[ExtractResolution]:
    """Resolve every EXTRACT leaf of a graph, independently.

    Never returns ``None`` and never lets one leaf blank the others: a leaf that
    cannot be reached (no healthy grounded snippet, a relation outside the current
    analysis, contradictory materialization evidence) comes back as a typed
    abstention alongside its fully-classified siblings.
    """
    library = SnippetLibrary(session, workspace_id=workspace_id)
    out: list[ExtractResolution] = []
    for step_id, step in graph.steps.items():
        if step.step_type != StepType.EXTRACT or step.source is None:
            continue
        out.append(
            _resolve_one_extract(
                session,
                duckdb_conn,
                library=library,
                step_id=step_id,
                step=step,
                workspace_id=workspace_id,
                catalogue_run_id=catalogue_run_id,
                read_schema=read_schema,
            )
        )
    return out


def _resolve_one_extract(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    library: SnippetLibrary,
    step_id: str,
    step: GraphStep,
    workspace_id: str,
    catalogue_run_id: str,
    read_schema: str | None,
) -> ExtractResolution:
    field = step.source.standard_field if step.source is not None else None
    resolved = grounded_select(library, workspace_id, step)
    if resolved is None:
        return ExtractResolution(step_id, field, abstain_reason=AbstainReason.UNRESOLVED_GROUNDING)
    select_expr, relation, _where = resolved
    served = served_relation(session, relation)
    if served is None:
        return ExtractResolution(
            step_id, field, abstain_reason=AbstainReason.RELATION_OUTSIDE_ANALYSIS
        )
    calls = parse_aggregate_calls(select_expr, duckdb_conn)
    columns = {col for call in calls for col in call.columns}
    temporal, conflicts = _materialization_by_served_column(
        session, served.columns_table_id, columns, catalogue_run_id
    )
    if conflicts:
        # The reconciled witness and the ontology prior disagree about whether this
        # measure is a stock or a flow. Two sources of truth in contradiction is
        # exactly the case where picking one silently is dishonest — abstain.
        return ExtractResolution(
            step_id, field, abstain_reason=AbstainReason.MATERIALIZATION_CONFLICT
        )
    snapshot = _fact_is_snapshot(session, served.fact_table_id, catalogue_run_id)
    is_ratio = select_expr_is_ratio(select_expr, duckdb_conn)
    return ExtractResolution(
        step_id,
        field,
        axis_class=classify_extract(calls, temporal, snapshot, is_ratio=is_ratio),
        time_axes=_time_axes(session, served.columns_table_id, read_schema),
    )


@dataclass(frozen=True)
class VerdictRow:
    """One persistable ``metric_axis_additivity`` row, minus the run stamp."""

    target_kind: str
    target_key: str
    axis_kind: str
    axis_key: str
    additivity: AxisAdditivity
    bucket_grain: str | None = None


def resolve_graph_verdicts(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    graph: TransformationGraph,
    graph_id: str,
    workspace_id: str,
    catalogue_run_id: str,
    read_schema: str | None = None,
) -> list[VerdictRow]:
    """Every verdict row a declared+parsed graph contributes (DAT-857/868).

    One CLASS row per (target, axis kind) for the metric and for each of its
    measures — always, verdict or typed abstention, so the drill can never find
    nothing — plus a refining row per concrete time axis carrying that axis's
    observed cadence.
    """
    resolutions = resolve_extract_classes(
        session,
        duckdb_conn,
        graph=graph,
        workspace_id=workspace_id,
        catalogue_run_id=catalogue_run_id,
        read_schema=read_schema,
    )

    rows: list[VerdictRow] = []
    by_field: dict[str, list[ExtractResolution]] = {}
    for r in resolutions:
        if r.standard_field:
            by_field.setdefault(r.standard_field, []).append(r)
    # An EXTRACT leaf with no standard_field has NO measure target, so it cannot
    # be looked up as a carrier — and the drill's recompute gate ("offer the
    # bucketing iff every carrier is additive") would then pass over it
    # VACUOUSLY, silently enabling exactly what the gate exists to refuse. The
    # metric abstains instead. Unreachable from the shipped catalogue (grounding
    # requires a standard_field), but a guard that knows about a case must not
    # drop it silently.
    # Falsy, not `is None`: an empty standard_field is just as unlookupable, and
    # this must match the `if r.standard_field` filter that builds by_field above
    # — a leaf that produced no measure row is exactly the one that escapes.
    anonymous_leaf = any(not r.standard_field for r in resolutions)
    for field, group in sorted(by_field.items()):
        rows.extend(_target_rows("measure", field, group))

    # The metric roll-up sees ONLY the classified leaves; a leaf it depends on
    # being absent already makes `roll_up_metric` refuse (it cannot tell a ratio
    # from a sum without it). We relabel that refusal to the more precise
    # MISSING_EXTRACT when a leaf really did abstain — an unreachable abstained
    # leaf leaves the roll-up untouched, which is why this is a relabel and not a
    # short-circuit.
    classified = {r.step_id: r.axis_class for r in resolutions if r.axis_class is not None}
    verdict = roll_up_metric(graph, classified)
    leaf_abstained = any(r.axis_class is None for r in resolutions)
    metric_axes = _common_time_axes([r for r in resolutions if r.axis_class is not None])
    for axis_kind in AxisKind:
        additivity = axis_additivity(verdict, axis_kind)
        if anonymous_leaf:
            additivity = abstained(AbstainReason.MISSING_EXTRACT)
        elif (
            leaf_abstained
            and additivity.status is AdditivityStatus.ABSTAINED
            and additivity.abstain_reason is AbstainReason.UNKNOWN_AGGREGATE
        ):
            additivity = abstained(AbstainReason.MISSING_EXTRACT)
        rows.append(VerdictRow("metric", graph_id, axis_kind.value, AXIS_KEY_ALL, additivity))
        if axis_kind is AxisKind.TIME:
            rows.extend(
                VerdictRow(
                    "metric", graph_id, axis_kind.value, a.column, additivity, a.bucket_grain
                )
                for a in metric_axes
            )
    return rows


def _target_rows(
    target_kind: str, target_key: str, group: list[ExtractResolution]
) -> list[VerdictRow]:
    """Class + per-time-axis rows for one target, folding its extracts conservatively.

    A standard_field can be extracted by more than one step; the target's verdict is
    the most restrictive of them, and ANY abstaining extract abstains the target —
    we cannot claim a measure sums when one of the things it sums is unreadable.
    """
    abstain = next((r.abstain_reason for r in group if r.abstain_reason is not None), None)
    folded: AxisClass | None = None
    if abstain is None:
        for r in group:
            if r.axis_class is None:  # unreachable: abstain is None
                continue
            folded = r.axis_class if folded is None else most_restrictive(folded, r.axis_class)
    axes = () if (abstain is not None or folded is None) else _common_time_axes(group)

    rows: list[VerdictRow] = []
    for axis_kind in AxisKind:
        if abstain is not None or folded is None:
            additivity = abstained(abstain or AbstainReason.UNRESOLVED_GROUNDING)
        else:
            additivity = axis_additivity(folded, axis_kind)
        rows.append(VerdictRow(target_kind, target_key, axis_kind.value, AXIS_KEY_ALL, additivity))
        if axis_kind is AxisKind.TIME:
            rows.extend(
                VerdictRow(
                    target_kind, target_key, axis_kind.value, a.column, additivity, a.bucket_grain
                )
                for a in axes
            )
    return rows


def _common_time_axes(group: list[ExtractResolution]) -> tuple[TimeAxis, ...]:
    """Time axes EVERY extract in the group can be bucketed on.

    An intersection, not a union: a metric can only be bucketed on a column all of
    its carriers carry, or the per-bucket recompute has nothing to join one carrier
    on. The shared cadence is the COARSEST of the carriers' — the finest bucket the
    slowest carrier actually supports — and an unknown cadence anywhere makes the
    shared one unknown (no claim).
    """
    if not group:
        return ()
    shared: set[str] | None = None
    for r in group:
        names = {a.column for a in r.time_axes}
        shared = names if shared is None else (shared & names)
    if not shared:
        return ()
    out: list[TimeAxis] = []
    for column in sorted(shared):
        grains = [a.bucket_grain for r in group for a in r.time_axes if a.column == column]
        coarsest: str | None = None
        for g in grains:
            if g is None:
                coarsest = None
                break
            if coarsest is None or BUCKET_GRAINS.index(g) > BUCKET_GRAINS.index(coarsest):
                coarsest = g
        out.append(TimeAxis(column=column, bucket_grain=coarsest))
    return tuple(out)


def grounded_select(
    library: SnippetLibrary, workspace_id: str, step: GraphStep
) -> tuple[str, str, list[str]] | None:
    """The extract's healthy grounded ``(select_expr, relation, where)`` from its snippet.

    Shared grounding-resolution primitive (also used by the period resolver,
    DAT-785): recovers the parts an EXTRACT step actually grounded to, or ``None``
    when it has no healthy snippet.

    ``where`` is the persisted predicate list (``parts["where"]``, possibly empty)
    — the SAME filter the executed flow SUM applies (``compose_extract_sql``). The
    period resolver needs it to observe the flow's window over exactly the rows the
    SUM scans, not the whole column; the additivity classifier ignores it.

    The lookup carries the step's DECLARED ``predicate`` (DAT-838). Omitting it did
    not abstain — it matched the UNRESTRICTED sibling, so a restricted step
    resolved to the wrong relation and the wrong ``where``, silently: the
    classifier then judged a row population the step never reads, and the period
    resolver bound its reporting instant over that same wrong population.
    """
    if step.source is None:
        return None
    match = library.find_by_key(
        "extract",
        workspace_id,
        standard_field=step.source.standard_field,
        statement=step.source.statement,
        aggregation=step.aggregation,
        predicate=step.source.predicate,
    )
    # find_by_key already filters failure_count == 0; the re-check is belt-and-braces.
    if match is None or (match.snippet.failure_count or 0) != 0:
        return None
    parts = match.snippet.parts or {}
    selects = parts.get("select") or []
    relations = parts.get("from") or []
    if not selects or not relations:
        return None
    expr = selects[0].get("expr")
    relation = relations[0]
    if not expr or not relation:
        return None
    where = [str(p) for p in (parts.get("where") or []) if p]
    return expr, relation, where


@dataclass(frozen=True)
class ServedRelation:
    """A grounded relation resolved to its enriched view + base fact (DAT-812).

    ``columns_table_id`` is the enriched view's OWN table — where the self-describing
    served columns live (DAT-811: the fact's ``f.*`` passthrough columns AND the joined
    dimension/header columns, each carrying a typed ``source_column_id``). Read the
    aggregated measures off THIS table so a dim/header-column measure is visible.
    ``fact_table_id`` is the base fact the view derives from — needed only for
    fact-grain reads (the periodic-snapshot role), a property of the fact itself.
    """

    columns_table_id: str
    fact_table_id: str


def served_relation(session: Session, relation: str) -> ServedRelation | None:
    """Resolve a grounded relation NAME to its enriched view + base fact (DAT-812).

    Shared grounding-resolution primitive (also used by the period resolver): a
    grounded EXTRACT reads an ENRICHED VIEW — post-DAT-811 EVERY fact has one (a
    dim-less fact gets a passthrough ``SELECT *`` view, and ``GraphAgent`` grounds on
    view names whenever any view exists), so the relation maps to its ``EnrichedView``
    row. Returns the view table (whose served columns describe the relation) and the
    base fact, or ``None`` when no current enriched view is named ``relation`` (or it
    never materialized) — the caller then classifies nothing rather than guess.
    """
    row = session.execute(
        select(EnrichedView.view_table_id, EnrichedView.fact_table_id).where(
            EnrichedView.view_name == relation, EnrichedView.view_table_id.isnot(None)
        )
    ).first()
    if row is None:
        return None
    return ServedRelation(columns_table_id=str(row[0]), fact_table_id=str(row[1]))


#: ``measure_aggregation_lineage.pattern`` → the ``temporal_behavior`` vocabulary
#: the classifier speaks. Same mapping ``og_columns.materialization`` uses.
_PATTERN_TO_BEHAVIOR: dict[str, str] = {
    PATTERN_PER_PERIOD: FLOW,
    PATTERN_CUMULATIVE: POINT_IN_TIME,
}


def _materialization_by_served_column(
    session: Session, view_table_id: str, column_names: set[str], run_id: str
) -> tuple[dict[str, str | None], set[str]]:
    """Stock/flow per served view column, witness-preferred (DAT-812 + DAT-868).

    An enriched view's served columns (DAT-811) each carry a ``source_column_id`` to
    the typed column they project — the fact's own ``f.*`` columns AND the joined
    dimension/header columns — so a DIM/header-column measure resolves correctly
    instead of silently missing. Keyed by the served column NAME as it appears in
    the view relation, exactly what the ``select_expr`` references.

    TWO sources of stock/flow evidence, and this used to read only the weaker one:

    * ``measure_aggregation_lineage.pattern`` — the DATA-RECONCILED witness
      (``per_period`` ⇒ flow, ``cumulative`` ⇒ stock). Only reconciled candidates
      persist, so a row here is an observation, not a guess.
    * ``column_concepts.temporal_behavior`` — the ontology PRIOR.

    The witness wins where both exist and agree, and where only the witness exists
    (the DAT-868 defect: a column with a real witness but a weak prior classified
    ``unknown_temporal`` and lost its time axis for no reason). Where they CONTRADICT
    each other, the column is returned in the conflict set and the caller abstains —
    deliberately unlike ``og_columns.materialization``, which COALESCEs the witness
    over the prior silently. Picking a winner between two disagreeing sources of
    truth is the kind of quiet guess this verdict exists to eliminate.

    Both tables are catalog-grain and read at the same ``run_id``.
    """
    if not column_names:
        return {}, set()
    rows = session.execute(
        select(
            Column.column_name,
            ColumnConcept.temporal_behavior,
            MeasureAggregationLineage.pattern,
        )
        .select_from(Column)
        .outerjoin(
            ColumnConcept,
            (ColumnConcept.column_id == Column.source_column_id) & (ColumnConcept.run_id == run_id),
        )
        .outerjoin(
            MeasureAggregationLineage,
            (MeasureAggregationLineage.measure_column_id == Column.source_column_id)
            & (MeasureAggregationLineage.run_id == run_id),
        )
        .where(Column.table_id == view_table_id, Column.column_name.in_(column_names))
    ).all()
    behaviors: dict[str, str | None] = {}
    conflicts: set[str] = set()
    for name, prior, pattern in rows:
        witness = _PATTERN_TO_BEHAVIOR.get(pattern) if pattern else None
        if witness is not None and prior is not None and witness != prior:
            conflicts.add(name)
            continue
        behaviors[name] = witness if witness is not None else prior
    return behaviors, conflicts


#: ``temporal_column_profiles.detected_granularity`` → the ``og_period_grain``
#: ladder rung a consumer may bucket at. Sub-day cadences collapse to the ladder's
#: finest rung; a WEEKly cadence has no rung of its own, so the finest HONEST rung
#: at or above it is month (bucketing weekly data by day is arithmetically fine for
#: a flow but renders mostly-empty buckets). ``irregular``/``unknown`` map to no
#: claim at all.
_GRANULARITY_TO_BUCKET_GRAIN: dict[str, str] = {
    "second": "day",
    "minute": "day",
    "hour": "day",
    "day": "day",
    "week": "month",
    "month": "month",
    "quarter": "quarter",
    "year": "year",
}


def _time_axes(
    session: Session, view_table_id: str, read_schema: str | None
) -> tuple[TimeAxis, ...]:
    """The relation's bucketable time columns and their observed cadence (DAT-730).

    A served column is a time axis when the typed column it projects has a temporal
    profile. The cadence is read through ``current_temporal_column_profiles`` — the
    profile is COLUMN-grain (sealed under the per-table generation head), so the
    head-resolving read view is the only run-correct door; reading the base table at
    the catalogue run would be a cross-run read.

    Guarded, and the guard yields NO AXES AT ALL — not axes with an empty cadence.
    Without the profile read there is nothing per-axis left to say, so writing a row
    per column would be pure duplication of the class row. A consumer that finds no
    row for its column falls back to the ``'*'`` class verdict and offers its normal
    grains, which is the same answer by a shorter path.
    """
    served = session.execute(
        select(Column.column_name, Column.source_column_id).where(
            Column.table_id == view_table_id, Column.source_column_id.isnot(None)
        )
    ).all()
    if not served:
        return ()
    source_ids = {str(row[1]) for row in served}
    cadence: dict[str, str] = {}
    if read_schema is not None:
        # The read must not be able to poison the caller's transaction. A failed
        # statement leaves a Postgres transaction ABORTED until it is rolled back,
        # and this runs INSIDE the metrics phase's per-metric SAVEPOINT — so it gets
        # a savepoint of its own. (A bare session.rollback() here would discard the
        # caller's work, not just this read.)
        profile_rows: Sequence[Row[Any]] = []
        try:
            with session.begin_nested():
                profile_rows = session.execute(
                    text(
                        f'SELECT column_id, detected_granularity FROM "{read_schema}".'  # noqa: S608
                        "current_temporal_column_profiles WHERE column_id IN :ids"
                    ).bindparams(bindparam("ids", expanding=True)),
                    {"ids": sorted(source_ids)},
                ).all()
        except SQLAlchemyError:
            profile_rows = []
        cadence = {str(cid): str(gran) for cid, gran in profile_rows}
    axes = [
        TimeAxis(
            column=str(name),
            bucket_grain=_GRANULARITY_TO_BUCKET_GRAIN.get(cadence.get(str(src), "")),
        )
        for name, src in served
        if str(src) in cadence
    ]
    return tuple(sorted(axes, key=lambda a: a.column))


def _fact_is_snapshot(session: Session, fact_table_id: str, run_id: str) -> bool | None:
    """Whether the fact is a periodic snapshot — read from the persisted table role.

    A snapshot fact re-states the same population each period, so a ``COUNT`` over
    it is non-additive across time. The grain∩time derivation now lives at
    classification (``derive_table_role``, DAT-728); this reads the persisted
    ``PeriodicSnapshot`` subtype. Returns ``None`` when the fact has no
    ``TableEntity`` for this run (grain unknown) — the classifier then denies
    ``COUNT`` the time axis rather than assuming an event fact.
    """
    row = session.execute(
        select(TableEntity.table_role).where(
            TableEntity.table_id == fact_table_id, TableEntity.run_id == run_id
        )
    ).first()
    if row is None:
        return None
    role = row[0]
    if role == TableRole.PERIODIC_SNAPSHOT:
        return True
    if role == TableRole.FACT:
        return False
    return None
