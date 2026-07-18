"""Compute a metric's additivity verdict from persisted grounding state (DAT-716).

Bridges the pure classifier (:mod:`dataraum.graphs.additivity`) to the workspace
catalog. For each of a metric's EXTRACT leaves it reads the grounded snippet's
``select_expr`` (the aggregate seam), resolves each aggregated SERVED column to its
typed source's ``temporal_behavior`` (DAT-812 — via ``source_column_id``, so a
DIM/header-column measure resolves too) and the fact's periodic-snapshot grain,
classifies, and rolls the per-extract atoms up through the DAG.

Returns ``None`` when the metric cannot be classified — an extract that never
grounded (no healthy snippet / no parts) or reads a relation outside the current
analysis. The caller then writes NO verdict rather than a misleading one.

Run-scoping: the enriched view is latest-only (one row per fact, keyed by name),
but ``temporal_behavior`` (``column_concepts``) and the fact grain
(``table_entities``) are run-versioned catalogue artifacts — read at the pinned
begin_session ``catalogue_run_id``, exactly as the drivers phase pins them, so a
Temporal redelivery can never pick an arbitrary run's behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity, TableRole
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.graphs.additivity import (
    MetricVerdict,
    classify_extract,
    parse_aggregate_calls,
    roll_up_metric,
    select_expr_is_ratio,
)
from dataraum.graphs.models import StepType
from dataraum.query.snippet_library import SnippetLibrary
from dataraum.storage import Column

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.graphs.additivity import AxisClass
    from dataraum.graphs.models import GraphStep, TransformationGraph


def compute_metric_verdict(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    graph: TransformationGraph,
    workspace_id: str,
    catalogue_run_id: str,
) -> MetricVerdict | None:
    """A metric's additivity verdict, or ``None`` if it cannot be classified.

    Every EXTRACT leaf must resolve to a grounded ``select_expr`` over a fact of
    the current analysis; a metric with an unresolved extract (or none at all)
    yields ``None`` so the caller persists nothing.
    """
    classes = classify_metric_extracts(
        session,
        duckdb_conn,
        graph=graph,
        workspace_id=workspace_id,
        catalogue_run_id=catalogue_run_id,
    )
    if classes is None:
        return None
    return roll_up_metric(graph, classes)


def classify_metric_extracts(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    graph: TransformationGraph,
    workspace_id: str,
    catalogue_run_id: str,
) -> dict[str, AxisClass] | None:
    """Classify each EXTRACT leaf of a metric, keyed by ``step_id``.

    Returns ``None`` if any extract is unresolved (no healthy grounded parts / a
    relation outside the analysis) — the caller then persists nothing. The caller
    rolls these up for the metric verdict AND maps them by ``standard_field`` for
    the per-measure verdicts (a measure node is one extract, classified directly).
    """
    library = SnippetLibrary(session, workspace_id=workspace_id)
    extract_classes: dict[str, AxisClass] = {}
    for step_id, step in graph.steps.items():
        if step.step_type != StepType.EXTRACT or step.source is None:
            continue
        resolved = grounded_select(library, workspace_id, step)
        if resolved is None:
            return None
        select_expr, relation, _where = resolved
        served = served_relation(session, relation)
        if served is None:
            return None
        calls = parse_aggregate_calls(select_expr, duckdb_conn)
        columns = {col for call in calls for col in call.columns}
        temporal = _temporal_by_served_column(
            session, served.columns_table_id, columns, catalogue_run_id
        )
        snapshot = _fact_is_snapshot(session, served.fact_table_id, catalogue_run_id)
        is_ratio = select_expr_is_ratio(select_expr, duckdb_conn)
        extract_classes[step_id] = classify_extract(calls, temporal, snapshot, is_ratio=is_ratio)
    if not extract_classes:
        return None
    return extract_classes


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
    """
    if step.source is None:
        return None
    match = library.find_by_key(
        "extract",
        workspace_id,
        standard_field=step.source.standard_field,
        statement=step.source.statement,
        aggregation=step.aggregation,
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


def _temporal_by_served_column(
    session: Session, view_table_id: str, column_names: set[str], run_id: str
) -> dict[str, str | None]:
    """``temporal_behavior`` per served view column, via its typed source (DAT-812).

    An enriched view's served columns (DAT-811) each carry a ``source_column_id`` to
    the typed column they project — the fact's own ``f.*`` columns AND the joined
    dimension/header columns. ``temporal_behavior`` (``column_concepts``) lives on that
    SOURCE column, pinned to the catalogue run, so a DIM/header-column measure resolves
    correctly instead of silently missing (it is not on the fact, so the retired
    by-name-on-the-fact lookup dropped it). Keyed by the served column NAME as it
    appears in the view relation — exactly what the ``select_expr`` references.
    """
    if not column_names:
        return {}
    rows = session.execute(
        select(Column.column_name, ColumnConcept.temporal_behavior)
        .join(
            ColumnConcept,
            (ColumnConcept.column_id == Column.source_column_id) & (ColumnConcept.run_id == run_id),
        )
        .where(Column.table_id == view_table_id, Column.column_name.in_(column_names))
    ).all()
    return {row[0]: row[1] for row in rows}


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
