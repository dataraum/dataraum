"""Compute a metric's additivity verdict from persisted grounding state (DAT-716).

Bridges the pure classifier (:mod:`dataraum.graphs.additivity`) to the workspace
catalog. For each of a metric's EXTRACT leaves it reads the grounded snippet's
``select_expr`` (the aggregate seam), resolves the base columns to their fact
``temporal_behavior`` and the fact's periodic-snapshot grain, classifies, and
rolls the per-extract atoms up through the DAG.

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

from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.graphs.additivity import (
    MetricVerdict,
    classify_extract,
    parse_aggregate_calls,
    roll_up_metric,
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
    library = SnippetLibrary(session, workspace_id=workspace_id)
    extract_classes: dict[str, AxisClass] = {}
    for step_id, step in graph.steps.items():
        if step.step_type != StepType.EXTRACT or step.source is None:
            continue
        resolved = _grounded_select(library, workspace_id, step)
        if resolved is None:
            return None
        select_expr, relation = resolved
        fact_id = _fact_table_id(session, relation)
        if fact_id is None:
            return None
        calls = parse_aggregate_calls(select_expr, duckdb_conn)
        columns = {col for call in calls for col in call.columns}
        temporal = _temporal_by_column(session, fact_id, columns, catalogue_run_id)
        snapshot = _fact_is_snapshot(session, fact_id, catalogue_run_id)
        extract_classes[step_id] = classify_extract(calls, temporal, snapshot)
    if not extract_classes:
        return None
    return roll_up_metric(graph, extract_classes)


def _grounded_select(
    library: SnippetLibrary, workspace_id: str, step: GraphStep
) -> tuple[str, str] | None:
    """The extract's healthy grounded ``(select_expr, relation)`` from its snippet."""
    if step.source is None:
        return None
    match = library.find_by_key(
        "extract",
        workspace_id,
        standard_field=step.source.standard_field,
        statement=step.source.statement,
        aggregation=step.aggregation,
    )
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
    return expr, relation


def _fact_table_id(session: Session, view_name: str) -> str | None:
    """The fact table behind an enriched view (latest-only, name-keyed)."""
    row = session.execute(
        select(EnrichedView.fact_table_id).where(EnrichedView.view_name == view_name)
    ).first()
    return row[0] if row else None


def _temporal_by_column(
    session: Session, fact_table_id: str, column_names: set[str], run_id: str
) -> dict[str, str | None]:
    """``temporal_behavior`` per fact base column, pinned to the catalogue run."""
    if not column_names:
        return {}
    rows = session.execute(
        select(Column.column_name, ColumnConcept.temporal_behavior)
        .join(
            ColumnConcept,
            (ColumnConcept.column_id == Column.column_id) & (ColumnConcept.run_id == run_id),
        )
        .where(Column.table_id == fact_table_id, Column.column_name.in_(column_names))
    ).all()
    return {row[0]: row[1] for row in rows}


def _fact_is_snapshot(session: Session, fact_table_id: str, run_id: str) -> bool:
    """Whether a time column sits in the fact's grain (a periodic snapshot).

    ``grain_columns`` is ``{"columns": [name, ...]}``; ``time_columns`` is a list
    of ``{"column": name, ...}``. A snapshot fact re-states the same population
    each period, so a ``COUNT`` over it is non-additive across time.
    """
    row = session.execute(
        select(TableEntity.grain_columns, TableEntity.time_columns).where(
            TableEntity.table_id == fact_table_id, TableEntity.run_id == run_id
        )
    ).first()
    if row is None:
        return False
    grain_columns, time_columns = row
    grain = set((grain_columns or {}).get("columns", []))
    times = {tc.get("column") for tc in (time_columns or []) if isinstance(tc, dict)}
    return bool(grain & times)
