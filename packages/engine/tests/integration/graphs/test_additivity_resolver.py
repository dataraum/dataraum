"""Additivity resolver against a real catalog (DAT-716).

Exercises the DB plumbing the pure classifier can't: the snippet ``select_expr``
lookup, fact-column resolution via the enriched view, the run-scoped
``temporal_behavior`` join, and the periodic-snapshot grain read. Two cases pin
the two axes that matter — a flow measure over an event fact (fully additive) and
a stock measure over a periodic-snapshot fact (time stripped, categorical kept) —
the latter being the cell the live finance workspace has no standalone metric for.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.graphs.additivity import UNKNOWN_TEMPORAL
from dataraum.graphs.additivity_db_models import MetricAdditivity
from dataraum.graphs.additivity_resolver import compute_metric_verdict
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepSource,
    StepType,
    TransformationGraph,
)
from dataraum.pipeline.phases.metrics_phase import _persist_additivity_verdicts
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.storage import Column, Source, Table

WS = "ws-additivity"
RUN = "run-cat-1"


def _seed(
    session: Session,
    *,
    fact_name: str,
    view_name: str,
    columns: dict[str, str],
    grain_columns: list[str],
    time_columns: list[str],
    field: str,
    select_expr: str,
    aggregation: str,
    bind_concepts: bool = True,
    create_view: bool = True,
) -> TransformationGraph:
    """Seed one fact + its columns/concepts/view/entity + the extract snippet, and
    return a single-extract metric graph grounding ``field`` to it.

    ``bind_concepts=False`` leaves the base columns without a ``ColumnConcept``
    (an unresolved temporal_behavior); ``create_view=False`` leaves the fact
    without an enriched view (a dimensionless fact the extract reads directly)."""
    source = Source(name=f"src_{fact_name}", source_type="csv")
    session.add(source)
    session.flush()
    fact = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name=fact_name,
        layer="typed",
        duckdb_path=f"typed_{fact_name}",
        row_count=100,
    )
    session.add(fact)
    session.flush()
    for pos, (name, behavior) in enumerate(columns.items()):
        col = Column(
            table_id=fact.table_id,
            column_name=name,
            column_position=pos,
            raw_type="VARCHAR",
            resolved_type="DECIMAL",
        )
        session.add(col)
        session.flush()
        if bind_concepts:
            session.add(
                ColumnConcept(column_id=col.column_id, run_id=RUN, temporal_behavior=behavior)
            )
    if create_view:
        session.add(EnrichedView(fact_table_id=fact.table_id, view_name=view_name, run_id=RUN))
    session.add(
        TableEntity(
            table_id=fact.table_id,
            run_id=RUN,
            detected_entity_type="event",
            is_fact_table=True,
            grain_columns={"columns": grain_columns},
            time_columns=[{"column": c, "aspect": "t", "note": ""} for c in time_columns],
        )
    )
    session.add(
        SQLSnippetRecord(
            workspace_id=WS,
            schema_mapping_id=WS,
            snippet_type="extract",
            standard_field=field,
            aggregation=aggregation,
            sql=f"SELECT {select_expr} AS value FROM {view_name}",
            source="graph:m",
            parts={
                "select": [{"expr": select_expr, "alias": "value"}],
                "from": [view_name],
                "where": [],
            },
        )
    )
    session.commit()
    return TransformationGraph(
        graph_id="m",
        version="1",
        metadata=GraphMetadata(name="m", description="", category="c", source=GraphSource.SYSTEM),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={
            "e": GraphStep(
                step_id="e",
                step_type=StepType.EXTRACT,
                source=StepSource(standard_field=field),
                aggregation=aggregation,
                output_step=True,
            )
        },
    )


def test_flow_over_event_fact_is_fully_additive(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "additive", "debit": "additive"},
        grain_columns=["line_id"],  # event fact — no time column in the grain
        time_columns=[],
        field="revenue",
        select_expr="COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0)",
        aggregation="sum",
    )
    verdict = compute_metric_verdict(
        session, duckdb_conn, graph=graph, workspace_id=WS, catalogue_run_id=RUN
    )
    assert verdict is not None
    assert verdict.categorical_additive is True
    assert verdict.time_additive is True


def test_stock_over_snapshot_fact_strips_time(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    graph = _seed(
        session,
        fact_name="trial_balance",
        view_name="enriched_trial_balance",
        columns={"debit_balance": "point_in_time", "credit_balance": "point_in_time"},
        grain_columns=["account_id", "period"],  # snapshot — period sits in the grain
        time_columns=["period"],
        field="current_assets",
        select_expr="SUM(debit_balance) - SUM(credit_balance)",
        aggregation="sum",
    )
    verdict = compute_metric_verdict(
        session, duckdb_conn, graph=graph, workspace_id=WS, catalogue_run_id=RUN
    )
    assert verdict is not None
    # A summed balance reconciles across accounts but not across time.
    assert verdict.categorical_additive is True
    assert verdict.time_additive is False
    assert verdict.time_reason == "stock"


def test_unresolved_extract_yields_no_verdict(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A metric whose extract has no grounded snippet is refused, not guessed."""
    graph = TransformationGraph(
        graph_id="m",
        version="1",
        metadata=GraphMetadata(name="m", description="", category="c", source=GraphSource.SYSTEM),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={
            "e": GraphStep(
                step_id="e",
                step_type=StepType.EXTRACT,
                source=StepSource(standard_field="nonexistent"),
                aggregation="sum",
                output_step=True,
            )
        },
    )
    verdict = compute_metric_verdict(
        session, duckdb_conn, graph=graph, workspace_id=WS, catalogue_run_id=RUN
    )
    assert verdict is None


def test_unresolved_temporal_strips_time(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A base column with no ColumnConcept (unresolved temporal) → time refused, not assumed flow."""
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "additive", "debit": "additive"},
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(credit) - SUM(debit)",
        aggregation="sum",
        bind_concepts=False,  # no concept rows → temporal unknown
    )
    verdict = compute_metric_verdict(
        session, duckdb_conn, graph=graph, workspace_id=WS, catalogue_run_id=RUN
    )
    assert verdict is not None
    assert verdict.categorical_additive is True
    assert verdict.time_additive is False
    assert verdict.time_reason == UNKNOWN_TEMPORAL


def test_dimensionless_fact_resolves_by_table_name(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A fact with no enriched view is resolved directly by table name, not dropped."""
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="journal_lines",  # the extract reads the typed table directly
        columns={"credit": "additive", "debit": "additive"},
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(credit) - SUM(debit)",
        aggregation="sum",
        create_view=False,
    )
    verdict = compute_metric_verdict(
        session, duckdb_conn, graph=graph, workspace_id=WS, catalogue_run_id=RUN
    )
    assert verdict is not None
    assert verdict.time_additive is True


def test_persist_upserts_idempotently(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """_persist_additivity_verdicts writes one row per (metric_key, run_id); a re-run re-derives it."""
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "additive", "debit": "additive"},
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(credit) - SUM(debit)",
        aggregation="sum",
    )

    def rows() -> list[MetricAdditivity]:
        return list(
            session.execute(select(MetricAdditivity).where(MetricAdditivity.metric_key == "m"))
            .scalars()
            .all()
        )

    for _ in range(2):
        _persist_additivity_verdicts(
            session,
            duckdb_conn,
            graphs={"m": graph},
            executed_keys={"m"},
            workspace_id=WS,
            run_id=RUN,
            catalogue_run_id=RUN,
        )
        session.commit()

    persisted = rows()
    assert len(persisted) == 1  # upsert, not a duplicate
    assert persisted[0].categorical_additive is True
    assert persisted[0].time_additive is True
