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

from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity, derive_table_role
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
        duckdb_path=fact_name,  # DAT-639: duckdb_path == table_name (no layer prefix)
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
            table_role=derive_table_role(True, grain_columns, time_columns),
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


def test_dimensionless_fact_resolves_to_typed_layer(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A fact with no enriched view resolves to its TYPED row, not the raw sibling.

    The typed and raw rows for a fact share the same table_name + duckdb_path
    (DAT-639), so the fallback must pin `layer == "typed"` — the raw row carries
    no ColumnConcept/TableEntity, and landing on it would wrongly strip time.
    """
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
    # The raw-layer sibling: identical name AND duckdb_path, no concepts.
    raw_src = Source(name="raw_src", source_type="csv")
    session.add(raw_src)
    session.flush()
    session.add(
        Table(
            table_id=str(uuid4()),
            source_id=raw_src.source_id,
            table_name="journal_lines",
            layer="raw",
            duckdb_path="journal_lines",
            row_count=100,
        )
    )
    session.commit()

    verdict = compute_metric_verdict(
        session, duckdb_conn, graph=graph, workspace_id=WS, catalogue_run_id=RUN
    )
    assert verdict is not None
    # Additive only if we resolved the TYPED row (which has the flow concepts);
    # the raw row would yield UNKNOWN_TEMPORAL and strip time.
    assert verdict.time_additive is True


def test_persist_is_fault_isolated(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """One metric's compute failure is skipped, never rolling back the phase session.

    The verdict is a best-effort annotation on the shared phase session; a bug in
    one metric must not discard another metric's row or unrelated pending work.
    """
    from unittest.mock import patch

    from dataraum.graphs.additivity import ADDITIVE

    def _graph(graph_id: str, field: str) -> TransformationGraph:
        return TransformationGraph(
            graph_id=graph_id,
            version="1",
            metadata=GraphMetadata(
                name=graph_id, description="", category="c", source=GraphSource.SYSTEM
            ),
            output=OutputDef(output_type=OutputType.SCALAR),
            steps={
                "e": GraphStep(
                    step_id="e",
                    step_type=StepType.EXTRACT,
                    source=StepSource(standard_field=field),
                    aggregation="sum",
                    output_step=True,
                )
            },
        )

    # An unrelated pending write already on the phase session (a prior verdict row).
    session.add(
        MetricAdditivity(
            run_id=RUN,
            target_kind="metric",
            target_key="prior",
            categorical_additive=True,
            time_additive=True,
        )
    )
    session.flush()

    def fake_classify(_session, _conn, *, graph, **_kw):
        if graph.graph_id == "bad":
            raise RuntimeError("boom - simulated resolver bug")
        return {"e": ADDITIVE}

    with patch(
        "dataraum.graphs.additivity_resolver.classify_metric_extracts", side_effect=fake_classify
    ):
        _persist_additivity_verdicts(
            session,
            duckdb_conn,
            graphs={"good": _graph("good", "good_measure"), "bad": _graph("bad", "bad_measure")},
            executed_keys={"good", "bad"},
            workspace_id=WS,
            run_id=RUN,
            catalogue_run_id=RUN,
        )
    session.commit()  # the session is not poisoned by the caught failure

    keys = {
        (r.target_kind, r.target_key)
        for r in session.execute(select(MetricAdditivity).where(MetricAdditivity.run_id == RUN))
        .scalars()
        .all()
    }
    assert ("metric", "good") in keys  # the healthy metric persisted...
    assert ("measure", "good_measure") in keys  # ...and its measure
    assert ("metric", "bad") not in keys  # the failed one was skipped, not written
    assert ("measure", "bad_measure") not in keys
    assert ("metric", "prior") in keys  # unrelated pending work survived the nested rollback


def test_persist_isolates_rollup_failure(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A roll-up (or measure-mapping) bug is caught inside the savepoint too.

    Regression: the roll_up_metric call + the extract→standard_field mapping must
    run INSIDE the per-metric savepoint, not after it — else an exception escapes
    `_persist_additivity_verdicts` and rolls back the whole phase session.
    """
    from unittest.mock import patch

    from dataraum.graphs.additivity import ADDITIVE, MetricVerdict

    def _graph(graph_id: str, field: str) -> TransformationGraph:
        return TransformationGraph(
            graph_id=graph_id,
            version="1",
            metadata=GraphMetadata(
                name=graph_id, description="", category="c", source=GraphSource.SYSTEM
            ),
            output=OutputDef(output_type=OutputType.SCALAR),
            steps={
                "e": GraphStep(
                    step_id="e",
                    step_type=StepType.EXTRACT,
                    source=StepSource(standard_field=field),
                    aggregation="sum",
                    output_step=True,
                )
            },
        )

    session.add(
        MetricAdditivity(
            run_id=RUN,
            target_kind="metric",
            target_key="prior",
            categorical_additive=True,
            time_additive=True,
        )
    )
    session.flush()

    def fake_rollup(graph, _classes):  # noqa: ANN001, ANN202
        if graph.graph_id == "bad":
            raise RuntimeError("boom - simulated roll_up bug")
        return MetricVerdict(categorical_additive=True, time_additive=True)

    with (
        patch(
            "dataraum.graphs.additivity_resolver.classify_metric_extracts",
            return_value={"e": ADDITIVE},
        ),
        patch("dataraum.graphs.additivity.roll_up_metric", side_effect=fake_rollup),
    ):
        _persist_additivity_verdicts(
            session,
            duckdb_conn,
            graphs={"good": _graph("good", "gf"), "bad": _graph("bad", "bf")},
            executed_keys={"good", "bad"},
            workspace_id=WS,
            run_id=RUN,
            catalogue_run_id=RUN,
        )
    session.commit()  # not poisoned by the roll-up failure

    keys = {
        (r.target_kind, r.target_key)
        for r in session.execute(select(MetricAdditivity).where(MetricAdditivity.run_id == RUN))
        .scalars()
        .all()
    }
    assert ("metric", "good") in keys
    assert ("metric", "bad") not in keys  # the roll-up bug was caught, not escaped
    assert ("metric", "prior") in keys


def test_persist_upserts_idempotently(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A re-run re-derives the same ``(target_kind, target_key, run_id)`` row (upsert)."""
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

    def metric_rows() -> list[MetricAdditivity]:
        return list(
            session.execute(
                select(MetricAdditivity).where(
                    MetricAdditivity.target_kind == "metric", MetricAdditivity.target_key == "m"
                )
            )
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

    persisted = metric_rows()
    assert len(persisted) == 1  # upsert, not a duplicate
    assert persisted[0].categorical_additive is True
    assert persisted[0].time_additive is True


def test_persist_writes_measure_verdicts(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A stock measure gets its own semi-additive MEASURE verdict (the live AC5 cell).

    `current_assets` is a drillable `measure:` node; its verdict is the extract's
    own class — categorical-additive (sums across accounts) but time-stripped.
    """
    graph = _seed(
        session,
        fact_name="trial_balance",
        view_name="enriched_trial_balance",
        columns={"debit_balance": "point_in_time", "credit_balance": "point_in_time"},
        grain_columns=["account_id", "period"],
        time_columns=["period"],
        field="current_assets",
        select_expr="SUM(debit_balance) - SUM(credit_balance)",
        aggregation="sum",
    )
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

    measure = session.execute(
        select(MetricAdditivity).where(
            MetricAdditivity.target_kind == "measure",
            MetricAdditivity.target_key == "current_assets",
        )
    ).scalar_one()
    assert measure.categorical_additive is True
    assert measure.time_additive is False
    assert measure.time_reason == "stock"
