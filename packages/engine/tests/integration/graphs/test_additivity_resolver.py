"""Additivity resolver against a real catalog (DAT-857/868).

Exercises the DB plumbing the pure classifier can't: the snippet ``select_expr``
lookup, fact-column resolution via the enriched view, the run-scoped stock/flow
join (witness + prior), and the periodic-snapshot grain read. The cases pin the
axes that matter — a flow measure over an event fact (additive), a stock measure
over a periodic-snapshot fact (semi-additive across time, additive across
categories; the cell the live finance workspace has no standalone metric for), and
a COUNT over a fact whose period is a FOREIGN KEY rather than a date column
(DAT-847), which reaches the snapshot rule only if the role derivation resolved
the period through the dimension.

Plus the DAT-868 universe contract: every declared target gets a row — a verdict
or a TYPED ABSTENTION — and one unresolvable extract no longer blanks its
siblings.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity, derive_table_role
from dataraum.analysis.semantic.models import (
    RelationshipOutput,
    TableEntityOutput,
    TableSynthesisOutput,
    TimeColumn,
)
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.graphs.additivity import (
    SNAPSHOT_COUNT,
    AbstainReason,
    AdditivityStatus,
    AxisAdditivity,
    AxisVerdict,
)
from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, MetricAxisAdditivity
from dataraum.graphs.additivity_resolver import VerdictRow, resolve_graph_verdicts
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


def _verdicts(session, duckdb_conn, graph, graph_id: str = "m") -> list[VerdictRow]:
    return resolve_graph_verdicts(
        session,
        duckdb_conn,
        graph=graph,
        graph_id=graph_id,
        workspace_id=WS,
        catalogue_run_id=RUN,
    )


def _axis(
    rows: list[VerdictRow],
    target_kind: str,
    target_key: str,
    axis_kind: str,
    axis_key: str = AXIS_KEY_ALL,
) -> AxisAdditivity:
    """The one verdict for a (target, axis) — the resolution a consumer performs."""
    matches = [
        r.additivity
        for r in rows
        if r.target_kind == target_kind
        and r.target_key == target_key
        and r.axis_kind == axis_kind
        and r.axis_key == axis_key
    ]
    assert len(matches) == 1, f"expected exactly one {target_kind}/{target_key}/{axis_kind}"
    return matches[0]


def _seed(
    session: Session,
    *,
    fact_name: str,
    view_name: str,
    columns: dict[str, str],
    grain_columns: list[str],
    time_columns: list[str],
    period_axis_columns: list[str] | None = None,
    field: str,
    select_expr: str,
    aggregation: str,
    bind_concepts: bool = True,
    dim_served: list[tuple[str, str, str, str]] | None = None,
    witness: dict[str, str] | None = None,
) -> TransformationGraph:
    """Seed one fact + its enriched view + the extract snippet, and return a
    single-extract metric graph grounding ``field`` to the view.

    Post-DAT-811 every fact has an enriched view whose SERVED columns describe it
    (DAT-812 reads measures off those). Each fact column in ``columns`` is mirrored as
    an ``origin='fact'`` served column carrying a ``source_column_id`` back to the fact
    column (an ``f.*`` passthrough). ``dim_served`` adds joined dimension/header served
    columns as ``(served_name, dim_table, source_col_name, behavior)`` tuples — an
    ``origin='dimension'`` served column sourced from a separate dim table's typed
    column, the ``{fk}__{col}`` shape a fact-by-name lookup cannot see.

    ``bind_concepts=False`` leaves the fact columns without a ``ColumnConcept`` (an
    unresolved temporal_behavior that must resolve through ``source_column_id`` to
    NULL, not silently to flow)."""
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
    view = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name=view_name,
        layer="enriched",
        duckdb_path=view_name,
        row_count=100,
    )
    session.add(view)
    session.flush()
    session.add(
        EnrichedView(
            fact_table_id=fact.table_id,
            view_table_id=view.table_id,
            view_name=view_name,
            run_id=RUN,
        )
    )
    pos = 0
    for name, behavior in columns.items():
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
        # The DATA-RECONCILED stock/flow witness (measure_aggregation_lineage),
        # which the resolver prefers over the ontology prior above.
        if witness and name in witness:
            session.add(
                MeasureAggregationLineage(
                    run_id=RUN,
                    measure_table_id=fact.table_id,
                    measure_column_id=col.column_id,
                    event_table_id=fact.table_id,
                    measure_time_axis_column="period",
                    event_time_axis_column="period",
                    measure_slice_column_id=col.column_id,
                    event_slice_column_id=col.column_id,
                    slice_dimension="account",
                    convention_sql="SELECT 1",
                    period_grain="month",
                    pattern=witness[name],
                    match_rate=1.0,
                    r_flow_median=1.0,
                    r_stock_median=0.0,
                    n_entities=10,
                    n_entities_fired=10,
                    sign_fired_primary=10,
                    sign_fired_mirror=0,
                    sign_fired_both=0,
                )
            )
        # The f.* served column on the view, sourced from the fact column.
        session.add(
            Column(
                table_id=view.table_id,
                column_name=name,
                column_position=pos,
                origin="fact",
                source_column_id=col.column_id,
            )
        )
        pos += 1
    for served_name, dim_table_name, source_col_name, behavior in dim_served or []:
        dim = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name=dim_table_name,
            layer="typed",
            duckdb_path=dim_table_name,
            row_count=50,
        )
        session.add(dim)
        session.flush()
        dim_col = Column(
            table_id=dim.table_id,
            column_name=source_col_name,
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="DECIMAL",
        )
        session.add(dim_col)
        session.flush()
        session.add(
            ColumnConcept(column_id=dim_col.column_id, run_id=RUN, temporal_behavior=behavior)
        )
        # The origin='dimension' served column — sourced from a DIFFERENT table's column.
        session.add(
            Column(
                table_id=view.table_id,
                column_name=served_name,
                column_position=pos,
                origin="dimension",
                source_column_id=dim_col.column_id,
            )
        )
        pos += 1
    session.add(
        TableEntity(
            table_id=fact.table_id,
            run_id=RUN,
            detected_entity_type="event",
            # The period is usually the fact's own event date; ``period_axis_columns``
            # overrides that for a fact whose period is an FK (DAT-847).
            table_role=derive_table_role(
                True,
                grain_columns,
                time_columns if period_axis_columns is None else period_axis_columns,
            ),
            grain_columns=grain_columns,
            time_columns=[
                {
                    "column": c,
                    "aspect": "t",
                    "role": "event",
                    "is_anchor": i == 0,
                    "note": "",
                }
                for i, c in enumerate(time_columns)
            ],
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
    rows = _verdicts(session, duckdb_conn, graph)
    for axis_kind in ("time", "categorical"):
        got = _axis(rows, "metric", "m", axis_kind)
        assert got.status is AdditivityStatus.CLASSIFIED
        assert got.verdict is AxisVerdict.ADDITIVE


def test_stock_over_snapshot_fact_is_semi_additive_across_time(
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
    rows = _verdicts(session, duckdb_conn, graph)
    # A summed balance reconciles across accounts; across time each period is
    # meaningful but the SUM of periods is not — semi-additive, not "refused".
    assert _axis(rows, "metric", "m", "categorical").verdict is AxisVerdict.ADDITIVE
    time = _axis(rows, "metric", "m", "time")
    assert time.verdict is AxisVerdict.SEMI_ADDITIVE
    assert time.reason == "stock"


def test_count_over_a_period_fk_snapshot_is_not_time_additive(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """DAT-847: the snapshot role must survive a period carried as a FOREIGN KEY.

    ``balances`` holds no date column at all — its period is the integer key
    ``period_id`` into a SURROGATE-keyed calendar, the standard warehouse shape,
    where no date appears in either table's grain. The role is derived through
    the production path rather than hand-set, so this pins the whole chain:
    synthesis output + cardinality witness → ``derive_table_role`` → the
    persisted ``table_role`` → the resolver's snapshot read → the COUNT rule.
    Before the fix the fact read as FACT and ``COUNT(*)`` came back additive
    across time, silently re-counting a population that is RE-STATED every
    period.
    """
    synthesis = TableSynthesisOutput(
        tables=[
            TableEntityOutput(
                table_name="balances",
                is_fact_table=True,
                grain=["account_id", "period_id"],
                time_columns=[],
                identity_columns=[],
            ),
            TableEntityOutput(
                table_name="dim_period",
                is_fact_table=False,
                grain=["period_id"],
                time_columns=[
                    TimeColumn(
                        column="period_date",
                        aspect="period",
                        role="event",
                        is_anchor=True,
                        note="One row per accounting period.",
                    )
                ],
                identity_columns=[],
            ),
        ],
        relationships=[
            RelationshipOutput(
                from_table="balances",
                from_column="period_id",
                to_table="dim_period",
                to_column="period_id",
                key_columns=[],
                relationship_type="foreign_key",
                confidence=0.95,
                reasoning="period_id keys the calendar dimension",
            )
        ],
    )
    fact = synthesis.tables[0]
    # One row per period — what makes dim_period a calendar rather than any other
    # surrogate-keyed dimension.
    period_dimensions = synthesis.period_columns_by_dimension(
        {"dim_period": {"period_id", "period_date"}}
    )
    graph = _seed(
        session,
        fact_name="balances",
        view_name="enriched_balances",
        columns={"balance": "point_in_time"},
        grain_columns=list(fact.grain),
        time_columns=[],  # no date column on the fact — the period is the FK
        period_axis_columns=sorted(synthesis.period_axis_columns(fact, period_dimensions)),
        field="account_count",
        select_expr="COUNT(*)",
        aggregation="count",
    )
    rows = _verdicts(session, duckdb_conn, graph)
    assert _axis(rows, "metric", "m", "categorical").verdict is AxisVerdict.ADDITIVE
    time = _axis(rows, "metric", "m", "time")
    # A snapshot COUNT is meaningful per period and meaningless summed across them.
    assert time.verdict is AxisVerdict.SEMI_ADDITIVE
    assert time.reason == SNAPSHOT_COUNT


def test_unresolved_extract_abstains_with_a_typed_reason(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A metric whose extract has no grounded snippet ABSTAINS — it does not vanish.

    The flipped DAT-868 contract. This used to return ``None`` for the whole graph
    and the phase then wrote NOTHING, so the drill could not tell "we judged this
    non-additive" from "we never judged it". Now both the measure and the metric
    say, in a row, exactly why they were not judged.
    """
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
    rows = _verdicts(session, duckdb_conn, graph)
    measure = _axis(rows, "measure", "nonexistent", "time")
    assert measure.status is AdditivityStatus.ABSTAINED
    assert measure.abstain_reason is AbstainReason.UNRESOLVED_GROUNDING
    metric = _axis(rows, "metric", "m", "time")
    assert metric.status is AdditivityStatus.ABSTAINED
    # The metric depends on that leaf, so it abstains too — naming the missing leaf.
    assert metric.abstain_reason is AbstainReason.MISSING_EXTRACT


def test_unresolved_temporal_strips_time(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A base column with no stock/flow evidence ABSTAINS on time, never assumes flow."""
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
    rows = _verdicts(session, duckdb_conn, graph)
    time = _axis(rows, "metric", "m", "time")
    assert time.status is AdditivityStatus.ABSTAINED
    assert time.abstain_reason is AbstainReason.UNKNOWN_TEMPORAL
    # ...while the categorical axis of the SAME target is still a real verdict:
    # per-axis means one unknown does not blank the other axis.
    assert _axis(rows, "metric", "m", "categorical").verdict is AxisVerdict.ADDITIVE


def test_dim_column_measure_resolves_temporal_via_source(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A measure aggregating a served DIM/header column resolves temporal_behavior
    via ``source_column_id`` — the crux of DAT-812.

    ``entry_id__amount`` is a header amount joined into the view; it is NOT a column
    of the ``journal_lines`` fact, so the retired by-name-on-the-fact lookup dropped
    it silently and the SUM classified as UNKNOWN_TEMPORAL (time stripped). Reading the
    served column and resolving through its source (``journal_entries.amount``,
    additive) now classifies it as an additive flow — summable across time.
    """
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"line_id": "point_in_time"},  # the fact carries no measure of its own
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(entry_id__amount)",  # aggregates the served HEADER amount
        aggregation="sum",
        dim_served=[("entry_id__amount", "journal_entries", "amount", "additive")],
    )
    rows = _verdicts(session, duckdb_conn, graph)
    assert _axis(rows, "metric", "m", "categorical").verdict is AxisVerdict.ADDITIVE
    # additive header amount → summable across time
    assert _axis(rows, "metric", "m", "time").verdict is AxisVerdict.ADDITIVE


def test_persist_is_fault_isolated(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """One metric's compute failure is skipped, never rolling back the phase session.

    The verdict is a best-effort annotation on the shared phase session; a bug in
    one metric must not discard another metric's row or unrelated pending work.
    """
    from unittest.mock import patch

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
        MetricAxisAdditivity(
            run_id=RUN,
            target_kind="metric",
            target_key="prior",
            axis_kind="time",
            axis_key=AXIS_KEY_ALL,
            status="classified",
            verdict="additive",
        )
    )
    session.flush()

    real = resolve_graph_verdicts

    def fake_resolve(_session, _conn, *, graph, **kw):
        if graph.graph_id == "bad":
            raise RuntimeError("boom - simulated resolver bug")
        return real(_session, _conn, graph=graph, **kw)

    with patch(
        "dataraum.graphs.additivity_resolver.resolve_graph_verdicts", side_effect=fake_resolve
    ):
        _persist_additivity_verdicts(
            session,
            duckdb_conn,
            graphs={"good": _graph("good", "good_measure"), "bad": _graph("bad", "bad_measure")},
            declared_keys={"good", "bad"},
            workspace_id=WS,
            run_id=RUN,
            catalogue_run_id=RUN,
        )
    session.commit()  # the session is not poisoned by the caught failure

    persisted = {
        (r.target_kind, r.target_key, r.axis_kind): r
        for r in session.execute(
            select(MetricAxisAdditivity).where(MetricAxisAdditivity.run_id == RUN)
        )
        .scalars()
        .all()
    }
    assert ("metric", "good", "time") in persisted  # the healthy metric persisted...
    assert ("measure", "good_measure", "time") in persisted  # ...and its measure
    # ...and the FAILED one is written as an abstention rather than skipped: a
    # silently missing row is the hole DAT-868 closed.
    bad = persisted[("metric", "bad", "time")]
    assert bad.status == "abstained"
    assert bad.abstain_reason == "unresolved_grounding"
    assert ("metric", "prior", "time") in persisted  # unrelated pending work survived


def test_persist_isolates_rollup_failure(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A roll-up (or measure-mapping) bug is caught inside the savepoint too.

    Regression: the roll_up_metric call + the extract→standard_field mapping must
    run INSIDE the per-metric savepoint, not after it — else an exception escapes
    `_persist_additivity_verdicts` and rolls back the whole phase session.
    """
    from unittest.mock import patch

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
        MetricAxisAdditivity(
            run_id=RUN,
            target_kind="metric",
            target_key="prior",
            axis_kind="time",
            axis_key=AXIS_KEY_ALL,
            status="classified",
            verdict="additive",
        )
    )
    session.flush()

    from dataraum.graphs.additivity import roll_up_metric as real_rollup

    def fake_rollup(graph, classes):  # noqa: ANN001, ANN202
        if graph.graph_id == "bad":
            raise RuntimeError("boom - simulated roll_up bug")
        return real_rollup(graph, classes)

    with patch("dataraum.graphs.additivity_resolver.roll_up_metric", side_effect=fake_rollup):
        _persist_additivity_verdicts(
            session,
            duckdb_conn,
            graphs={"good": _graph("good", "gf"), "bad": _graph("bad", "bf")},
            declared_keys={"good", "bad"},
            workspace_id=WS,
            run_id=RUN,
            catalogue_run_id=RUN,
        )
    session.commit()  # not poisoned by the roll-up failure

    persisted = {
        (r.target_kind, r.target_key, r.axis_kind): r
        for r in session.execute(
            select(MetricAxisAdditivity).where(MetricAxisAdditivity.run_id == RUN)
        )
        .scalars()
        .all()
    }
    assert ("metric", "good", "time") in persisted
    # the roll-up bug was caught, not escaped — and it abstains rather than vanishing
    assert persisted[("metric", "bad", "time")].status == "abstained"
    assert ("metric", "prior", "time") in persisted


def test_persist_upserts_idempotently(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A re-run re-derives the same (target, axis, run) row — upsert, not a duplicate.

    The sentinel ``axis_key='*'`` earns its keep here: a NULLable axis_key would make
    the ON CONFLICT inference NULLS-DISTINCT, and every class row would duplicate on
    each re-run instead of updating.
    """
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

    def metric_rows() -> list[MetricAxisAdditivity]:
        return list(
            session.execute(
                select(MetricAxisAdditivity).where(
                    MetricAxisAdditivity.target_kind == "metric",
                    MetricAxisAdditivity.target_key == "m",
                    MetricAxisAdditivity.axis_key == AXIS_KEY_ALL,
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
            declared_keys={"m"},
            workspace_id=WS,
            run_id=RUN,
            catalogue_run_id=RUN,
        )
        session.commit()

    persisted = {r.axis_kind: r for r in metric_rows()}
    assert len(persisted) == 2  # one class row per axis kind, upserted not duplicated
    assert persisted["categorical"].verdict == "additive"
    assert persisted["time"].verdict == "additive"


def test_persist_writes_measure_verdicts(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A stock measure gets its own semi-additive MEASURE verdict (the live AC5 cell).

    `current_assets` is a drillable `measure:` node; its verdict is the extract's
    own class — additive across accounts, semi-additive across time.
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
        declared_keys={"m"},
        workspace_id=WS,
        run_id=RUN,
        catalogue_run_id=RUN,
    )
    session.commit()

    rows = {
        (r.axis_kind, r.axis_key): r
        for r in session.execute(
            select(MetricAxisAdditivity).where(
                MetricAxisAdditivity.target_kind == "measure",
                MetricAxisAdditivity.target_key == "current_assets",
            )
        )
        .scalars()
        .all()
    }
    assert rows[("categorical", AXIS_KEY_ALL)].verdict == "additive"
    time_class = rows[("time", AXIS_KEY_ALL)]
    assert time_class.verdict == "semi_additive"
    assert time_class.reason == "stock"


def test_persist_writes_abstentions_when_there_is_no_catalogue_run(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """No promoted catalogue head ⇒ typed abstentions, not silence (DAT-868).

    This wrote NOTHING AT ALL before — a total, invisible hole for the whole run.
    A declared metric's IDENTITY needs no catalogue, so neither does saying that we
    could not judge it.
    """
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
    _persist_additivity_verdicts(
        session,
        duckdb_conn,
        graphs={"m": graph},
        declared_keys={"m"},
        workspace_id=WS,
        run_id="run-no-catalogue",
        catalogue_run_id=None,
    )
    session.commit()

    rows = list(
        session.execute(
            select(MetricAxisAdditivity).where(MetricAxisAdditivity.run_id == "run-no-catalogue")
        )
        .scalars()
        .all()
    )
    assert {r.axis_kind for r in rows} == {"time", "categorical"}
    assert all(r.status == "abstained" for r in rows)
    assert all(r.abstain_reason == "no_catalogue_run" for r in rows)


def test_persist_covers_a_declared_metric_whose_dag_would_not_parse(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A declared metric absent from ``graphs`` (GraphLoader refused it) still gets rows.

    The cockpit decides drillability with its OWN parser, so a graph our loader
    rejected may well be offered for drilling. An abstention row is what keeps that
    divergence honest instead of silent.
    """
    _persist_additivity_verdicts(
        session,
        duckdb_conn,
        graphs={},
        declared_keys={"unparseable_metric"},
        workspace_id=WS,
        run_id="run-parse-fail",
        catalogue_run_id=RUN,
    )
    session.commit()

    rows = list(
        session.execute(
            select(MetricAxisAdditivity).where(MetricAxisAdditivity.run_id == "run-parse-fail")
        )
        .scalars()
        .all()
    )
    assert len(rows) == 2  # one class row per axis kind, metric-kind only
    assert all(r.target_kind == "metric" for r in rows)
    assert all(r.abstain_reason == "graph_parse_failed" for r in rows)


def test_persist_keeps_a_healthy_sibling_when_one_extract_is_unresolvable(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """The DAT-868 headline: one bad leaf no longer blanks its siblings.

    A two-extract metric where only one extract grounded. The metric itself must
    abstain (it cannot be composed), but the GROUNDED measure keeps its real
    verdict — previously the whole graph returned None and BOTH measures vanished.
    """
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "additive", "debit": "additive"},
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0)",
        aggregation="sum",
    )
    graph.steps["missing"] = GraphStep(
        step_id="missing",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="never_grounded"),
        aggregation="sum",
    )
    _persist_additivity_verdicts(
        session,
        duckdb_conn,
        graphs={"m": graph},
        declared_keys={"m"},
        workspace_id=WS,
        run_id="run-mixed",
        catalogue_run_id=RUN,
    )
    session.commit()

    rows = {
        (r.target_kind, r.target_key, r.axis_kind): r
        for r in session.execute(
            select(MetricAxisAdditivity).where(MetricAxisAdditivity.run_id == "run-mixed")
        )
        .scalars()
        .all()
    }
    healthy = rows[("measure", "revenue", "time")]
    assert healthy.status == "classified"
    assert healthy.verdict == "additive"
    unresolved = rows[("measure", "never_grounded", "time")]
    assert unresolved.status == "abstained"
    assert unresolved.abstain_reason == "unresolved_grounding"


# --- stock/flow witness ride-along (DAT-868) ---------------------------------
# The resolver reads TWO sources of stock/flow evidence. Before this lane it read
# only the weaker one, so a column with a real reconciled witness but a NULL
# ontology prior classified `unknown_temporal` and lost its time axis for nothing.


def test_witness_resolves_a_column_whose_ontology_prior_is_missing(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """Witness present, prior absent ⇒ the witness decides (it is an OBSERVATION)."""
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "additive"},
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(credit)",
        aggregation="sum",
        bind_concepts=False,  # no ColumnConcept at all — prior is NULL
        witness={"credit": "per_period"},  # ...but the data reconciled it as a FLOW
    )
    rows = _verdicts(session, duckdb_conn, graph)
    time = _axis(rows, "metric", "m", "time")
    assert time.status is AdditivityStatus.CLASSIFIED
    assert time.verdict is AxisVerdict.ADDITIVE  # was: abstained/unknown_temporal


def test_cumulative_witness_makes_a_column_a_stock(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    graph = _seed(
        session,
        fact_name="trial_balance",
        view_name="enriched_trial_balance",
        columns={"balance": "point_in_time"},
        grain_columns=["account_id"],
        time_columns=[],
        field="current_assets",
        select_expr="SUM(balance)",
        aggregation="sum",
        bind_concepts=False,
        witness={"balance": "cumulative"},
    )
    rows = _verdicts(session, duckdb_conn, graph)
    time = _axis(rows, "metric", "m", "time")
    assert time.verdict is AxisVerdict.SEMI_ADDITIVE
    assert time.reason == "stock"


def test_witness_contradicting_the_prior_ABSTAINS(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """Two sources of truth in contradiction ⇒ abstain, never a silent pick.

    Deliberately unlike ``og_columns.materialization``, which COALESCEs the witness
    over the prior with no signal that they disagreed.
    """
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "point_in_time"},  # prior says STOCK...
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(credit)",
        aggregation="sum",
        witness={"credit": "per_period"},  # ...witness says FLOW
    )
    rows = _verdicts(session, duckdb_conn, graph)
    # The MEASURE names the contradiction itself...
    for axis_kind in ("time", "categorical"):
        measure = _axis(rows, "measure", "revenue", axis_kind)
        assert measure.status is AdditivityStatus.ABSTAINED
        assert measure.abstain_reason is AbstainReason.MATERIALIZATION_CONFLICT
        # ...and the metric built on it abstains too, naming the missing leaf
        # rather than re-stating a cause it did not observe.
        metric = _axis(rows, "metric", "m", axis_kind)
        assert metric.status is AdditivityStatus.ABSTAINED
        assert metric.abstain_reason is AbstainReason.MISSING_EXTRACT


# --- per-axis rows + cadence (DAT-857/730) -----------------------------------


def test_time_axes_carry_the_observed_cadence_from_the_read_view(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """The producer of the drill's grain floor: a per-axis row per time column.

    The cadence is COLUMN-grain, so it is read through the head-resolving read
    view; this stands up a minimal one and asserts the mapped ladder rung reaches
    the served row (`week` → `month`: a weekly cadence has no rung of its own, and
    day buckets on weekly data are mostly empty).
    """
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "additive", "booked_on": "additive"},
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(credit)",
        aggregation="sum",
    )
    source_id = session.execute(
        select(Column.source_column_id).where(
            Column.column_name == "booked_on", Column.source_column_id.isnot(None)
        )
    ).scalar_one()
    # The read schema is a real Postgres schema in production; on this suite's
    # in-memory SQLite the equivalent namespace is an ATTACHed database, which
    # makes the same `"<schema>".<table>` reference resolve.
    read_schema = "ws_additivity_read"
    session.execute(text(f"ATTACH DATABASE ':memory:' AS \"{read_schema}\""))
    session.execute(
        text(
            f'CREATE TABLE "{read_schema}".current_temporal_column_profiles '
            "(column_id VARCHAR, detected_granularity VARCHAR)"
        )
    )
    session.execute(
        text(f'INSERT INTO "{read_schema}".current_temporal_column_profiles VALUES (:cid, :g)'),
        {"cid": source_id, "g": "week"},
    )
    session.flush()

    rows = resolve_graph_verdicts(
        session,
        duckdb_conn,
        graph=graph,
        graph_id="m",
        workspace_id=WS,
        catalogue_run_id=RUN,
        read_schema=read_schema,
    )
    per_axis = [
        r
        for r in rows
        if r.target_kind == "metric" and r.axis_kind == "time" and r.axis_key != AXIS_KEY_ALL
    ]
    assert [r.axis_key for r in per_axis] == ["booked_on"]
    assert {r.bucket_grain for r in per_axis} == {"month"}
    # The measure target gets the same refinement — both are drillable.
    assert [
        r.bucket_grain for r in rows if r.target_kind == "measure" and r.axis_key == "booked_on"
    ] == ["month"]
    # ...and the class row is still there for every consumer that finds no
    # refinement for its column.
    assert _axis(rows, "metric", "m", "time").verdict is AxisVerdict.ADDITIVE


def test_no_read_schema_yields_NO_per_axis_rows_only_the_class_row(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """The guard's real behaviour: no cadence source ⇒ nothing per-axis to say."""
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "additive", "booked_on": "additive"},
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(credit)",
        aggregation="sum",
    )
    rows = _verdicts(session, duckdb_conn, graph)  # read_schema=None
    assert [r.axis_key for r in rows if r.axis_kind == "time"] == [
        AXIS_KEY_ALL,
        AXIS_KEY_ALL,
    ]


def test_an_anonymous_extract_leaf_abstains_the_metric(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """An EXTRACT with no standard_field has no measure target, so it can never be
    checked as a CARRIER — the drill's recompute gate would pass over it vacuously.
    The metric abstains rather than being silently enabled."""
    graph = _seed(
        session,
        fact_name="journal_lines",
        view_name="enriched_journal_lines",
        columns={"credit": "additive"},
        grain_columns=["line_id"],
        time_columns=[],
        field="revenue",
        select_expr="SUM(credit)",
        aggregation="sum",
    )
    graph.steps["anon"] = GraphStep(
        step_id="anon",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field=""),
        aggregation="sum",
    )
    rows = _verdicts(session, duckdb_conn, graph)
    metric = _axis(rows, "metric", "m", "time")
    assert metric.status is AdditivityStatus.ABSTAINED
    assert metric.abstain_reason is AbstainReason.MISSING_EXTRACT
