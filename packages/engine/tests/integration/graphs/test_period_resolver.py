"""``days_in_period`` derivation against a real read surface (DAT-785).

Exercises the plumbing the unit guards can't reach: the flow snippet lookup, the
relation → fact resolution, and the read off the Postgres property graph
(``og_columns.anchor_time_axis``, DAT-780) joined to the typed temporal profile
(``current_temporal_column_profiles.span_days``, DAT-783). One case pins the
happy path — a quarterly flow yields its observed ~273-day span, NOT the config
30 — and four pin the fall-loud contract (K6): every way the window can't be
observed keeps the config default but flags it, never a silent 30.

Seeds one controlled, fully-promoted workspace (no pipeline, no LLM) and
materializes the read views + property graph exactly as the engine bootstrap
does, mirroring ``tests/integration/storage/test_property_graph.py``.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import duckdb
import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.temporal.db_models import TemporalColumnProfile
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    ParameterDef,
    StepSource,
    StepType,
    TransformationGraph,
)
from dataraum.graphs.period_resolver import resolve_days_in_period
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.server.workspace import schema_name_for
from dataraum.storage import Column, Table
from dataraum.storage.property_graph import (
    drop_property_graph,
    materialize_property_graph,
)
from dataraum.storage.read_views import materialize_read_schema
from dataraum.storage.snapshot_head import MetadataSnapshotHead

WS_ID = os.environ["DATARAUM_WORKSPACE_ID"]
SRC = "00000000-0000-0000-0000-000000000002"  # baseline Source seeded by the fixture
RUN = "00000000-0000-0000-0000-000000000001"
TS = datetime(2026, 1, 1, tzinfo=UTC)
# A quarterly corpus: four quarter-end postings spanning ~273 days — the number the
# hardcoded 30 must be replaced by. min/max are what the temporal profile serves.
MIN_TS = datetime(2025, 3, 31, tzinfo=UTC)
MAX_TS = datetime(2025, 12, 29, tzinfo=UTC)
SPAN_DAYS = (MAX_TS - MIN_TS).total_seconds() / 86400  # 273.0


def _boot(engine: Engine) -> None:
    """Materialize the read views + property graph exactly as ConnectionManager does."""
    schema = schema_name_for(WS_ID)
    with engine.begin() as conn:
        drop_property_graph(conn, schema)
        materialize_read_schema(conn, schema)
        materialize_property_graph(conn, schema)


def _seed(
    session: Session,
    *,
    ground_flow: bool = True,
    declare_anchor: bool = True,
    profile_axis: bool = True,
    with_revenue: bool = False,
) -> str:
    """Seed an income-statement fact whose COGS flow trends by a quarterly axis.

    ``ground_flow`` false leaves the flow extract without a snippet (ungrounded);
    ``declare_anchor`` false leaves the fact with no anchor time axis (the DAT-801
    null-anchor shape); ``profile_axis`` false leaves the axis column without a
    temporal profile; ``with_revenue`` adds a second income-statement flow (dso's
    revenue) on the same fact/axis. Returns the fact ``table_id``.
    """
    fact = Table(
        table_id="t_is",
        source_id=SRC,
        table_name="income_stmt",
        layer="typed",
        duckdb_path="income_stmt",  # DAT-639: duckdb_path == table_name
    )
    session.add(fact)
    session.add_all(
        [
            Column(table_id="t_is", column_name="cogs", column_position=1),
            Column(table_id="t_is", column_name="posting_date", column_position=2),
        ]
    )
    session.flush()
    posting_col = (
        session.query(Column)
        .filter(Column.table_id == "t_is", Column.column_name == "posting_date")
        .one()
    )
    # Promote the fact's generation head (current_columns + column-grain temporal
    # profiles) and the catalog head (current_table_entities).
    session.add_all(
        [
            MetadataSnapshotHead(
                head_id="h_t_is",
                target="table:t_is",
                stage="generation",
                run_id=RUN,
                promoted_at=TS,
            ),
            MetadataSnapshotHead(
                head_id="h_cat", target="catalog", stage="catalog", run_id=RUN, promoted_at=TS
            ),
        ]
    )
    time_columns = (
        [
            {
                "column": "posting_date",
                "aspect": "posting",
                "role": "event",
                "is_anchor": True,
                "note": "",
            }
        ]
        if declare_anchor
        else []
    )
    session.add(
        TableEntity(
            table_id="t_is",
            run_id=RUN,
            detected_entity_type="entity",
            table_role="fact",
            time_columns=time_columns,
            detected_at=TS,
        )
    )
    if profile_axis:
        session.add(
            TemporalColumnProfile(
                profile_id="tp_posting",
                column_id=posting_col.column_id,
                run_id=RUN,
                profiled_at=TS,
                min_timestamp=MIN_TS,
                max_timestamp=MAX_TS,
                span_days=SPAN_DAYS,
                detected_granularity="quarter",
                granularity_confidence=0.9,
                actual_periods=4,
                gaps=[],
            )
        )
    if ground_flow:
        session.add(
            SQLSnippetRecord(
                workspace_id=WS_ID,
                schema_mapping_id=WS_ID,
                snippet_type="extract",
                standard_field="cost_of_goods_sold",
                statement="income_statement",
                aggregation="sum",
                sql="SELECT SUM(cogs) AS value FROM income_stmt",
                source="graph:dpo",
                parts={
                    "select": [{"expr": "SUM(cogs)", "alias": "value"}],
                    "from": ["income_stmt"],
                    "where": [],
                },
            )
        )
    if with_revenue:
        # A second income-statement flow (dso's revenue) on the SAME fact, anchored
        # to the SAME axis → the two ccc flows observe one window and agree.
        session.add(Column(table_id="t_is", column_name="revenue_amt", column_position=3))
        session.add(
            SQLSnippetRecord(
                workspace_id=WS_ID,
                schema_mapping_id=WS_ID,
                snippet_type="extract",
                standard_field="revenue",
                statement="income_statement",
                aggregation="sum",
                sql="SELECT SUM(revenue_amt) AS value FROM income_stmt",
                source="graph:ccc",
                parts={
                    "select": [{"expr": "SUM(revenue_amt)", "alias": "value"}],
                    "from": ["income_stmt"],
                    "where": [],
                },
            )
        )
    session.commit()
    return "t_is"


def _seed_revenue_on_second_fact(session: Session, *, span_days: float) -> None:
    """Seed a SECOND income-statement fact carrying the ``revenue`` flow with its own
    (different-span) anchor axis — the divergent-window case ccc must fall loud on."""
    session.add(
        Table(
            table_id="t_is2",
            source_id=SRC,
            table_name="income_stmt2",
            layer="typed",
            duckdb_path="income_stmt2",
        )
    )
    session.add_all(
        [
            Column(table_id="t_is2", column_name="revenue_amt", column_position=1),
            Column(table_id="t_is2", column_name="rev_date", column_position=2),
        ]
    )
    session.add(
        MetadataSnapshotHead(
            head_id="h_t_is2", target="table:t_is2", stage="generation", run_id=RUN, promoted_at=TS
        )
    )
    session.flush()
    rev_date_col = (
        session.query(Column)
        .filter(Column.table_id == "t_is2", Column.column_name == "rev_date")
        .one()
    )
    session.add(
        TableEntity(
            table_id="t_is2",
            run_id=RUN,
            detected_entity_type="entity",
            table_role="fact",
            time_columns=[
                {
                    "column": "rev_date",
                    "aspect": "rev",
                    "role": "event",
                    "is_anchor": True,
                    "note": "",
                }
            ],
            detected_at=TS,
        )
    )
    session.add(
        TemporalColumnProfile(
            profile_id="tp_rev",
            column_id=rev_date_col.column_id,
            run_id=RUN,
            profiled_at=TS,
            min_timestamp=MIN_TS,
            max_timestamp=MIN_TS + timedelta(days=span_days),
            span_days=span_days,
            detected_granularity="month",
            granularity_confidence=0.9,
            actual_periods=3,
            gaps=[],
        )
    )
    session.add(
        SQLSnippetRecord(
            workspace_id=WS_ID,
            schema_mapping_id=WS_ID,
            snippet_type="extract",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            sql="SELECT SUM(revenue_amt) AS value FROM income_stmt2",
            source="graph:ccc",
            parts={
                "select": [{"expr": "SUM(revenue_amt)", "alias": "value"}],
                "from": ["income_stmt2"],
                "where": [],
            },
        )
    )
    session.commit()


def _dpo_graph(*, flow_statement: str = "income_statement") -> TransformationGraph:
    """A dpo-shaped graph with a period parameter and one flow extract.

    ``flow_statement`` lets a test flip the flow off the income statement to prove
    the "no flow to observe" fall-loud path.
    """
    return TransformationGraph(
        graph_id="dpo",
        version="1",
        metadata=GraphMetadata(
            name="dpo", description="", category="working_capital", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="dpo", unit="days"),
        parameters=[ParameterDef(name="days_in_period", param_type="integer", default=30)],
        steps={
            "cost_of_goods_sold": GraphStep(
                step_id="cost_of_goods_sold",
                step_type=StepType.EXTRACT,
                source=StepSource(standard_field="cost_of_goods_sold", statement=flow_statement),
                aggregation="sum",
            ),
            "days_in_period": GraphStep(
                step_id="days_in_period",
                step_type=StepType.CONSTANT,
                parameter="days_in_period",
            ),
            "dpo": GraphStep(
                step_id="dpo",
                step_type=StepType.FORMULA,
                expression="(accounts_payable / cost_of_goods_sold) * days_in_period",
                depends_on=["cost_of_goods_sold", "days_in_period"],
                output_step=True,
            ),
        },
    )


def _ccc_graph() -> TransformationGraph:
    """A ccc-shaped graph: TWO income-statement flows (revenue + COGS) feeding ONE
    shared days_in_period constant — the case that must observe one agreed window."""
    return TransformationGraph(
        graph_id="cash_conversion_cycle",
        version="1",
        metadata=GraphMetadata(
            name="ccc", description="", category="working_capital", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="ccc", unit="days"),
        parameters=[ParameterDef(name="days_in_period", param_type="integer", default=30)],
        steps={
            "revenue": GraphStep(
                step_id="revenue",
                step_type=StepType.EXTRACT,
                source=StepSource(standard_field="revenue", statement="income_statement"),
                aggregation="sum",
            ),
            "cost_of_goods_sold": GraphStep(
                step_id="cost_of_goods_sold",
                step_type=StepType.EXTRACT,
                source=StepSource(
                    standard_field="cost_of_goods_sold", statement="income_statement"
                ),
                aggregation="sum",
            ),
            "days_in_period": GraphStep(
                step_id="days_in_period",
                step_type=StepType.CONSTANT,
                parameter="days_in_period",
            ),
            "ccc": GraphStep(
                step_id="ccc",
                step_type=StepType.FORMULA,
                expression="(revenue / cost_of_goods_sold) * days_in_period",
                depends_on=["revenue", "cost_of_goods_sold", "days_in_period"],
                output_step=True,
            ),
        },
    )


@pytest.fixture
def pg_session(integration_engine: Engine) -> Session:
    factory = sessionmaker(bind=integration_engine, expire_on_commit=False)
    with factory() as sess:
        yield sess


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """A plain in-memory DuckDB — parse_aggregate_calls is a catalog-free parse."""
    conn = duckdb.connect()
    yield conn
    conn.close()


def test_derives_the_observed_flow_span_not_the_config_30(
    integration_engine: Engine,
    pg_session: Session,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """The quarterly COGS flow's observed ~273-day span replaces the hardcoded 30."""
    _seed(pg_session)
    _boot(integration_engine)
    resolution = resolve_days_in_period(pg_session, con, graph=_dpo_graph(), workspace_id=WS_ID)
    assert resolution is not None
    assert resolution.derived is True
    assert resolution.flag is None
    assert resolution.days == pytest.approx(SPAN_DAYS)
    assert resolution.days != 30
    assert resolution.evidence["anchor_time_axis"] == ["posting_date"]


def test_no_income_statement_flow_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A metric with a period parameter but no flow to observe keeps 30, flagged."""
    _seed(pg_session)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, con, graph=_dpo_graph(flow_statement="balance_sheet"), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert resolution.derived is False
    assert "no income-statement flow extract" in (resolution.flag or "")


def test_ungrounded_flow_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A flow whose extract never grounded (no snippet) keeps 30, flagged."""
    _seed(pg_session, ground_flow=False)
    _boot(integration_engine)
    resolution = resolve_days_in_period(pg_session, con, graph=_dpo_graph(), workspace_id=WS_ID)
    assert resolution is not None
    assert resolution.days == 30.0
    assert "did not ground" in (resolution.flag or "")


def test_null_anchor_axis_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A flow fact with no anchor time axis (the DAT-801 header-date shape) abstains
    and flags — never a silent 30 derived off a missing axis."""
    _seed(pg_session, declare_anchor=False)
    _boot(integration_engine)
    resolution = resolve_days_in_period(pg_session, con, graph=_dpo_graph(), workspace_id=WS_ID)
    assert resolution is not None
    assert resolution.days == 30.0
    assert "no observable anchor-axis span" in (resolution.flag or "")


def test_axis_without_temporal_profile_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """An anchor axis that was never temporally profiled has no span to observe."""
    _seed(pg_session, profile_axis=False)
    _boot(integration_engine)
    resolution = resolve_days_in_period(pg_session, con, graph=_dpo_graph(), workspace_id=WS_ID)
    assert resolution is not None
    assert resolution.days == 30.0
    assert "no observable anchor-axis span" in (resolution.flag or "")


def test_ccc_two_flows_sharing_a_window_derive_one_span(
    integration_engine: Engine,
    pg_session: Session,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """ccc's revenue + COGS flows on one fact/axis agree → one derived span, no flag."""
    _seed(pg_session, with_revenue=True)
    _boot(integration_engine)
    resolution = resolve_days_in_period(pg_session, con, graph=_ccc_graph(), workspace_id=WS_ID)
    assert resolution is not None
    assert resolution.derived is True
    assert resolution.flag is None
    assert resolution.days == pytest.approx(SPAN_DAYS)


def test_ccc_flows_disagreeing_on_window_fall_loud(
    integration_engine: Engine,
    pg_session: Session,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """ccc's two flows observing DIFFERENT windows (COGS 273d, revenue 90d on a second
    fact) cannot reconcile to one shared days_in_period → keep 30, flagged. This is
    the case a fact-keyed accumulator would silently collapse."""
    _seed(pg_session)  # COGS on t_is, ~273-day span
    _seed_revenue_on_second_fact(pg_session, span_days=90.0)  # revenue on t_is2, 90-day span
    _boot(integration_engine)
    resolution = resolve_days_in_period(pg_session, con, graph=_ccc_graph(), workspace_id=WS_ID)
    assert resolution is not None
    assert resolution.days == 30.0
    assert resolution.derived is False
    assert "disagree on the period window" in (resolution.flag or "")
