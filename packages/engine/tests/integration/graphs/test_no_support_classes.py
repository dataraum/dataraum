"""The absent concept, end to end (DAT-658).

The one live path that can WRITE a ``concept_absent`` retained row is drift:
contract-v2 validation (DAT-787) forces every declared filter value to be a
served value at authoring time, so an authored grounding can only stop matching
when the DATA moves under a healthy cached snippet. This drives that story
through the REAL agent twice against a real DuckDB relation and a real Postgres
read surface — no pipeline, no LLM (the provider is mocked once, for the first
authoring):

1. run 1 AUTHORS ``category = 'COGS'`` while COGS rows exist and the served
   enumeration carries 'COGS' → a healthy grounding row;
2. the data drifts (COGS rows deleted, the served enumeration re-profiled
   without 'COGS');
3. run 2 assembles from cache (no LLM call), aggregates to NULL, and the
   classifier — probing the extract's own parts on the same connection and
   screening its declared family values against the complete served sets —
   returns the NON-REVISABLE ``concept_absent``. The stale healthy row is
   demoted (DAT-709: a verdict with strictly more information than the writer
   had) and the typed provenance is readable via ``current_groundings``.
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import duckdb
import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from dataraum.core.models.base import Result
from dataraum.graphs.agent import ExecutionContext, GraphAgent
from dataraum.graphs.context_models import (
    ColumnContext,
    GraphExecutionContext,
    TableContext,
)
from dataraum.graphs.field_mapping import ColumnMeaning
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
from dataraum.server.workspace import schema_name_for
from dataraum.storage.property_graph import (
    drop_property_graph,
    materialize_property_graph,
)
from dataraum.storage.read_views import materialize_read_schema, read_schema_name_for

WS_ID = os.environ["DATARAUM_WORKSPACE_ID"]


@pytest.fixture
def pg_session(integration_engine: Engine) -> Session:
    factory = sessionmaker(bind=integration_engine, expire_on_commit=False)
    with factory() as sess:
        yield sess


def _boot(engine: Engine) -> None:
    """Materialize the read views + property graph exactly as ConnectionManager does."""
    schema = schema_name_for(WS_ID)
    with engine.begin() as conn:
        drop_property_graph(conn, schema)
        materialize_read_schema(conn, schema)
        materialize_property_graph(conn, schema)


def _graph() -> TransformationGraph:
    """A single-extract mini-graph, the warm pass's authoring shape (DAT-646)."""
    return TransformationGraph(
        graph_id="cost_of_goods_sold",
        version="1.0",
        metadata=GraphMetadata(
            name="COGS", description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={
            "value": GraphStep(
                step_id="value",
                step_type=StepType.EXTRACT,
                source=StepSource(
                    standard_field="cost_of_goods_sold", statement="income_statement"
                ),
                aggregation="sum",
                output_step=True,
            )
        },
    )


def _context(
    duckdb_conn: duckdb.DuckDBPyConnection, category_values: list[str]
) -> ExecutionContext:
    """The served context at one point in time: ``category``'s top_values ARE the
    complete enumeration (``distinct_count == len(top_values)``), which is both
    what validation enforces members against and what the classifier screens
    absence against — one reference, one meaning of "served"."""
    rich = GraphExecutionContext(
        tables=[
            TableContext(
                table_id="t_ledger",
                table_name="drift_ledger",
                duckdb_name="drift_ledger",
                columns=[
                    ColumnContext(
                        column_id="c_cat",
                        column_name="category",
                        table_name="drift_ledger",
                        distinct_count=len(category_values),
                        top_values=[{"value": v, "count": 1} for v in category_values],
                    ),
                    ColumnContext(
                        column_id="c_amt", column_name="amount", table_name="drift_ledger"
                    ),
                ],
            )
        ],
        field_mappings=[
            ColumnMeaning(
                column_id="c_amt",
                column_name="amount",
                table_name="drift_ledger",
                meaning="Ledger amount",
            )
        ],
    )
    return ExecutionContext(duckdb_conn=duckdb_conn, schema_mapping_id=WS_ID, rich_context=rich)


def _authoring_agent() -> GraphAgent:
    """A GraphAgent whose mocked LLM grounds COGS via ``category = 'COGS'`` with the
    member declared — the DAT-787-honest grounding shape."""
    config = MagicMock()
    config.limits.max_output_tokens_per_request = 4000
    config.features.graph_sql_generation = None
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user")
    agent = GraphAgent(config=config, provider=MagicMock(), prompt_renderer=renderer)
    agent.provider.get_model_for_tier.return_value = "test-model"
    response = MagicMock()
    response.tool_calls = []
    response.content = json.dumps(
        {
            "grounding": "cost_of_goods_sold via category = 'COGS' (complete 2-value set)",
            "relation": "drift_ledger",
            "where": ["category = 'COGS'"],
            "select_expr": "SUM(amount)",
            "description": "COGS: SUM(amount) where category = 'COGS'",
            "assumptions": [],
            "provenance": {
                "column_mappings_basis": [
                    {
                        "concept": "cost_of_goods_sold",
                        "basis": {
                            "measure_columns": ["amount"],
                            "filter_columns": ["category"],
                            "filter": "category = 'COGS'",
                            "filter_members": [{"column": "category", "value": "COGS"}],
                        },
                    }
                ],
            },
        }
    )
    agent.provider.converse = MagicMock(return_value=Result.ok(response))
    return agent


def test_drifted_absent_concept_demotes_to_typed_non_revisable_provenance(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    duckdb_conn.execute("CREATE TABLE drift_ledger (category VARCHAR, amount DOUBLE)")
    duckdb_conn.execute(
        "INSERT INTO drift_ledger VALUES ('COGS', 40.0), ('Rent', 10.0), ('Rent', 5.0)"
    )
    _boot(integration_engine)
    graph = _graph()

    # Run 1 — authoring against served data that carries 'COGS': a healthy grounding.
    agent = _authoring_agent()
    result = agent.execute(
        pg_session, graph, _context(duckdb_conn, ["COGS", "Rent"]), workspace_id=WS_ID
    )
    assert result.success, result.error
    assert result.value.output_value == 40.0
    pg_session.flush()

    groundings = f'"{read_schema_name_for(schema_name_for(WS_ID))}".current_groundings'
    row = pg_session.execute(
        text(f"SELECT failed, relation FROM {groundings} WHERE concept = 'cost_of_goods_sold'")  # noqa: S608
    ).one()
    assert row[0] is False
    assert row[1] == "drift_ledger"

    # The drift: the data no longer carries any COGS row, and the re-profiled served
    # enumeration no longer carries the value.
    duckdb_conn.execute("DELETE FROM drift_ledger WHERE category = 'COGS'")

    # Run 2 — assembled from cache (the LLM must NOT be called again), NULL support,
    # classified CONCEPT_ABSENT, and the stale healthy row demoted with the typed,
    # evidence-backed provenance.
    agent.provider.converse.reset_mock()
    result = agent.execute(pg_session, graph, _context(duckdb_conn, ["Rent"]), workspace_id=WS_ID)
    agent.provider.converse.assert_not_called()
    assert not result.success
    assert "concept_absent" in result.error
    assert "'COGS'" in result.error
    pg_session.flush()

    row = pg_session.execute(
        text(  # noqa: S608
            f"SELECT failed, provenance, relation, parts FROM {groundings} "
            f"WHERE concept = 'cost_of_goods_sold'"
        )
    ).one()
    assert row[0] is True, "the stale healthy row must be demoted, not served forever"
    provenance = row[1]
    assert provenance["failure_mode"] == "verifier_rejected"
    assert provenance["no_support_class"] == "concept_absent"
    # The evidence names the absent value AND the complete served enumeration, so a
    # reader (and the next authoring turn) can verify the verdict against the data.
    assert "'COGS'" in provenance["no_support_evidence"]
    assert "Rent" in provenance["no_support_evidence"]
    # Demotion RETAINS, it does not degrade: the row keeps its sql AND its parts
    # through the follow-up retained-failure write (whose cache-composed step dicts
    # carry no parts — a naive refresh nulled them, wiping the row's relation off
    # this very view; the demoted rows are skipped by that write instead).
    assert row[2] == "drift_ledger"
    assert row[3] is not None
    assert row[3]["where"] == ["category = 'COGS'"]
