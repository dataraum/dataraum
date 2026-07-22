"""The grounding agent's bounded catalog search (DAT-699).

High-cardinality discriminators are served size+sample only; their exact
values live behind the search_values tool. The agent resolves them by
substring search inside a small budget, then grounds its IN-list on the
results — before this, concepts present by name in a several-hundred-value
column were unreachable and the agent emitted SELECT NULL for them.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import duckdb

from dataraum.graphs.agent import ExecutionContext, GraphAgent
from dataraum.graphs.context import ColumnContext, GraphExecutionContext, TableContext


def _context(conn: duckdb.DuckDBPyConnection | None = None) -> ExecutionContext:
    coa = TableContext(
        table_id="t1",
        table_name="chart_of_account_ob",
        duckdb_name="coa",
        columns=[
            ColumnContext(
                column_id="c1", column_name="account_name", table_name="chart_of_account_ob"
            ),
        ],
    )
    rich = GraphExecutionContext(tables=[coa])
    return ExecutionContext(
        duckdb_conn=conn if conn is not None else MagicMock(),
        schema_mapping_id="ws",
        rich_context=rich,
    )


def _agent() -> GraphAgent:
    return GraphAgent.__new__(GraphAgent)


def _search(agent: GraphAgent, ctx: ExecutionContext, **kwargs: str) -> str:
    return agent._run_value_search(ctx, dict(kwargs))


class TestRunValueSearch:
    def _conn(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        conn.execute(
            "CREATE TABLE coa AS SELECT * FROM (VALUES "
            "('Depreciation'), ('Depreciation'), ('Taxes & Licenses'), "
            "('Cost of Labor'), (NULL)) v(account_name)"
        )
        return conn

    def test_matches_are_frequency_ordered_with_counts(self) -> None:
        out = _search(
            _agent(),
            _context(self._conn()),
            table="chart_of_account_ob",
            column="account_name",
            pattern="deprec",
        )
        assert "Depreciation (2 rows)" in out

    def test_no_match_reports_honestly(self) -> None:
        out = _search(
            _agent(),
            _context(self._conn()),
            table="chart_of_account_ob",
            column="account_name",
            pattern="inventory",
        )
        assert "no values matching 'inventory'" in out

    def test_pattern_wildcards_are_literal(self) -> None:
        """% and _ in the pattern are text, not SQL wildcards — '%' must not
        match everything."""
        out = _search(
            _agent(),
            _context(self._conn()),
            table="chart_of_account_ob",
            column="account_name",
            pattern="%",
        )
        assert "no values matching" in out

    def test_unknown_table_returns_correctable_text(self) -> None:
        out = _search(_agent(), _context(), table="nope", column="account_name", pattern="tax")
        assert "unknown table 'nope'" in out
        assert "chart_of_account_ob" in out  # the agent can correct itself

    def test_unknown_column_returns_correctable_text(self) -> None:
        out = _search(
            _agent(),
            _context(),
            table="chart_of_account_ob",
            column="nope",
            pattern="tax",
        )
        assert "unknown column 'nope'" in out
        assert "account_name" in out

    def test_invalid_input_returns_text_never_raises(self) -> None:
        out = _agent()._run_value_search(_context(), {"table": "coa"})
        assert "invalid search_values input" in out


# --- The bounded search loop inside _generate_sql ---------------------------------

from dataraum.graphs.models import (  # noqa: E402
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepSource,
    StepType,
    TransformationGraph,
)

_VALID_OUTPUT = {
    "grounding": "depreciation via account_name IN ('Depreciation') (searched)",
    "relation": "t",
    "where": ["account_name IN ('Depreciation')"],
    "select_expr": "SUM(amount)",
    "description": "Depreciation expense",
    # Contract v2 (DAT-727): enumerate the columns the parts touch, by role.
    "assumptions": [],
    "provenance": {
        "column_mappings_basis": [
            {
                "concept": "depreciation",
                "basis": {
                    "measure_columns": ["amount"],
                    "filter_columns": ["account_name"],
                    "filter": "Depreciation",
                    "filter_members": [{"column": "account_name", "value": "Depreciation"}],
                },
            }
        ],
    },
}

# The relation's served schema — feeds the prompt AND the contract enforcement.
_SCHEMA_INFO = {
    "tables": [
        {
            "table_name": "t",
            "columns": [
                {"name": "amount", "type": "DECIMAL"},
                {"name": "account_name", "type": "VARCHAR"},
            ],
            "row_count": 1,
        }
    ]
}


def _graph() -> TransformationGraph:
    ext = GraphStep(
        step_id="depreciation",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="depreciation", statement="income_statement"),
        aggregation="sum",
        output_step=True,
    )
    return TransformationGraph(
        graph_id="depreciation",
        version="1.0",
        metadata=GraphMetadata(
            name="depreciation", description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={"depreciation": ext},
    )


def _search_turn(tool_input: dict) -> MagicMock:
    """A turn that calls the ONE real tool instead of finishing."""
    from dataraum.llm.providers.base import ToolCall

    response = MagicMock()
    response.content = ""
    response.raw_content = None
    # Real ToolCall models — the loop echoes them back into a Message, which
    # validates its fields (a MagicMock would be rejected).
    response.tool_calls = [ToolCall(id="tc-1", name="search_values", input=tool_input)]
    return response


def _grounding(payload: dict) -> MagicMock:
    """A finished turn: the structured-output grounding, no tool call."""
    response = MagicMock()
    response.content = json.dumps(payload)
    response.raw_content = None
    response.tool_calls = []
    return response


def _loop_agent(provider: MagicMock) -> GraphAgent:
    agent = GraphAgent.__new__(GraphAgent)
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user", 0.0)
    agent.renderer = renderer  # type: ignore[attr-defined]
    agent.provider = provider  # type: ignore[attr-defined]
    config = MagicMock()
    config.limits.max_output_tokens_per_request = 4000
    config.features.graph_sql_generation = None
    agent.config = config  # type: ignore[attr-defined]
    agent._build_schema_info = MagicMock(return_value=_SCHEMA_INFO)  # type: ignore[method-assign]
    agent._build_prior_context = MagicMock(return_value="")  # type: ignore[method-assign]
    return agent


def _rich_exec_ctx(conn: duckdb.DuckDBPyConnection) -> ExecutionContext:
    ctx = _context(conn)
    ctx.rich_context.field_mappings = [object()]
    ctx.rich_context.conventions = ""
    return ctx


def _provider(*responses: MagicMock) -> MagicMock:
    provider = MagicMock()
    provider.get_model_for_tier.side_effect = lambda tier: f"model-{tier}"
    provider.converse.side_effect = [MagicMock(unwrap=MagicMock(return_value=r)) for r in responses]
    return provider


def test_search_then_generate_grounds_with_the_searched_values(monkeypatch) -> None:
    monkeypatch.setattr("dataraum.graphs.context.format_served_context", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_meanings_for_prompt", lambda f: "M")
    conn = duckdb.connect()
    conn.execute("CREATE TABLE coa AS SELECT * FROM (VALUES ('Depreciation')) v(account_name)")
    provider = _provider(
        _search_turn(
            {"table": "chart_of_account_ob", "column": "account_name", "pattern": "deprec"}
        ),
        _grounding(_VALID_OUTPUT),
    )
    agent = _loop_agent(provider)

    result = agent._generate_sql(MagicMock(), _graph(), _rich_exec_ctx(conn), {}, workspace_id="ws")

    assert result.success
    assert provider.converse.call_count == 2
    # The second request's conversation carries the assistant turn + the
    # search result the model grounds on.
    second = provider.converse.call_args_list[1].args[0]
    assert len(second.messages) == 3
    tool_result = second.messages[2].content[0]
    assert "Depreciation (1 rows)" in tool_result.content
    assert tool_result.tool_use_id == "tc-1"


def test_search_budget_exhaustion_fails_loud(monkeypatch) -> None:
    """A model that never stops searching hits the budget and fails loud —
    the last allowed search's result carries the budget notice."""
    monkeypatch.setattr("dataraum.graphs.context.format_served_context", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_meanings_for_prompt", lambda f: "M")
    conn = duckdb.connect()
    conn.execute("CREATE TABLE coa AS SELECT * FROM (VALUES ('Depreciation')) v(account_name)")
    search = {"table": "chart_of_account_ob", "column": "account_name", "pattern": "x"}
    provider = _provider(*[_search_turn(search) for _ in range(5)])
    agent = _loop_agent(provider)

    result = agent._generate_sql(MagicMock(), _graph(), _rich_exec_ctx(conn), {}, workspace_id="ws")

    assert not result.success
    assert "search budget exhausted" in (result.error or "")
    assert provider.converse.call_count == 5  # 1 initial + 4 budgeted continuations
    # The final tool_result told the model to emit generate_sql now.
    last = provider.converse.call_args_list[4].args[0]
    assert "stop searching and answer with the grounding now" in (
        last.messages[-1].content[0].content
    )
