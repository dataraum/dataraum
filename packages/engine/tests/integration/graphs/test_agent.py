"""Tests for the GraphAgent.

Tests cover:
- SQL generation from graph specifications
- Snippet-based SQL reuse (the database snippet library)
- SQL execution
- Error handling
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.core.models.base import Result
from dataraum.graphs.agent import (
    ExecutionContext,
    GeneratedCode,
    GraphAgent,
)
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
from tests.conftest import baseline_run_id


@pytest.fixture
def sample_graph() -> TransformationGraph:
    """Create a simple test graph."""
    return TransformationGraph(
        graph_id="test_metric",
        version="1.0",
        metadata=GraphMetadata(
            name="Test Metric",
            description="A test metric",
            category="test",
            source=GraphSource.SYSTEM,
            tags=[],
        ),
        output=OutputDef(
            output_type=OutputType.SCALAR,
            metric_id="test",
            unit="count",
            decimal_places=0,
        ),
        parameters=[],
        steps={
            "value": GraphStep(
                step_id="value",
                step_type=StepType.EXTRACT,
                source=StepSource(
                    standard_field="test_field",
                    statement="test_table",
                ),
                aggregation="sum",
                depends_on=[],
                output_step=True,
            ),
        },
        interpretation=None,
    )


@pytest.fixture
def duckdb_with_data():
    """Create a DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE test_data (id INT, amount DECIMAL(10,2))")
    conn.execute("INSERT INTO test_data VALUES (1, 100.00), (2, 200.00), (3, 300.00)")
    yield conn
    conn.close()


def _make_execution_context(
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    schema_mapping_id: str = "test-mapping",
) -> ExecutionContext:
    """Create an ExecutionContext with minimal rich_context and field mappings.

    This is the correct way to build an ExecutionContext for tests that
    exercise SQL generation. Using ExecutionContext without rich_context
    will fail fast with a clear error.
    """
    from dataraum.graphs.context import GraphExecutionContext, TableContext
    from dataraum.graphs.field_mapping import ColumnCandidate, FieldMappings

    rich_context = GraphExecutionContext(
        tables=[
            TableContext(
                table_id="t1",
                table_name="test_data",
                duckdb_name="test_data",
            ),
        ],
        total_tables=1,
        field_mappings=FieldMappings(
            mappings={
                "test_field": [
                    ColumnCandidate(
                        column_id="c1",
                        column_name="amount",
                        table_name="test_data",
                        confidence=1.0,
                    )
                ],
            },
            table_ids=["t1"],
        ),
    )
    return ExecutionContext(
        duckdb_conn=duckdb_conn,
        schema_mapping_id=schema_mapping_id,
        rich_context=rich_context,
    )


class TestGeneratedCode:
    """Tests for GeneratedCode dataclass."""

    def test_create_generated_code(self):
        """Test creating a GeneratedCode instance."""
        code = GeneratedCode(
            code_id="test-123",
            graph_id="dso",
            summary="Calculates Days Sales Outstanding (DSO) metric.",
            steps=[{"step_id": "ar", "sql": "SELECT 1", "description": "test"}],
            final_sql="SELECT 1",
            column_mappings={"accounts_receivable": "ar_column"},
            llm_model="claude-3",
            prompt_hash="abc123",
            generated_at=datetime.now(UTC),
        )

        assert code.code_id == "test-123"
        assert code.graph_id == "dso"
        assert code.summary == "Calculates Days Sales Outstanding (DSO) metric."
        assert len(code.steps) == 1


class TestExecutionContext:
    """Tests for ExecutionContext dataclass."""

    def test_create_context(self, duckdb_with_data):
        """Test creating an ExecutionContext."""
        context = ExecutionContext(
            duckdb_conn=duckdb_with_data,
            schema_mapping_id="test-mapping",
        )

        assert context.schema_mapping_id == "test-mapping"


class TestDescribeTable:
    """Tests for _describe_table static method."""

    def test_describe_table(self, duckdb_with_data):
        """Test describing a DuckDB table."""
        result = GraphAgent._describe_table(duckdb_with_data, "test_data")

        assert result is not None
        assert result["table_name"] == "test_data"
        assert result["row_count"] == 3
        assert len(result["columns"]) == 2

        col_names = [c["name"] for c in result["columns"]]
        assert "id" in col_names
        assert "amount" in col_names
        # DAT-616: no per-column DISTINCT/LIMIT-5 self-fetch — name+type only; the
        # authoritative value enumeration is the rich-context Value sets block.
        assert "sample_values" not in result["columns"][0]

    def test_describe_nonexistent_table(self, duckdb_with_data):
        """Test describing a table that doesn't exist returns None."""
        result = GraphAgent._describe_table(duckdb_with_data, "nonexistent")
        assert result is None


class TestGraphAgentExecution:
    """Tests for GraphAgent SQL execution."""

    def test_build_schema_info_with_rich_context(self, duckdb_with_data):
        """Test building multi-table schema from rich context."""
        from dataraum.graphs.context import TableContext

        agent = GraphAgent(
            config=MagicMock(),
            provider=MagicMock(),
            prompt_renderer=MagicMock(),
        )

        # Create a mock rich context with table info
        rich_context = MagicMock()
        rich_context.tables = [
            TableContext(
                table_id="t1",
                table_name="test_data",
                duckdb_name="test_data",
            ),
        ]
        rich_context.enriched_views = []

        context = ExecutionContext(
            duckdb_conn=duckdb_with_data,
            rich_context=rich_context,
        )

        result = agent._build_schema_info(context)

        assert "tables" in result
        assert len(result["tables"]) == 1
        assert result["tables"][0]["table_name"] == "test_data"
        assert result["tables"][0]["row_count"] == 3

        col_names = [c["name"] for c in result["tables"][0]["columns"]]
        assert "id" in col_names
        assert "amount" in col_names


class TestGraphAgentIntegration:
    """Integration tests for GraphAgent with mocked LLM."""

    def test_execute_with_mocked_llm(
        self,
        session: Session,
        duckdb_with_data,
        sample_graph,
    ):
        """Test full execution flow with mocked LLM."""
        # Create mocked provider
        mock_provider = MagicMock()
        mock_provider.get_model_for_tier.return_value = "test-model"

        # Create agent
        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000
        mock_config.limits.cache_ttl_seconds = 3600

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "Test prompt", 0.0)

        agent = GraphAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
        )

        # Mock the LLM converse call with tool response
        mock_tool_call = MagicMock()
        mock_tool_call.name = "generate_sql"  # Set as attribute, not constructor kwarg
        mock_tool_call.input = {
            "summary": "Calculates the sum of all amounts in the test data.",
            "steps": [
                {
                    "step_id": "sum",
                    "sql": "SELECT SUM(amount) FROM test_data",
                    "description": "Sum amounts",
                }
            ],
            "final_sql": "SELECT SUM(amount) AS total FROM test_data",
            "column_mappings": {"amount": "amount"},
        }

        mock_tool_response = MagicMock()
        mock_tool_response.tool_calls = [mock_tool_call]
        mock_tool_response.content = None
        agent.provider.converse = MagicMock(return_value=Result.ok(mock_tool_response))

        context = _make_execution_context(duckdb_with_data)

        # Execute
        result = agent.execute(session, sample_graph, context, workspace_id=baseline_run_id())

        assert result.success
        assert result.value is not None
        execution = result.value
        assert execution.graph_id == "test_metric"
        assert execution.output_value == 600.0  # Sum of 100 + 200 + 300


def _agent_with_sql(steps: list[dict[str, str]], final_sql: str) -> GraphAgent:
    """A GraphAgent whose mocked LLM emits the given steps + final SQL."""
    mock_config = MagicMock()
    mock_config.limits.max_output_tokens_per_request = 4000
    mock_config.limits.cache_ttl_seconds = 3600
    mock_renderer = MagicMock()
    mock_renderer.render_split.return_value = ("System prompt", "Test prompt", 0.0)

    agent = GraphAgent(config=mock_config, provider=MagicMock(), prompt_renderer=mock_renderer)
    agent.provider.get_model_for_tier.return_value = "test-model"

    tool_call = MagicMock()
    tool_call.name = "generate_sql"
    tool_call.input = {
        "summary": "test",
        "steps": steps,
        "final_sql": final_sql,
        "column_mappings": {"amount": "amount"},
    }
    response = MagicMock()
    response.tool_calls = [tool_call]
    response.content = None
    agent.provider.converse = MagicMock(return_value=Result.ok(response))
    return agent


class TestGraphAgentVerifier:
    """The post-execution verifier converts silently-wrong metrics into honest fails (DAT-616)."""

    def test_empty_support_extract_fails_grounded_and_retains_failure(
        self, session: Session, duckdb_with_data, sample_graph
    ):
        """An extract whose filter matches no rows is inconclusive, not executed-green.

        Reproduces the long-format finance bug: a SUM over an empty filter (no
        COALESCE mask) yields NULL → the metric stays grounded with a 'no support'
        reason. Flag-not-drop (DAT-543): the failed SQL is RETAINED as a decayed
        snippet (``failure_count > 0``, provenance failure_mode) so the next run can
        feed the agent the prior attempt + reason — but ``find_by_key`` still excludes
        it from reuse, so it never gets promoted as a working snippet.
        """
        from sqlalchemy import select

        from dataraum.query.snippet_models import SQLSnippetRecord

        agent = _agent_with_sql(
            steps=[
                {
                    "step_id": "value",
                    "sql": "SELECT SUM(amount) AS value FROM test_data WHERE id = 999",
                    "description": "empty filter",
                }
            ],
            final_sql="SELECT * FROM value",
        )
        context = _make_execution_context(duckdb_with_data)

        result = agent.execute(session, sample_graph, context, workspace_id=baseline_run_id())

        assert not result.success
        assert "no support" in result.error
        # The bad SQL is retained as a DECAYED failure, never a reusable snippet.
        snippets = list(session.execute(select(SQLSnippetRecord)).scalars().all())
        assert len(snippets) == 1
        retained = snippets[0]
        assert retained.failure_count > 0
        assert retained.provenance.get("failure_mode") == "verifier_rejected"

    def test_genuine_zero_metric_executes(self, session: Session, duckdb_with_data, sample_graph):
        """A metric that genuinely computes 0 (rows matched, summing to 0) passes.

        `id = 1` matches a row; `amount * 0` sums to a real 0 — support exists, so
        the metric is executed with value 0, not rejected as degenerate."""
        agent = _agent_with_sql(
            steps=[
                {
                    "step_id": "value",
                    "sql": "SELECT SUM(amount * 0) AS value FROM test_data WHERE id = 1",
                    "description": "genuine zero with support",
                }
            ],
            final_sql="SELECT * FROM value",
        )
        context = _make_execution_context(duckdb_with_data)

        result = agent.execute(session, sample_graph, context, workspace_id=baseline_run_id())

        assert result.success
        assert result.value.output_value == 0


class TestComposeMetricFromDag:
    """Per-metric composition from the DAG — no cross-metric formula reuse (DAT-646).

    Formulas/constants are composed HERE, not warmed or cached: an EXTRACT leaf uses
    its warmed cached snippet; a FORMULA/CONSTANT is composed from THIS metric's graph.
    So two metrics that share an arithmetic shape can no longer alias a formula snippet
    (the net_margin/ebitda_margin collision). The composer returns ``None`` on a missing
    leaf / malformed step (the caller honest-fails) — never the LLM."""

    @staticmethod
    def _agent() -> GraphAgent:
        return GraphAgent(config=MagicMock(), provider=MagicMock(), prompt_renderer=MagicMock())

    @staticmethod
    def _ext(sid: str) -> GraphStep:
        return GraphStep(
            step_id=sid,
            step_type=StepType.EXTRACT,
            source=StepSource(standard_field=sid, statement="income_statement"),
            aggregation="sum",
        )

    def _metric(self, graph_id: str, steps: dict[str, GraphStep]) -> TransformationGraph:
        return TransformationGraph(
            graph_id=graph_id,
            version="1.0",
            metadata=GraphMetadata(
                name=graph_id, description="", category="profitability", source=GraphSource.SYSTEM
            ),
            output=OutputDef(output_type=OutputType.SCALAR),
            steps=steps,
        )

    def test_formula_composed_from_cached_extract_leaves(self) -> None:
        graph = self._metric(
            "gross_profit",
            {
                "revenue": self._ext("revenue"),
                "cost_of_goods_sold": self._ext("cost_of_goods_sold"),
                "gp": GraphStep(
                    step_id="gp",
                    step_type=StepType.FORMULA,
                    expression="revenue - cost_of_goods_sold",
                    depends_on=["revenue", "cost_of_goods_sold"],
                    output_step=True,
                ),
            },
        )
        cached = {
            "revenue": {"sql": "SELECT 1000 AS value", "description": ""},
            "cost_of_goods_sold": {"sql": "SELECT 600 AS value", "description": ""},
        }
        code = self._agent()._compose_metric_from_dag(graph, cached, {})
        assert code is not None
        assert code.llm_model == "composed"
        # Extract leaves use their cached snippet; the formula is COMPOSED, not looked up.
        by_id = {s["step_id"]: s["sql"] for s in code.steps}
        assert by_id["revenue"] == "SELECT 1000 AS value"
        assert by_id["gp"] == (
            "SELECT ((SELECT value FROM revenue) - (SELECT value FROM cost_of_goods_sold)) AS value"
        )
        assert code.final_sql == "SELECT * FROM gp"

    def test_missing_extract_leaf_returns_none(self) -> None:
        graph = self._metric(
            "gross_profit",
            {
                "revenue": self._ext("revenue"),
                "cost_of_goods_sold": self._ext("cost_of_goods_sold"),
                "gp": GraphStep(
                    step_id="gp",
                    step_type=StepType.FORMULA,
                    expression="revenue - cost_of_goods_sold",
                    depends_on=["revenue", "cost_of_goods_sold"],
                    output_step=True,
                ),
            },
        )
        # cost_of_goods_sold leaf absent → None (the caller honest-fails it as ungroundable).
        cached = {"revenue": {"sql": "SELECT 1000 AS value", "description": ""}}
        assert self._agent()._compose_metric_from_dag(graph, cached, {}) is None

    def test_constant_composed_from_resolved_param(self) -> None:
        graph = self._metric(
            "dip",
            {
                "out": GraphStep(
                    step_id="out",
                    step_type=StepType.CONSTANT,
                    parameter="days_in_period",
                    output_step=True,
                )
            },
        )
        code = self._agent()._compose_metric_from_dag(graph, {}, {"days_in_period": 30})
        assert code is not None
        assert code.steps[0]["sql"] == "SELECT 30 AS value"
        assert code.final_sql == "SELECT * FROM out"

    def test_constant_without_value_returns_none(self) -> None:
        graph = self._metric(
            "dip",
            {
                "out": GraphStep(
                    step_id="out", step_type=StepType.CONSTANT, parameter="d", output_step=True
                )
            },
        )
        assert self._agent()._compose_metric_from_dag(graph, {}, {}) is None

    def test_formula_without_expression_returns_none(self) -> None:
        graph = self._metric(
            "bad",
            {
                "out": GraphStep(
                    step_id="out", step_type=StepType.FORMULA, expression="", output_step=True
                )
            },
        )
        assert self._agent()._compose_metric_from_dag(graph, {}, {}) is None

    def test_nested_formula_composes_inner_formula_per_metric(self) -> None:
        """A formula-over-formula composes the INNER formula per-metric (DAT-646): the
        intermediate gross_profit is composed HERE (not from a shared cache), and its
        extract CTEs are materialized before it."""
        graph = self._metric(
            "operating_income",
            {
                "revenue": self._ext("revenue"),
                "cost_of_goods_sold": self._ext("cost_of_goods_sold"),
                "operating_expense": self._ext("operating_expense"),
                "gross_profit": GraphStep(
                    step_id="gross_profit",
                    step_type=StepType.FORMULA,
                    expression="revenue - cost_of_goods_sold",
                    depends_on=["revenue", "cost_of_goods_sold"],
                ),
                "operating_income": GraphStep(
                    step_id="operating_income",
                    step_type=StepType.FORMULA,
                    expression="gross_profit - operating_expense",
                    depends_on=["gross_profit", "operating_expense"],
                    output_step=True,
                ),
            },
        )
        # Only the EXTRACT leaves are cached — gross_profit is NOT (composed per-metric).
        cached = {
            "revenue": {"sql": "SELECT 1000 AS value", "description": ""},
            "cost_of_goods_sold": {"sql": "SELECT 600 AS value", "description": ""},
            "operating_expense": {"sql": "SELECT 100 AS value", "description": ""},
        }
        code = self._agent()._compose_metric_from_dag(graph, cached, {})
        assert code is not None
        step_ids = [s["step_id"] for s in code.steps]
        assert step_ids[-1] == "operating_income"  # output last
        # The inner formula CTE is composed AFTER its extract deps (valid order).
        assert step_ids.index("gross_profit") > step_ids.index("revenue")
        assert step_ids.index("gross_profit") > step_ids.index("cost_of_goods_sold")
        by_id = {s["step_id"]: s["sql"] for s in code.steps}
        assert by_id["gross_profit"] == (
            "SELECT ((SELECT value FROM revenue) - (SELECT value FROM cost_of_goods_sold)) AS value"
        )
        assert by_id["operating_income"] == (
            "SELECT ((SELECT value FROM gross_profit) - "
            "(SELECT value FROM operating_expense)) AS value"
        )
        assert code.final_sql == "SELECT * FROM operating_income"

    def test_same_shape_metrics_compose_distinctly_no_alias(self) -> None:
        """THE DAT-646 fix: two margins with the SAME arithmetic shape compose their own
        SQL — no cross-metric formula aliasing (was: net_margin reused ebitda's CTE)."""

        def _margin(graph_id: str, numerator: str) -> TransformationGraph:
            return self._metric(
                graph_id,
                {
                    numerator: self._ext(numerator),
                    "revenue": self._ext("revenue"),
                    "m": GraphStep(
                        step_id="m",
                        step_type=StepType.FORMULA,
                        expression=f"{numerator} / revenue",
                        depends_on=[numerator, "revenue"],
                        output_step=True,
                    ),
                },
            )

        agent = self._agent()
        leaf = {"sql": "SELECT 1 AS value", "description": ""}
        code_a = agent._compose_metric_from_dag(
            _margin("ebitda_margin", "ebitda"), {"ebitda": leaf, "revenue": leaf}, {}
        )
        code_b = agent._compose_metric_from_dag(
            _margin("net_margin", "net_income"), {"net_income": leaf, "revenue": leaf}, {}
        )
        assert code_a is not None and code_b is not None
        a_sql = next(s["sql"] for s in code_a.steps if s["step_id"] == "m")
        b_sql = next(s["sql"] for s in code_b.steps if s["step_id"] == "m")
        # Each references its OWN numerator — no aliasing to the other metric's operand.
        assert "ebitda" in a_sql and "net_income" not in a_sql
        assert "net_income" in b_sql and "ebitda" not in b_sql

    def test_composed_metric_executes_through_duckdb(self, duckdb_with_data) -> None:
        """End-to-end: a composed extract+formula metric runs through DuckDB to the right
        number — the multi-CTE composition is valid SQL, not just a well-formed string.

        Two same-shape margins (numerator / revenue) compose AND execute distinctly: each
        yields its own value, proving the DAT-646 no-alias fix survives real execution, not
        only string assembly."""

        def _margin(graph_id: str, numerator: str) -> TransformationGraph:
            return self._metric(
                graph_id,
                {
                    numerator: self._ext(numerator),
                    "revenue": self._ext("revenue"),
                    "m": GraphStep(
                        step_id="m",
                        step_type=StepType.FORMULA,
                        expression=f"{numerator} / revenue",
                        depends_on=[numerator, "revenue"],
                        output_step=True,
                    ),
                },
            )

        agent = self._agent()
        context = _make_execution_context(duckdb_with_data)
        # test_data amounts: id1=100, id2=200, id3=300 (total 600).
        revenue = {"sql": "SELECT SUM(amount) AS value FROM test_data", "description": ""}  # 600

        # margin_a = id1 / total = 100/600; margin_b = (id1+id2) / total = 300/600.
        graph_a = _margin("margin_a", "part_a")
        code_a = agent._compose_metric_from_dag(
            graph_a,
            {
                "part_a": {"sql": "SELECT SUM(amount) AS value FROM test_data WHERE id = 1"},
                "revenue": revenue,
            },
            {},
        )
        graph_b = _margin("margin_b", "part_b")
        code_b = agent._compose_metric_from_dag(
            graph_b,
            {
                "part_b": {"sql": "SELECT SUM(amount) AS value FROM test_data WHERE id <= 2"},
                "revenue": revenue,
            },
            {},
        )
        assert code_a is not None and code_b is not None

        res_a = agent._execute_sql(code_a, context, graph_a)
        res_b = agent._execute_sql(code_b, context, graph_b)
        assert res_a.success and res_b.success
        assert res_a.value.output_value == pytest.approx(100 / 600)
        assert res_b.value.output_value == pytest.approx(300 / 600)


class TestSaveComposedSnippets:
    """Per-metric FORMULA/CONSTANT persistence for the cockpit reuse KB (DAT-646 P2).

    The warm pass saves only shared EXTRACT leaves; a metric's composed formula/constants
    are persisted here, sourced to ``graph:{graph_id}`` so the cockpit groups them under
    THIS metric — and formulas are keyed per-source so two same-shape margins never alias."""

    @staticmethod
    def _agent() -> GraphAgent:
        return GraphAgent(config=MagicMock(), provider=MagicMock(), prompt_renderer=MagicMock())

    @staticmethod
    def _ext(sid: str) -> GraphStep:
        return GraphStep(
            step_id=sid,
            step_type=StepType.EXTRACT,
            source=StepSource(standard_field=sid, statement="income_statement"),
            aggregation="sum",
        )

    def _metric(self, graph_id: str, steps: dict[str, GraphStep]) -> TransformationGraph:
        return TransformationGraph(
            graph_id=graph_id,
            version="1.0",
            metadata=GraphMetadata(
                name=graph_id, description="", category="profitability", source=GraphSource.SYSTEM
            ),
            output=OutputDef(output_type=OutputType.SCALAR),
            steps=steps,
        )

    def _margin(self, graph_id: str, numerator: str) -> TransformationGraph:
        return self._metric(
            graph_id,
            {
                numerator: self._ext(numerator),
                "revenue": self._ext("revenue"),
                "m": GraphStep(
                    step_id="m",
                    step_type=StepType.FORMULA,
                    expression=f"{numerator} / revenue",
                    depends_on=[numerator, "revenue"],
                    output_step=True,
                ),
            },
        )

    def _compose_and_save(self, agent, session, graph, cached, resolved_params=None) -> None:
        code = agent._compose_metric_from_dag(graph, cached, resolved_params or {})
        assert code is not None
        agent._save_composed_snippets(
            session=session,
            graph=graph,
            generated_code=code,
            schema_mapping_id="schema_abc",
            resolved_params=resolved_params or {},
            workspace_id=baseline_run_id(),
        )
        session.flush()

    def test_same_shape_metrics_persist_distinct_sourced_snippets(self, session: Session):
        from sqlalchemy import select

        from dataraum.query.snippet_models import SQLSnippetRecord

        agent = self._agent()
        self._compose_and_save(
            agent,
            session,
            self._margin("ebitda_margin", "ebitda"),
            {
                "ebitda": {"sql": "SELECT SUM(amount) AS value FROM t WHERE k='ebitda'"},
                "revenue": {"sql": "SELECT SUM(amount) AS value FROM t"},
            },
        )
        self._compose_and_save(
            agent,
            session,
            self._margin("net_margin", "net_income"),
            {
                "net_income": {"sql": "SELECT SUM(amount) AS value FROM t WHERE k='net_income'"},
                "revenue": {"sql": "SELECT SUM(amount) AS value FROM t"},
            },
        )

        formulas = list(
            session.execute(
                select(SQLSnippetRecord).where(SQLSnippetRecord.snippet_type == "formula")
            ).scalars()
        )
        assert len(formulas) == 2
        by_source = {f.source: f for f in formulas}
        assert set(by_source) == {"graph:ebitda_margin", "graph:net_margin"}
        # Each metric's snippet sql is its OWN standalone computation (extract CTEs + the
        # formula), with no operand from the sibling metric — the no-alias guarantee.
        assert "ebitda" in by_source["graph:ebitda_margin"].sql
        assert "net_income" not in by_source["graph:ebitda_margin"].sql
        assert "net_income" in by_source["graph:net_margin"].sql
        assert "ebitda" not in by_source["graph:net_margin"].sql
        # The formula snippet is the WHOLE metric as one statement (a WITH composition).
        assert by_source["graph:ebitda_margin"].sql.startswith("WITH")

    def test_resave_is_idempotent(self, session: Session):
        from sqlalchemy import func, select

        from dataraum.query.snippet_models import SQLSnippetRecord

        agent = self._agent()
        graph = self._margin("net_margin", "net_income")
        cached = {
            "net_income": {"sql": "SELECT 1 AS value"},
            "revenue": {"sql": "SELECT 2 AS value"},
        }
        self._compose_and_save(agent, session, graph, cached)
        self._compose_and_save(agent, session, graph, cached)  # re-run = no-op

        total = session.scalar(
            select(func.count())
            .select_from(SQLSnippetRecord)
            .where(SQLSnippetRecord.snippet_type == "formula")
        )
        assert total == 1

    def test_constant_step_persisted_keyed_by_param_value(self, session: Session):
        from sqlalchemy import select

        from dataraum.query.snippet_models import SQLSnippetRecord

        agent = self._agent()
        graph = self._metric(
            "dso",
            {
                "ar": self._ext("accounts_receivable"),
                "revenue": self._ext("revenue"),
                "dip": GraphStep(
                    step_id="dip",
                    step_type=StepType.CONSTANT,
                    parameter="days_in_period",
                ),
                "m": GraphStep(
                    step_id="m",
                    step_type=StepType.FORMULA,
                    expression="ar / revenue * dip",
                    depends_on=["ar", "revenue", "dip"],
                    output_step=True,
                ),
            },
        )
        self._compose_and_save(
            agent,
            session,
            graph,
            {
                "ar": {"sql": "SELECT 100 AS value"},
                "revenue": {"sql": "SELECT 1000 AS value"},
            },
            resolved_params={"days_in_period": 30},
        )

        const = session.execute(
            select(SQLSnippetRecord).where(SQLSnippetRecord.snippet_type == "constant")
        ).scalar_one()
        assert const.standard_field == "days_in_period"
        assert const.parameter_value == "30"
        assert const.source == "graph:dso"


class TestGraphAgentSnippets:
    """Tests for GraphAgent snippet lifecycle."""

    def test_execute_saves_snippets(
        self,
        session: Session,
        duckdb_with_data,
        sample_graph,
    ):
        """Test that executing a graph saves SQL snippets."""
        from sqlalchemy import select

        from dataraum.query.snippet_models import SQLSnippetRecord

        mock_provider = MagicMock()
        mock_provider.get_model_for_tier.return_value = "test-model"

        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000
        mock_config.limits.cache_ttl_seconds = 3600

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "Test prompt", 0.0)

        agent = GraphAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
        )

        # Mock LLM response with step matching the graph's "value" extract step
        mock_tool_call = MagicMock()
        mock_tool_call.name = "generate_sql"
        mock_tool_call.input = {
            "summary": "Extracts sum of amounts.",
            "steps": [
                {
                    "step_id": "value",
                    "sql": "SELECT SUM(amount) AS value FROM test_data",
                    "description": "Sum amounts from test data",
                }
            ],
            "final_sql": "SELECT * FROM value",
            "column_mappings": {"amount": "amount"},
        }

        mock_response = MagicMock()
        mock_response.tool_calls = [mock_tool_call]
        mock_response.content = None
        agent.provider.converse = MagicMock(return_value=Result.ok(mock_response))

        context = _make_execution_context(duckdb_with_data)

        result = agent.execute(session, sample_graph, context, workspace_id=baseline_run_id())
        assert result.success

        # Verify snippet was saved
        snippets = list(session.execute(select(SQLSnippetRecord)).scalars().all())
        assert len(snippets) >= 1

        extract_snippet = next((s for s in snippets if s.snippet_type == "extract"), None)
        assert extract_snippet is not None
        assert extract_snippet.standard_field == "test_field"
        assert extract_snippet.statement == "test_table"
        assert extract_snippet.schema_mapping_id == "test-mapping"
        assert "SUM(amount)" in extract_snippet.sql

    def test_execute_reuses_snippets(
        self,
        session: Session,
        duckdb_with_data,
        sample_graph,
    ):
        """Test that second execution reuses snippets without LLM call."""
        from dataraum.query.snippet_library import SnippetLibrary

        mock_provider = MagicMock()
        mock_provider.get_model_for_tier.return_value = "test-model"

        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000
        mock_config.limits.cache_ttl_seconds = 3600

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "Test prompt", 0.0)

        # Pre-populate snippet library with a matching snippet
        library = SnippetLibrary(session, workspace_id=baseline_run_id())
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) AS value FROM test_data",
            description="Sum amounts from test data",
            schema_mapping_id="test-mapping",
            source="graph:test_metric",
            standard_field="test_field",
            statement="test_table",
            aggregation="sum",
        )
        session.flush()

        agent = GraphAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
        )

        context = _make_execution_context(duckdb_with_data)

        # Execute — should assemble from snippets (no LLM call)
        result = agent.execute(session, sample_graph, context, workspace_id=baseline_run_id())
        assert result.success
        assert result.value.output_value == 600.0  # 100 + 200 + 300
        assert agent.provider.converse.call_count == 0  # No LLM call needed

    def test_snippet_usage_tracked_on_assembly(
        self,
        session: Session,
        duckdb_with_data,
        sample_graph,
    ):
        """Test that snippet usage is tracked when assembled from cache."""
        from sqlalchemy import select

        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_models import SnippetUsageRecord

        mock_provider = MagicMock()
        mock_provider.get_model_for_tier.return_value = "test-model"

        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000
        mock_config.limits.cache_ttl_seconds = 3600

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "Test prompt", 0.0)

        # Pre-populate snippet
        library = SnippetLibrary(session, workspace_id=baseline_run_id())
        snippet = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) AS value FROM test_data",
            description="Sum amounts",
            schema_mapping_id="test-mapping",
            source="graph:test_metric",
            standard_field="test_field",
            statement="test_table",
            aggregation="sum",
        )
        session.flush()

        agent = GraphAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
        )

        context = _make_execution_context(duckdb_with_data)

        result = agent.execute(session, sample_graph, context, workspace_id=baseline_run_id())
        assert result.success

        # Verify usage record was created
        usages = list(session.execute(select(SnippetUsageRecord)).scalars().all())
        assert len(usages) >= 1

        # Should be an exact_reuse since snippet was assembled without LLM
        exact_reuse = next((u for u in usages if u.usage_type == "exact_reuse"), None)
        assert exact_reuse is not None
        assert exact_reuse.execution_type == "graph"
        assert exact_reuse.snippet_id == snippet.snippet_id

    def test_snippet_column_mappings_preserved(
        self,
        session: Session,
        duckdb_with_data,
        sample_graph,
    ):
        """Test that column_mappings are preserved when assembling from snippets."""
        from dataraum.query.snippet_library import SnippetLibrary

        mock_provider = MagicMock()
        mock_provider.get_model_for_tier.return_value = "test-model"

        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000
        mock_config.limits.cache_ttl_seconds = 3600

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "Test prompt", 0.0)

        # Pre-populate snippet with column_mappings
        library = SnippetLibrary(session, workspace_id=baseline_run_id())
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) AS value FROM test_data",
            description="Sum amounts",
            schema_mapping_id="test-mapping",
            source="graph:test_metric",
            standard_field="test_field",
            statement="test_table",
            aggregation="sum",
            column_mappings={"test_field": "amount"},
        )
        session.flush()

        agent = GraphAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
        )

        # Access internal _lookup_snippets to verify column_mappings are returned
        cached = agent._lookup_snippets(session, sample_graph, "test-mapping")

        assert "value" in cached
        assert cached["value"]["column_mappings"] == {"test_field": "amount"}

    def test_usage_tracked_without_cached_snippets(
        self,
        session: Session,
        duckdb_with_data,
        sample_graph,
    ):
        """Test that usage is tracked on first-time execution (no cached snippets)."""
        from sqlalchemy import select

        from dataraum.query.snippet_models import SnippetUsageRecord

        mock_provider = MagicMock()
        mock_provider.get_model_for_tier.return_value = "test-model"

        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000
        mock_config.limits.cache_ttl_seconds = 3600

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "Test prompt", 0.0)

        agent = GraphAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
        )

        # Mock LLM response
        mock_tool_call = MagicMock()
        mock_tool_call.name = "generate_sql"
        mock_tool_call.input = {
            "summary": "Extracts sum of amounts.",
            "steps": [
                {
                    "step_id": "value",
                    "sql": "SELECT SUM(amount) AS value FROM test_data",
                    "description": "Sum amounts from test data",
                }
            ],
            "final_sql": "SELECT * FROM value",
            "column_mappings": {"amount": "amount"},
        }

        mock_response = MagicMock()
        mock_response.tool_calls = [mock_tool_call]
        mock_response.content = None
        agent.provider.converse = MagicMock(return_value=Result.ok(mock_response))

        context = _make_execution_context(duckdb_with_data)

        # Execute — no cached snippets (first time), should still track usage
        result = agent.execute(session, sample_graph, context, workspace_id=baseline_run_id())
        assert result.success

        # Verify usage records were created
        usages = list(session.execute(select(SnippetUsageRecord)).scalars().all())
        assert len(usages) >= 1

        # All steps should be newly_generated
        newly_generated = [u for u in usages if u.usage_type == "newly_generated"]
        assert len(newly_generated) >= 1
        assert newly_generated[0].execution_type == "graph"


class TestPriorContextFeedback:
    """DAT-616 feedback loops: prior honest-fail reason + prior groundings."""

    def _agent(self) -> GraphAgent:
        return GraphAgent(config=MagicMock(), provider=MagicMock(), prompt_renderer=MagicMock())

    def test_prior_groundings_from_cached_snippets(self) -> None:
        """column_mappings_basis on a cached snippet is fed back as a prior grounding."""
        session = MagicMock()
        # No prior lifecycle reason.
        session.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
            None
        )
        graph = MagicMock()
        graph.graph_id = "gross_margin"
        cached = {
            "revenue": {
                "sql": "SELECT 1 AS value",
                "column_mappings_basis": {
                    "revenue": {"column": "account_type", "filter": "IN (...)"}
                },
            }
        }
        out = self._agent()._build_prior_context(session, graph, cached, "default")
        assert "Prior value→concept groundings" in out
        assert "account_type" in out

    def test_prior_reason_fed_back(self) -> None:
        """A prior run's honest-fail state_reason is fed back verbatim with an abstain steer."""
        session = MagicMock()
        prior = MagicMock()
        # Sentinel reason — the test asserts the MECHANISM (the prior reason is
        # echoed into the next run's context), not any domain-specific wording.
        prior.state_reason = "SENTINEL_PRIOR_REASON"
        session.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
            prior
        )
        graph = MagicMock()
        graph.graph_id = "some_metric"
        out = self._agent()._build_prior_context(session, graph, None, "default")
        assert "Last run this metric was flagged" in out
        assert "SENTINEL_PRIOR_REASON" in out
        assert "abstain" in out

    def test_empty_when_nothing_prior(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
            None
        )
        graph = MagicMock()
        graph.graph_id = "g"
        assert self._agent()._build_prior_context(session, graph, None, "default") == ""

    def test_retained_failure_fed_back(self, session: Session, sample_graph) -> None:
        """DAT-543: a retained FAILED extract's SQL + reason reach the next authoring.

        The retain-don't-drop PAYOFF, end-to-end over a REAL graph (the MagicMock
        graphs above yield no steps, so they never exercise this block). Save a failed
        snippet under ``sample_graph``'s EXTRACT key, then assert ``_build_prior_context``
        feeds the exact prior SQL + reason back with the revise/abstain steer. This is
        the test that fails loudly if the save↔read key or scoping ever drifts.
        """
        from dataraum.query.snippet_library import SnippetLibrary

        # sample_graph's lone EXTRACT step: standard_field="test_field",
        # statement="test_table", aggregation="sum". Save a matching failed snippet.
        SnippetLibrary(session, workspace_id=baseline_run_id()).save_snippet(
            snippet_type="extract",
            sql='SELECT SUM(x) AS value FROM t WHERE "period" = (SELECT MAX("period") FROM t)',
            description="prior attempt",
            schema_mapping_id="default",
            source="graph:test_metric",
            standard_field="test_field",
            statement="test_table",
            aggregation="sum",
            provenance={
                "failure_mode": "verifier_rejected",
                "failure_reason": "SENTINEL_NO_SUPPORT",
            },
            failed=True,
        )
        session.flush()

        out = self._agent()._build_prior_context(session, sample_graph, None, "default")

        assert "verifier_rejected" in out
        assert "SENTINEL_NO_SUPPORT" in out
        assert "SELECT SUM(x)" in out
        assert "do NOT re-emit unchanged" in out

    def test_retained_failure_reuse_excluded_but_fed_back(
        self, session: Session, sample_graph
    ) -> None:
        """A retained failure is fed to prior_context but NOT offered as a reusable snippet.

        Guards the two halves of flag-not-drop staying consistent: ``find_by_key`` (reuse)
        must skip a ``failure_count>0`` row while ``_build_prior_context`` still surfaces it.
        """
        from dataraum.query.snippet_library import SnippetLibrary

        lib = SnippetLibrary(session, workspace_id=baseline_run_id())
        lib.save_snippet(
            snippet_type="extract",
            sql="SELECT 1 AS value",
            description="bad",
            schema_mapping_id="default",
            source="graph:test_metric",
            standard_field="test_field",
            statement="test_table",
            aggregation="sum",
            provenance={"failure_mode": "execution_failed", "failure_reason": "boom"},
            failed=True,
        )
        session.flush()

        # Reuse must NOT return it.
        assert (
            lib.find_by_key(
                snippet_type="extract",
                schema_mapping_id="default",
                standard_field="test_field",
                statement="test_table",
                aggregation="sum",
            )
            is None
        )
        # …but prior_context must.
        out = self._agent()._build_prior_context(session, sample_graph, None, "default")
        assert "boom" in out
