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

    def test_empty_support_extract_fails_grounded_and_caches_nothing(
        self, session: Session, duckdb_with_data, sample_graph
    ):
        """An extract whose filter matches no rows is inconclusive, not executed-green.

        Reproduces the long-format finance bug: a SUM over an empty filter (no
        COALESCE mask) yields NULL → the metric stays grounded with a 'no support'
        reason and its SQL is NOT promoted into the reuse cache.
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
        # The bad SQL must NOT enter the shared snippet cache (the verifier gates it).
        snippets = list(session.execute(select(SQLSnippetRecord)).scalars().all())
        assert snippets == []

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


def _formula_graph() -> TransformationGraph:
    """A single-output FORMULA graph (its composed SQL lives in final_sql)."""
    return TransformationGraph(
        graph_id="gross_profit",
        version="1.0",
        metadata=GraphMetadata(
            name="Gross Profit",
            description="",
            category="test",
            source=GraphSource.SYSTEM,
            tags=[],
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="gp"),
        parameters=[],
        steps={
            "gp": GraphStep(
                step_id="gp",
                step_type=StepType.FORMULA,
                expression="revenue - cost_of_goods_sold",
                depends_on=[],
                output_step=True,
            ),
        },
        interpretation=None,
    )


def _formula_with_deps(expression: str, depends_on: list[str]) -> TransformationGraph:
    """A single formula output step (deps supplied via generated_code, not graph steps)."""
    return TransformationGraph(
        graph_id="margin",
        version="1.0",
        metadata=GraphMetadata(
            name="Margin", description="", category="test", source=GraphSource.SYSTEM, tags=[]
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="m"),
        parameters=[],
        steps={
            "out": GraphStep(
                step_id="out",
                step_type=StepType.FORMULA,
                expression=expression,
                depends_on=depends_on,
                output_step=True,
            ),
        },
        interpretation=None,
    )


class TestFormulaShadowCompare:
    """The deterministic composer shadow-runs beside the LLM path and logs agreement."""

    @staticmethod
    def _gen(steps: list[dict[str, str]]) -> GeneratedCode:
        return GeneratedCode(
            code_id="c",
            graph_id="margin",
            summary="",
            steps=steps,
            final_sql="SELECT 1 AS value",  # the LLM's composition is not used by the shadow
            column_mappings={},
            llm_model="x",
            prompt_hash="y",
            generated_at=datetime.now(UTC),
        )

    def test_shadow_agrees_when_deterministic_matches_llm(self, duckdb_with_data, monkeypatch):
        import dataraum.graphs.agent as agent_module

        agent = _agent_with_sql(steps=[], final_sql="")
        graph = _formula_with_deps(
            "revenue - cost_of_goods_sold", ["revenue", "cost_of_goods_sold"]
        )
        gen = self._gen(
            [
                {"step_id": "revenue", "sql": "SELECT 1000 AS value", "description": ""},
                {"step_id": "cost_of_goods_sold", "sql": "SELECT 600 AS value", "description": ""},
            ]
        )
        context = _make_execution_context(duckdb_with_data)
        log = MagicMock()
        monkeypatch.setattr(agent_module, "logger", log)

        agent._shadow_compare_formula(graph, gen, context, llm_value=400.0)

        log.info.assert_called_once()
        kwargs = log.info.call_args.kwargs
        assert kwargs["agree"] is True
        assert kwargs["deterministic_value"] == 400.0

    def test_shadow_flags_divergence(self, duckdb_with_data, monkeypatch):
        import dataraum.graphs.agent as agent_module

        agent = _agent_with_sql(steps=[], final_sql="")
        graph = _formula_with_deps(
            "revenue - cost_of_goods_sold", ["revenue", "cost_of_goods_sold"]
        )
        gen = self._gen(
            [
                {"step_id": "revenue", "sql": "SELECT 1000 AS value", "description": ""},
                {"step_id": "cost_of_goods_sold", "sql": "SELECT 600 AS value", "description": ""},
            ]
        )
        context = _make_execution_context(duckdb_with_data)
        log = MagicMock()
        monkeypatch.setattr(agent_module, "logger", log)

        # LLM claims 999 but deterministic computes 400 → divergence.
        agent._shadow_compare_formula(graph, gen, context, llm_value=999.0)

        assert log.info.call_args.kwargs["agree"] is False


class TestFormulaSnippetRoundTrip:
    """A formula's composed SQL must persist even when the model omits it from `steps`.

    The generate_sql tool makes `steps` OPTIONAL (default []) and `final_sql`
    REQUIRED — the composition reliably lands in final_sql, not steps. Persisting
    the formula from the optional `steps` left the node grounded-in-binding-map but
    absent-from-cache, so the per-metric assembly could never find it (DAT-636).
    """

    def test_formula_output_saved_from_final_sql_when_steps_empty(
        self, session: Session, duckdb_with_data
    ):
        from sqlalchemy import select

        from dataraum.query.snippet_models import SQLSnippetRecord

        # The bug condition: model returns ONLY final_sql, no entry in `steps`.
        agent = _agent_with_sql(steps=[], final_sql="SELECT 42 AS value")
        context = _make_execution_context(duckdb_with_data)

        result = agent.execute(session, _formula_graph(), context, workspace_id=baseline_run_id())

        assert result.success
        formulas = [
            s
            for s in session.execute(select(SQLSnippetRecord)).scalars().all()
            if s.snippet_type == "formula"
        ]
        assert len(formulas) == 1, "formula snippet must persist from final_sql"
        assert formulas[0].sql == "SELECT 42 AS value"
        assert formulas[0].normalized_expression  # keyed by the normalized expression


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
        cached = agent._lookup_snippets(session, sample_graph, "test-mapping", {})

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
        out = self._agent()._build_prior_context(session, graph, cached)
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
        out = self._agent()._build_prior_context(session, graph, None)
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
        assert self._agent()._build_prior_context(session, graph, None) == ""
