"""Tests for the GraphAgent.

Tests cover:
- SQL generation from graph specifications
- Caching (in-memory and database)
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
            graph_version="1.0",
            schema_mapping_id="mapping-456",
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

    def test_describe_nonexistent_table(self, duckdb_with_data):
        """Test describing a table that doesn't exist returns None."""
        result = GraphAgent._describe_table(duckdb_with_data, "nonexistent")
        assert result is None


class TestGraphAgentCaching:
    """Tests for GraphAgent caching behavior."""

    def test_cache_key_generation(self, sample_graph):
        """Test that cache keys are generated correctly."""
        # Create agent with mocked dependencies
        agent = GraphAgent(
            config=MagicMock(),
            provider=MagicMock(),
            prompt_renderer=MagicMock(),
        )

        key1 = agent._cache_key(sample_graph, "mapping-1")
        key2 = agent._cache_key(sample_graph, "mapping-2")

        assert key1 == "test_metric:1.0:mapping-1"
        assert key2 == "test_metric:1.0:mapping-2"
        assert key1 != key2


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
        result = agent.execute(session, sample_graph, context)

        assert result.success
        assert result.value is not None
        execution = result.value
        assert execution.graph_id == "test_metric"
        assert execution.output_value == 600.0  # Sum of 100 + 200 + 300

    def test_execute_uses_cache_on_second_call(
        self,
        session: Session,
        duckdb_with_data,
        sample_graph,
    ):
        """Test that second execution uses cached code."""
        # Setup mocks
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

        # First execution - should call LLM
        result1 = agent.execute(session, sample_graph, context)
        assert result1.success
        assert agent.provider.converse.call_count == 1

        # Second execution - should use in-memory cache
        result2 = agent.execute(session, sample_graph, context)
        assert result2.success
        assert agent.provider.converse.call_count == 1  # No additional call

        # Both should produce same result
        assert result1.value.output_value == result2.value.output_value


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

        result = agent.execute(session, sample_graph, context)
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
        library = SnippetLibrary(session)
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
        result = agent.execute(session, sample_graph, context)
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
        library = SnippetLibrary(session)
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

        result = agent.execute(session, sample_graph, context)
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
        library = SnippetLibrary(session)
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
        result = agent.execute(session, sample_graph, context)
        assert result.success

        # Verify usage records were created
        usages = list(session.execute(select(SnippetUsageRecord)).scalars().all())
        assert len(usages) >= 1

        # All steps should be newly_generated
        newly_generated = [u for u in usages if u.usage_type == "newly_generated"]
        assert len(newly_generated) >= 1
        assert newly_generated[0].execution_type == "graph"
