"""Tests for the GraphAgent.

Tests cover:
- SQL generation from graph specifications
- Caching (in-memory and database)
- SQL execution
- Error handling
"""

import json
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
    TableSchema,
)
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    GraphType,
    OutputDef,
    OutputType,
    StepSource,
    StepType,
    TransformationGraph,
)
from dataraum.llm.providers.base import LLMResponse


@pytest.fixture
def sample_graph() -> TransformationGraph:
    """Create a simple test graph."""
    return TransformationGraph(
        graph_id="test_metric",
        graph_type=GraphType.METRIC,
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
        requires_filters=[],
        steps={
            "value": GraphStep(
                step_id="value",
                level=1,
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
def mock_llm_response() -> LLMResponse:
    """Create a mock LLM response with generated SQL."""
    return LLMResponse(
        content=json.dumps(
            {
                "summary": "Calculates the sum of all amounts in the test data.",
                "steps": [
                    {
                        "step_id": "value",
                        "sql": "SELECT SUM(amount) as value FROM test_data",
                        "description": "Sum the amount column",
                    }
                ],
                "final_sql": "SELECT SUM(amount) as value FROM test_data",
                "column_mappings": {"test_field": "amount"},
            }
        ),
        model="test-model",
        input_tokens=100,
        output_tokens=50,
    )


@pytest.fixture
def duckdb_with_data():
    """Create a DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE test_data (id INT, amount DECIMAL(10,2))")
    conn.execute("INSERT INTO test_data VALUES (1, 100.00), (2, 200.00), (3, 300.00)")
    yield conn
    conn.close()


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
        assert code.is_validated is False
        assert code.validation_errors == []


class TestExecutionContext:
    """Tests for ExecutionContext dataclass."""

    def test_create_context(self, duckdb_with_data):
        """Test creating an ExecutionContext."""
        context = ExecutionContext(
            duckdb_conn=duckdb_with_data,
            table_name="test_data",
            schema_mapping_id="test-mapping",
        )

        assert context.table_name == "test_data"
        assert context.schema_mapping_id == "test-mapping"
        assert context.period is None


class TestTableSchema:
    """Tests for TableSchema dataclass."""

    def test_create_table_schema(self):
        """Test creating a TableSchema."""
        schema = TableSchema(
            table_name="transactions",
            columns=[
                {"name": "id", "type": "INTEGER", "sample_values": ["1", "2"]},
                {"name": "amount", "type": "DECIMAL", "sample_values": ["100", "200"]},
            ],
            row_count=1000,
        )

        assert schema.table_name == "transactions"
        assert len(schema.columns) == 2
        assert schema.row_count == 1000


class TestGraphAgentCaching:
    """Tests for GraphAgent caching behavior."""

    def test_cache_key_generation(self, sample_graph):
        """Test that cache keys are generated correctly."""
        # Create agent with mocked dependencies
        agent = GraphAgent(
            config=MagicMock(),
            provider=MagicMock(),
            prompt_renderer=MagicMock(),
            cache=MagicMock(),
        )

        key1 = agent._cache_key(sample_graph, "mapping-1")
        key2 = agent._cache_key(sample_graph, "mapping-2")

        assert key1 == "test_metric:1.0:mapping-1"
        assert key2 == "test_metric:1.0:mapping-2"
        assert key1 != key2

    def test_db_cache_save_and_load(self, session: Session, sample_graph):
        """Test saving and loading generated code from database."""
        agent = GraphAgent(
            config=MagicMock(),
            provider=MagicMock(),
            prompt_renderer=MagicMock(),
            cache=MagicMock(),
        )

        # Create generated code
        code = GeneratedCode(
            code_id="test-save-load",
            graph_id=sample_graph.graph_id,
            graph_version=sample_graph.version,
            schema_mapping_id="test-mapping",
            summary="Calculates sum of test values.",
            steps=[{"step_id": "value", "sql": "SELECT 1", "description": "test"}],
            final_sql="SELECT SUM(amount) FROM test",
            column_mappings={"test_field": "amount"},
            llm_model="test-model",
            prompt_hash="hash123",
            generated_at=datetime.now(UTC),
            is_validated=True,
        )

        # Save to DB
        agent._save_to_db(session, code)
        session.commit()

        # Load from DB
        loaded = agent._load_from_db(
            session,
            sample_graph.graph_id,
            sample_graph.version,
            "test-mapping",
        )

        assert loaded is not None
        assert loaded.code_id == code.code_id
        assert loaded.final_sql == code.final_sql
        assert loaded.column_mappings == code.column_mappings
        assert loaded.is_validated is True

    def test_db_cache_miss(self, session: Session):
        """Test that cache miss returns None."""
        agent = GraphAgent(
            config=MagicMock(),
            provider=MagicMock(),
            prompt_renderer=MagicMock(),
            cache=MagicMock(),
        )

        loaded = agent._load_from_db(
            session,
            "nonexistent",
            "1.0",
            "mapping",
        )

        assert loaded is None


class TestGraphAgentExecution:
    """Tests for GraphAgent SQL execution."""

    def test_get_table_schema(self, duckdb_with_data):
        """Test extracting table schema from DuckDB."""
        agent = GraphAgent(
            config=MagicMock(),
            provider=MagicMock(),
            prompt_renderer=MagicMock(),
            cache=MagicMock(),
        )

        context = ExecutionContext(
            duckdb_conn=duckdb_with_data,
            table_name="test_data",
        )

        result = agent._get_table_schema(context)

        assert result.success
        assert result.value is not None
        schema = result.value
        assert schema.table_name == "test_data"
        assert schema.row_count == 3
        assert len(schema.columns) == 2

        # Check column names
        col_names = [c["name"] for c in schema.columns]
        assert "id" in col_names
        assert "amount" in col_names

    def test_validate_sql_valid(self, duckdb_with_data):
        """Test SQL validation with valid SQL."""
        agent = GraphAgent(
            config=MagicMock(),
            provider=MagicMock(),
            prompt_renderer=MagicMock(),
            cache=MagicMock(),
        )

        context = ExecutionContext(
            duckdb_conn=duckdb_with_data,
            table_name="test_data",
        )

        code = GeneratedCode(
            code_id="test",
            graph_id="test",
            graph_version="1.0",
            schema_mapping_id="test",
            summary="Test query.",
            steps=[],
            final_sql="SELECT SUM(amount) FROM test_data",
            column_mappings={},
            llm_model="test",
            prompt_hash="test",
            generated_at=datetime.now(UTC),
        )

        result = agent._validate_sql(code, context)
        assert result.success

    def test_validate_sql_invalid(self, duckdb_with_data):
        """Test SQL validation with invalid SQL."""
        agent = GraphAgent(
            config=MagicMock(),
            provider=MagicMock(),
            prompt_renderer=MagicMock(),
            cache=MagicMock(),
        )

        context = ExecutionContext(
            duckdb_conn=duckdb_with_data,
            table_name="test_data",
        )

        code = GeneratedCode(
            code_id="test",
            graph_id="test",
            graph_version="1.0",
            schema_mapping_id="test",
            summary="Test query.",
            steps=[],
            final_sql="SELECT * FROM nonexistent_table",
            column_mappings={},
            llm_model="test",
            prompt_hash="test",
            generated_at=datetime.now(UTC),
        )

        result = agent._validate_sql(code, context)
        assert not result.success


class TestGraphAgentIntegration:
    """Integration tests for GraphAgent with mocked LLM."""

    def test_execute_with_mocked_llm(
        self,
        session: Session,
        duckdb_with_data,
        sample_graph,
        mock_llm_response,
    ):
        """Test full execution flow with mocked LLM."""
        # Create mocked provider
        mock_provider = MagicMock()
        mock_provider.get_model_for_tier.return_value = "test-model"

        # Mock LLM call
        mock_cache = MagicMock()
        mock_cache.get = MagicMock(return_value=None)
        mock_cache.put = MagicMock()

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
            cache=mock_cache,
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

        context = ExecutionContext(
            duckdb_conn=duckdb_with_data,
            table_name="test_data",
            schema_mapping_id="test-mapping",
        )

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
        mock_llm_response,
    ):
        """Test that second execution uses cached code."""
        # Setup mocks
        mock_provider = MagicMock()
        mock_provider.get_model_for_tier.return_value = "test-model"

        mock_cache = MagicMock()
        mock_cache.get = MagicMock(return_value=None)
        mock_cache.put = MagicMock()

        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000
        mock_config.limits.cache_ttl_seconds = 3600

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "Test prompt", 0.0)

        agent = GraphAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
            cache=mock_cache,
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

        context = ExecutionContext(
            duckdb_conn=duckdb_with_data,
            table_name="test_data",
            schema_mapping_id="test-mapping",
        )

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
