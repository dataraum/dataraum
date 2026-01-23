"""Tests for the QueryAgent.

Tests cover:
- SQL generation from natural language questions
- SQL execution and safety checks
- Schema info building
- Answer formatting
- Contract-based confidence evaluation
"""

from unittest.mock import MagicMock

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.core.models.base import Result
from dataraum.entropy.contracts import ConfidenceLevel
from dataraum.graphs.context import ColumnContext, GraphExecutionContext, TableContext
from dataraum.query.agent import QueryAgent, QueryContext
from dataraum.query.models import QueryAnalysisOutput


@pytest.fixture
def duckdb_with_data():
    """Create a DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            amount DECIMAL(10,2),
            status VARCHAR
        )
    """
    )
    conn.execute(
        """
        INSERT INTO orders VALUES
            (1, 100, 150.00, 'completed'),
            (2, 101, 250.00, 'completed'),
            (3, 100, 100.00, 'pending'),
            (4, 102, 300.00, 'completed')
    """
    )
    yield conn
    conn.close()


@pytest.fixture
def sample_execution_context(duckdb_with_data) -> GraphExecutionContext:
    """Create a sample execution context."""
    return GraphExecutionContext(
        tables=[
            TableContext(
                table_id="orders_001",
                table_name="orders",
                row_count=4,
                columns=[
                    ColumnContext(
                        column_id="col_id",
                        column_name="id",
                        table_name="orders",
                        data_type="INTEGER",
                        semantic_role="identifier",
                    ),
                    ColumnContext(
                        column_id="col_customer",
                        column_name="customer_id",
                        table_name="orders",
                        data_type="INTEGER",
                        semantic_role="foreign_key",
                    ),
                    ColumnContext(
                        column_id="col_amount",
                        column_name="amount",
                        table_name="orders",
                        data_type="DECIMAL",
                        semantic_role="measure",
                        business_concept="revenue",
                    ),
                    ColumnContext(
                        column_id="col_status",
                        column_name="status",
                        table_name="orders",
                        data_type="VARCHAR",
                        semantic_role="category",
                    ),
                ],
            )
        ]
    )


@pytest.fixture
def mock_agent() -> QueryAgent:
    """Create a QueryAgent with mocked dependencies."""
    mock_config = MagicMock()
    mock_config.limits.max_output_tokens_per_request = 4000

    return QueryAgent(
        config=mock_config,
        provider=MagicMock(),
        prompt_renderer=MagicMock(),
        cache=MagicMock(),
    )


class TestQueryContext:
    """Tests for QueryContext dataclass."""

    def test_create_context(self, duckdb_with_data, session: Session):
        """Test creating a QueryContext."""
        context = QueryContext(
            session=session,
            duckdb_conn=duckdb_with_data,
            table_ids=["orders_001"],
            source_id="source_123",
        )

        assert context.table_ids == ["orders_001"]
        assert context.source_id == "source_123"
        assert context.execution_context is None
        assert context.auto_contract is False

    def test_context_with_auto_contract(self, duckdb_with_data, session: Session):
        """Test creating a QueryContext with auto contract."""
        context = QueryContext(
            session=session,
            duckdb_conn=duckdb_with_data,
            table_ids=["orders_001"],
            auto_contract=True,
        )

        assert context.auto_contract is True
        assert context.contract_name is None


class TestQueryAgentSchemaBuilding:
    """Tests for QueryAgent schema building."""

    def test_build_schema_info(self, mock_agent, sample_execution_context):
        """Test building schema information for prompt."""
        schema_info = mock_agent._build_schema_info(sample_execution_context)

        assert "tables" in schema_info
        assert len(schema_info["tables"]) == 1

        table = schema_info["tables"][0]
        assert table["name"] == "orders"
        assert table["row_count"] == 4
        assert len(table["columns"]) == 4

        # Check column with business concept
        amount_col = next(c for c in table["columns"] if c["name"] == "amount")
        assert amount_col["data_type"] == "DECIMAL"
        assert amount_col["semantic_role"] == "measure"
        assert amount_col["business_concept"] == "revenue"

    def test_build_schema_info_multiple_tables(self, mock_agent):
        """Test schema building with multiple tables."""
        context = GraphExecutionContext(
            tables=[
                TableContext(
                    table_id="t1",
                    table_name="customers",
                    row_count=100,
                    columns=[
                        ColumnContext(
                            column_id="c1",
                            column_name="id",
                            table_name="customers",
                            data_type="INTEGER",
                        ),
                    ],
                ),
                TableContext(
                    table_id="t2",
                    table_name="orders",
                    row_count=500,
                    columns=[
                        ColumnContext(
                            column_id="c2",
                            column_name="customer_id",
                            table_name="orders",
                            data_type="INTEGER",
                        ),
                    ],
                ),
            ]
        )

        schema_info = mock_agent._build_schema_info(context)

        assert len(schema_info["tables"]) == 2
        table_names = [t["name"] for t in schema_info["tables"]]
        assert "customers" in table_names
        assert "orders" in table_names


class TestQueryAgentExecution:
    """Tests for QueryAgent SQL execution."""

    def test_execute_valid_select(self, mock_agent, duckdb_with_data):
        """Test executing a valid SELECT query."""
        result = mock_agent._execute_query(
            sql="SELECT SUM(amount) as total FROM orders WHERE status = 'completed'",
            duckdb_conn=duckdb_with_data,
        )

        assert result.success
        columns, data = result.value
        assert columns == ["total"]
        assert len(data) == 1
        assert data[0]["total"] == 700.00  # 150 + 250 + 300

    def test_execute_multiple_rows(self, mock_agent, duckdb_with_data):
        """Test executing a query returning multiple rows."""
        result = mock_agent._execute_query(
            sql="SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id ORDER BY customer_id",
            duckdb_conn=duckdb_with_data,
        )

        assert result.success
        columns, data = result.value
        assert columns == ["customer_id", "total"]
        assert len(data) == 3
        # Customer 100: 150 + 100 = 250
        assert data[0]["total"] == 250.00

    def test_execute_invalid_sql(self, mock_agent, duckdb_with_data):
        """Test executing invalid SQL."""
        result = mock_agent._execute_query(
            sql="SELECT * FROM nonexistent_table",
            duckdb_conn=duckdb_with_data,
        )

        assert not result.success
        assert "SQL execution failed" in result.error

    def test_execute_blocks_insert(self, mock_agent, duckdb_with_data):
        """Test that INSERT statements are blocked."""
        result = mock_agent._execute_query(
            sql="INSERT INTO orders VALUES (5, 103, 400.00, 'new')",
            duckdb_conn=duckdb_with_data,
        )

        assert not result.success
        assert "dangerous keyword: INSERT" in result.error

    def test_execute_blocks_delete(self, mock_agent, duckdb_with_data):
        """Test that DELETE statements are blocked."""
        result = mock_agent._execute_query(
            sql="DELETE FROM orders WHERE id = 1",
            duckdb_conn=duckdb_with_data,
        )

        assert not result.success
        assert "dangerous keyword: DELETE" in result.error

    def test_execute_blocks_drop(self, mock_agent, duckdb_with_data):
        """Test that DROP statements are blocked."""
        result = mock_agent._execute_query(
            sql="DROP TABLE orders",
            duckdb_conn=duckdb_with_data,
        )

        assert not result.success
        assert "dangerous keyword: DROP" in result.error


class TestQueryAgentFormatting:
    """Tests for QueryAgent response formatting."""

    def test_format_scalar_answer(self, mock_agent):
        """Test formatting a scalar result."""
        answer = mock_agent._format_answer(
            question="What is total revenue?",
            data=[{"total": 1000.00}],
            columns=["total"],
            metric_type="scalar",
            assumptions=[],
            confidence_level=ConfidenceLevel.GREEN,
        )

        assert "1000" in answer
        assert "answer is" in answer.lower()

    def test_format_tabular_answer(self, mock_agent):
        """Test formatting a tabular result."""
        answer = mock_agent._format_answer(
            question="Show revenue by customer",
            data=[
                {"customer_id": 100, "total": 250.00},
                {"customer_id": 101, "total": 250.00},
            ],
            columns=["customer_id", "total"],
            metric_type="table",
            assumptions=[],
            confidence_level=ConfidenceLevel.GREEN,
        )

        assert "2 result(s)" in answer
        assert "2 column(s)" in answer

    def test_format_empty_result(self, mock_agent):
        """Test formatting empty result."""
        answer = mock_agent._format_answer(
            question="Any orders over 1000?",
            data=[],
            columns=["id"],
            metric_type="scalar",
            assumptions=[],
            confidence_level=ConfidenceLevel.YELLOW,
        )

        assert "no data" in answer.lower()

    def test_format_none_data(self, mock_agent):
        """Test formatting when data is None."""
        answer = mock_agent._format_answer(
            question="What happened?",
            data=None,
            columns=None,
            metric_type="scalar",
            assumptions=[],
            confidence_level=ConfidenceLevel.RED,
        )

        assert "unable" in answer.lower()


class TestQueryAgentIntegration:
    """Integration tests for QueryAgent with mocked LLM."""

    def test_generate_query_with_tool_response(self, sample_execution_context):
        """Test SQL generation with mocked LLM tool response."""
        # Create agent with mocked dependencies
        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "User prompt", 0.0)

        mock_provider = MagicMock()

        agent = QueryAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
            cache=MagicMock(),
        )

        # Create mock tool response
        mock_tool_call = MagicMock()
        mock_tool_call.name = "analyze_query"
        mock_tool_call.input = {
            "summary": "Calculates total revenue from completed orders only.",
            "interpreted_question": "Calculate total revenue from completed orders",
            "metric_type": "scalar",
            "steps": [],
            "final_sql": "SELECT SUM(amount) FROM orders WHERE status = 'completed'",
            "column_mappings": {"revenue": "amount"},
            "assumptions": [],
            "validation_notes": [],
        }

        mock_response = MagicMock()
        mock_response.tool_calls = [mock_tool_call]
        mock_response.content = None

        agent.provider.converse = MagicMock(return_value=Result.ok(mock_response))

        # Call _generate_query
        result = agent._generate_query(
            question="What was total revenue from completed orders?",
            execution_id="test-exec-123",
            execution_context=sample_execution_context,
            entropy_context=None,
        )

        assert result.success
        output = result.value
        assert isinstance(output, QueryAnalysisOutput)
        assert output.metric_type == "scalar"
        assert "SUM(amount)" in output.final_sql
        assert output.interpreted_question == "Calculate total revenue from completed orders"

    def test_generate_query_without_tool_call(self, sample_execution_context):
        """Test handling when LLM doesn't use tool."""
        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "User prompt", 0.0)

        agent = QueryAgent(
            config=mock_config,
            provider=MagicMock(),
            prompt_renderer=mock_renderer,
            cache=MagicMock(),
        )

        # Response without tool calls
        mock_response = MagicMock()
        mock_response.tool_calls = []
        mock_response.content = "I cannot answer this question."

        agent.provider.converse = MagicMock(return_value=Result.ok(mock_response))

        result = agent._generate_query(
            question="What is the meaning of life?",
            execution_id="test-exec-456",
            execution_context=sample_execution_context,
            entropy_context=None,
        )

        assert not result.success
        assert "did not use" in result.error.lower() or "tool" in result.error.lower()

    def test_generate_query_wrong_tool_name(self, sample_execution_context):
        """Test handling when LLM uses wrong tool."""
        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "User prompt", 0.0)

        agent = QueryAgent(
            config=mock_config,
            provider=MagicMock(),
            prompt_renderer=mock_renderer,
            cache=MagicMock(),
        )

        # Tool call with wrong name
        mock_tool_call = MagicMock()
        mock_tool_call.name = "wrong_tool"
        mock_tool_call.input = {}

        mock_response = MagicMock()
        mock_response.tool_calls = [mock_tool_call]
        mock_response.content = None

        agent.provider.converse = MagicMock(return_value=Result.ok(mock_response))

        result = agent._generate_query(
            question="Test question",
            execution_id="test-exec-789",
            execution_context=sample_execution_context,
            entropy_context=None,
        )

        assert not result.success
        assert "wrong_tool" in result.error

    def test_generate_query_llm_failure(self, sample_execution_context):
        """Test handling LLM call failure."""
        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000

        mock_renderer = MagicMock()
        mock_renderer.render_split.return_value = ("System prompt", "User prompt", 0.0)

        agent = QueryAgent(
            config=mock_config,
            provider=MagicMock(),
            prompt_renderer=mock_renderer,
            cache=MagicMock(),
        )

        # LLM returns failure
        agent.provider.converse = MagicMock(return_value=Result.fail("API rate limit exceeded"))

        result = agent._generate_query(
            question="Test question",
            execution_id="test-exec-fail",
            execution_context=sample_execution_context,
            entropy_context=None,
        )

        assert not result.success
        assert "rate limit" in result.error.lower() or "failed" in result.error.lower()

    def test_generate_query_prompt_render_failure(self, sample_execution_context):
        """Test handling prompt rendering failure."""
        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000

        mock_renderer = MagicMock()
        mock_renderer.render_split.side_effect = FileNotFoundError("Prompt template not found")

        agent = QueryAgent(
            config=mock_config,
            provider=MagicMock(),
            prompt_renderer=mock_renderer,
            cache=MagicMock(),
        )

        result = agent._generate_query(
            question="Test question",
            execution_id="test-exec-render-fail",
            execution_context=sample_execution_context,
            entropy_context=None,
        )

        assert not result.success
        assert "prompt" in result.error.lower()
