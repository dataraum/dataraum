"""Tests for the QueryAgent.

Tests cover:
- SQL generation from natural language questions
- SQL execution and safety checks
- Schema info building
- Answer formatting
- Contract-based confidence evaluation
"""

from unittest.mock import MagicMock, patch

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
        # Table name is prefixed with 'typed_' to match DuckDB table names
        assert table["name"] == "typed_orders"
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
        # Table names are prefixed with 'typed_' to match DuckDB table names
        assert "typed_customers" in table_names
        assert "typed_orders" in table_names


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
        )

        result = agent._generate_query(
            question="Test question",
            execution_id="test-exec-render-fail",
            execution_context=sample_execution_context,
            entropy_context=None,
        )

        assert not result.success
        assert "prompt" in result.error.lower()


class TestQueryAgentSnippets:
    """Tests for QueryAgent snippet lifecycle."""

    @pytest.fixture
    def mock_manager(self):
        """Create a mock ConnectionManager for snippet library tests."""
        return MagicMock()

    def test_discover_snippets_returns_matching(self, mock_agent, session: Session):
        """Test that _discover_snippets returns snippets via similarity search."""
        from dataraum.query.snippet_library import SnippetLibrary, SnippetMatch

        mgr = MagicMock()

        # Save a snippet
        library = SnippetLibrary(session, mgr)
        snippet = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) AS revenue FROM typed_orders",
            description="Sum of order amounts (revenue)",
            schema_mapping_id="source_123",
            source="graph:revenue",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            confidence=1.0,
        )
        session.flush()

        # Mock find_by_similarity to return the snippet
        mock_match = SnippetMatch(
            snippet=snippet,
            match_confidence=0.85,
            match_strategy="semantic_similarity",
        )

        with patch.object(
            SnippetLibrary, "find_by_similarity", return_value=[mock_match]
        ):
            snippets = mock_agent._discover_snippets(
                session=session,
                question="What is total revenue?",
                schema_mapping_id="source_123",
                manager=mgr,
            )

        assert len(snippets) == 1
        assert snippets[0]["step_id"] == "revenue"
        assert "SUM(amount)" in snippets[0]["sql"]
        assert snippets[0]["snippet_id"] is not None

    def test_discover_snippets_empty_when_no_match(
        self, mock_agent, session: Session
    ):
        """Test that _discover_snippets returns empty when similarity finds nothing."""
        from dataraum.query.snippet_library import SnippetLibrary

        mgr = MagicMock()

        # Save snippet for a different schema
        library = SnippetLibrary(session, mgr)
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="other_source",
            source="graph:test",
            standard_field="test",
            confidence=1.0,
        )
        session.flush()

        with patch.object(
            SnippetLibrary, "find_by_similarity", return_value=[]
        ):
            snippets = mock_agent._discover_snippets(
                session=session,
                question="What is total revenue?",
                schema_mapping_id="source_123",
                manager=mgr,
            )

        assert len(snippets) == 0

    def test_track_snippet_usage_exact_reuse(
        self, mock_agent, session: Session, mock_manager
    ):
        """Test that _track_snippet_usage records exact reuse correctly."""
        from sqlalchemy import select

        from dataraum.query.models import QueryAnalysisOutput, SQLStepOutput
        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_models import SnippetUsageRecord

        # Create a real snippet to satisfy FK constraint
        library = SnippetLibrary(session, mock_manager)
        snippet = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) FROM typed_orders",
            description="Sum revenue",
            schema_mapping_id="source_123",
            source="graph:test",
            standard_field="revenue",
            confidence=1.0,
        )
        session.flush()

        analysis = QueryAnalysisOutput(
            summary="Test",
            interpreted_question="Test",
            metric_type="scalar",
            steps=[
                SQLStepOutput(
                    step_id="revenue",
                    sql="SELECT SUM(amount) FROM typed_orders",
                    description="Sum revenue",
                ),
            ],
            final_sql="SELECT * FROM revenue",
        )

        provided = {
            "revenue": {
                "step_id": "revenue",
                "sql": "SELECT SUM(amount) FROM typed_orders",
                "snippet_id": snippet.snippet_id,
                "confidence": 1.0,
            },
        }

        mock_agent._track_snippet_usage(
            session=session,
            execution_id="exec-001",
            analysis_output=analysis,
            provided_snippets=provided,
            manager=mock_manager,
        )
        session.flush()

        usages = list(session.execute(select(SnippetUsageRecord)).scalars().all())
        assert len(usages) == 1
        assert usages[0].usage_type == "exact_reuse"
        assert usages[0].execution_type == "query"
        assert usages[0].snippet_id == snippet.snippet_id

    def test_track_snippet_usage_newly_generated(
        self, mock_agent, session: Session, mock_manager
    ):
        """Test that _track_snippet_usage records newly generated steps."""
        from sqlalchemy import select

        from dataraum.query.models import QueryAnalysisOutput, SQLStepOutput
        from dataraum.query.snippet_models import SnippetUsageRecord

        analysis = QueryAnalysisOutput(
            summary="Test",
            interpreted_question="Test",
            metric_type="scalar",
            steps=[
                SQLStepOutput(
                    step_id="new_calc",
                    sql="SELECT COUNT(*) FROM typed_orders",
                    description="Count orders",
                ),
            ],
            final_sql="SELECT * FROM new_calc",
        )

        mock_agent._track_snippet_usage(
            session=session,
            execution_id="exec-002",
            analysis_output=analysis,
            provided_snippets={},
            manager=mock_manager,
        )
        session.flush()

        usages = list(session.execute(select(SnippetUsageRecord)).scalars().all())
        assert len(usages) == 1
        assert usages[0].usage_type == "newly_generated"
        assert usages[0].snippet_id is None

    def test_save_novel_snippets(
        self, mock_agent, session: Session, mock_manager
    ):
        """Test that _save_novel_snippets saves new query steps."""
        from sqlalchemy import select

        from dataraum.query.models import QueryAnalysisOutput, SQLStepOutput
        from dataraum.query.snippet_models import SQLSnippetRecord

        analysis = QueryAnalysisOutput(
            summary="Calculates total revenue",
            interpreted_question="What is total revenue?",
            metric_type="scalar",
            steps=[
                SQLStepOutput(
                    step_id="total_rev",
                    sql="SELECT SUM(amount) AS total FROM typed_orders",
                    description="Sum all order amounts",
                ),
            ],
            final_sql="SELECT * FROM total_rev",
            column_mappings={"revenue": "amount"},
        )

        mock_agent._save_novel_snippets(
            session=session,
            execution_id="exec-003",
            analysis_output=analysis,
            schema_mapping_id="source_123",
            provided_snippets={},
            manager=mock_manager,
        )
        session.flush()

        snippets = list(session.execute(select(SQLSnippetRecord)).scalars().all())
        assert len(snippets) == 1
        assert snippets[0].snippet_type == "query"
        assert snippets[0].standard_field == "total_rev"
        assert snippets[0].schema_mapping_id == "source_123"
        assert "SUM(amount)" in snippets[0].sql
        assert snippets[0].column_mappings == {"revenue": "amount"}

    def test_save_novel_snippets_skips_existing(
        self, mock_agent, session: Session, mock_manager
    ):
        """Test that _save_novel_snippets skips steps that came from snippets."""
        from sqlalchemy import select

        from dataraum.query.models import QueryAnalysisOutput, SQLStepOutput
        from dataraum.query.snippet_models import SQLSnippetRecord

        analysis = QueryAnalysisOutput(
            summary="Test",
            interpreted_question="Test",
            metric_type="scalar",
            steps=[
                SQLStepOutput(
                    step_id="reused_step",
                    sql="SELECT SUM(amount) FROM typed_orders",
                    description="Reused from snippet",
                ),
                SQLStepOutput(
                    step_id="new_step",
                    sql="SELECT COUNT(*) FROM typed_orders",
                    description="New step",
                ),
            ],
            final_sql="SELECT * FROM new_step",
        )

        provided = {
            "reused_step": {
                "step_id": "reused_step",
                "sql": "SELECT SUM(amount) FROM typed_orders",
                "snippet_id": "existing-snip",
            },
        }

        mock_agent._save_novel_snippets(
            session=session,
            execution_id="exec-004",
            analysis_output=analysis,
            schema_mapping_id="source_123",
            provided_snippets=provided,
            manager=mock_manager,
        )
        session.flush()

        snippets = list(session.execute(select(SQLSnippetRecord)).scalars().all())
        assert len(snippets) == 1
        assert snippets[0].standard_field == "new_step"

    def test_discover_snippets_includes_query_type(self, mock_agent, session: Session):
        """Test that _discover_snippets returns query-type snippets."""
        from dataraum.query.snippet_library import SnippetLibrary, SnippetMatch

        mgr = MagicMock()

        library = SnippetLibrary(session, mgr)
        snippet = library.save_snippet(
            snippet_type="query",
            sql="SELECT COUNT(*) AS order_count FROM typed_orders",
            description="Count all orders",
            schema_mapping_id="source_123",
            source="query:exec-001",
            standard_field="order_count",
            confidence=0.5,
        )
        session.flush()

        mock_match = SnippetMatch(
            snippet=snippet,
            match_confidence=0.7,
            match_strategy="semantic_similarity",
        )

        with patch.object(
            SnippetLibrary, "find_by_similarity", return_value=[mock_match]
        ):
            snippets = mock_agent._discover_snippets(
                session=session,
                question="How many orders are there?",
                schema_mapping_id="source_123",
                manager=mgr,
            )

        assert len(snippets) == 1
        assert snippets[0]["snippet_type"] == "query"
        assert snippets[0]["step_id"] == "order_count"

    def test_usage_tracked_without_provided_snippets(
        self, mock_agent, session: Session, mock_manager
    ):
        """Test that usage is tracked even when no snippets were provided (first-time)."""
        from sqlalchemy import select

        from dataraum.query.models import QueryAnalysisOutput, SQLStepOutput
        from dataraum.query.snippet_models import SnippetUsageRecord

        analysis = QueryAnalysisOutput(
            summary="Test first-time execution",
            interpreted_question="Test",
            metric_type="scalar",
            steps=[
                SQLStepOutput(
                    step_id="first_calc",
                    sql="SELECT SUM(amount) FROM typed_orders",
                    description="Sum amounts",
                ),
            ],
            final_sql="SELECT * FROM first_calc",
        )

        mock_agent._track_snippet_usage(
            session=session,
            execution_id="exec-first",
            analysis_output=analysis,
            provided_snippets={},
            manager=mock_manager,
        )
        session.flush()

        usages = list(session.execute(select(SnippetUsageRecord)).scalars().all())
        assert len(usages) == 1
        assert usages[0].usage_type == "newly_generated"
        assert usages[0].step_id == "first_calc"
        assert usages[0].snippet_id is None
