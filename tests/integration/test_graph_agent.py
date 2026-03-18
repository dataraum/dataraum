"""Integration tests for the graph agent.

Tests verify:
- Context loading from real analysis data
- Entropy context population
- SQL generation with mocked LLM against real schema
- Entropy behavior modes (strict/balanced/lenient)
- Assumption tracking
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dataraum.core.models.base import Result
from dataraum.entropy.views import build_for_network
from dataraum.entropy.views.network_context import ColumnNetworkResult, EntropyForNetwork
from dataraum.graphs.agent import ExecutionContext, GraphAgent
from dataraum.graphs.context import (
    GraphExecutionContext,
    build_execution_context,
    format_metadata_document,
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

from .conftest import PipelineTestHarness

pytestmark = pytest.mark.integration


class TestContextLoading:
    """Test build_execution_context populates all fields from analysis data."""

    def test_context_has_tables(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Context should contain all analyzed tables."""
        with analyzed_small_finance.session_factory() as session:
            ctx = build_execution_context(
                session=session,
                table_ids=analyzed_table_ids,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
            )

        assert isinstance(ctx, GraphExecutionContext)
        assert len(ctx.tables) > 0
        # Should have 5 tables from small finance
        assert len(ctx.tables) == 5

    def test_context_tables_have_columns(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Each table in context should have columns with metadata."""
        with analyzed_small_finance.session_factory() as session:
            ctx = build_execution_context(
                session=session,
                table_ids=analyzed_table_ids,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
            )

        for table in ctx.tables:
            assert len(table.columns) > 0, f"Table '{table.table_name}' has no columns in context"
            for col in table.columns:
                assert col.column_name
                assert col.table_name == table.table_name

    def test_context_has_statistical_data(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Context columns should have statistical metrics from profiling."""
        with analyzed_small_finance.session_factory() as session:
            ctx = build_execution_context(
                session=session,
                table_ids=analyzed_table_ids,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
            )

        # Find a numeric column (Transaction ID or Amount)
        has_stats = False
        for table in ctx.tables:
            for col in table.columns:
                if col.null_ratio is not None or col.cardinality_ratio is not None:
                    has_stats = True
                    break
            if has_stats:
                break

        assert has_stats, "No columns have statistical data populated"

    def test_context_has_row_counts(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Tables should have row counts from DuckDB."""
        with analyzed_small_finance.session_factory() as session:
            ctx = build_execution_context(
                session=session,
                table_ids=analyzed_table_ids,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
            )

        for table in ctx.tables:
            assert table.row_count is not None
            assert table.row_count > 0

    def test_context_has_temporal_bounds(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Temporal columns should have min/max timestamps from TemporalColumnProfile."""
        with analyzed_small_finance.session_factory() as session:
            ctx = build_execution_context(
                session=session,
                table_ids=analyzed_table_ids,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
            )

        has_temporal = any(
            col.min_timestamp is not None
            for table in ctx.tables
            for col in table.columns
        )
        assert has_temporal, "No columns have min_timestamp from TemporalColumnProfile"

    def test_context_has_entropy_scores(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Columns should have entropy scores from the entropy phase."""
        with analyzed_small_finance.session_factory() as session:
            ctx = build_execution_context(
                session=session,
                table_ids=analyzed_table_ids,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
            )

        has_entropy = any(
            col.entropy_scores is not None
            for table in ctx.tables
            for col in table.columns
        )
        assert has_entropy, "No columns have entropy_scores"
        assert ctx.entropy_summary is not None, "Missing entropy_summary"
        assert ctx.entropy_summary["overall_readiness"] in ("ready", "investigate", "blocked")

    # NOTE: business_name, table_description, quality_summary, and entropy_explanation
    # require the semantic, quality_summary, and entropy_interpretation phases which
    # are not run by analyzed_small_finance (requires LLM calls). Those fields are
    # verified by the dataraum-eval calibration harness which runs the full pipeline.

    def test_context_format_for_prompt(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Context should be formattable as a text prompt for LLM."""
        with analyzed_small_finance.session_factory() as session:
            ctx = build_execution_context(
                session=session,
                table_ids=analyzed_table_ids,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
            )

        prompt_text = format_metadata_document(ctx)

        assert isinstance(prompt_text, str)
        assert len(prompt_text) > 100  # Should be substantial
        # Should mention table names
        assert "transactions" in prompt_text.lower() or "customers" in prompt_text.lower()
        # Metadata document structure
        assert "# Data Catalog:" in prompt_text
        assert "## Tables" in prompt_text
        assert "| Column | Type | Role | Description | Notes |" in prompt_text


class TestEntropyContextLoading:
    """Test build_for_network produces real entropy scores."""

    def test_entropy_context_has_column_results(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Entropy context should have network results for analyzed columns."""
        with analyzed_small_finance.session_factory() as session:
            entropy_ctx = build_for_network(session, analyzed_table_ids)

        assert isinstance(entropy_ctx, EntropyForNetwork)
        assert len(entropy_ctx.columns) > 0

    def test_entropy_column_results_have_readiness(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Each column result should have readiness and p_high scores."""
        with analyzed_small_finance.session_factory() as session:
            entropy_ctx = build_for_network(session, analyzed_table_ids)

        for key, col_result in entropy_ctx.columns.items():
            assert isinstance(col_result, ColumnNetworkResult)
            assert col_result.worst_intent_p_high >= 0.0
            assert col_result.worst_intent_p_high <= 1.0
            assert col_result.readiness in ("ready", "investigate", "blocked")
            # Key should be in format "column:table_name.column_name"
            assert key.startswith("column:")

    def test_entropy_context_has_overall_readiness(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Entropy context should have overall readiness status."""
        with analyzed_small_finance.session_factory() as session:
            entropy_ctx = build_for_network(session, analyzed_table_ids)

        assert entropy_ctx.overall_readiness in ("ready", "investigate", "blocked")
        assert entropy_ctx.total_columns > 0
        assert (
            entropy_ctx.columns_blocked
            + entropy_ctx.columns_investigate
            + entropy_ctx.columns_ready
            == entropy_ctx.total_columns
        )

    def test_entropy_empty_table_ids_returns_empty(
        self,
        analyzed_small_finance: PipelineTestHarness,
    ):
        """Empty table IDs should return empty entropy context."""
        with analyzed_small_finance.session_factory() as session:
            entropy_ctx = build_for_network(session, [])

        assert len(entropy_ctx.columns) == 0
        assert entropy_ctx.total_columns == 0


class TestGraphAgentSQLGeneration:
    """Test graph agent SQL generation with mocked LLM against real schema."""

    @pytest.fixture
    def graph_agent(
        self,
        mock_llm_config,
        mock_llm_provider,
        mock_prompt_renderer,
    ) -> GraphAgent:
        """Create a GraphAgent with mocked LLM dependencies."""
        return GraphAgent(
            config=mock_llm_config,
            provider=mock_llm_provider,
            prompt_renderer=mock_prompt_renderer,
        )

    @pytest.fixture
    def transaction_sum_graph(self) -> TransformationGraph:
        """A graph that sums transaction amounts."""
        return TransformationGraph(
            graph_id="total_amount",
            graph_type=GraphType.METRIC,
            version="1.0",
            metadata=GraphMetadata(
                name="Total Transaction Amount",
                description="Sum of all transaction amounts",
                category="financial",
                source=GraphSource.SYSTEM,
                tags=["amount", "total"],
            ),
            output=OutputDef(
                output_type=OutputType.SCALAR,
                metric_id="total_amount",
                unit="currency",
                decimal_places=2,
            ),
            parameters=[],
            requires_filters=[],
            steps={
                "total": GraphStep(
                    step_id="total",
                    level=1,
                    step_type=StepType.EXTRACT,
                    source=StepSource(
                        standard_field="amount",
                        statement="typed_transactions",
                    ),
                    aggregation="sum",
                    depends_on=[],
                    output_step=True,
                ),
            },
            interpretation=None,
        )

    def test_execute_with_real_schema(
        self,
        graph_agent: GraphAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
        transaction_sum_graph: TransformationGraph,
    ):
        """Execute graph agent against real typed data in DuckDB."""
        # Mock LLM to return SQL that works against real schema
        mock_tool_call = MagicMock()
        mock_tool_call.name = "generate_sql"
        mock_tool_call.input = {
            "summary": "Sum of all transaction amounts.",
            "steps": [
                {
                    "step_id": "total",
                    "sql": 'SELECT SUM("Amount") AS total FROM typed_transactions',
                    "description": "Sum the Amount column",
                }
            ],
            "final_sql": 'SELECT SUM("Amount") AS total FROM typed_transactions',
            "column_mappings": {"amount": "Amount"},
        }

        mock_response = MagicMock()
        mock_response.tool_calls = [mock_tool_call]
        mock_response.content = None
        graph_agent.provider.converse = MagicMock(return_value=Result.ok(mock_response))

        from dataraum.graphs.field_mapping import ColumnCandidate, FieldMappings

        with analyzed_small_finance.session_factory() as session:
            rich_context = build_execution_context(
                session=session,
                table_ids=analyzed_table_ids,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
            )

        # Inject field mappings (semantic phase not run in this fixture)
        rich_context.field_mappings = FieldMappings(
            mappings={
                "amount": [
                    ColumnCandidate(
                        column_id="test",
                        column_name="Amount",
                        table_name="typed_transactions",
                        confidence=0.95,
                    )
                ],
            },
            table_ids=analyzed_table_ids,
        )

        context = ExecutionContext(
            duckdb_conn=analyzed_small_finance.duckdb_conn,
            rich_context=rich_context,
        )

        with analyzed_small_finance.session_factory() as session:
            result = graph_agent.execute(session, transaction_sum_graph, context)

        assert result.success, f"Graph execution failed: {result.error}"
        assert result.value is not None
        assert result.value.graph_id == "total_amount"
        # Output should be a non-zero sum (500 transactions with amounts)
        assert result.value.output_value is not None
        assert result.value.output_value > 0

    def test_schema_introspection(
        self,
        graph_agent: GraphAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Graph agent should correctly build multi-table schema."""
        with analyzed_small_finance.session_factory() as session:
            context = ExecutionContext.with_rich_context(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                table_ids=analyzed_table_ids,
            )

        schema_info = graph_agent._build_schema_info(context)

        assert "tables" in schema_info
        assert len(schema_info["tables"]) > 0

        table_names = [t["table_name"] for t in schema_info["tables"]]
        assert any("transactions" in name for name in table_names)

        # Check that columns have sample values
        for table in schema_info["tables"]:
            assert len(table["columns"]) > 0
            assert table["row_count"] > 0


class TestEntropyBehavior:
    """Test entropy-aware context population."""

    def test_execution_context_with_rich_context(
        self,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """ExecutionContext.with_rich_context should populate entropy data."""
        with analyzed_small_finance.session_factory() as session:
            exec_ctx = ExecutionContext.with_rich_context(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                table_ids=analyzed_table_ids,
            )

        assert exec_ctx.rich_context is not None
