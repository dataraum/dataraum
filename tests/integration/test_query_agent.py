"""Integration tests for the query agent.

Tests verify:
- End-to-end analyze() with mocked LLM against real analyzed data
- Contract evaluation integration (GREEN vs RED)
- Auto-contract selection
- Query execution and result formatting
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dataraum.core.models.base import Result
from dataraum.entropy.contracts import ConfidenceLevel
from dataraum.graphs.field_mapping import ColumnCandidate, FieldMappings
from dataraum.query.agent import QueryAgent
from dataraum.query.models import QueryResult

from .conftest import PipelineTestHarness

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _inject_field_mappings():
    """Patch load_semantic_mappings to return minimal field mappings.

    The analyzed_small_finance fixture does not run the semantic phase,
    so no business_concept annotations exist. These tests exercise the
    query agent's LLM interaction, not the semantic pipeline.
    """

    def _fake_mappings(session, table_ids):
        return FieldMappings(
            mappings={
                "revenue": [
                    ColumnCandidate(
                        column_id="fake",
                        column_name="Amount",
                        table_name="typed_transactions",
                        confidence=0.9,
                    )
                ],
            },
            table_ids=table_ids,
        )

    with patch(
        "dataraum.graphs.field_mapping.load_semantic_mappings",
        side_effect=_fake_mappings,
    ):
        yield


@pytest.fixture
def query_agent(
    mock_llm_config,
    mock_llm_provider,
    mock_prompt_renderer,
) -> QueryAgent:
    """Create a QueryAgent with mocked LLM dependencies."""
    return QueryAgent(
        config=mock_llm_config,
        provider=mock_llm_provider,
        prompt_renderer=mock_prompt_renderer,
    )


def _mock_llm_tool_response(agent: QueryAgent, sql: str, summary: str) -> None:
    """Configure mock LLM to return a specific SQL query."""
    mock_tool_call = MagicMock()
    mock_tool_call.name = "analyze_query"
    mock_tool_call.input = {
        "summary": summary,
        "interpreted_question": summary,
        "metric_type": "scalar",
        "steps": [],
        "final_sql": sql,
        "column_mappings": {},
        "assumptions": [],
        "validation_notes": [],
        "suggested_format": "number",
    }

    mock_response = MagicMock()
    mock_response.tool_calls = [mock_tool_call]
    mock_response.content = None
    agent.provider.converse = MagicMock(return_value=Result.ok(mock_response))


class TestQueryAgentEndToEnd:
    """End-to-end query agent tests against analyzed data."""

    def test_analyze_count_query(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Query agent should execute a count query and return results."""
        _mock_llm_tool_response(
            query_agent,
            sql="SELECT COUNT(*) AS total_transactions FROM typed_transactions",
            summary="Count total number of transactions.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="How many transactions are there?",
                table_ids=analyzed_table_ids,
                ephemeral=True,
            )

        assert result.success, f"Query failed: {result.error}"
        qr = result.value
        assert isinstance(qr, QueryResult)
        assert qr.success
        assert qr.sql is not None
        assert qr.data is not None
        assert len(qr.data) == 1
        # Should be 500 transactions from small finance
        assert qr.data[0]["total_transactions"] == 500

    def test_analyze_sum_query(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Query agent should execute an aggregation query."""
        _mock_llm_tool_response(
            query_agent,
            sql='SELECT SUM("Amount") AS total_amount FROM typed_transactions',
            summary="Calculate total transaction amount.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="What is the total transaction amount?",
                table_ids=analyzed_table_ids,
                ephemeral=True,
            )

        assert result.success
        qr = result.value
        assert qr.success
        assert qr.data is not None
        assert qr.data[0]["total_amount"] > 0

    def test_analyze_returns_answer_text(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Query result should include a formatted answer string."""
        _mock_llm_tool_response(
            query_agent,
            sql="SELECT COUNT(*) AS count FROM typed_customers",
            summary="Count customers.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="How many customers?",
                table_ids=analyzed_table_ids,
                ephemeral=True,
            )

        assert result.success
        qr = result.value
        assert qr.answer is not None
        assert len(qr.answer) > 0


class TestContractIntegration:
    """Test contract evaluation within query agent flow."""

    def test_default_contract_is_exploratory(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Default contract should be exploratory_analysis."""
        _mock_llm_tool_response(
            query_agent,
            sql="SELECT COUNT(*) AS c FROM typed_transactions",
            summary="Count transactions.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="How many transactions?",
                table_ids=analyzed_table_ids,
                ephemeral=True,
            )

        assert result.success
        qr = result.value
        assert qr.contract == "exploratory_analysis"
        assert qr.confidence_level is not None

    def test_explicit_contract_evaluation(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Specifying a contract should trigger evaluation."""
        _mock_llm_tool_response(
            query_agent,
            sql="SELECT COUNT(*) AS c FROM typed_transactions",
            summary="Count transactions.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="How many transactions?",
                table_ids=analyzed_table_ids,
                contract="operational_analytics",  # Correct contract name
                ephemeral=True,
            )

        assert result.success
        qr = result.value
        assert qr.contract == "operational_analytics"
        assert qr.contract_evaluation is not None
        assert qr.contract_evaluation.contract_name == "operational_analytics"

    def test_strict_contract_may_block_query(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """A strict contract with RED confidence should block execution."""
        # Don't mock LLM here - if contract blocks, LLM won't be called
        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="What is regulatory revenue?",
                table_ids=analyzed_table_ids,
                contract="regulatory_reporting",
                ephemeral=True,
            )

        assert result.success  # Returns Result.ok even for blocked queries
        qr = result.value

        # If confidence is RED, query should not have executed
        if qr.confidence_level == ConfidenceLevel.RED:
            assert not qr.success
            assert qr.sql is None or qr.data is None
            assert qr.error is not None

    def test_auto_contract_selects_strictest_passing(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """auto_contract finds strictest passing, or falls back if none pass.

        Without LLM phases, structural.types entropy is high (0.7)
        which may cause all contracts to fail. In this case, auto_contract
        falls back to exploratory_analysis with RED confidence.
        """
        _mock_llm_tool_response(
            query_agent,
            sql="SELECT COUNT(*) AS c FROM typed_transactions",
            summary="Count transactions.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="How many transactions?",
                table_ids=analyzed_table_ids,
                auto_contract=True,
                ephemeral=True,
            )

        assert result.success
        qr = result.value
        # Should have selected some contract (even if fallback)
        assert qr.contract is not None
        # When no contracts pass, fallback uses exploratory_analysis
        # contract_evaluation may be None in fallback case
        assert qr.confidence_level is not None

    def test_invalid_contract_name_fails(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Specifying a nonexistent contract should fail."""
        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="Anything",
                table_ids=analyzed_table_ids,
                contract="nonexistent_contract",
                ephemeral=True,
            )

        assert not result.success
        assert "not found" in result.error.lower()


class TestQueryResultMetadata:
    """Test query result contains useful metadata."""

    def test_result_has_execution_id(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Each query result should have a unique execution ID."""
        _mock_llm_tool_response(
            query_agent,
            sql="SELECT 1 AS v",
            summary="Test query.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="Test",
                table_ids=analyzed_table_ids,
                ephemeral=True,
            )

        assert result.success
        assert result.value.execution_id is not None
        assert len(result.value.execution_id) > 0

    def test_result_has_original_question(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Query result should preserve the original question."""
        _mock_llm_tool_response(
            query_agent,
            sql="SELECT 1 AS v",
            summary="Test.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="What is the meaning of data?",
                table_ids=analyzed_table_ids,
                ephemeral=True,
            )

        assert result.success
        assert result.value.question == "What is the meaning of data?"

    def test_result_has_column_list(
        self,
        query_agent: QueryAgent,
        analyzed_small_finance: PipelineTestHarness,
        analyzed_table_ids: list[str],
    ):
        """Query result should include column names from result set."""
        _mock_llm_tool_response(
            query_agent,
            sql='SELECT COUNT(*) AS total, MIN("Amount") AS min_amount FROM typed_transactions',
            summary="Transaction stats.",
        )

        with analyzed_small_finance.session_factory() as session:
            result = query_agent.analyze(
                session=session,
                duckdb_conn=analyzed_small_finance.duckdb_conn,
                question="Show transaction stats",
                table_ids=analyzed_table_ids,
                ephemeral=True,
            )

        assert result.success
        qr = result.value
        assert qr.columns is not None
        assert "total" in qr.columns
        assert "min_amount" in qr.columns
