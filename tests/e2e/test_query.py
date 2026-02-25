"""E2E tests: verify the query agent against real pipeline output.

Runs answer_question() against the clean pipeline's output databases
and verifies the response structure. The query agent always evaluates
entropy contracts — even clean data may have blocked columns (e.g.,
fx_rates.rate), causing the default contract to block the query.
Tests validate both the blocking path and the query structure.
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select

from dataraum.core.connections import ConnectionManager
from dataraum.pipeline.runner import RunResult
from dataraum.query.core import answer_question
from dataraum.query.db_models import QueryExecutionRecord
from dataraum.query.models import QueryResult
from dataraum.query.snippet_models import SQLSnippetRecord

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="session")
def query_result(
    output_manager: ConnectionManager,
    pipeline_run: RunResult,
    typed_table_ids: list[str],
) -> QueryResult:
    """Run a single query against the clean pipeline output.

    Session-scoped to avoid repeated LLM calls — all tests share this result.
    Uses default contract evaluation (exploratory_analysis).
    """
    with output_manager.session_scope() as session:
        with output_manager.duckdb_cursor() as cursor:
            result = answer_question(
                "What is the total revenue?",
                session=session,
                duckdb_conn=cursor,
                source_id=pipeline_run.source_id,
                table_ids=typed_table_ids,
            )
            return result.unwrap()


# =============================================================================
# Query agent response
# =============================================================================


class TestQueryAgent:
    """Verify the query agent produces valid structured responses."""

    def test_query_returns_result(self, query_result: QueryResult) -> None:
        """Query should return a QueryResult with an execution_id."""
        assert query_result.execution_id, "No execution_id"
        assert query_result.question == "What is the total revenue?"

    def test_query_has_answer(self, query_result: QueryResult) -> None:
        """Query should produce a non-empty answer (even if blocked)."""
        assert query_result.answer, "Answer is empty"

    def test_query_has_confidence(self, query_result: QueryResult) -> None:
        """Query should have a confidence level set."""
        assert query_result.confidence_level is not None, "No confidence level"

    def test_query_has_contract_evaluation(self, query_result: QueryResult) -> None:
        """Query should have evaluated a contract (default: exploratory_analysis)."""
        assert query_result.contract is not None, "No contract evaluated"
        assert query_result.contract_evaluation is not None, "No contract evaluation"

    def test_blocked_query_has_correct_structure(self, query_result: QueryResult) -> None:
        """If query was blocked by contract, verify the blocking response."""
        if query_result.success:
            pytest.skip("Query succeeded — blocking path not exercised")

        assert query_result.error is not None, "Blocked query should have error"
        assert query_result.contract_evaluation is not None
        assert len(query_result.contract_evaluation.violations) > 0, (
            "Blocked query should have contract violations"
        )

    def test_successful_query_has_sql_and_data(self, query_result: QueryResult) -> None:
        """If query succeeded, verify SQL and data are present."""
        if not query_result.success:
            pytest.skip("Query was blocked by contract — SQL path not exercised")

        assert query_result.sql, "Successful query should have SQL"
        assert query_result.data is not None and len(query_result.data) > 0
        assert query_result.columns is not None and len(query_result.columns) > 0

    def test_query_execution_persisted(
        self,
        query_result: QueryResult,
        output_manager: ConnectionManager,
    ) -> None:
        """Query execution should be persisted in the database (if not blocked)."""
        if not query_result.success:
            # Blocked queries may not persist an execution record
            with output_manager.session_scope() as session:
                count = session.execute(
                    select(func.count())
                    .select_from(QueryExecutionRecord)
                ).scalar()
                # Even if this query was blocked, there should be no
                # spurious records — just verify the table exists
                assert count is not None  # Table is queryable
            return

        with output_manager.session_scope() as session:
            record = session.execute(
                select(QueryExecutionRecord).where(
                    QueryExecutionRecord.execution_id == query_result.execution_id
                )
            ).scalar_one_or_none()
            assert record is not None, (
                f"No QueryExecutionRecord for execution_id={query_result.execution_id}"
            )
            assert record.success is True
            assert record.sql_executed is not None

    def test_graph_snippets_exist(
        self,
        output_manager: ConnectionManager,
    ) -> None:
        """SQL snippets should exist from graph execution (graph phase ran before query)."""
        with output_manager.session_scope() as session:
            count = session.execute(
                select(func.count()).select_from(SQLSnippetRecord)
            ).scalar()
            assert count is not None and count > 0, (
                "No SQL snippets exist — expected snippets from graph execution"
            )
