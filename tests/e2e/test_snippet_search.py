"""E2E tests: verify snippet search mode (tool-based key lookup).

Forces tool search mode by patching _MAX_FULL_INJECT=0, then runs real
LLM queries to verify the model correctly selects keys from the vocabulary
and finds relevant snippet graphs.

Uses the same cached pipeline output as other e2e tests.

GROUND TRUTH: Do not modify assertions to fix failures — fix the production code instead.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import func, select

from dataraum.core.connections import ConnectionManager
from dataraum.pipeline.runner import RunResult
from dataraum.query.core import answer_question
from dataraum.query.models import QueryResult
from dataraum.query.snippet_models import SQLSnippetRecord

pytestmark = pytest.mark.e2e


# =============================================================================
# Preconditions
# =============================================================================


@pytest.fixture(scope="session")
def snippet_count(output_manager: ConnectionManager) -> int:
    """Count snippets — must be > 0 for search mode to have anything to find."""
    with output_manager.session_scope() as session:
        count = session.execute(
            select(func.count()).select_from(SQLSnippetRecord)
        ).scalar()
        return count or 0


# =============================================================================
# Search-mode query fixtures (real LLM, threshold bypassed)
# =============================================================================


@pytest.fixture(scope="session")
def search_mode_result(
    output_manager: ConnectionManager,
    pipeline_run: RunResult,
    typed_table_ids: list[str],
    snippet_count: int,
) -> QueryResult:
    """Run a query with tool search mode forced (_MAX_FULL_INJECT=0).

    Session-scoped to avoid repeated LLM calls — all tests share this result.
    """
    assert snippet_count > 0, (
        "No snippets exist — graph phase must run before search mode tests"
    )

    with patch("dataraum.query.agent._MAX_FULL_INJECT", 0):
        with output_manager.session_scope() as session:
            with output_manager.duckdb_cursor() as cursor:
                result = answer_question(
                    "What is our DSO (Days Sales Outstanding)?",
                    session=session,
                    duckdb_conn=cursor,
                    source_id=pipeline_run.source_id,
                    table_ids=typed_table_ids,
                )
                return result.unwrap()


# =============================================================================
# Tests
# =============================================================================


class TestSnippetSearchMode:
    """Verify the query agent works in tool search mode with real LLM."""

    def test_precondition_snippets_exist(self, snippet_count: int) -> None:
        """Pipeline must have produced snippets for search to be meaningful."""
        assert snippet_count > 0

    def test_search_mode_succeeds(self, search_mode_result: QueryResult) -> None:
        """Query should succeed in search mode — LLM found snippets and produced SQL."""
        assert search_mode_result.execution_id, "No execution_id"
        assert search_mode_result.success, (
            f"Query failed in search mode: {search_mode_result.error}"
        )

    def test_search_mode_has_answer(self, search_mode_result: QueryResult) -> None:
        """LLM should produce a meaningful answer via search mode."""
        assert search_mode_result.answer, "Answer is empty"

    def test_search_mode_has_sql_and_data(self, search_mode_result: QueryResult) -> None:
        """Search mode should produce SQL and data like full-injection mode."""
        assert search_mode_result.sql, "No SQL generated"
        assert search_mode_result.data is not None and len(search_mode_result.data) > 0, (
            "No data returned"
        )
        assert search_mode_result.columns is not None and len(search_mode_result.columns) > 0, (
            "No columns"
        )

    def test_search_mode_has_confidence(self, search_mode_result: QueryResult) -> None:
        """Search mode should evaluate confidence."""
        assert search_mode_result.confidence_level is not None, "No confidence level"
