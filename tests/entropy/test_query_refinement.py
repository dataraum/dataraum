"""Tests for query-time entropy refinement.

Tests the simplified query refinement that passes query context to the LLM.
"""

import pytest

from dataraum_context.entropy.models import ColumnEntropyProfile, EntropyContext
from dataraum_context.entropy.query_refinement import (
    QueryRefinementResult,
    find_columns_in_query,
    refine_interpretations_for_query,
)


class TestQueryRefinementResult:
    """Tests for QueryRefinementResult dataclass."""

    def test_create_result(self) -> None:
        """Test creating a refinement result."""
        result = QueryRefinementResult(
            query="SELECT amount FROM orders",
            matched_columns=["orders.amount"],
        )

        assert result.query == "SELECT amount FROM orders"
        assert result.matched_columns == ["orders.amount"]
        assert result.column_interpretations == {}


class TestFindColumnsInQuery:
    """Tests for find_columns_in_query function."""

    @pytest.fixture
    def sample_context(self) -> EntropyContext:
        """Create a sample entropy context."""
        ctx = EntropyContext()

        ctx.column_profiles["orders.amount"] = ColumnEntropyProfile(
            column_name="amount",
            table_name="orders",
        )
        ctx.column_profiles["orders.status"] = ColumnEntropyProfile(
            column_name="status",
            table_name="orders",
        )
        ctx.column_profiles["users.name"] = ColumnEntropyProfile(
            column_name="name",
            table_name="users",
        )

        return ctx

    def test_finds_single_column(self, sample_context: EntropyContext) -> None:
        """Test finding a single column in query."""
        query = "SELECT amount FROM orders"
        matched = find_columns_in_query(query, sample_context)

        assert "orders.amount" in matched

    def test_finds_multiple_columns(self, sample_context: EntropyContext) -> None:
        """Test finding multiple columns in query."""
        query = "SELECT amount, status FROM orders"
        matched = find_columns_in_query(query, sample_context)

        assert "orders.amount" in matched
        assert "orders.status" in matched

    def test_case_insensitive(self, sample_context: EntropyContext) -> None:
        """Test that matching is case insensitive."""
        query = "SELECT AMOUNT, Status FROM orders"
        matched = find_columns_in_query(query, sample_context)

        assert "orders.amount" in matched
        assert "orders.status" in matched

    def test_word_boundary_matching(self, sample_context: EntropyContext) -> None:
        """Test that partial matches are avoided."""
        # 'amount' should not match 'total_amount'
        ctx = EntropyContext()
        ctx.column_profiles["orders.amount"] = ColumnEntropyProfile(
            column_name="amount",
            table_name="orders",
        )

        query = "SELECT total_amount FROM orders"
        matched = find_columns_in_query(query, ctx)

        # Should not match because 'amount' is part of 'total_amount'
        assert "orders.amount" not in matched

    def test_matches_in_where_clause(self, sample_context: EntropyContext) -> None:
        """Test finding columns in WHERE clause."""
        query = "SELECT * FROM orders WHERE status = 'active'"
        matched = find_columns_in_query(query, sample_context)

        assert "orders.status" in matched

    def test_matches_in_join(self, sample_context: EntropyContext) -> None:
        """Test finding columns in JOIN conditions."""
        query = "SELECT * FROM orders JOIN users ON orders.name = users.name"
        matched = find_columns_in_query(query, sample_context)

        assert "users.name" in matched

    def test_no_matches(self, sample_context: EntropyContext) -> None:
        """Test when no columns match."""
        query = "SELECT unknown_column FROM other_table"
        matched = find_columns_in_query(query, sample_context)

        assert len(matched) == 0

    def test_natural_language_query(self, sample_context: EntropyContext) -> None:
        """Test matching in natural language query."""
        query = "Show me the total amount by status"
        matched = find_columns_in_query(query, sample_context)

        assert "orders.amount" in matched
        assert "orders.status" in matched


class TestRefineInterpretationsForQuery:
    """Tests for refine_interpretations_for_query function."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        from unittest.mock import MagicMock

        session = MagicMock()
        session.execute = MagicMock()
        return session

    @pytest.fixture
    def sample_entropy_context(self) -> EntropyContext:
        """Create a sample entropy context with column profiles."""
        ctx = EntropyContext()

        ctx.column_profiles["orders.amount"] = ColumnEntropyProfile(
            column_name="amount",
            table_name="orders",
            structural_entropy=0.2,
            semantic_entropy=0.6,
            value_entropy=0.3,
            computational_entropy=0.1,
            composite_score=0.35,
            readiness="investigate",
        )

        ctx.column_profiles["orders.status"] = ColumnEntropyProfile(
            column_name="status",
            table_name="orders",
            structural_entropy=0.1,
            semantic_entropy=0.2,
            value_entropy=0.1,
            computational_entropy=0.0,
            composite_score=0.1,
            readiness="ready",
        )

        return ctx

    def test_refine_with_fallback(
        self,
        mock_session,
        sample_entropy_context: EntropyContext,
    ) -> None:
        """Test refinement with fallback interpretation."""
        query = "SELECT SUM(amount) FROM orders WHERE status = 'active'"

        result = refine_interpretations_for_query(
            session=mock_session,
            entropy_context=sample_entropy_context,
            query=query,
            use_fallback=True,
        )

        assert isinstance(result, QueryRefinementResult)
        assert result.query == query
        assert "orders.amount" in result.matched_columns
        assert "orders.status" in result.matched_columns
        # Without interpreter, no interpretations (fallback was removed)
        assert len(result.column_interpretations) == 0

    def test_refine_without_fallback(
        self,
        mock_session,
        sample_entropy_context: EntropyContext,
    ) -> None:
        """Test refinement without fallback."""
        query = "SELECT amount FROM orders"

        result = refine_interpretations_for_query(
            session=mock_session,
            entropy_context=sample_entropy_context,
            query=query,
            interpreter=None,
            use_fallback=False,
        )

        # Without interpreter or fallback, should have no interpretations
        assert "orders.amount" in result.matched_columns
        assert len(result.column_interpretations) == 0

    def test_no_interpretation_without_interpreter(
        self,
        mock_session,
        sample_entropy_context: EntropyContext,
    ) -> None:
        """Test that no interpretation is generated without an interpreter.

        Fallback was removed - interpretations only come from LLM now.
        """
        query = "SELECT SUM(amount) FROM orders GROUP BY status"

        result = refine_interpretations_for_query(
            session=mock_session,
            entropy_context=sample_entropy_context,
            query=query,
            use_fallback=True,
        )

        # Columns are matched but no interpretations without interpreter
        assert "orders.amount" in result.matched_columns
        assert "orders.status" in result.matched_columns
        assert len(result.column_interpretations) == 0

    def test_no_matching_columns(
        self,
        mock_session,
        sample_entropy_context: EntropyContext,
    ) -> None:
        """Test when query has no matching columns."""
        query = "SELECT unknown FROM other_table"

        result = refine_interpretations_for_query(
            session=mock_session,
            entropy_context=sample_entropy_context,
            query=query,
            use_fallback=True,
        )

        assert len(result.matched_columns) == 0
        assert len(result.column_interpretations) == 0
