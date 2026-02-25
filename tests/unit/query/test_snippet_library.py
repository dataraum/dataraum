"""Tests for SQL Snippet Library."""

import pytest

from dataraum.query.snippet_library import SnippetLibrary
from dataraum.query.snippet_models import SQLSnippetRecord


class TestSnippetLibraryFindByKey:
    """Tests for exact key lookup."""

    def test_find_extract_snippet(self, session):
        """Find an extract snippet by exact key."""
        library = SnippetLibrary(session)

        # Save a snippet
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) AS value FROM typed_orders",
            description="Sum of revenue",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            confidence=1.0,
        )
        session.flush()

        # Find it
        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_abc",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )

        assert match is not None
        assert match.match_confidence == 1.0
        assert match.match_strategy == "exact_key"
        assert match.snippet.standard_field == "revenue"
        assert match.snippet.sql == "SELECT SUM(amount) AS value FROM typed_orders"

    def test_find_no_match(self, session):
        """No snippet for this key."""
        library = SnippetLibrary(session)

        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_abc",
            standard_field="nonexistent",
        )
        assert match is None

    def test_find_different_schema(self, session):
        """Same field but different schema doesn't match."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )
        session.flush()

        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_xyz",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )
        assert match is None

    def test_find_constant_snippet(self, session):
        """Find a constant snippet including parameter value."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="constant",
            sql="SELECT 30 AS value",
            description="30 day period",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            standard_field="days_in_period",
            parameter_value="30",
        )
        session.flush()

        # Find with matching parameter value
        match = library.find_by_key(
            snippet_type="constant",
            schema_mapping_id="schema_abc",
            standard_field="days_in_period",
            parameter_value="30",
        )
        assert match is not None
        assert match.snippet.parameter_value == "30"

        # Different parameter value doesn't match
        match2 = library.find_by_key(
            snippet_type="constant",
            schema_mapping_id="schema_abc",
            standard_field="days_in_period",
            parameter_value="365",
        )
        assert match2 is None

    def test_null_fields_match_correctly(self, session):
        """Null fields in key must match null (not anything)."""
        library = SnippetLibrary(session)

        # Snippet with no statement
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="total_assets",
            # statement=None (default)
            aggregation="sum",
        )
        session.flush()

        # Match with no statement
        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_abc",
            standard_field="total_assets",
            aggregation="sum",
        )
        assert match is not None

        # Should NOT match if we ask for a specific statement
        match2 = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_abc",
            standard_field="total_assets",
            statement="balance_sheet",
            aggregation="sum",
        )
        assert match2 is None


class TestSnippetLibrarySave:
    """Tests for snippet save with upsert semantics."""

    def test_save_new_snippet(self, session):
        """Save creates a new record."""
        library = SnippetLibrary(session)

        record = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(x) AS value FROM t",
            description="Sum of x",
            schema_mapping_id="schema_abc",
            source="graph:metric_a",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            confidence=1.0,
        )
        session.flush()

        assert record.snippet_id is not None
        assert record.sql == "SELECT SUM(x) AS value FROM t"

    def test_save_updates_existing(self, session):
        """Save with same key updates instead of duplicating."""
        library = SnippetLibrary(session)

        # First save
        record1 = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(x) AS value FROM t",
            description="Original",
            schema_mapping_id="schema_abc",
            source="graph:v1",
            standard_field="revenue",
            aggregation="sum",
        )
        session.flush()
        snippet_id_1 = record1.snippet_id

        # Second save with same key
        record2 = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(y) AS value FROM t2",
            description="Updated",
            schema_mapping_id="schema_abc",
            source="graph:v2",
            standard_field="revenue",
            aggregation="sum",
        )
        session.flush()

        # Should be same record, updated
        assert record2.snippet_id == snippet_id_1
        assert record2.sql == "SELECT SUM(y) AS value FROM t2"
        assert record2.description == "Updated"
        assert record2.source == "graph:v2"

    def test_save_formula_snippet(self, session):
        """Save a formula snippet with normalized expression."""
        library = SnippetLibrary(session)

        record = library.save_snippet(
            snippet_type="formula",
            sql="SELECT (SELECT value FROM ar) / (SELECT value FROM rev) * 30 AS value",
            description="DSO calculation",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            normalized_expression="({A} / {B}) * {C}",
            input_fields=["accounts_receivable", "days_in_period", "revenue"],
            confidence=0.9,
        )
        session.flush()

        assert record.normalized_expression == "({A} / {B}) * {C}"
        assert record.input_fields == ["accounts_receivable", "days_in_period", "revenue"]

    def test_save_with_column_mappings(self, session):
        """Column mappings are persisted."""
        library = SnippetLibrary(session)

        record = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
            column_mappings={"revenue": "Betrag", "type": "Kontoart"},
        )
        session.flush()

        fetched = session.get(SQLSnippetRecord, record.snippet_id)
        assert fetched.column_mappings == {"revenue": "Betrag", "type": "Kontoart"}


class TestSnippetLibraryFindByExpression:
    """Tests for formula pattern matching."""

    def test_find_formula(self, session):
        """Find a formula by normalized expression."""
        from dataraum.query.snippet_utils import normalize_expression

        library = SnippetLibrary(session)

        # Normalize the expression the same way find_by_expression will
        expr = "(accounts_receivable / revenue) * days_in_period"
        normalized, sorted_fields, bindings = normalize_expression(expr)

        library.save_snippet(
            snippet_type="formula",
            sql="SELECT (SELECT value FROM ar) / (SELECT value FROM rev) * 30",
            description="DSO formula",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            normalized_expression=normalized,
            input_fields=sorted_fields,
        )
        session.flush()

        match = library.find_by_expression(
            expression=expr,
            schema_mapping_id="schema_abc",
        )

        assert match is not None
        assert match.match_confidence == 0.9
        assert match.match_strategy == "expression_pattern"

    def test_find_formula_no_match(self, session):
        """No formula for a different expression."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="formula",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            normalized_expression="({A} / {B}) * {C}",
            input_fields=["a", "b", "c"],
        )
        session.flush()

        # Different expression
        match = library.find_by_expression(
            expression="x + y",
            schema_mapping_id="schema_abc",
        )
        assert match is None


class TestSnippetLibraryRecordUsage:
    """Tests for usage tracking."""

    def test_record_exact_reuse(self, session):
        """Record an exact reuse and update snippet stats."""
        library = SnippetLibrary(session)

        snippet = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
        )
        session.flush()
        assert snippet.execution_count == 0

        usage = library.record_usage(
            execution_id="exec_001",
            execution_type="graph",
            usage_type="exact_reuse",
            snippet_id=snippet.snippet_id,
            match_confidence=1.0,
            sql_match_ratio=1.0,
            step_id="revenue",
        )
        session.flush()

        assert usage.usage_type == "exact_reuse"
        assert usage.step_id == "revenue"

        # Snippet stats should be updated
        session.refresh(snippet)
        assert snippet.execution_count == 1
        assert snippet.last_used_at is not None

    def test_record_newly_generated(self, session):
        """Record a newly generated step (no snippet)."""
        library = SnippetLibrary(session)

        usage = library.record_usage(
            execution_id="exec_002",
            execution_type="query",
            usage_type="newly_generated",
            step_id="monthly_revenue",
        )
        session.flush()

        assert usage.snippet_id is None
        assert usage.usage_type == "newly_generated"

    def test_record_provided_not_used(self, session):
        """Record when snippet was provided but LLM ignored it."""
        library = SnippetLibrary(session)

        snippet = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
        )
        session.flush()

        library.record_usage(
            execution_id="exec_003",
            execution_type="query",
            usage_type="provided_not_used",
            snippet_id=snippet.snippet_id,
            match_confidence=0.7,
            sql_match_ratio=0.3,
        )
        session.flush()

        # provided_not_used should NOT increment execution_count
        session.refresh(snippet)
        assert snippet.execution_count == 0


class TestSnippetLibraryStats:
    """Tests for stabilization metrics."""

    def test_empty_stats(self, session):
        """Stats with no data."""
        library = SnippetLibrary(session)
        stats = library.get_stats()

        assert stats["total_snippets"] == 0
        assert stats["cache_hit_rate"] == 0.0

    def test_basic_stats(self, session):
        """Stats with some snippets and usages."""
        library = SnippetLibrary(session)

        # Create snippets
        s1 = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="Revenue",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            standard_field="revenue",
            aggregation="sum",
        )
        s2 = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 2",
            description="AR",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            standard_field="accounts_receivable",
            aggregation="end_of_period",
        )
        session.flush()

        # Record usages: 2 reused, 1 newly generated
        library.record_usage(
            execution_id="exec_1", execution_type="graph",
            usage_type="exact_reuse", snippet_id=s1.snippet_id,
            match_confidence=1.0, sql_match_ratio=1.0,
        )
        library.record_usage(
            execution_id="exec_1", execution_type="graph",
            usage_type="exact_reuse", snippet_id=s2.snippet_id,
            match_confidence=1.0, sql_match_ratio=1.0,
        )
        library.record_usage(
            execution_id="exec_1", execution_type="graph",
            usage_type="newly_generated",
        )
        session.flush()

        stats = library.get_stats()

        assert stats["total_snippets"] == 2
        assert stats["snippets_by_type"]["extract"] == 2
        assert stats["total_usages"] == 3
        assert stats["steps_from_cache"] == 2
        assert stats["steps_generated_fresh"] == 1
        # cache_hit_rate = 2 / 3 = 0.667
        assert stats["cache_hit_rate"] == pytest.approx(0.667, abs=0.001)

    def test_stats_filtered_by_schema(self, session):
        """Stats can be filtered by schema_mapping_id."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract", sql="SELECT 1", description="test",
            schema_mapping_id="schema_abc", source="graph:test",
            standard_field="revenue",
        )
        library.save_snippet(
            snippet_type="extract", sql="SELECT 2", description="test",
            schema_mapping_id="schema_xyz", source="graph:test",
            standard_field="revenue",
        )
        session.flush()

        stats_abc = library.get_stats(schema_mapping_id="schema_abc")
        assert stats_abc["total_snippets"] == 1

        stats_all = library.get_stats()
        assert stats_all["total_snippets"] == 2


class TestSnippetLibraryInvalidation:
    """Tests for schema change invalidation."""

    def test_invalidate_for_schema(self, session):
        """Invalidating a schema marks all its snippets as unvalidated."""
        library = SnippetLibrary(session)

        s1 = library.save_snippet(
            snippet_type="extract", sql="SELECT 1", description="test",
            schema_mapping_id="schema_abc", source="graph:test",
            standard_field="revenue", confidence=1.0,
        )
        s2 = library.save_snippet(
            snippet_type="extract", sql="SELECT 2", description="test",
            schema_mapping_id="schema_abc", source="graph:test",
            standard_field="cost", confidence=1.0,
        )
        # Different schema - should not be affected
        s3 = library.save_snippet(
            snippet_type="extract", sql="SELECT 3", description="test",
            schema_mapping_id="schema_xyz", source="graph:test",
            standard_field="revenue", confidence=1.0,
        )
        session.flush()

        # Mark as validated
        s1.is_validated = True
        s2.is_validated = True
        s3.is_validated = True
        session.flush()

        count = library.invalidate_for_schema("schema_abc")
        session.flush()

        assert count == 2
        session.refresh(s1)
        session.refresh(s2)
        session.refresh(s3)
        assert not s1.is_validated
        assert not s2.is_validated
        assert s3.is_validated  # Not affected


class TestSnippetLibraryFindAllForSchema:
    """Tests for find_all_for_schema."""

    def test_find_all(self, session):
        """Find all snippets for a schema."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract", sql="SELECT 1", description="rev",
            schema_mapping_id="schema_abc", source="graph:test",
            standard_field="revenue",
        )
        library.save_snippet(
            snippet_type="constant", sql="SELECT 30", description="days",
            schema_mapping_id="schema_abc", source="graph:test",
            standard_field="days", parameter_value="30",
        )
        library.save_snippet(
            snippet_type="extract", sql="SELECT 2", description="other",
            schema_mapping_id="schema_xyz", source="graph:test",
            standard_field="revenue",
        )
        session.flush()

        results = library.find_all_for_schema("schema_abc")
        assert len(results) == 2

    def test_find_all_with_type_filter(self, session):
        """Filter by snippet type."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract", sql="SELECT 1", description="rev",
            schema_mapping_id="schema_abc", source="graph:test",
            standard_field="revenue",
        )
        library.save_snippet(
            snippet_type="constant", sql="SELECT 30", description="days",
            schema_mapping_id="schema_abc", source="graph:test",
            standard_field="days", parameter_value="30",
        )
        session.flush()

        results = library.find_all_for_schema("schema_abc", snippet_types=["extract"])
        assert len(results) == 1
        assert results[0].snippet_type == "extract"
