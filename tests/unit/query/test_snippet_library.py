"""Tests for SQL Snippet Library."""

import pytest

from dataraum.query.snippet_library import SnippetGraph, SnippetLibrary
from dataraum.query.snippet_models import SQLSnippetRecord


class TestSnippetLibraryFindById:
    """Tests for primary key lookup."""

    def test_find_existing_snippet(self, session):
        """Find a snippet by its primary key."""
        library = SnippetLibrary(session)

        record = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) AS value FROM typed_orders",
            description="Sum of revenue",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            standard_field="revenue",
        )
        session.flush()

        found = library.find_by_id(record.snippet_id)
        assert found is not None
        assert found.snippet_id == record.snippet_id
        assert found.sql == "SELECT SUM(amount) AS value FROM typed_orders"

    def test_find_nonexistent_returns_none(self, session):
        """Unknown snippet_id returns None."""
        library = SnippetLibrary(session)

        found = library.find_by_id("nonexistent-id")
        assert found is None


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
            execution_id="exec_1",
            execution_type="graph",
            usage_type="exact_reuse",
            snippet_id=s1.snippet_id,
            match_confidence=1.0,
            sql_match_ratio=1.0,
        )
        library.record_usage(
            execution_id="exec_1",
            execution_type="graph",
            usage_type="exact_reuse",
            snippet_id=s2.snippet_id,
            match_confidence=1.0,
            sql_match_ratio=1.0,
        )
        library.record_usage(
            execution_id="exec_1",
            execution_type="graph",
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
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 2",
            description="test",
            schema_mapping_id="schema_xyz",
            source="graph:test",
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
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
        )
        s2 = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 2",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="cost",
        )
        # Different schema - should not be affected
        s3 = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 3",
            description="test",
            schema_mapping_id="schema_xyz",
            source="graph:test",
            standard_field="revenue",
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
            snippet_type="extract",
            sql="SELECT 1",
            description="rev",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
        )
        library.save_snippet(
            snippet_type="constant",
            sql="SELECT 30",
            description="days",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="days",
            parameter_value="30",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 2",
            description="other",
            schema_mapping_id="schema_xyz",
            source="graph:test",
            standard_field="revenue",
        )
        session.flush()

        results = library.find_all_for_schema("schema_abc")
        assert len(results) == 2

    def test_find_all_with_type_filter(self, session):
        """Filter by snippet type."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="rev",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
        )
        library.save_snippet(
            snippet_type="constant",
            sql="SELECT 30",
            description="days",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="days",
            parameter_value="30",
        )
        session.flush()

        results = library.find_all_for_schema("schema_abc", snippet_types=["extract"])
        assert len(results) == 1
        assert results[0].snippet_type == "extract"


class TestSnippetGraphs:
    """Tests for snippet graph discovery."""

    def test_find_all_graphs(self, session):
        """All snippets grouped by source."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(ar) AS value FROM t",
            description="AR",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="accounts_receivable",
            statement="balance_sheet",
            aggregation="end_of_period",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(rev) AS value FROM t",
            description="Revenue",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(cost) AS value FROM t",
            description="Cost",
            schema_mapping_id="s1",
            source="graph:margin",
            standard_field="cost",
            statement="income_statement",
            aggregation="sum",
        )
        session.flush()

        graphs = library.find_all_graphs("s1")
        assert len(graphs) == 2  # dso + margin
        assert all(isinstance(g, SnippetGraph) for g in graphs)

        dso_graph = next(g for g in graphs if g.graph_id == "dso")
        assert len(dso_graph.snippets) == 2
        assert dso_graph.source_type == "graph"

    def test_find_all_graphs_multiple_sources(self, session):
        """Separate graphs for different sources."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="a",
            schema_mapping_id="s1",
            source="graph:alpha",
            standard_field="a",
        )
        library.save_snippet(
            snippet_type="query",
            sql="SELECT 2",
            description="b",
            schema_mapping_id="s1",
            source="query:exec_123",
            standard_field="b",
        )
        session.flush()

        graphs = library.find_all_graphs("s1")
        assert len(graphs) == 2

        query_graph = next(g for g in graphs if g.source_type == "query")
        assert query_graph.graph_id == "exec_123"

    def test_find_all_graphs_empty(self, session):
        """No snippets returns empty."""
        library = SnippetLibrary(session)
        assert library.find_all_graphs("nonexistent") == []

    def test_get_search_vocabulary(self, session):
        """Extract vocabulary from snippet index."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="rev",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 2",
            description="ar",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="accounts_receivable",
            statement="balance_sheet",
            aggregation="end_of_period",
        )
        library.save_snippet(
            snippet_type="constant",
            sql="SELECT 30",
            description="days",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="days_in_period",
        )
        session.flush()

        vocab = library.get_search_vocabulary("s1")

        assert "accounts_receivable" in vocab["standard_fields"]
        assert "revenue" in vocab["standard_fields"]
        assert "days_in_period" in vocab["standard_fields"]
        assert "income_statement" in vocab["statements"]
        assert "balance_sheet" in vocab["statements"]
        assert "sum" in vocab["aggregations"]
        assert "end_of_period" in vocab["aggregations"]
        assert "dso" in vocab["graph_ids"]

    def test_find_graphs_by_keys_standard_field(self, session):
        """Search by standard_field returns correct graphs."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(ar) AS value FROM t",
            description="AR",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="accounts_receivable",
            statement="balance_sheet",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(rev) AS value FROM t",
            description="Revenue",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="revenue",
            statement="income_statement",
        )
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            standard_fields=["accounts_receivable"],
        )

        assert len(graphs) == 1
        assert graphs[0].graph_id == "dso"
        assert len(graphs[0].snippets) == 2  # Full graph returned

    def test_find_graphs_by_keys_statement(self, session):
        """Search by statement returns correct graphs."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(rev) AS value FROM t",
            description="Revenue",
            schema_mapping_id="s1",
            source="graph:revenue",
            standard_field="revenue",
            statement="income_statement",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(ar) AS value FROM t",
            description="AR",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="accounts_receivable",
            statement="balance_sheet",
        )
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            statements=["balance_sheet"],
        )

        assert len(graphs) == 1
        assert graphs[0].graph_id == "dso"

    def test_find_graphs_by_keys_graph_id(self, session):
        """Search by graph_id returns correct graph."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="a",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="revenue",
        )
        library.save_snippet(
            snippet_type="constant",
            sql="SELECT 30",
            description="days",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="days_in_period",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 2",
            description="cost",
            schema_mapping_id="s1",
            source="graph:margin",
            standard_field="cost",
        )
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            graph_ids=["dso"],
        )

        assert len(graphs) == 1
        assert graphs[0].graph_id == "dso"
        assert len(graphs[0].snippets) == 2

    def test_find_graphs_by_keys_multi_category(self, session):
        """Search by concept + statement returns union of matches."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(rev)",
            description="Rev",
            schema_mapping_id="s1",
            source="graph:revenue",
            standard_field="revenue",
            statement="income_statement",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(ar)",
            description="AR",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="accounts_receivable",
            statement="balance_sheet",
        )
        session.flush()

        # Search by concept (revenue) + statement (balance_sheet) — hits both graphs
        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            standard_fields=["revenue"],
            statements=["balance_sheet"],
        )

        assert len(graphs) == 2
        graph_ids = {g.graph_id for g in graphs}
        assert graph_ids == {"dso", "revenue"}

    def test_find_graphs_by_keys_expands_full_graph(self, session):
        """One matching snippet expands to full graph."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(ar)",
            description="AR",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="accounts_receivable",
        )
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(rev)",
            description="Rev",
            schema_mapping_id="s1",
            source="graph:dso",
            standard_field="revenue",
        )
        library.save_snippet(
            snippet_type="formula",
            sql="SELECT ar/rev*30",
            description="DSO",
            schema_mapping_id="s1",
            source="graph:dso",
            normalized_expression="({A}/{B})*{C}",
        )
        session.flush()

        # Only "revenue" matches, but entire graph returned
        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            standard_fields=["revenue"],
        )

        assert len(graphs) == 1
        assert len(graphs[0].snippets) == 3

    def test_find_graphs_by_keys_no_match(self, session):
        """No matching keys returns empty."""
        library = SnippetLibrary(session)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="s1",
            source="graph:test",
            standard_field="revenue",
        )
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            standard_fields=["nonexistent_field"],
        )
        assert graphs == []

    def test_find_graphs_by_keys_limit(self, session):
        """Respects limit parameter."""
        library = SnippetLibrary(session)

        # Create 3 separate graphs
        for name in ["alpha", "beta", "gamma"]:
            library.save_snippet(
                snippet_type="extract",
                sql=f"SELECT {name}",
                description=name,
                schema_mapping_id="s1",
                source=f"graph:{name}",
                standard_field=name,
                statement="income_statement",
            )
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            statements=["income_statement"],
            limit=2,
        )

        assert len(graphs) == 2
