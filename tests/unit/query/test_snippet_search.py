"""Integration tests for snippet search handler.

Tests the full search flow (tool input → find_graphs_by_keys → JSON output)
without LLM calls.
"""

from dataraum.query.snippet_library import SnippetLibrary


def _create_dso_graph(library: SnippetLibrary, schema: str = "s1") -> None:
    """Create a realistic DSO graph: 3 extracts + 1 constant + 1 formula."""
    library.save_snippet(
        snippet_type="extract",
        sql="SELECT SUM(ar) AS value FROM typed_transactions WHERE konto_art = 'AR'",
        description="Accounts receivable (end of period)",
        schema_mapping_id=schema,
        source="graph:dso",
        standard_field="accounts_receivable",
        statement="balance_sheet",
        aggregation="end_of_period",
    )
    library.save_snippet(
        snippet_type="extract",
        sql="SELECT SUM(rev) AS value FROM typed_transactions WHERE konto_art = 'Revenue'",
        description="Revenue (sum)",
        schema_mapping_id=schema,
        source="graph:dso",
        standard_field="revenue",
        statement="income_statement",
        aggregation="sum",
    )
    library.save_snippet(
        snippet_type="extract",
        sql="SELECT SUM(cost) AS value FROM typed_transactions WHERE konto_art = 'COGS'",
        description="Cost of goods sold (sum)",
        schema_mapping_id=schema,
        source="graph:dso",
        standard_field="cost_of_goods_sold",
        statement="income_statement",
        aggregation="sum",
    )
    library.save_snippet(
        snippet_type="constant",
        sql="SELECT 30 AS value",
        description="Days in period",
        schema_mapping_id=schema,
        source="graph:dso",
        standard_field="days_in_period",
        parameter_value="30",
    )
    library.save_snippet(
        snippet_type="formula",
        sql="SELECT (ar / rev) * 30 AS value",
        description="DSO = (AR / Revenue) * Days",
        schema_mapping_id=schema,
        source="graph:dso",
        normalized_expression="({A} / {B}) * {C}",
        input_fields=["accounts_receivable", "days_in_period", "revenue"],
    )


class TestSnippetSearchByConcept:
    """Test searching by business concepts."""

    def test_single_concept_returns_graph(self, session):
        """Searching by 'accounts_receivable' returns DSO graph."""
        library = SnippetLibrary(session)
        _create_dso_graph(library)
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            standard_fields=["accounts_receivable"],
        )

        assert len(graphs) == 1
        assert graphs[0].graph_id == "dso"
        assert len(graphs[0].snippets) == 5

    def test_shared_concept_returns_multiple_graphs(self, session):
        """Same standard_field in different key combos returns multiple graphs."""
        library = SnippetLibrary(session)

        # Two graphs with 'revenue' but different key combos (no upsert collision)
        library.save_snippet(
            snippet_type="extract", sql="SELECT SUM(rev)",
            description="Revenue (sum)", schema_mapping_id="s1",
            source="graph:dso", standard_field="revenue",
            statement="income_statement", aggregation="sum",
        )
        library.save_snippet(
            snippet_type="extract", sql="SELECT AVG(rev)",
            description="Revenue (avg)", schema_mapping_id="s1",
            source="graph:trend", standard_field="revenue",
            statement="income_statement", aggregation="average",
        )
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            standard_fields=["revenue"],
        )

        assert len(graphs) == 2
        graph_ids = {g.graph_id for g in graphs}
        assert graph_ids == {"dso", "trend"}


class TestSnippetSearchByGraphId:
    """Test searching by graph ID."""

    def test_direct_graph_id_lookup(self, session):
        """Searching by graph_id 'dso' returns the full DSO graph."""
        library = SnippetLibrary(session)
        _create_dso_graph(library)
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            graph_ids=["dso"],
        )

        assert len(graphs) == 1
        assert graphs[0].graph_id == "dso"
        assert len(graphs[0].snippets) == 5

    def test_multiple_graph_ids(self, session):
        """Searching multiple graph_ids returns all matching graphs."""
        library = SnippetLibrary(session)

        # Two independent graphs with non-overlapping keys
        library.save_snippet(
            snippet_type="extract", sql="SELECT SUM(ar)",
            description="AR", schema_mapping_id="s1",
            source="graph:dso", standard_field="accounts_receivable",
            statement="balance_sheet", aggregation="end_of_period",
        )
        library.save_snippet(
            snippet_type="extract", sql="SELECT SUM(margin)",
            description="Margin", schema_mapping_id="s1",
            source="graph:gross_margin", standard_field="gross_margin",
            statement="income_statement", aggregation="sum",
        )
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            graph_ids=["dso", "gross_margin"],
        )

        assert len(graphs) == 2


class TestSnippetSearchCombined:
    """Test combined key searches."""

    def test_concept_plus_graph_id_union(self, session):
        """Concept + graph_id returns union of matches."""
        library = SnippetLibrary(session)

        # Two independent graphs
        library.save_snippet(
            snippet_type="extract", sql="SELECT SUM(ar)",
            description="AR", schema_mapping_id="s1",
            source="graph:dso", standard_field="accounts_receivable",
            statement="balance_sheet", aggregation="end_of_period",
        )
        library.save_snippet(
            snippet_type="extract", sql="SELECT SUM(margin)",
            description="Margin", schema_mapping_id="s1",
            source="graph:gross_margin", standard_field="gross_margin",
            statement="income_statement", aggregation="sum",
        )
        session.flush()

        # accounts_receivable → dso, graph_id gross_margin → gross_margin
        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            standard_fields=["accounts_receivable"],
            graph_ids=["gross_margin"],
        )

        assert len(graphs) == 2
        graph_ids = {g.graph_id for g in graphs}
        assert graph_ids == {"dso", "gross_margin"}


class TestSnippetSearchEdgeCases:
    """Test edge cases."""

    def test_empty_search_returns_empty(self, session):
        """No keys provided returns empty list."""
        library = SnippetLibrary(session)
        _create_dso_graph(library)
        session.flush()

        graphs = library.find_graphs_by_keys(schema_mapping_id="s1")
        assert graphs == []

    def test_invalid_keys_return_empty(self, session):
        """Non-existent keys return empty list."""
        library = SnippetLibrary(session)
        _create_dso_graph(library)
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            standard_fields=["nonexistent_concept"],
            statements=["nonexistent_statement"],
        )
        assert graphs == []

    def test_wrong_schema_returns_empty(self, session):
        """Correct keys but wrong schema returns empty."""
        library = SnippetLibrary(session)
        _create_dso_graph(library, schema="s1")
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s2",
            standard_fields=["revenue"],
        )
        assert graphs == []

    def test_nonexistent_graph_id_returns_empty(self, session):
        """Graph ID that doesn't exist in DB returns empty."""
        library = SnippetLibrary(session)
        _create_dso_graph(library)
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            graph_ids=["nonexistent_graph"],
        )
        assert graphs == []


class TestSearchResultFormat:
    """Test that search results have the expected structure."""

    def test_graph_structure(self, session):
        """Each returned graph has source, graph_id, source_type, and snippets."""
        library = SnippetLibrary(session)
        _create_dso_graph(library)
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            graph_ids=["dso"],
        )

        assert len(graphs) == 1
        graph = graphs[0]
        assert graph.source == "graph:dso"
        assert graph.graph_id == "dso"
        assert graph.source_type == "graph"

        # All snippet types present
        types = {s.snippet_type for s in graph.snippets}
        assert types == {"extract", "constant", "formula"}

    def test_snippet_fields_present(self, session):
        """Each snippet in a graph has required fields."""
        library = SnippetLibrary(session)
        _create_dso_graph(library)
        session.flush()

        graphs = library.find_graphs_by_keys(
            schema_mapping_id="s1",
            graph_ids=["dso"],
        )

        for snippet in graphs[0].snippets:
            assert snippet.snippet_id is not None
            assert snippet.sql is not None
            assert snippet.description is not None
            assert snippet.snippet_type is not None
