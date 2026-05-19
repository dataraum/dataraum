"""Tests for search_snippets MCP tool."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.storage import Source, Table


def _id() -> str:
    return str(uuid4())


def _setup_source(session: Session) -> str:
    """Insert Source + Table. Returns source_id."""
    source_id = _id()
    table_id = _id()

    session.add(Source(source_id=source_id, name="test_source", source_type="csv"))
    session.add(
        Table(
            table_id=table_id,
            source_id=source_id,
            table_name="orders",
            layer="typed",
            duckdb_path="typed_orders",
            row_count=100,
        )
    )
    session.flush()
    return source_id


def _add_snippet(
    session: Session,
    source_id: str,
    *,
    snippet_type: str = "extract",
    standard_field: str | None = None,
    statement: str | None = None,
    aggregation: str | None = None,
    source: str = "graph:dso",
    sql: str = "SELECT 1",
    description: str = "test snippet",
) -> SQLSnippetRecord:
    """Add a snippet to the knowledge base."""
    record = SQLSnippetRecord(
        snippet_id=_id(),
        snippet_type=snippet_type,
        standard_field=standard_field,
        statement=statement,
        aggregation=aggregation,
        schema_mapping_id=source_id,
        source=source,
        sql=sql,
        description=description,
    )
    session.add(record)
    session.flush()
    return record


class TestSearchSnippetsVocabulary:
    def test_empty_returns_hint(self, session: Session) -> None:
        source_id = _setup_source(session)

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _search_snippets

            result = _search_snippets(session)

        assert "vocabulary" in result
        assert "hint" in result

    def test_returns_vocabulary(self, session: Session) -> None:
        source_id = _setup_source(session)
        _add_snippet(
            session,
            source_id,
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            source="graph:dso",
        )
        _add_snippet(
            session,
            source_id,
            standard_field="accounts_receivable",
            statement="balance_sheet",
            aggregation="end_of_period",
            source="graph:dso",
        )

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _search_snippets

            result = _search_snippets(session)

        vocab = result["vocabulary"]
        assert "revenue" in vocab["standard_fields"]
        assert "accounts_receivable" in vocab["standard_fields"]
        assert "income_statement" in vocab["statements"]
        assert "dso" in vocab["graph_ids"]


class TestSearchSnippetsByConceptAndGraphId:
    def test_search_by_concept(self, session: Session) -> None:
        source_id = _setup_source(session)
        _add_snippet(
            session,
            source_id,
            standard_field="revenue",
            source="graph:margin",
            sql="SELECT SUM(amount) FROM typed_orders",
            description="Revenue extraction",
        )
        _add_snippet(
            session,
            source_id,
            standard_field="cost",
            source="graph:margin",
            sql="SELECT SUM(cost) FROM typed_orders",
            description="Cost extraction",
        )

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _search_snippets

            result = _search_snippets(session, concepts=["revenue"])

        assert "matches" in result
        # Should return the full graph (both snippets share source "graph:margin")
        assert len(result["matches"]) == 1
        graph = result["matches"][0]
        assert graph["graph_id"] == "margin"
        assert len(graph["snippets"]) == 2

    def test_search_by_graph_id(self, session: Session) -> None:
        source_id = _setup_source(session)
        _add_snippet(
            session,
            source_id,
            standard_field="revenue",
            source="graph:dso",
            sql="SELECT 1",
        )

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _search_snippets

            result = _search_snippets(session, graph_ids=["dso"])

        assert len(result["matches"]) == 1
        assert result["matches"][0]["graph_id"] == "dso"

    def test_no_matches_returns_vocabulary(self, session: Session) -> None:
        source_id = _setup_source(session)
        _add_snippet(session, source_id, standard_field="revenue", source="graph:dso")

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _search_snippets

            result = _search_snippets(session, concepts=["nonexistent"])

        assert result["matches"] == []
        assert "vocabulary" in result
        assert "hint" in result

    def test_snippet_includes_sql_and_metadata(self, session: Session) -> None:
        source_id = _setup_source(session)
        _add_snippet(
            session,
            source_id,
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            source="graph:test",
            sql="SELECT SUM(amount) FROM typed_orders",
            description="Total revenue",
        )

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _search_snippets

            result = _search_snippets(session, concepts=["revenue"])

        snippet = result["matches"][0]["snippets"][0]
        assert snippet["sql"] == "SELECT SUM(amount) FROM typed_orders"
        assert snippet["description"] == "Total revenue"
        assert snippet["snippet_type"] == "extract"
        assert snippet["standard_field"] == "revenue"
        assert snippet["statement"] == "income_statement"
        assert snippet["aggregation"] == "sum"
