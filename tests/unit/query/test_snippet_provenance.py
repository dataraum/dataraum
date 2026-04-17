"""Tests for snippet provenance and vocabulary harmonization (DAT-263)."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from dataraum.query.snippet_library import SnippetLibrary
from dataraum.query.snippet_models import SQLSnippetRecord

SOURCE_ID = "test_source"


def _add_snippet(
    session: Session,
    source_id: str,
    *,
    standard_field: str = "revenue",
    source: str = "graph:dso",
    provenance: dict | None = None,
) -> SQLSnippetRecord:
    record = SQLSnippetRecord(
        snippet_type="extract",
        standard_field=standard_field,
        statement="income_statement",
        aggregation="sum",
        schema_mapping_id=source_id,
        sql="SELECT SUM(amount) FROM t",
        description="Test snippet",
        column_mappings={},
        source=source,
        provenance=provenance,
        execution_count=0,
        failure_count=0,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    session.add(record)
    session.flush()
    return record


class TestSnippetProvenance:
    """Tests for provenance storage and retrieval."""

    def test_save_snippet_with_provenance(self, session: Session) -> None:
        """Provenance dict roundtrips through save_snippet."""
        library = SnippetLibrary(session)
        provenance = {
            "field_resolution": "inferred",
            "was_repaired": False,
            "column_mappings_basis": {
                "revenue": {"column": "t.amount", "resolution": "inferred_from_enriched_view"}
            },
            "llm_reasoning": "Mapped via account type filtering",
        }

        record = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) FROM t",
            description="Revenue sum",
            schema_mapping_id=SOURCE_ID,
            source="graph:dso",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            provenance=provenance,
        )

        assert record.provenance == provenance
        assert record.provenance["field_resolution"] == "inferred"

    def test_save_snippet_without_provenance(self, session: Session) -> None:
        """Provenance is None when not provided."""
        library = SnippetLibrary(session)
        record = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) FROM t",
            description="Revenue sum",
            schema_mapping_id=SOURCE_ID,
            source="graph:dso",
            standard_field="revenue",
        )

        assert record.provenance is None

    def test_provenance_survives_find_by_key(self, session: Session) -> None:
        """Provenance is available after finding snippet by key."""
        provenance = {"field_resolution": "direct", "was_repaired": False}
        _add_snippet(session, SOURCE_ID, provenance=provenance)

        library = SnippetLibrary(session)
        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id=SOURCE_ID,
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )

        assert match is not None
        assert match.snippet.provenance == provenance

    def test_provenance_updated_on_failed_snippet_replace(self, session: Session) -> None:
        """When a failed snippet is replaced, provenance is updated."""
        record = _add_snippet(session, SOURCE_ID, provenance={"field_resolution": "inferred"})
        record.failure_count = 1
        session.flush()

        library = SnippetLibrary(session)
        new_provenance = {"field_resolution": "direct", "was_repaired": True}
        updated = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(new_amount) FROM t",
            description="Updated",
            schema_mapping_id=SOURCE_ID,
            source="graph:dso",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            provenance=new_provenance,
        )

        assert updated.provenance == new_provenance
        assert updated.failure_count == 0


class TestVocabularyHarmonization:
    """Tests for vocabulary filtering — only graph: sources contribute."""

    def test_mcp_snippets_excluded_from_vocabulary(self, session: Session) -> None:
        """run_sql snippets (source like mcp:%) should not appear in vocabulary."""
        # Add a graph snippet (should be in vocabulary)
        _add_snippet(session, SOURCE_ID, standard_field="revenue", source="graph:dso")
        # Add a run_sql snippet (should NOT be in vocabulary)
        record = SQLSnippetRecord(
            snippet_type="query",
            standard_field="query_a1b2c3d4",
            schema_mapping_id=SOURCE_ID,
            sql="SELECT * FROM t",
            description="Ad-hoc query",
            column_mappings={},
            source="mcp:session_abc123",
            execution_count=0,
            failure_count=0,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        session.add(record)
        session.flush()

        library = SnippetLibrary(session)
        vocab = library.get_search_vocabulary(schema_mapping_id=SOURCE_ID)

        assert "revenue" in vocab["standard_fields"]
        assert "query_a1b2c3d4" not in vocab["standard_fields"]
        assert "dso" in vocab["graph_ids"]
        # mcp session sources should not produce graph_ids
        assert not any(gid.startswith("session_") for gid in vocab["graph_ids"])

    def test_query_agent_snippets_excluded_from_vocabulary(self, session: Session) -> None:
        """Query agent snippets (source like query:%) should NOT appear in vocabulary.

        Query agent snippets are per-execution artifacts — step_ids are LLM-generated
        names (like revenue_march_2025) and the source UUID pollutes graph_ids.
        They remain searchable via find_by_id / find_by_key.
        """
        # Add a graph snippet (should be in vocabulary)
        _add_snippet(session, SOURCE_ID, standard_field="revenue", source="graph:dso")
        # Add a query agent snippet (should NOT be in vocabulary)
        record = SQLSnippetRecord(
            snippet_type="query",
            standard_field="revenue_march_2025",
            schema_mapping_id=SOURCE_ID,
            sql="SELECT SUM(amount) FROM t WHERE month = 3",
            description="Monthly revenue query",
            column_mappings={},
            source="query:df2b8659-7d7d-47f5-969f-076c3912503c",
            execution_count=0,
            failure_count=0,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        session.add(record)
        session.flush()

        library = SnippetLibrary(session)
        vocab = library.get_search_vocabulary(schema_mapping_id=SOURCE_ID)

        # One-off step_id from query agent should not pollute vocabulary
        assert "revenue_march_2025" not in vocab["standard_fields"]
        # Execution UUID should not appear as graph_id
        assert "df2b8659-7d7d-47f5-969f-076c3912503c" not in vocab["graph_ids"]
        # Graph snippet vocabulary still present
        assert "revenue" in vocab["standard_fields"]
        assert "dso" in vocab["graph_ids"]
