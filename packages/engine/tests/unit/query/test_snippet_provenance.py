"""Tests for snippet provenance and vocabulary harmonization (DAT-263)."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from dataraum.query.snippet_library import SnippetLibrary
from dataraum.query.snippet_models import SQLSnippetRecord

SOURCE_ID = "test_source"
WORKSPACE_ID = "test"


def _add_snippet(
    session: Session,
    source_id: str,
    *,
    standard_field: str = "revenue",
    source: str = "graph:dso",
    provenance: dict | None = None,
) -> SQLSnippetRecord:
    record = SQLSnippetRecord(
        workspace_id=WORKSPACE_ID,
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
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)
        provenance = {
            "field_resolution": "inferred",
            "was_repaired": False,
            "column_mappings_basis": {
                "revenue": {"column": "t.amount", "resolution": "inferred_from_enriched_view"}
            },
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
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)
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

        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)
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

        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)
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
