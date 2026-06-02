"""Tests for relationships phase (DAT-401: session-scoped, source-free).

The phase scopes purely by ``ctx.table_ids`` — the begin_session selection —
never ``ctx.source_id``. Tables still belong to a source (the ingestion FK),
but the phase is driven by the selected table ids, which may span sources.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.relationships_phase import RelationshipsPhase
from dataraum.storage import Source, Table

if TYPE_CHECKING:
    import duckdb


def _typed_table(session: Session, source_id: str, name: str) -> str:
    table_id = str(uuid4())
    session.add(
        Table(
            table_id=table_id,
            source_id=source_id,
            table_name=name,
            layer="typed",
            duckdb_path=f"typed_{name}",
            row_count=10,
        )
    )
    return table_id


_TEST_SESSION_ID = "00000000-0000-0000-0000-0000000004a1"


def _ctx(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection, table_ids: list[str]
) -> PhaseContext:
    # relationships is session-scoped (should_skip filters by session_id), so the
    # ctx must carry one. These tests seed no relationships, so any id reads 0.
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        config={},
        session_id=_TEST_SESSION_ID,
    )


class TestRelationshipsPhase:
    """Tests for RelationshipsPhase scoped by the session's selected tables."""

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Empty selection → nothing to relate."""
        skip_reason = RelationshipsPhase().should_skip(_ctx(session, duckdb_conn, []))
        assert skip_reason is not None
        assert "No typed tables" in skip_reason

    def test_skip_when_single_table(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """A single-table selection is valid but has no relationships to detect."""
        source_id = str(uuid4())
        session.add(Source(source_id=source_id, name="s", source_type="csv"))
        table_id = _typed_table(session, source_id, "test_table")
        session.commit()

        ctx = _ctx(session, duckdb_conn, [table_id])

        skip_reason = RelationshipsPhase().should_skip(ctx)
        assert skip_reason is not None
        assert "at least 2 tables" in skip_reason

        # Running the phase also succeeds with empty results.
        result = RelationshipsPhase().run(ctx)
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["relationship_candidates"] == []
        assert "at least 2" in result.outputs.get("message", "")

    def test_fails_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Running with an empty selection fails loud."""
        result = RelationshipsPhase().run(_ctx(session, duckdb_conn, []))
        assert result.status == PhaseStatus.FAILED
        assert "No typed tables" in (result.error or "")

    def test_does_not_skip_with_multiple_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """A multi-table selection with no existing relationships runs."""
        source_id = str(uuid4())
        session.add(Source(source_id=source_id, name="s", source_type="csv"))
        table_ids = [_typed_table(session, source_id, f"test_table_{i}") for i in range(3)]
        session.commit()

        skip_reason = RelationshipsPhase().should_skip(_ctx(session, duckdb_conn, table_ids))
        assert skip_reason is None

    def test_scopes_to_selection_across_sources(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """The selection may span sources; tables outside it are ignored.

        Two tables from two different sources form a valid ≥2 selection (so the
        phase runs), while a third typed table left out of ``table_ids`` is not
        seen — proving the phase scopes by the selection, not by any source.
        """
        src_a, src_b = str(uuid4()), str(uuid4())
        session.add(Source(source_id=src_a, name="a", source_type="csv"))
        session.add(Source(source_id=src_b, name="b", source_type="csv"))
        a = _typed_table(session, src_a, "a_tbl")
        b = _typed_table(session, src_b, "b_tbl")
        _typed_table(session, src_a, "a_tbl_excluded")  # not in the selection
        session.commit()

        phase = RelationshipsPhase()
        selected = phase._typed_tables(_ctx(session, duckdb_conn, [a, b]))
        assert {t.table_id for t in selected} == {a, b}
        # A ≥2-table cross-source selection is not skipped on the count gate.
        assert phase.should_skip(_ctx(session, duckdb_conn, [a, b])) is None
