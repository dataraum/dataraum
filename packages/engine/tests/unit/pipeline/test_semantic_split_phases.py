"""Unit tests for the split semantic phases' should_skip logic (DAT-362).

The _run paths drive a live LLM and are exercised by integration/calibration;
here we pin the skip gates that decide whether each phase re-runs.
"""

from __future__ import annotations

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.semantic_per_column_phase import SemanticPerColumnPhase
from dataraum.pipeline.phases.semantic_per_table_phase import SemanticPerTablePhase
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_session_id


def _source(session: Session) -> Source:
    src = Source(name="s", source_type="csv")
    session.add(src)
    session.flush()
    return src


def _typed_table(session: Session, source_id: str, name: str, cols: list[str]) -> Table:
    t = Table(source_id=source_id, table_name=name, layer="typed", row_count=10)
    session.add(t)
    session.flush()
    for pos, c in enumerate(cols):
        session.add(
            Column(table_id=t.table_id, column_name=c, column_position=pos, resolved_type="VARCHAR")
        )
    session.flush()
    return t


def _annotate(session: Session, table: Table) -> None:
    for col in table.columns:
        session.add(
            SemanticAnnotation(
                session_id=baseline_session_id(),
                column_id=col.column_id,
                semantic_role="attribute",
                annotation_source="llm",
                confidence=0.9,
            )
        )
    session.flush()


def _ctx(session: Session, duckdb_conn: duckdb.DuckDBPyConnection, source_id: str) -> PhaseContext:
    return PhaseContext(session=session, duckdb_conn=duckdb_conn, source_id=source_id)


class TestPerColumnShouldSkip:
    def test_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        assert SemanticPerColumnPhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) == (
            "No typed tables found"
        )

    def test_runs_when_columns_unannotated(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        _typed_table(session, src.source_id, "t1", ["a", "b"])
        assert (
            SemanticPerColumnPhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) is None
        )

    def test_skips_when_all_annotated(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a", "b"])
        _annotate(session, t1)
        assert SemanticPerColumnPhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) == (
            "All columns already have semantic annotations"
        )


class TestPerTableShouldSkip:
    def test_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        assert SemanticPerTablePhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) == (
            "No typed tables found"
        )

    def test_runs_when_a_table_lacks_an_entity(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        _typed_table(session, src.source_id, "t1", ["a"])
        assert (
            SemanticPerTablePhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) is None
        )

    def test_skips_when_all_tables_classified(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a"])
        session.add(
            TableEntity(
                session_id=baseline_session_id(),
                table_id=t1.table_id,
                detected_entity_type="thing",
                confidence=0.9,
                detection_source="llm",
            )
        )
        session.flush()
        assert SemanticPerTablePhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) == (
            "All tables already classified"
        )
