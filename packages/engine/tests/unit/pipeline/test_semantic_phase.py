"""Tests for SemanticPhase.should_skip logic."""

from __future__ import annotations

from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.semantic_phase import SemanticPhase
from dataraum.storage.models import Column, Source, Table


def _setup_annotated_source(session: Session) -> tuple[Source, Table, Column]:
    """Create a source with a typed table, column, and LLM annotation."""
    from dataraum.analysis.semantic.db_models import SemanticAnnotation

    source = Source(name=f"test_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(source_id=source.source_id, table_name="orders", layer="typed", row_count=100)
    session.add(table)
    session.flush()

    col = Column(table_id=table.table_id, column_name="amount", column_position=0, raw_type="FLOAT")
    session.add(col)
    session.flush()

    annotation = SemanticAnnotation(
        column_id=col.column_id,
        annotation_source="llm",
        semantic_role="measure",
    )
    session.add(annotation)
    session.flush()

    return source, table, col


def _make_ctx(
    session: Session, source_id: str, duckdb_conn: duckdb.DuckDBPyConnection
) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        source_id=source_id,
    )


@pytest.fixture
def duck() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


class TestShouldSkip:
    def test_skip_when_all_annotated(
        self, session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Returns skip message when all columns have semantic annotations."""
        source, _, _ = _setup_annotated_source(session)
        ctx = _make_ctx(session, source.source_id, duck)

        phase = SemanticPhase()
        result = phase.should_skip(ctx)
        assert result == "All columns already have semantic annotations"

    def test_no_skip_when_unannotated(
        self, session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Returns None when columns lack annotations."""
        source = Source(name=f"test_{uuid4().hex[:8]}", source_type="csv")
        session.add(source)
        session.flush()

        table = Table(source_id=source.source_id, table_name="orders", layer="typed", row_count=100)
        session.add(table)
        session.flush()

        col = Column(
            table_id=table.table_id, column_name="amount", column_position=0, raw_type="FLOAT"
        )
        session.add(col)
        session.flush()

        ctx = _make_ctx(session, source.source_id, duck)
        phase = SemanticPhase()
        result = phase.should_skip(ctx)
        assert result is None
