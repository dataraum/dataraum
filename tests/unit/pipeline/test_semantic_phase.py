"""Tests for SemanticPhase.should_skip stale-fix detection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.documentation.db_models import FixLedgerEntry
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.db_models import PhaseLog, PipelineRun
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


def _make_ctx(session: Session, source_id: str, duckdb_conn: duckdb.DuckDBPyConnection) -> PhaseContext:
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


class TestShouldSkipStaleFixes:
    def test_skip_when_no_fixes(self, session: Session, duck: duckdb.DuckDBPyConnection) -> None:
        """Returns skip message when all annotated and no fixes exist."""
        source, _, _ = _setup_annotated_source(session)
        ctx = _make_ctx(session, source.source_id, duck)

        phase = SemanticPhase()
        result = phase.should_skip(ctx)
        assert result == "All columns already have semantic annotations"

    def test_no_skip_when_fixes_newer_than_last_run(
        self, session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Returns None (don't skip) when fixes are newer than last semantic run."""
        source, _, _ = _setup_annotated_source(session)

        # Create a completed semantic phase log in the past
        run = PipelineRun(source_id=source.source_id)
        session.add(run)
        session.flush()

        past = datetime.now(UTC) - timedelta(hours=1)
        log = PhaseLog(
            run_id=run.run_id,
            source_id=source.source_id,
            phase_name="semantic",
            status="completed",
            started_at=past,
            completed_at=past,
            duration_seconds=10.0,
        )
        session.add(log)

        # Create a fix that is newer
        fix = FixLedgerEntry(
            source_id=source.source_id,
            action_name="document_unit",
            table_name="orders",
            column_name="amount",
            user_input="USD",
            interpretation="The amount column is in USD.",
            created_at=datetime.now(UTC),
        )
        session.add(fix)
        session.flush()

        ctx = _make_ctx(session, source.source_id, duck)
        phase = SemanticPhase()
        result = phase.should_skip(ctx)
        assert result is None  # Don't skip

    def test_skip_when_fixes_older_than_last_run(
        self, session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Returns skip message when fixes are older than last semantic run."""
        source, _, _ = _setup_annotated_source(session)

        past = datetime.now(UTC) - timedelta(hours=2)

        # Create a fix in the past
        fix = FixLedgerEntry(
            source_id=source.source_id,
            action_name="document_unit",
            table_name="orders",
            column_name="amount",
            user_input="USD",
            interpretation="The amount column is in USD.",
            created_at=past,
        )
        session.add(fix)
        session.flush()

        # Create a completed semantic phase log after the fix
        run = PipelineRun(source_id=source.source_id)
        session.add(run)
        session.flush()

        now = datetime.now(UTC)
        log = PhaseLog(
            run_id=run.run_id,
            source_id=source.source_id,
            phase_name="semantic",
            status="completed",
            started_at=now,
            completed_at=now,
            duration_seconds=10.0,
        )
        session.add(log)
        session.flush()

        ctx = _make_ctx(session, source.source_id, duck)
        phase = SemanticPhase()
        result = phase.should_skip(ctx)
        assert result == "All columns already have semantic annotations"
