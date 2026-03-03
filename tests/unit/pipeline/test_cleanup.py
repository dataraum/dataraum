"""Tests for pipeline phase cleanup."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.pipeline.cleanup import _CLEANUP_MAP, cleanup_phase
from dataraum.pipeline.db_models import PhaseCheckpoint, PipelineRun
from dataraum.storage.models import Column, Source, Table


def _make_source(session: Session) -> Source:
    """Create and flush a test source."""
    source = Source(name=f"test_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()
    return source


def _make_table(session: Session, source_id: str, layer: str = "raw") -> Table:
    """Create and flush a test table."""
    table = Table(
        source_id=source_id,
        table_name=f"tbl_{uuid4().hex[:8]}",
        layer=layer,
        row_count=100,
    )
    session.add(table)
    session.flush()
    return table


def _make_column(session: Session, table_id: str) -> Column:
    """Create and flush a test column."""
    col = Column(
        table_id=table_id,
        column_name=f"col_{uuid4().hex[:8]}",
        column_position=0,
        raw_type="VARCHAR",
    )
    session.add(col)
    session.flush()
    return col


def _make_checkpoint(session: Session, source_id: str, phase_name: str) -> PhaseCheckpoint:
    """Create a pipeline run + checkpoint."""
    run = PipelineRun(source_id=source_id)
    session.add(run)
    session.flush()

    cp = PhaseCheckpoint(
        run_id=run.run_id,
        source_id=source_id,
        phase_name=phase_name,
        status="completed",
        started_at=datetime.now(UTC),
        duration_seconds=1.0,
    )
    session.add(cp)
    session.flush()
    return cp


@pytest.fixture
def duck() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


class TestCleanupUnknownPhase:
    def test_unknown_phase_is_noop(self, session: Session, duck: duckdb.DuckDBPyConnection) -> None:
        """Unknown phase returns 0, no error."""
        result = cleanup_phase("nonexistent_phase", "src_001", session, duck)
        assert result == 0

    def test_import_not_in_cleanup_map(self) -> None:
        """Import phase is not registered in the cleanup map."""
        assert "import" not in _CLEANUP_MAP


class TestCleanupStatistics:
    def test_deletes_profiles(self, session: Session, duck: duckdb.DuckDBPyConnection) -> None:
        """Cleanup statistics deletes StatisticalProfile records."""
        from dataraum.analysis.statistics.db_models import StatisticalProfile

        source = _make_source(session)
        table = _make_table(session, source.source_id, layer="typed")
        col = _make_column(session, table.table_id)

        profile = StatisticalProfile(
            column_id=col.column_id,
            layer="typed",
            total_count=100,
            null_count=0,
            null_ratio=0.0,
            distinct_count=10,
            cardinality_ratio=0.1,
            profile_data={},
        )
        session.add(profile)
        _make_checkpoint(session, source.source_id, "statistics")
        session.flush()

        count = cleanup_phase("statistics", source.source_id, session, duck)
        session.flush()

        assert count >= 2  # profile + checkpoint
        remaining = session.query(StatisticalProfile).filter_by(column_id=col.column_id).all()
        assert len(remaining) == 0


class TestCleanupBusinessCycles:
    def test_deletes_by_source(self, session: Session, duck: duckdb.DuckDBPyConnection) -> None:
        """Cleanup business_cycles deletes DetectedBusinessCycle records scoped to source."""
        from dataraum.analysis.cycles.db_models import DetectedBusinessCycle

        source = _make_source(session)
        other_source = _make_source(session)

        cycle = DetectedBusinessCycle(
            source_id=source.source_id,
            cycle_name="Monthly Billing",
            cycle_type="monthly",
            confidence=0.9,
        )
        other_cycle = DetectedBusinessCycle(
            source_id=other_source.source_id,
            cycle_name="Quarterly Review",
            cycle_type="quarterly",
            confidence=0.8,
        )
        session.add_all([cycle, other_cycle])
        _make_checkpoint(session, source.source_id, "business_cycles")
        session.flush()

        count = cleanup_phase("business_cycles", source.source_id, session, duck)
        session.flush()

        assert count >= 2  # cycle + checkpoint

        # Other source's cycle should remain
        remaining = (
            session.query(DetectedBusinessCycle).filter_by(source_id=other_source.source_id).all()
        )
        assert len(remaining) == 1


class TestCleanupCheckpoint:
    def test_always_deletes_checkpoint(
        self, session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Cleanup always deletes PhaseCheckpoint for the phase+source, even if no other data."""
        source = _make_source(session)
        _make_checkpoint(session, source.source_id, "statistics")
        session.flush()

        count = cleanup_phase("statistics", source.source_id, session, duck)
        session.flush()

        assert count >= 1
        remaining = (
            session.query(PhaseCheckpoint)
            .filter_by(source_id=source.source_id, phase_name="statistics")
            .all()
        )
        assert len(remaining) == 0


class TestCleanupTyping:
    def test_deletes_typed_layer(self, session: Session, duck: duckdb.DuckDBPyConnection) -> None:
        """Cleanup typing deletes Table(layer='typed') and its Columns via cascade."""
        source = _make_source(session)
        raw_table = _make_table(session, source.source_id, layer="raw")
        typed_table = _make_table(session, source.source_id, layer="typed")
        typed_col = _make_column(session, typed_table.table_id)
        _make_checkpoint(session, source.source_id, "typing")
        session.flush()

        typed_table_id = typed_table.table_id
        typed_col_id = typed_col.column_id

        count = cleanup_phase("typing", source.source_id, session, duck)
        session.flush()
        session.expire_all()  # Clear identity map so get() hits DB

        assert count >= 2  # typed table + checkpoint

        # Typed table and column should be gone
        assert session.get(Table, typed_table_id) is None
        assert session.get(Column, typed_col_id) is None

        # Raw table should remain
        assert session.get(Table, raw_table.table_id) is not None
