"""Tests for temporal phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum_context.pipeline.base import PhaseContext, PhaseStatus
from dataraum_context.pipeline.phases import TemporalPhase
from dataraum_context.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestTemporalPhase:
    """Tests for TemporalPhase."""

    def test_phase_properties(self):
        phase = TemporalPhase()
        assert phase.name == "temporal"
        assert phase.description == "Temporal pattern and trend analysis"
        assert phase.dependencies == ["typing"]
        assert phase.outputs == ["temporal_profiles"]

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = TemporalPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "No typed tables" in skip_reason

    def test_skip_when_no_temporal_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when typed tables have no temporal columns."""
        phase = TemporalPhase()
        source_id = str(uuid4())

        # Create a source with a typed table containing only non-temporal columns
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        session.add(table)

        # Add only non-temporal columns
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name="name",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",
        )
        session.add(col)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "No temporal columns" in skip_reason

    def test_fails_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test failure when run without typed tables."""
        phase = TemporalPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "No typed tables" in (result.error or "")

    def test_returns_empty_when_no_temporal_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test returns empty results when no temporal columns exist."""
        phase = TemporalPhase()
        source_id = str(uuid4())

        # Create a typed table with non-temporal columns only
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        session.add(table)

        # Add only non-temporal column
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name="category",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",
        )
        session.add(col)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        result = phase.run(ctx)

        # Should succeed with message about no temporal columns
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["temporal_profiles"] == []
        assert "No temporal columns" in result.outputs.get("message", "")

    def test_does_not_skip_with_temporal_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when temporal columns exist and aren't profiled."""
        phase = TemporalPhase()
        source_id = str(uuid4())

        # Create a source with a typed table containing temporal columns
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        session.add(table)

        # Add a DATE column
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name="created_at",
            column_position=0,
            raw_type="DATE",
            resolved_type="DATE",
        )
        session.add(col)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should not skip - has temporal columns that need profiling
        assert skip_reason is None

    def test_recognizes_timestamp_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test that TIMESTAMP columns are recognized as temporal."""
        phase = TemporalPhase()
        source_id = str(uuid4())

        # Create a source with a typed table containing TIMESTAMP columns
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        session.add(table)

        # Add a TIMESTAMP column
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name="event_time",
            column_position=0,
            raw_type="TIMESTAMP",
            resolved_type="TIMESTAMP",
        )
        session.add(col)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should not skip - TIMESTAMP is a temporal type
        assert skip_reason is None

    def test_recognizes_timestamptz_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test that TIMESTAMPTZ columns are recognized as temporal."""
        phase = TemporalPhase()
        source_id = str(uuid4())

        # Create a source with a typed table containing TIMESTAMPTZ columns
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        session.add(table)

        # Add a TIMESTAMPTZ column
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name="created_at_utc",
            column_position=0,
            raw_type="TIMESTAMPTZ",
            resolved_type="TIMESTAMPTZ",
        )
        session.add(col)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should not skip - TIMESTAMPTZ is a temporal type
        assert skip_reason is None
