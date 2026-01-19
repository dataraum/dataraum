"""Tests for context phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum_context.pipeline.base import PhaseContext, PhaseStatus
from dataraum_context.pipeline.phases import ContextPhase
from dataraum_context.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestContextPhase:
    """Tests for ContextPhase."""

    def test_phase_properties(self):
        phase = ContextPhase()
        assert phase.name == "context"
        assert phase.description == "Build execution context for graph agent"
        assert phase.dependencies == ["entropy_interpretation", "quality_summary"]
        assert phase.outputs == ["execution_context"]
        assert phase.is_llm_phase is False

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = ContextPhase()
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

    def test_fails_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test failure when run without typed tables."""
        phase = ContextPhase()
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

    def test_does_not_skip_with_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when typed tables exist."""
        phase = ContextPhase()
        source_id = str(uuid4())

        # Create a source with a typed table
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
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should not skip - need to build context
        assert skip_reason is None

    def test_success_builds_context(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """Test successful context building with minimal data."""
        phase = ContextPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())

        # Create a source with a typed table and column
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        # Create table in DuckDB
        duckdb_conn.execute("""
            CREATE TABLE typed_test_table (
                id INTEGER,
                name VARCHAR
            )
        """)
        duckdb_conn.execute("""
            INSERT INTO typed_test_table VALUES (1, 'test'), (2, 'test2')
        """)

        table = Table(
            table_id=table_id,
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=2,
        )
        session.add(table)

        column1 = Column(
            column_id=str(uuid4()),
            table_id=table_id,
            column_name="id",
            column_position=0,
            raw_type="INTEGER",
        )
        session.add(column1)

        column2 = Column(
            column_id=str(uuid4()),
            table_id=table_id,
            column_name="name",
            column_position=1,
            raw_type="VARCHAR",
        )
        session.add(column2)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["tables"] == 1
        assert result.outputs["columns"] == 2
        assert result.records_created == 1  # One context object

    def test_context_with_slice_filter(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test context building with slice filter config."""
        phase = ContextPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())

        # Create a source with a typed table
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        # Create table in DuckDB
        duckdb_conn.execute("""
            CREATE TABLE typed_test_table (
                id INTEGER,
                region VARCHAR
            )
        """)
        duckdb_conn.execute("""
            INSERT INTO typed_test_table VALUES (1, 'US'), (2, 'EU')
        """)

        table = Table(
            table_id=table_id,
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=2,
        )
        session.add(table)

        column1 = Column(
            column_id=str(uuid4()),
            table_id=table_id,
            column_name="id",
            column_position=0,
            raw_type="INTEGER",
        )
        session.add(column1)

        column2 = Column(
            column_id=str(uuid4()),
            table_id=table_id,
            column_name="region",
            column_position=1,
            raw_type="VARCHAR",
        )
        session.add(column2)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={
                "slice_column": "region",
                "slice_value": "US",
            },
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["tables"] == 1
