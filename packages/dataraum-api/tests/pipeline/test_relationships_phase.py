"""Tests for relationships phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import RelationshipsPhase
from dataraum.storage import Source, Table

if TYPE_CHECKING:
    import duckdb


class TestRelationshipsPhase:
    """Tests for RelationshipsPhase."""

    def test_phase_properties(self):
        phase = RelationshipsPhase()
        assert phase.name == "relationships"
        assert phase.description == "Cross-table relationship detection"
        assert phase.dependencies == ["statistics"]
        assert phase.outputs == ["relationship_candidates"]

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = RelationshipsPhase()
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

    def test_skip_when_single_table(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """Test skip when only one typed table exists."""
        phase = RelationshipsPhase()
        source_id = str(uuid4())

        # Create a source with a single typed table
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
        assert skip_reason is not None
        assert "at least 2 tables" in skip_reason

    def test_fails_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test failure when run without typed tables."""
        phase = RelationshipsPhase()
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

    def test_returns_success_with_single_table(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test returns success with message when only one table exists."""
        phase = RelationshipsPhase()
        source_id = str(uuid4())

        # Create a source with a single typed table
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

        result = phase.run(ctx)

        # Should succeed with message about needing 2 tables
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["relationship_candidates"] == []
        assert "at least 2" in result.outputs.get("message", "")

    def test_does_not_skip_with_multiple_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when multiple tables exist and no relationships detected."""
        phase = RelationshipsPhase()
        source_id = str(uuid4())

        # Create a source with multiple typed tables
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        for i in range(3):
            table = Table(
                table_id=str(uuid4()),
                source_id=source_id,
                table_name=f"test_table_{i}",
                layer="typed",
                duckdb_path=f"typed_test_table_{i}",
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
        # Should not skip - has multiple tables and no existing relationships
        assert skip_reason is None
