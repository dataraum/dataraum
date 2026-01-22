"""Tests for slicing phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import SlicingPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestSlicingPhase:
    """Tests for SlicingPhase."""

    def test_phase_properties(self):
        phase = SlicingPhase()
        assert phase.name == "slicing"
        assert phase.description == "LLM-powered slice dimension identification"
        assert phase.dependencies == ["semantic"]
        assert phase.outputs == ["slice_definitions", "slice_queries"]
        assert phase.is_llm_phase is True

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = SlicingPhase()
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

    def test_does_not_skip_with_unsliced_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when tables exist without slice definitions."""
        phase = SlicingPhase()
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
            row_count=100,
        )
        session.add(table)

        # Add some columns
        for i, name in enumerate(["category", "region", "amount"]):
            col = Column(
                column_id=str(uuid4()),
                table_id=table.table_id,
                column_name=name,
                column_position=i,
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
        # Should not skip - tables need slicing analysis
        assert skip_reason is None

    def test_fails_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test failure when run without typed tables."""
        phase = SlicingPhase()
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

    def test_skip_when_all_sliced(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """Test skip when all tables already have slice definitions."""
        from dataraum.analysis.slicing.db_models import SliceDefinition

        phase = SlicingPhase()
        source_id = str(uuid4())

        # Create a source with a typed table
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table_id = str(uuid4())
        col_id = str(uuid4())

        table = Table(
            table_id=table_id,
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=100,
        )
        session.add(table)

        col = Column(
            column_id=col_id,
            table_id=table_id,
            column_name="category",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",
        )
        session.add(col)

        # Add existing slice definition
        slice_def = SliceDefinition(
            table_id=table_id,
            column_id=col_id,
            slice_priority=1,
            slice_type="categorical",
            distinct_values=["A", "B", "C"],
            value_count=3,
            reasoning="Test slice",
            detection_source="llm",
        )
        session.add(slice_def)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should skip - all tables already have slice definitions
        assert skip_reason is not None
        assert "already have slice definitions" in skip_reason
