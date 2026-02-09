"""Tests for slice analysis phase."""

from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import SliceAnalysisPhase
from dataraum.storage import Column, Source, Table


class TestSliceAnalysisPhase:
    """Tests for SliceAnalysisPhase."""

    def test_phase_properties(self):
        phase = SliceAnalysisPhase()
        assert phase.name == "slice_analysis"
        assert phase.description == "Execute slice SQL and analyze slice tables"
        assert phase.dependencies == ["slicing"]
        assert phase.outputs == ["slice_profiles"]
        assert phase.is_llm_phase is False

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = SliceAnalysisPhase()
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

    def test_skip_when_no_slice_definitions(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no slice definitions exist."""
        phase = SliceAnalysisPhase()
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
        assert skip_reason is not None
        assert "No slice definitions" in skip_reason

    def test_does_not_skip_with_slice_definitions(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when slice definitions exist."""
        phase = SliceAnalysisPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())
        col_id = str(uuid4())

        # Create a source with a typed table
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

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

        # Add slice definition with values
        slice_def = SliceDefinition(
            table_id=table_id,
            column_id=col_id,
            slice_priority=1,
            slice_type="categorical",
            distinct_values=["A", "B", "C"],
            value_count=3,
            reasoning="Test slice",
            detection_source="llm",
            sql_template="CREATE TABLE IF NOT EXISTS slice_category_{value} AS SELECT * FROM typed_test_table WHERE category = '{value}'",
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
        # Should not skip - slice definitions exist but tables not yet created
        assert skip_reason is None

    def test_skip_when_all_slices_analyzed(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when all slice tables already exist."""
        phase = SliceAnalysisPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())
        col_id = str(uuid4())

        # Create a source with a typed table
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

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

        # Add slice definition
        slice_def = SliceDefinition(
            table_id=table_id,
            column_id=col_id,
            slice_priority=1,
            slice_type="categorical",
            distinct_values=["A"],  # One slice value
            value_count=1,
            reasoning="Test slice",
            detection_source="llm",
        )
        session.add(slice_def)

        # Create existing slice table
        slice_table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="slice_category_a",
            layer="slice",
            duckdb_path="slice_category_a",
            row_count=50,
        )
        session.add(slice_table)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should skip - all slices already exist
        assert skip_reason is not None
        assert "already analyzed" in skip_reason

    def test_fails_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test failure when run without typed tables."""
        phase = SliceAnalysisPhase()
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

    def test_success_no_slice_definitions(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test success with no slice definitions returns empty result."""
        phase = SliceAnalysisPhase()
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
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["slice_profiles"] == 0
        assert "No slice definitions" in result.outputs.get("message", "")
