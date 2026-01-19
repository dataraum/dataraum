"""Tests for quality summary phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum_context.pipeline.base import PhaseContext, PhaseStatus
from dataraum_context.pipeline.phases import QualitySummaryPhase
from dataraum_context.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestQualitySummaryPhase:
    """Tests for QualitySummaryPhase."""

    def test_phase_properties(self):
        phase = QualitySummaryPhase()
        assert phase.name == "quality_summary"
        assert phase.description == "LLM quality report generation"
        assert phase.dependencies == ["slice_analysis"]
        assert phase.outputs == ["quality_reports", "quality_grades"]
        assert phase.is_llm_phase is True

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = QualitySummaryPhase()
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
        phase = QualitySummaryPhase()
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

    def test_skip_when_no_slice_definitions(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no slice definitions exist."""
        phase = QualitySummaryPhase()
        source_id = str(uuid4())

        # Create a source with a typed table but no slice definitions
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
        assert "No slice definitions" in skip_reason

    def test_does_not_skip_with_slice_definitions(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when slice definitions exist without summaries."""
        from dataraum_context.analysis.slicing.db_models import SliceDefinition

        phase = QualitySummaryPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())
        column_id = str(uuid4())

        # Create a source with a typed table and slice definition
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
            row_count=10,
        )
        session.add(table)

        column = Column(
            column_id=column_id,
            table_id=table_id,
            column_name="region",
            column_position=0,
            raw_type="VARCHAR",
        )
        session.add(column)

        slice_def = SliceDefinition(
            table_id=table_id,
            column_id=column_id,
            slice_priority=1,
            slice_type="categorical",
            distinct_values=["US", "EU", "APAC"],
            reasoning="Geographic segmentation",
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
        # Should not skip - slices need quality summary
        assert skip_reason is None

    def test_success_no_slice_definitions(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test returns success (empty) when no slice definitions."""
        phase = QualitySummaryPhase()
        source_id = str(uuid4())

        # Create a source with a typed table but no slice definitions
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

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["quality_reports"] == 0
        assert "No slice definitions" in result.outputs.get("message", "")
