"""Tests for quality summary phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.quality_summary_phase import QualitySummaryPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestQualitySummaryPhase:
    """Tests for QualitySummaryPhase."""

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
        from dataraum.analysis.slicing.db_models import SliceDefinition

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

    def test_skip_when_already_generated(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when quality reports already exist for source columns."""
        from dataraum.analysis.quality_summary.db_models import ColumnQualityReport
        from dataraum.analysis.slicing.db_models import SliceDefinition

        phase = QualitySummaryPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())
        column_id = str(uuid4())

        # Create source, typed table, column, slice definition
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
            distinct_values=["US", "EU"],
            reasoning="Geographic segmentation",
        )
        session.add(slice_def)
        session.flush()

        # Add an existing quality report for this column
        report = ColumnQualityReport(
            report_id=str(uuid4()),
            source_column_id=column_id,
            slice_column_id=column_id,
            column_name="region",
            source_table_name="test_table",
            slice_column_name="region",
            slice_count=2,
            overall_quality_score=0.85,
            quality_grade="B",
            summary="Good quality",
            report_data={},
            investigation_views=[],
        )
        session.add(report)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "already generated" in skip_reason

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
