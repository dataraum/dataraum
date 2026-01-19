"""Tests for temporal slice analysis phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum_context.pipeline.base import PhaseContext, PhaseStatus
from dataraum_context.pipeline.phases import TemporalSliceAnalysisPhase
from dataraum_context.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestTemporalSliceAnalysisPhase:
    """Tests for TemporalSliceAnalysisPhase."""

    def test_phase_properties(self):
        phase = TemporalSliceAnalysisPhase()
        assert phase.name == "temporal_slice_analysis"
        assert phase.description == "Temporal + topology analysis on slices"
        assert phase.dependencies == ["slice_analysis", "temporal"]
        assert phase.outputs == ["temporal_slice_profiles", "slice_topology", "topology_drift"]
        assert phase.is_llm_phase is False

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = TemporalSliceAnalysisPhase()
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
        phase = TemporalSliceAnalysisPhase()
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
        phase = TemporalSliceAnalysisPhase()
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

    def test_skip_when_no_temporal_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no temporal columns detected."""
        from dataraum_context.analysis.slicing.db_models import SliceDefinition

        phase = TemporalSliceAnalysisPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())
        column_id = str(uuid4())

        # Create a source with slice definitions but no temporal columns
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

    def test_does_not_skip_with_slices_and_temporal(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when both slice definitions and temporal columns exist."""
        from dataraum_context.analysis.slicing.db_models import SliceDefinition
        from dataraum_context.analysis.temporal import TemporalColumnProfile

        phase = TemporalSliceAnalysisPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())
        region_col_id = str(uuid4())
        date_col_id = str(uuid4())

        # Create a source with slice definitions and temporal columns
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

        region_col = Column(
            column_id=region_col_id,
            table_id=table_id,
            column_name="region",
            column_position=0,
            raw_type="VARCHAR",
        )
        session.add(region_col)

        date_col = Column(
            column_id=date_col_id,
            table_id=table_id,
            column_name="created_at",
            column_position=1,
            raw_type="TIMESTAMP",
        )
        session.add(date_col)

        slice_def = SliceDefinition(
            table_id=table_id,
            column_id=region_col_id,
            slice_priority=1,
            slice_type="categorical",
            distinct_values=["US", "EU"],
            reasoning="Geographic segmentation",
        )
        session.add(slice_def)
        session.commit()

        # Add temporal profile for the date column (after parent records exist)
        from datetime import UTC, datetime
        from uuid import uuid4 as uuid4_func

        temporal_profile = TemporalColumnProfile(
            profile_id=str(uuid4_func()),
            column_id=date_col_id,
            profiled_at=datetime.now(UTC),
            min_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            max_timestamp=datetime(2024, 12, 31, tzinfo=UTC),
            detected_granularity="daily",
            profile_data={},
        )
        session.add(temporal_profile)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should not skip - have both slice definitions and temporal columns
        assert skip_reason is None

    def test_success_no_slice_definitions(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test returns success (empty) when no slice definitions."""
        phase = TemporalSliceAnalysisPhase()
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
        assert result.outputs["temporal_analyses"] == 0
        assert "No slice definitions" in result.outputs.get("message", "")
