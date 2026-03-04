"""Tests for temporal slice analysis phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.temporal_slice_analysis_phase import TemporalSliceAnalysisPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestTemporalSliceAnalysisPhase:
    """Tests for TemporalSliceAnalysisPhase."""

    def test_phase_properties(self):
        phase = TemporalSliceAnalysisPhase()
        assert phase.name == "temporal_slice_analysis"
        assert phase.description == "Distribution drift analysis on slices"
        assert phase.dependencies == ["slice_analysis", "temporal"]

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
        from dataraum.analysis.slicing.db_models import SliceDefinition

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
        assert "temporal" in skip_reason.lower()

    def test_does_not_skip_with_slices_and_temporal(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when both slice definitions and temporal columns exist."""
        from dataraum.analysis.slicing.db_models import SliceDefinition
        from dataraum.analysis.temporal import TemporalColumnProfile

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

    def test_multi_table_per_table_time_column_resolution(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Tables with VARCHAR 'date' column are skipped; DATE 'date' tables are analyzed.

        Regression test: the phase used to pick ONE global time_column name and
        apply it to all tables. When table_a has 'date' as DATE and table_b has
        'date' as VARCHAR, slices from table_b would fail with CAST errors.
        Now time column resolution is per-table, so table_b is simply skipped.
        """
        from datetime import UTC, datetime

        from dataraum.analysis.slicing.db_models import SliceDefinition
        from dataraum.analysis.temporal import TemporalColumnProfile

        phase = TemporalSliceAnalysisPhase()
        source_id = str(uuid4())

        # -- Source
        source = Source(source_id=source_id, name="multi", source_type="csv")
        session.add(source)

        # -- Table A: "date" column is DATE (should be analyzed)
        table_a_id = str(uuid4())
        table_a = Table(
            table_id=table_a_id,
            source_id=source_id,
            table_name="typed_transactions",
            layer="typed",
            duckdb_path="typed_transactions",
            row_count=100,
        )
        session.add(table_a)

        date_col_a_id = str(uuid4())
        date_col_a = Column(
            column_id=date_col_a_id,
            table_id=table_a_id,
            column_name="date",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="DATE",
        )
        session.add(date_col_a)

        region_col_a_id = str(uuid4())
        region_col_a = Column(
            column_id=region_col_a_id,
            table_id=table_a_id,
            column_name="region",
            column_position=1,
            raw_type="VARCHAR",
        )
        session.add(region_col_a)

        # -- Table B: "date" column is VARCHAR (should be skipped)
        table_b_id = str(uuid4())
        table_b = Table(
            table_id=table_b_id,
            source_id=source_id,
            table_name="typed_invoices",
            layer="typed",
            duckdb_path="typed_invoices",
            row_count=50,
        )
        session.add(table_b)

        date_col_b_id = str(uuid4())
        date_col_b = Column(
            column_id=date_col_b_id,
            table_id=table_b_id,
            column_name="date",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",  # NOT a temporal type
        )
        session.add(date_col_b)

        category_col_b_id = str(uuid4())
        category_col_b = Column(
            column_id=category_col_b_id,
            table_id=table_b_id,
            column_name="category",
            column_position=1,
            raw_type="VARCHAR",
        )
        session.add(category_col_b)

        # -- Temporal profiles (both tables have one, but B's column is VARCHAR)
        session.commit()

        tp_a = TemporalColumnProfile(
            profile_id=str(uuid4()),
            column_id=date_col_a_id,
            profiled_at=datetime.now(UTC),
            min_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            max_timestamp=datetime(2024, 6, 30, tzinfo=UTC),
            detected_granularity="daily",
            profile_data={},
        )
        session.add(tp_a)

        tp_b = TemporalColumnProfile(
            profile_id=str(uuid4()),
            column_id=date_col_b_id,
            profiled_at=datetime.now(UTC),
            min_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            max_timestamp=datetime(2024, 6, 30, tzinfo=UTC),
            detected_granularity="daily",
            profile_data={},
        )
        session.add(tp_b)

        # -- Semantic annotation: only table A has a time_column identified
        from dataraum.analysis.semantic.db_models import TableEntity

        entity_a = TableEntity(
            table_id=table_a_id,
            detected_entity_type="transaction",
            time_column="date",
        )
        session.add(entity_a)

        # -- Slice definitions for both tables
        slice_def_a = SliceDefinition(
            table_id=table_a_id,
            column_id=region_col_a_id,
            slice_priority=1,
            slice_type="categorical",
            distinct_values=["US", "EU"],
            reasoning="region",
        )
        session.add(slice_def_a)

        slice_def_b = SliceDefinition(
            table_id=table_b_id,
            column_id=category_col_b_id,
            slice_priority=1,
            slice_type="categorical",
            distinct_values=["food", "rent"],
            reasoning="category",
        )
        session.add(slice_def_b)
        session.commit()

        # -- Create DuckDB slice tables for table A only (table B would fail anyway)
        duckdb_conn.execute("""
            CREATE TABLE slice_region_us AS
            SELECT '2024-01-15'::DATE AS date, 'US' AS region, 100.0 AS amount
            UNION ALL
            SELECT '2024-02-15'::DATE, 'US', 200.0
            UNION ALL
            SELECT '2024-03-15'::DATE, 'US', 150.0
        """)
        duckdb_conn.execute("""
            CREATE TABLE slice_region_eu AS
            SELECT '2024-01-20'::DATE AS date, 'EU' AS region, 300.0 AS amount
            UNION ALL
            SELECT '2024-02-20'::DATE, 'EU', 250.0
        """)

        # Register slice tables in metadata
        for name in ["slice_region_us", "slice_region_eu"]:
            st = Table(
                table_id=str(uuid4()),
                source_id=source_id,
                table_name=name,
                layer="slice",
                duckdb_path=name,
                row_count=3 if "us" in name else 2,
            )
            session.add(st)
        session.commit()

        # -- Run the phase
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        result = phase.run(ctx)

        # Phase should succeed — no errors from table B's VARCHAR column
        assert result.status == PhaseStatus.COMPLETED, f"Phase failed: {result.error}, outputs: {result.outputs}"
        assert result.error is None
        errors = result.outputs.get("errors", [])
        assert not errors, f"Unexpected errors: {errors}"

        # Only table A's time column should have been used
        assert "time_columns" in result.outputs, f"Missing time_columns in outputs: {result.outputs}"
        assert result.outputs["time_columns"] == ["date"]

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
        assert result.outputs["drift_summaries"] == 0
        assert "No slice definitions" in result.outputs.get("message", "")
