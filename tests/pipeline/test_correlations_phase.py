"""Tests for correlations phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum_context.pipeline.base import PhaseContext, PhaseStatus
from dataraum_context.pipeline.phases import CorrelationsPhase
from dataraum_context.storage import Source, Table

if TYPE_CHECKING:
    import duckdb


class TestCorrelationsPhase:
    """Tests for CorrelationsPhase."""

    def test_phase_properties(self):
        phase = CorrelationsPhase()
        assert phase.name == "correlations"
        assert phase.description == "Within-table correlation analysis"
        assert phase.dependencies == ["statistics"]
        assert phase.outputs == ["correlations", "derived_columns"]

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = CorrelationsPhase()
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
        phase = CorrelationsPhase()
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

    def test_does_not_skip_with_unanalyzed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when tables haven't been analyzed."""
        phase = CorrelationsPhase()
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
        # Should not skip - tables need analysis
        assert skip_reason is None

    def test_returns_empty_when_all_tables_analyzed(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test returns empty results when all tables already analyzed."""
        from datetime import UTC, datetime

        from dataraum_context.analysis.correlation.db_models import CorrelationAnalysisRun

        phase = CorrelationsPhase()
        source_id = str(uuid4())

        # Create a source with a typed table
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table_id = str(uuid4())
        table = Table(
            table_id=table_id,
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        session.add(table)

        # Mark table as already analyzed
        run_record = CorrelationAnalysisRun(
            target_id=table_id,
            target_type="table",
            rows_analyzed=10,
            columns_analyzed=5,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            duration_seconds=1.0,
        )
        session.add(run_record)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        result = phase.run(ctx)

        # Should succeed with empty results (all tables already analyzed)
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["correlations"] == []
        assert result.records_processed == 0

    def test_skip_when_all_tables_analyzed(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip check when all tables have been analyzed."""
        from datetime import UTC, datetime

        from dataraum_context.analysis.correlation.db_models import CorrelationAnalysisRun

        phase = CorrelationsPhase()
        source_id = str(uuid4())

        # Create a source with a typed table
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table_id = str(uuid4())
        table = Table(
            table_id=table_id,
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        session.add(table)

        # Mark table as already analyzed
        run_record = CorrelationAnalysisRun(
            target_id=table_id,
            target_type="table",
            rows_analyzed=10,
            columns_analyzed=5,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            duration_seconds=1.0,
        )
        session.add(run_record)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "already have correlation analysis" in skip_reason
