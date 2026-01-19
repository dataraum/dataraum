"""Tests for business cycles phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum_context.pipeline.base import PhaseContext, PhaseStatus
from dataraum_context.pipeline.phases import BusinessCyclesPhase
from dataraum_context.storage import Source, Table

if TYPE_CHECKING:
    import duckdb


class TestBusinessCyclesPhase:
    """Tests for BusinessCyclesPhase."""

    def test_phase_properties(self):
        phase = BusinessCyclesPhase()
        assert phase.name == "business_cycles"
        assert phase.description == "Expert LLM cycle detection"
        assert phase.dependencies == ["semantic", "temporal"]
        assert phase.outputs == ["detected_cycles", "business_processes"]
        assert phase.is_llm_phase is True

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = BusinessCyclesPhase()
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
        phase = BusinessCyclesPhase()
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
        """Test does not skip when typed tables exist without analysis."""
        phase = BusinessCyclesPhase()
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
        # Should not skip - tables need business cycle analysis
        assert skip_reason is None

    def test_skip_when_already_analyzed(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when business cycle analysis already run."""
        from datetime import UTC, datetime

        from dataraum_context.analysis.cycles.db_models import BusinessCycleAnalysisRun

        phase = BusinessCyclesPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())

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
            row_count=10,
        )
        session.add(table)

        # Add business cycle analysis run
        analysis_run = BusinessCycleAnalysisRun(
            table_ids=[table_id],
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            total_cycles_detected=2,
            detected_processes=["order_to_cash"],
        )
        session.add(analysis_run)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "already run" in skip_reason
