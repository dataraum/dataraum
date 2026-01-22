"""Tests for validation phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import ValidationPhase
from dataraum.storage import Source, Table

if TYPE_CHECKING:
    import duckdb


class TestValidationPhase:
    """Tests for ValidationPhase."""

    def test_phase_properties(self):
        phase = ValidationPhase()
        assert phase.name == "validation"
        assert phase.description == "LLM-powered validation checks"
        assert phase.dependencies == ["semantic"]
        assert phase.outputs == ["validation_results"]
        assert phase.is_llm_phase is True

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = ValidationPhase()
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
        phase = ValidationPhase()
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
        """Test does not skip when typed tables exist without validation."""
        phase = ValidationPhase()
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
        # Should not skip - tables need validation
        assert skip_reason is None

    def test_skip_when_already_validated(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when all tables already validated."""
        from datetime import UTC, datetime

        from dataraum.analysis.validation.db_models import ValidationRunRecord

        phase = ValidationPhase()
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

        # Add validation run
        validation_run = ValidationRunRecord(
            table_ids=[table_id],
            table_name="test_table",
            total_checks=5,
            passed_checks=4,
            failed_checks=1,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )
        session.add(validation_run)
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
