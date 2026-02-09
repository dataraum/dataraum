"""Tests for entropy phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import EntropyPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestEntropyPhase:
    """Tests for EntropyPhase."""

    def test_phase_properties(self):
        phase = EntropyPhase()
        assert phase.name == "entropy"
        assert phase.description == "Entropy detection across all dimensions"
        assert phase.dependencies == [
            "typing",
            "statistics",
            "semantic",
            "relationships",
            "correlations",
            "quality_summary",
        ]
        assert phase.outputs == ["entropy_profiles", "compound_risks"]
        assert phase.is_llm_phase is False

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = EntropyPhase()
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
        phase = EntropyPhase()
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

    def test_skip_when_no_columns(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """Test skip when typed tables have no columns."""
        phase = EntropyPhase()
        source_id = str(uuid4())

        # Create a source with a typed table but no columns
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
        assert "No columns" in skip_reason

    def test_does_not_skip_with_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when columns exist without entropy profiles."""
        phase = EntropyPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())

        # Create a source with a typed table and columns
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
            column_id=str(uuid4()),
            table_id=table_id,
            column_name="test_column",
            column_position=0,
            raw_type="VARCHAR",
        )
        session.add(column)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should not skip - columns need entropy analysis
        assert skip_reason is None

    def test_skip_when_all_have_entropy(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when all columns already have entropy profiles."""
        from dataraum.entropy.db_models import EntropyObjectRecord

        phase = EntropyPhase()
        source_id = str(uuid4())
        table_id = str(uuid4())
        column_id = str(uuid4())

        # Create a source with a typed table and columns
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
            column_name="test_column",
            column_position=0,
            raw_type="VARCHAR",
        )
        session.add(column)
        session.commit()

        # Add entropy record for the column (after parent records exist)
        entropy_record = EntropyObjectRecord(
            source_id=source_id,
            table_id=table_id,
            column_id=column_id,
            target="column:test_table.test_column",
            layer="structural",
            dimension="schema",
            sub_dimension="naming",
            score=0.3,
            detector_id="test",
        )
        session.add(entropy_record)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "already have entropy" in skip_reason
