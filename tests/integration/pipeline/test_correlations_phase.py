"""Tests for correlations phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import CorrelationsPhase
from dataraum.storage import Source, Table

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
        from dataraum.analysis.correlation.db_models import DerivedColumn as DerivedColumnDB
        from dataraum.storage import Column

        phase = CorrelationsPhase()
        source_id = str(uuid4())

        # Create a source with a typed table and column
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

        col1_id = str(uuid4())
        col2_id = str(uuid4())
        session.add(
            Column(column_id=col1_id, table_id=table_id, column_name="a", column_position=0)
        )
        session.add(
            Column(column_id=col2_id, table_id=table_id, column_name="b", column_position=1)
        )
        session.flush()

        # Seed a DerivedColumn so the table is considered analyzed
        session.add(
            DerivedColumnDB(
                table_id=table_id,
                derived_column_id=col2_id,
                source_column_ids=[col1_id],
                derivation_type="sum",
                formula="a + a",
                match_rate=1.0,
                total_rows=10,
                matching_rows=10,
            )
        )
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
        from dataraum.analysis.correlation.db_models import DerivedColumn as DerivedColumnDB
        from dataraum.storage import Column

        phase = CorrelationsPhase()
        source_id = str(uuid4())

        # Create a source with a typed table and column
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

        col1_id = str(uuid4())
        col2_id = str(uuid4())
        session.add(
            Column(column_id=col1_id, table_id=table_id, column_name="a", column_position=0)
        )
        session.add(
            Column(column_id=col2_id, table_id=table_id, column_name="b", column_position=1)
        )
        session.flush()

        # Seed a DerivedColumn so the table is considered analyzed
        session.add(
            DerivedColumnDB(
                table_id=table_id,
                derived_column_id=col2_id,
                source_column_ids=[col1_id],
                derivation_type="sum",
                formula="a + a",
                match_rate=1.0,
                total_rows=10,
                matching_rows=10,
            )
        )
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
