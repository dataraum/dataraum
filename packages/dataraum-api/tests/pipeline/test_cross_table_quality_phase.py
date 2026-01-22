"""Tests for cross-table quality phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import CrossTableQualityPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestCrossTableQualityPhase:
    """Tests for CrossTableQualityPhase."""

    def test_phase_properties(self):
        phase = CrossTableQualityPhase()
        assert phase.name == "cross_table_quality"
        assert phase.description == "Cross-table correlation analysis"
        assert phase.dependencies == ["semantic"]
        assert phase.outputs == ["cross_table_correlations", "multicollinearity_groups"]
        # Note: Despite the name, this is actually a non-LLM phase (statistical analysis)
        assert phase.is_llm_phase is False

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = CrossTableQualityPhase()
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
        phase = CrossTableQualityPhase()
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

    def test_skip_when_no_confirmed_relationships(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no confirmed relationships exist."""
        phase = CrossTableQualityPhase()
        source_id = str(uuid4())

        # Create a source with typed tables but no relationships
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table1 = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table_1",
            layer="typed",
            duckdb_path="typed_test_table_1",
            row_count=10,
        )
        session.add(table1)

        table2 = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table_2",
            layer="typed",
            duckdb_path="typed_test_table_2",
            row_count=10,
        )
        session.add(table2)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "No confirmed relationships" in skip_reason

    def test_does_not_skip_with_confirmed_relationships(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when confirmed relationships exist."""
        from dataraum.analysis.relationships.db_models import Relationship

        phase = CrossTableQualityPhase()
        source_id = str(uuid4())
        table1_id = str(uuid4())
        table2_id = str(uuid4())
        col1_id = str(uuid4())
        col2_id = str(uuid4())

        # Create a source with typed tables
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        session.add(source)

        table1 = Table(
            table_id=table1_id,
            source_id=source_id,
            table_name="test_table_1",
            layer="typed",
            duckdb_path="typed_test_table_1",
            row_count=10,
        )
        session.add(table1)

        table2 = Table(
            table_id=table2_id,
            source_id=source_id,
            table_name="test_table_2",
            layer="typed",
            duckdb_path="typed_test_table_2",
            row_count=10,
        )
        session.add(table2)

        col1 = Column(
            column_id=col1_id,
            table_id=table1_id,
            column_name="id",
            column_position=0,
            raw_type="INTEGER",
        )
        session.add(col1)

        col2 = Column(
            column_id=col2_id,
            table_id=table2_id,
            column_name="parent_id",
            column_position=0,
            raw_type="INTEGER",
        )
        session.add(col2)

        # Add a confirmed (LLM-detected) relationship
        relationship = Relationship(
            from_table_id=table1_id,
            from_column_id=col1_id,
            to_table_id=table2_id,
            to_column_id=col2_id,
            relationship_type="foreign_key",
            confidence=0.95,
            detection_method="llm",
        )
        session.add(relationship)
        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should not skip - relationships need quality analysis
        assert skip_reason is None

    def test_success_with_no_relationships(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test returns success (empty results) when no confirmed relationships."""
        phase = CrossTableQualityPhase()
        source_id = str(uuid4())

        # Create a source with typed tables but no relationships
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
        assert result.outputs["relationships_analyzed"] == 0
        assert "No confirmed relationships" in result.outputs.get("message", "")
