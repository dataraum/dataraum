"""Tests for semantic phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import SemanticPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestSemanticPhase:
    """Tests for SemanticPhase."""

    def test_phase_properties(self):
        phase = SemanticPhase()
        assert phase.name == "semantic"
        assert phase.description == "LLM-powered semantic analysis"
        assert phase.dependencies == ["statistics", "relationships", "correlations"]
        assert phase.outputs == ["annotations", "entities", "confirmed_relationships"]
        assert phase.is_llm_phase is True

    def test_skip_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = SemanticPhase()
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

    def test_skip_when_no_columns(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """Test skip when typed tables have no columns."""
        phase = SemanticPhase()
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

    def test_does_not_skip_with_unannotated_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test does not skip when columns exist without annotations."""
        phase = SemanticPhase()
        source_id = str(uuid4())

        # Create a source with a typed table and columns
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

        # Add some columns
        for i, name in enumerate(["id", "name", "amount"]):
            col = Column(
                column_id=str(uuid4()),
                table_id=table.table_id,
                column_name=name,
                column_position=i,
                raw_type="VARCHAR",
                resolved_type="VARCHAR",
            )
            session.add(col)

        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should not skip - columns need annotation
        assert skip_reason is None

    def test_fails_when_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test failure when run without typed tables."""
        phase = SemanticPhase()
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

    def test_skip_when_all_annotated(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when all columns already have semantic annotations."""
        from dataraum.analysis.semantic.db_models import SemanticAnnotation

        phase = SemanticPhase()
        source_id = str(uuid4())

        # Create a source with a typed table and columns
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

        # Add columns with annotations
        for i, name in enumerate(["id", "name"]):
            col_id = str(uuid4())
            col = Column(
                column_id=col_id,
                table_id=table.table_id,
                column_name=name,
                column_position=i,
                raw_type="VARCHAR",
                resolved_type="VARCHAR",
            )
            session.add(col)

            # Add LLM annotation
            annotation = SemanticAnnotation(
                column_id=col_id,
                semantic_role="identifier" if name == "id" else "attribute",
                annotation_source="llm",
                confidence=0.9,
            )
            session.add(annotation)

        session.commit()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = phase.should_skip(ctx)
        # Should skip - all columns already annotated
        assert skip_reason is not None
        assert "already have semantic annotations" in skip_reason
