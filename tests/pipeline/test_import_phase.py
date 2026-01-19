"""Tests for import phase."""

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum_context.pipeline.base import PhaseContext, PhaseStatus
from dataraum_context.pipeline.phases import ImportPhase
from dataraum_context.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    """Create a simple CSV file for testing."""
    csv_path = tmp_path / "test_data.csv"
    csv_path.write_text(
        """id,name,value
1,Alice,100.5
2,Bob,200.3
3,Charlie,300.1
"""
    )
    return csv_path


@pytest.fixture
def csv_directory(tmp_path: Path) -> Path:
    """Create a directory with multiple CSV files."""
    (tmp_path / "table1.csv").write_text(
        """id,category
1,A
2,B
"""
    )
    (tmp_path / "table2.csv").write_text(
        """id,amount
1,100
2,200
"""
    )
    return tmp_path


class TestImportPhase:
    """Tests for ImportPhase."""

    def test_phase_properties(self):
        phase = ImportPhase()
        assert phase.name == "import"
        assert phase.description == "Load CSV files into raw tables"
        assert phase.dependencies == []
        assert phase.outputs == ["raw_tables"]

    def test_import_single_csv(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, csv_file: Path
    ):
        """Test importing a single CSV file."""
        phase = ImportPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"source_path": str(csv_file)},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert "raw_tables" in result.outputs
        assert len(result.outputs["raw_tables"]) == 1
        assert result.records_processed == 3  # 3 rows
        assert result.records_created == 1  # 1 table

        # Verify Source was created
        source = session.get(Source, source_id)
        assert source is not None
        assert source.source_type == "csv"

        # Verify Table was created
        stmt = select(Table).where(Table.source_id == source_id)
        result_tables = session.execute(stmt)
        tables = result_tables.scalars().all()
        assert len(tables) == 1
        assert tables[0].layer == "raw"
        assert tables[0].row_count == 3

        # Verify Columns were created
        stmt = select(Column).where(Column.table_id == tables[0].table_id)
        result_cols = session.execute(stmt)
        columns = result_cols.scalars().all()
        assert len(columns) == 3
        column_names = {c.column_name for c in columns}
        assert column_names == {"id", "name", "value"}

    def test_import_directory(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        csv_directory: Path,
    ):
        """Test importing multiple CSV files from a directory."""
        phase = ImportPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"source_path": str(csv_directory)},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert "raw_tables" in result.outputs
        assert len(result.outputs["raw_tables"]) == 2
        assert result.records_created == 2  # 2 tables

        # Verify Tables were created
        stmt = select(Table).where(Table.source_id == source_id)
        result_tables = session.execute(stmt)
        tables = result_tables.scalars().all()
        assert len(tables) == 2

    def test_import_missing_path(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """Test error when source_path is not provided."""
        phase = ImportPhase()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=str(uuid4()),
            config={},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "source_path not provided" in (result.error or "")

    def test_import_nonexistent_path(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test error when path doesn't exist."""
        phase = ImportPhase()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=str(uuid4()),
            config={"source_path": "/nonexistent/path.csv"},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "not found" in (result.error or "")

    def test_skip_if_tables_exist(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, csv_file: Path
    ):
        """Test that import is skipped if tables already exist."""
        source_id = str(uuid4())

        # First, create a source with tables
        source = Source(
            source_id=source_id,
            name="existing_source",
            source_type="csv",
        )
        session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="existing_table",
            layer="raw",
            duckdb_path="raw_existing_table",
            row_count=10,
        )
        session.add(table)
        session.commit()

        # Now try to import
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"source_path": str(csv_file)},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "already has" in skip_reason

    def test_force_reimport(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, csv_file: Path
    ):
        """Test force_reimport config bypasses skip."""
        source_id = str(uuid4())

        # Create existing source
        source = Source(
            source_id=source_id,
            name="existing_source",
            source_type="csv",
        )
        session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="existing_table",
            layer="raw",
            duckdb_path="raw_existing_table",
            row_count=10,
        )
        session.add(table)
        session.commit()

        # Try with force_reimport
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"source_path": str(csv_file), "force_reimport": True},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is None  # Should not skip with force_reimport

    def test_drop_junk_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ):
        """Test that junk columns are dropped."""
        # Create CSV with junk column
        csv_path = tmp_path / "with_junk.csv"
        csv_path.write_text(
            """id,name,Unnamed: 0
1,Alice,0
2,Bob,1
"""
        )

        phase = ImportPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={
                "source_path": str(csv_path),
                "junk_columns": ["Unnamed: 0"],
            },
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED

        # Verify junk column was removed from metadata
        table_id = result.outputs["raw_tables"][0]
        stmt = select(Column).where(Column.table_id == table_id)
        result_cols = session.execute(stmt)
        columns = result_cols.scalars().all()

        column_names = {c.column_name for c in columns}
        assert "Unnamed: 0" not in column_names
        assert column_names == {"id", "name"}
