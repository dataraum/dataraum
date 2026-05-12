"""Tests for import phase.

Per DAT-290, the import phase runs against a single source whose Source
row is already in the session DB. These tests pre-create the Source row
and populate ``ctx.config`` with the keys that ``setup_pipeline`` would
otherwise supply.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


def _seed_source(
    session: Session,
    source_id: str,
    name: str,
    path: Path,
    source_type: str = "csv",
) -> None:
    """Insert a Source row mimicking what begin_session / setup_pipeline writes."""
    session.add(
        Source(
            source_id=source_id,
            name=name,
            source_type=source_type,
            connection_config={"path": str(path)},
            status="configured",
        )
    )
    session.flush()


def _file_ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_id: str,
    name: str,
    path: Path,
    source_type: str = "csv",
    extra: dict[str, Any] | None = None,
) -> PhaseContext:
    """Build a PhaseContext for a file-source pipeline run (Source row pre-seeded)."""
    _seed_source(session, source_id, name, path, source_type)
    config: dict[str, Any] = {
        "source_name": name,
        "source_type": source_type,
        "source_connection_config": {"path": str(path)},
        "source_path": str(path),
    }
    if extra:
        config.update(extra)
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        source_id=source_id,
        config=config,
    )


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

    def test_import_single_csv(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, csv_file: Path
    ):
        """Test importing a single CSV file."""
        phase = ImportPhase()
        source_id = str(uuid4())
        ctx = _file_ctx(session, duckdb_conn, source_id, "test_data", csv_file)

        result = phase.run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert "raw_tables" in result.outputs
        assert len(result.outputs["raw_tables"]) == 1
        assert result.records_processed == 3  # 3 rows
        assert result.records_created == 1  # 1 table

        # Source row was pre-seeded by the helper
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
        ctx = _file_ctx(session, duckdb_conn, source_id, "test_dir", csv_directory)

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

    def test_import_missing_config(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """Empty config: import phase reports the missing identity fields."""
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=str(uuid4()),
            config={},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        err = (result.error or "").lower()
        assert "source_name" in err
        assert "source_type" in err

    def test_import_nonexistent_path(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test error when path doesn't exist (Source row exists, file does not)."""
        phase = ImportPhase()
        source_id = str(uuid4())
        ghost_path = Path("/nonexistent/path.csv")
        _seed_source(session, source_id, "ghost", ghost_path)
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={
                "source_name": "ghost",
                "source_type": "csv",
                "source_connection_config": {"path": str(ghost_path)},
                "source_path": str(ghost_path),
            },
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
        ctx = _file_ctx(
            session,
            duckdb_conn,
            source_id,
            "with_junk",
            csv_path,
            extra={"junk_columns": ["Unnamed: 0"]},
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
