"""Tests for CSV directory loading functionality.

Tests the load_directory() method of CSVLoader for loading multiple CSV files.
Uses small_finance fixture data from tests/integration/fixtures/.
"""

from pathlib import Path

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from dataraum.sources.csv import CSVLoader
from dataraum.storage import init_database

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "small_finance"


@pytest.fixture
def test_session():
    """Create an in-memory SQLite session for testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
    )

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    init_database(engine)

    factory = sessionmaker(bind=engine, expire_on_commit=False)

    with factory() as session:
        yield session

    engine.dispose()


@pytest.fixture
def test_duckdb():
    """Create an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


class TestCSVDirectoryLoader:
    """Tests for CSVLoader.load_directory() method."""

    def test_load_directory_all_files(self, test_duckdb, test_session):
        """Test loading all CSV files from a directory."""
        loader = CSVLoader()
        result = loader.load_directory(
            directory_path=str(FIXTURES_DIR),
            source_name="small_finance",
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert result.success, f"Load failed: {result.error}"

        staging_result = result.value
        assert staging_result is not None

        # small_finance has 5 CSV files
        assert len(staging_result.tables) == 5

        # Check that tables exist in DuckDB
        duckdb_tables = test_duckdb.execute("SHOW TABLES").fetchall()
        table_names = {t[0] for t in duckdb_tables}

        for table in staging_result.tables:
            assert table.raw_table_name in table_names

    def test_load_directory_with_pattern(self, test_duckdb, test_session):
        """Test loading with a glob pattern (subset of files)."""
        loader = CSVLoader()
        result = loader.load_directory(
            directory_path=str(FIXTURES_DIR),
            source_name="subset",
            duckdb_conn=test_duckdb,
            session=test_session,
            file_pattern="vendor*.csv",
        )

        assert result.success

        staging_result = result.value
        assert staging_result is not None
        assert len(staging_result.tables) == 1

        assert staging_result.tables[0].table_name == "vendors"

    def test_load_directory_not_found(self, test_duckdb, test_session):
        """Test loading from non-existent directory."""
        loader = CSVLoader()
        result = loader.load_directory(
            directory_path="/nonexistent/path",
            source_name="test",
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert not result.success
        assert "not found" in result.error.lower()

    def test_load_directory_empty_pattern(self, test_duckdb, test_session):
        """Test loading with a pattern that matches no files."""
        loader = CSVLoader()
        result = loader.load_directory(
            directory_path=str(FIXTURES_DIR),
            source_name="no_match",
            duckdb_conn=test_duckdb,
            session=test_session,
            file_pattern="*.nonexistent",
        )

        assert not result.success
        assert "no csv files" in result.error.lower()

    def test_load_directory_total_rows(self, test_duckdb, test_session):
        """Test that total_rows is the sum across all loaded tables."""
        loader = CSVLoader()
        result = loader.load_directory(
            directory_path=str(FIXTURES_DIR),
            source_name="row_check",
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert result.success
        staging_result = result.value
        expected_total = sum(t.row_count for t in staging_result.tables)
        assert staging_result.total_rows == expected_total
