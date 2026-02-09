"""Tests for CSV loader.

Tests the sources.csv module which implements VARCHAR-first CSV loading.
Uses small_finance fixture data from tests/integration/fixtures/.
"""

from pathlib import Path

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from dataraum.core.models import SourceConfig
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


class TestCSVLoader:
    """Tests for CSVLoader."""

    def test_type_system_strength(self):
        """CSV loader should be classified as untyped."""
        loader = CSVLoader()
        assert loader.type_system_strength.value == "untyped"

    def test_get_schema(self, test_duckdb):
        """Test getting schema from a CSV file."""
        loader = CSVLoader()
        config = SourceConfig(
            name="payment_methods",
            source_type="csv",
            path=str(FIXTURES_DIR / "payment_methods.csv"),
        )

        result = loader.get_schema(config)

        assert result.success
        columns = result.value
        assert columns
        assert len(columns) == 3  # Business Id, Payment method, Credit card
        assert columns[0].position == 0
        assert columns[0].source_type == "VARCHAR"
        assert columns[0].nullable is True
        assert len(columns[0].sample_values) > 0

    def test_get_schema_missing_file(self):
        """Test error handling for missing file."""
        loader = CSVLoader()
        config = SourceConfig(
            name="missing",
            source_type="csv",
            path="nonexistent.csv",
        )

        result = loader.get_schema(config)

        assert not result.success
        assert result.error
        assert "not found" in result.error.lower()

    def test_get_schema_no_path(self):
        """Test error handling when path is not set."""
        loader = CSVLoader()
        config = SourceConfig(name="no_path", source_type="csv")

        result = loader.get_schema(config)

        assert not result.success
        assert "path" in result.error.lower()

    def test_load_single_file(self, test_duckdb, test_session):
        """Test loading a single CSV file."""
        loader = CSVLoader()
        config = SourceConfig(
            name="payment_methods",
            source_type="csv",
            path=str(FIXTURES_DIR / "payment_methods.csv"),
        )

        result = loader.load(config, test_duckdb, test_session)

        assert result.success, f"Load failed: {result.error}"

        staging_result = result.value
        assert staging_result.source_id is not None
        assert len(staging_result.tables) == 1

        table = staging_result.tables[0]
        assert table.table_name == "payment_methods"
        assert table.raw_table_name == "raw_payment_methods"
        assert table.row_count > 0
        assert table.column_count == 3

        # Verify table exists in DuckDB
        tables = test_duckdb.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()
        table_names = [t[0] for t in tables]
        assert "raw_payment_methods" in table_names

    def test_load_all_columns_varchar(self, test_duckdb, test_session):
        """Verify all loaded columns are VARCHAR (VARCHAR-first approach)."""
        loader = CSVLoader()
        config = SourceConfig(
            name="customers",
            source_type="csv",
            path=str(FIXTURES_DIR / "customers.csv"),
        )

        result = loader.load(config, test_duckdb, test_session)
        assert result.success

        schema = test_duckdb.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'raw_customers'
            ORDER BY ordinal_position
        """).fetchall()

        for col_name, data_type in schema:
            assert data_type == "VARCHAR", f"Column {col_name} is {data_type}, expected VARCHAR"

    def test_load_null_values_recognized(self, test_duckdb, test_session):
        """Test that null values (--) are converted to NULL during loading."""
        loader = CSVLoader()
        config = SourceConfig(
            name="transactions",
            source_type="csv",
            path=str(FIXTURES_DIR / "transactions.csv"),
        )

        result = loader.load(config, test_duckdb, test_session)
        assert result.success, f"Load failed: {result.error}"

        # Transactions has -- values in Customer name and Vendor name columns
        null_count = test_duckdb.execute("""
            SELECT COUNT(*)
            FROM raw_transactions
            WHERE "Customer name" IS NULL
        """).fetchone()[0]

        assert null_count > 0, "Expected some NULL values from -- conversion"

    def test_load_missing_file(self, test_duckdb, test_session):
        """Test loading a non-existent file."""
        loader = CSVLoader()
        config = SourceConfig(
            name="missing",
            source_type="csv",
            path="nonexistent.csv",
        )

        result = loader.load(config, test_duckdb, test_session)
        assert not result.success
        assert "not found" in result.error.lower()
