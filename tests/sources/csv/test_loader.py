"""Tests for CSV loader.

Tests the sources.csv module which implements VARCHAR-first CSV loading.
"""

from pathlib import Path

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from dataraum_context.core.models import SourceConfig
from dataraum_context.sources.csv import CSVLoader
from dataraum_context.storage import init_database


@pytest.fixture
def test_session():
    """Create an in-memory SQLite session for testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
    )

    # Enable foreign keys for SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    # Initialize database schema
    init_database(engine)

    # Create session
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


@pytest.fixture
def payment_method_csv():
    """Path to payment_method.csv example."""
    return Path("examples/finance_csv_example/payment_method.csv")


@pytest.fixture
def customer_table_csv():
    """Path to customer_table.csv example."""
    return Path("examples/finance_csv_example/customer_table.csv")


class TestCSVLoader:
    """Tests for CSVLoader."""

    def test_type_system_strength(self):
        """CSV loader should be classified as untyped."""
        loader = CSVLoader()
        assert loader.type_system_strength.value == "untyped"

    def test_get_schema_payment_method(self, payment_method_csv):
        """Test getting schema from payment_method.csv."""
        if not payment_method_csv.exists():
            pytest.skip("Example CSV not found")

        loader = CSVLoader()
        config = SourceConfig(
            name="payment_method",
            source_type="csv",
            path=str(payment_method_csv),
        )

        result = loader.get_schema(config)

        assert result.success
        columns = result.value

        # Check we got columns
        assert columns
        assert len(columns) > 0

        # Check column properties
        assert columns[0].position == 0
        assert columns[0].source_type == "VARCHAR"
        assert columns[0].nullable is True

        # Check we have sample values
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

    def test_load_payment_method(
        self,
        payment_method_csv,
        test_duckdb,
        test_session,
    ):
        """Test loading payment_method.csv."""
        if not payment_method_csv.exists():
            pytest.skip("Example CSV not found")

        loader = CSVLoader()
        config = SourceConfig(
            name="payment_method",
            source_type="csv",
            path=str(payment_method_csv),
        )

        result = loader.load(config, test_duckdb, test_session)

        assert result.success, f"Load failed: {result.error}"

        staging_result = result.value
        assert staging_result
        assert staging_result.source_id is not None
        assert len(staging_result.tables) == 1

        table = staging_result.tables[0]
        assert table.table_name == "payment_method"
        assert table.raw_table_name == "raw_payment_method"
        assert table.row_count > 0
        assert table.column_count > 0

        # Verify table exists in DuckDB
        tables = test_duckdb.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()

        table_names = [t[0] for t in tables]
        assert "raw_payment_method" in table_names

        # Verify all columns are VARCHAR
        schema = test_duckdb.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'raw_payment_method'
            ORDER BY ordinal_position
        """).fetchall()

        for col_name, data_type in schema:
            assert data_type == "VARCHAR", f"Column {col_name} is {data_type}, expected VARCHAR"

    def test_load_with_null_values(
        self,
        customer_table_csv,
        test_duckdb,
        test_session,
    ):
        """Test that null values (--) are handled correctly."""
        if not customer_table_csv.exists():
            pytest.skip("Example CSV not found")

        loader = CSVLoader()
        config = SourceConfig(
            name="customer_table",
            source_type="csv",
            path=str(customer_table_csv),
        )

        result = loader.load(config, test_duckdb, test_session)

        assert result.success, f"Load failed: {result.error}"

        # Query for null values in Balance column
        null_count = test_duckdb.execute("""
            SELECT COUNT(*)
            FROM raw_customer_table
            WHERE "Balance" IS NULL
        """).fetchone()[0]

        # Should have null values (-- gets converted to NULL)
        assert null_count > 0, "Expected some NULL values in Balance column"

    def test_sanitize_table_name(self):
        """Test table name sanitization."""
        loader = CSVLoader()

        assert loader._sanitize_table_name("My Table.csv") == "my_table"
        assert loader._sanitize_table_name("table-name.csv") == "table_name"
        assert loader._sanitize_table_name("123table") == "t_123table"
        assert loader._sanitize_table_name("valid_name") == "valid_name"
