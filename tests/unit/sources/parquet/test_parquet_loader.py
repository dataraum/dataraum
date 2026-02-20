"""Tests for Parquet loader.

Tests the sources.parquet module which implements strongly-typed Parquet loading.
Uses dynamically generated Parquet fixtures via PyArrow.
"""

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from dataraum.core.models import SourceConfig
from dataraum.sources.parquet import ParquetLoader
from dataraum.storage import init_database


@pytest.fixture
def test_session():
    """Create an in-memory SQLite session for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)

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


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample Parquet file with typed columns."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            "amount": pa.array([10.5, 20.0, 30.75], type=pa.float64()),
            "active": pa.array([True, False, True], type=pa.bool_()),
            "created_at": pa.array(
                ["2024-01-01", "2024-02-15", "2024-03-20"], type=pa.string()
            ).cast(pa.date32()),
        }
    )
    path = tmp_path / "sample.parquet"
    pq.write_table(table, path)
    return path


@pytest.fixture
def parquet_directory(tmp_path):
    """Create a directory with multiple Parquet files."""
    for name, data in [
        (
            "customers",
            {
                "customer_id": pa.array([1, 2], type=pa.int64()),
                "email": pa.array(["a@b.com", "c@d.com"], type=pa.string()),
            },
        ),
        (
            "orders",
            {
                "order_id": pa.array([100, 200, 300], type=pa.int64()),
                "total": pa.array([99.99, 149.50, 200.00], type=pa.float64()),
            },
        ),
    ]:
        table = pa.table(data)
        pq.write_table(table, tmp_path / f"{name}.parquet")
    return tmp_path


class TestParquetLoader:
    """Tests for ParquetLoader."""

    def test_type_system_strength(self):
        """Parquet loader should be classified as strongly typed."""
        loader = ParquetLoader()
        assert loader.type_system_strength.value == "strong"

    def test_get_schema(self, sample_parquet):
        """Test getting schema from a Parquet file."""
        loader = ParquetLoader()
        config = SourceConfig(
            name="sample",
            source_type="parquet",
            path=str(sample_parquet),
        )

        result = loader.get_schema(config)

        assert result.success
        columns = result.value
        assert columns is not None
        assert len(columns) == 5

        # Check types are preserved from Parquet
        assert columns[0].name == "id"
        assert columns[0].source_type == "BIGINT"
        assert columns[1].name == "name"
        assert columns[1].source_type == "VARCHAR"
        assert columns[2].name == "amount"
        assert columns[2].source_type == "DOUBLE"
        assert columns[3].name == "active"
        assert columns[3].source_type == "BOOLEAN"
        assert columns[4].name == "created_at"
        assert columns[4].source_type == "DATE"

    def test_get_schema_missing_file(self):
        """Test error handling for missing file."""
        loader = ParquetLoader()
        config = SourceConfig(
            name="missing",
            source_type="parquet",
            path="nonexistent.parquet",
        )

        result = loader.get_schema(config)

        assert not result.success
        assert result.error
        assert "not found" in result.error.lower()

    def test_get_schema_no_path(self):
        """Test error handling when path is not set."""
        loader = ParquetLoader()
        config = SourceConfig(name="no_path", source_type="parquet")

        result = loader.get_schema(config)

        assert not result.success
        assert "path" in result.error.lower()

    def test_load_single_file(self, test_duckdb, test_session, sample_parquet):
        """Test loading a single Parquet file."""
        loader = ParquetLoader()
        config = SourceConfig(
            name="sample",
            source_type="parquet",
            path=str(sample_parquet),
        )

        result = loader.load(config, test_duckdb, test_session)

        assert result.success, f"Load failed: {result.error}"

        staging_result = result.value
        assert staging_result.source_id is not None
        assert len(staging_result.tables) == 1

        table = staging_result.tables[0]
        assert table.table_name == "sample"
        assert table.raw_table_name == "raw_sample"
        assert table.row_count == 3
        assert table.column_count == 5

        # Verify table exists in DuckDB
        tables = test_duckdb.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()
        table_names = [t[0] for t in tables]
        assert "raw_sample" in table_names

    def test_load_preserves_types(self, test_duckdb, test_session, sample_parquet):
        """Verify Parquet types are preserved (not all VARCHAR like CSV)."""
        loader = ParquetLoader()
        config = SourceConfig(
            name="typed_test",
            source_type="parquet",
            path=str(sample_parquet),
        )

        result = loader.load(config, test_duckdb, test_session)
        assert result.success

        # Table name comes from file stem (sample.parquet -> raw_sample)
        schema = test_duckdb.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'raw_sample'
            ORDER BY ordinal_position
        """).fetchall()

        type_map = {col: dtype for col, dtype in schema}
        assert type_map["id"] == "BIGINT"
        assert type_map["name"] == "VARCHAR"
        assert type_map["amount"] == "DOUBLE"
        assert type_map["active"] == "BOOLEAN"
        assert type_map["created_at"] == "DATE"

    def test_load_normalizes_column_names(self, test_duckdb, test_session, tmp_path):
        """Test that column names with spaces/special chars are normalized."""
        table = pa.table(
            {
                "Customer ID": pa.array([1], type=pa.int64()),
                "First Name": pa.array(["Alice"], type=pa.string()),
                "total-amount": pa.array([100.0], type=pa.float64()),
            }
        )
        path = tmp_path / "special_cols.parquet"
        pq.write_table(table, path)

        loader = ParquetLoader()
        config = SourceConfig(
            name="special_cols",
            source_type="parquet",
            path=str(path),
        )

        result = loader.load(config, test_duckdb, test_session)
        assert result.success

        schema = test_duckdb.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'raw_special_cols'
            ORDER BY ordinal_position
        """).fetchall()

        col_names = [c[0] for c in schema]
        assert col_names == ["customer_id", "first_name", "totalamount"]

    def test_load_missing_file(self, test_duckdb, test_session):
        """Test loading a non-existent file."""
        loader = ParquetLoader()
        config = SourceConfig(
            name="missing",
            source_type="parquet",
            path="nonexistent.parquet",
        )

        result = loader.load(config, test_duckdb, test_session)
        assert not result.success
        assert "not found" in result.error.lower()

    def test_load_directory(self, test_duckdb, test_session, parquet_directory):
        """Test loading a directory of Parquet files."""
        loader = ParquetLoader()

        result = loader.load_directory(
            directory_path=str(parquet_directory),
            source_name="multi_parquet",
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert result.success, f"Load failed: {result.error}"

        staging_result = result.value
        assert len(staging_result.tables) == 2
        table_names = {t.table_name for t in staging_result.tables}
        assert "customers" in table_names
        assert "orders" in table_names

    def test_sqlalchemy_metadata_created(self, test_duckdb, test_session, sample_parquet):
        """Test that SQLAlchemy Table and Column records are created."""
        from dataraum.storage import Column, Source, Table

        loader = ParquetLoader()
        config = SourceConfig(
            name="metadata_test",
            source_type="parquet",
            path=str(sample_parquet),
        )

        result = loader.load(config, test_duckdb, test_session)
        assert result.success

        # Check Source record
        from sqlalchemy import select

        source = test_session.execute(
            select(Source).where(Source.name == "metadata_test")
        ).scalar_one()
        assert source.source_type == "parquet"

        # Check Table record
        table = test_session.execute(
            select(Table).where(Table.source_id == source.source_id)
        ).scalar_one()
        assert table.layer == "raw"
        # Table name comes from file stem (sample.parquet -> sample)
        assert table.table_name == "sample"

        # Check Column records
        columns = (
            test_session.execute(select(Column).where(Column.table_id == table.table_id))
            .scalars()
            .all()
        )
        assert len(columns) == 5

        # Verify raw_type is set from Parquet (not all VARCHAR)
        col_types = {c.column_name: c.raw_type for c in columns}
        assert col_types["id"] == "BIGINT"
        assert col_types["amount"] == "DOUBLE"
        assert col_types["active"] == "BOOLEAN"
