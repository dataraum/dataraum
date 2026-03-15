"""Tests for Parquet loader.

Tests the sources.parquet module which implements strongly-typed Parquet loading.
Uses DuckDB to generate Parquet test fixtures.
"""

import duckdb
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
    path = tmp_path / "sample.parquet"
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                (1::BIGINT, 'Alice'::VARCHAR, 10.5::DOUBLE, true::BOOLEAN, '2024-01-01'::DATE),
                (2::BIGINT, 'Bob'::VARCHAR, 20.0::DOUBLE, false::BOOLEAN, '2024-02-15'::DATE),
                (3::BIGINT, 'Charlie'::VARCHAR, 30.75::DOUBLE, true::BOOLEAN, '2024-03-20'::DATE)
            ) AS t(id, name, amount, active, created_at)
        ) TO '{path}' (FORMAT PARQUET)
    """)
    conn.close()
    return path


class TestParquetLoader:
    """Tests for ParquetLoader."""

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

        type_map = dict(schema)
        assert type_map["id"] == "BIGINT"
        assert type_map["name"] == "VARCHAR"
        assert type_map["amount"] == "DOUBLE"
        assert type_map["active"] == "BOOLEAN"
        assert type_map["created_at"] == "DATE"

    def test_load_normalizes_column_names(self, test_duckdb, test_session, tmp_path):
        """Test that column names with spaces/special chars are normalized."""
        path = tmp_path / "special_cols.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT 1::BIGINT AS "Customer ID",
                       'Alice'::VARCHAR AS "First Name",
                       100.0::DOUBLE AS "total-amount"
            ) TO '{path}' (FORMAT PARQUET)
        """)
        conn.close()

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
