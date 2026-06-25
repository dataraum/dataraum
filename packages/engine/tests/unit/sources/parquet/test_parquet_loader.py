"""Tests for Parquet loader.

Tests the sources.parquet module which implements strongly-typed Parquet loading.
Uses DuckDB to generate Parquet test fixtures.
"""

from __future__ import annotations

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from dataraum.core.models import SourceConfig
from dataraum.sources.csv.models import StagedTable
from dataraum.sources.parquet import ParquetLoader
from dataraum.storage import Source, init_database


@pytest.fixture
def test_session():
    """Create an in-memory SQLite session for testing.

    ``StaticPool`` keeps a single connection that ``dispose()`` closes
    deterministically — silences Python 3.12+ ``ResourceWarning`` on
    GC'd sqlite3 connections.
    """
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    init_database(engine)

    factory = sessionmaker(bind=engine, expire_on_commit=False)

    try:
        with factory() as session:
            yield session
    finally:
        engine.dispose()


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


def _stage_parquet(
    conn: duckdb.DuckDBPyConnection,
    session: Session,
    source_uri: str,
    *,
    source_name: str,
) -> StagedTable:
    """Seed a Source row and load a single Parquet via ``_load_single_file``.

    Mirrors ``import_phase`` (create the Source row, then call the loader's
    ``_load_single_file``); returns the resulting ``StagedTable``.
    """
    source_id = f"src-{source_name}"
    source = Source(
        source_id=source_id,
        name=source_name,
        source_type="parquet",
        connection_config={"path": source_uri},
    )
    session.add(source)
    session.flush()

    loader = ParquetLoader()
    result = loader._load_single_file(
        source_uri=source_uri,
        source_id=source_id,
        duckdb_conn=conn,
        session=session,
    )
    assert result.success, result.error
    return result.unwrap()


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
        """A missing URI surfaces DuckDB's read error via Result.fail (DAT-389).

        ``get_schema`` no longer stats the filesystem (the path is an opaque
        URI handed verbatim to ``read_parquet``); an unreadable path fails the
        Result with DuckDB's IO error rather than a pathlib pre-check.
        """
        loader = ParquetLoader()
        config = SourceConfig(
            name="missing",
            source_type="parquet",
            path="nonexistent.parquet",
        )

        result = loader.get_schema(config)

        assert not result.success
        assert result.error
        assert "Parquet schema" in result.error

    def test_get_schema_no_path(self):
        """Test error handling when path is not set."""
        loader = ParquetLoader()
        config = SourceConfig(name="no_path", source_type="parquet")

        result = loader.get_schema(config)

        assert not result.success
        assert "path" in result.error.lower()

    def test_load_single_file(self, test_session, sample_parquet, lake_anchor, lake_clean):
        """Test loading a single Parquet file."""
        from dataraum.server.storage import connect_session

        conn = connect_session()
        try:
            staged = _stage_parquet(conn, test_session, str(sample_parquet), source_name="sample")

            # DAT-639: bare name is NARROW (the file stem, no source prefix).
            assert staged.table_name == "sample"
            assert staged.raw_table_name == "sample"
            assert staged.row_count == 3
            assert staged.column_count == 5

            # Verify table exists in lake.raw
            tables = conn.execute(
                "SELECT table_name FROM duckdb_tables() "
                "WHERE database_name = 'lake' AND schema_name = 'raw'"
            ).fetchall()
            table_names = [t[0] for t in tables]
            assert "sample" in table_names
        finally:
            conn.close()

    def test_load_preserves_types(self, test_session, sample_parquet, lake_anchor, lake_clean):
        """Verify Parquet types are preserved (not all VARCHAR like CSV)."""
        from dataraum.server.storage import connect_session

        conn = connect_session()
        try:
            _stage_parquet(conn, test_session, str(sample_parquet), source_name="typed_test")

            schema = conn.execute('DESCRIBE lake.raw."sample"').fetchall()
            # DESCRIBE returns (column_name, column_type, null, key, default, extra)
            type_map = {row[0]: row[1] for row in schema}
            assert type_map["id"] == "BIGINT"
            assert type_map["name"] == "VARCHAR"
            assert type_map["amount"] == "DOUBLE"
            assert type_map["active"] == "BOOLEAN"
            assert type_map["created_at"] == "DATE"
        finally:
            conn.close()

    def test_load_normalizes_column_names(self, test_session, tmp_path, lake_anchor, lake_clean):
        """Test that column names with spaces/special chars are normalized."""
        from dataraum.server.storage import connect_session

        path = tmp_path / "special_cols.parquet"
        helper = duckdb.connect()
        helper.execute(f"""
            COPY (
                SELECT 1::BIGINT AS "Customer ID",
                       'Alice'::VARCHAR AS "First Name",
                       100.0::DOUBLE AS "total-amount"
            ) TO '{path}' (FORMAT PARQUET)
        """)
        helper.close()

        conn = connect_session()
        try:
            _stage_parquet(conn, test_session, str(path), source_name="special_cols")

            schema = conn.execute('DESCRIBE lake.raw."special_cols"').fetchall()
            col_names = [row[0] for row in schema]
            assert col_names == ["customer_id", "first_name", "totalamount"]
        finally:
            conn.close()

    def test_load_missing_file(self, test_session, lake_anchor, lake_clean):
        """A missing URI fails the load via Result.fail (DuckDB error, DAT-389)."""
        from dataraum.server.storage import connect_session

        source = Source(
            source_id="src-missing",
            name="missing",
            source_type="parquet",
            connection_config={"path": "nonexistent.parquet"},
        )
        test_session.add(source)
        test_session.flush()

        loader = ParquetLoader()
        conn = connect_session()
        try:
            result = loader._load_single_file(
                source_uri="nonexistent.parquet",
                source_id="src-missing",
                duckdb_conn=conn,
                session=test_session,
            )
            assert not result.success
            assert result.error
        finally:
            conn.close()

    def test_sqlalchemy_metadata_created(
        self, test_session, sample_parquet, lake_anchor, lake_clean
    ):
        """Test that SQLAlchemy Table and Column records are created."""
        from sqlalchemy import select

        from dataraum.server.storage import connect_session
        from dataraum.storage import Column, Table

        conn = connect_session()
        try:
            _stage_parquet(conn, test_session, str(sample_parquet), source_name="metadata_test")

            # Check Source record
            source = test_session.execute(
                select(Source).where(Source.name == "metadata_test")
            ).scalar_one()
            assert source.source_type == "parquet"

            # Check Table record
            table = test_session.execute(
                select(Table).where(Table.source_id == source.source_id)
            ).scalar_one()
            assert table.layer == "raw"
            # DAT-639: table_name is the NARROW file stem (no source prefix).
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
        finally:
            conn.close()
