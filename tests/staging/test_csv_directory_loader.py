"""Tests for CSV directory loading functionality."""

from pathlib import Path

import duckdb
import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dataraum_context.dataflows.pipeline import run_pipeline
from dataraum_context.staging.loaders.csv import CSVLoader
from dataraum_context.storage.schema import init_database


@pytest.fixture
async def test_session():
    """Create an in-memory SQLite session for testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    await init_database(engine)

    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        yield session

    await engine.dispose()


@pytest.fixture
def test_duckdb():
    """Create an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def mock_semantic_agent():
    """Create a mock semantic agent for testing."""
    from unittest.mock import AsyncMock, MagicMock

    from dataraum_context.core.models.base import Result
    from dataraum_context.enrichment.models import SemanticEnrichmentResult

    mock = MagicMock()
    mock.analyze = AsyncMock(
        return_value=Result.ok(
            SemanticEnrichmentResult(
                annotations=[],
                entity_detections=[],
                relationships=[],
            )
        )
    )
    return mock


@pytest.fixture
def finance_csv_directory():
    """Path to finance CSV example directory."""
    return Path("examples/finance_csv_example")


class TestCSVDirectoryLoader:
    """Tests for CSVLoader.load_directory() method."""

    async def test_load_directory_all_files(
        self,
        finance_csv_directory,
        test_duckdb,
        test_session,
    ):
        """Test loading all CSV files from a directory."""
        if not finance_csv_directory.exists():
            pytest.skip("Finance CSV example directory not found")

        loader = CSVLoader()
        result = await loader.load_directory(
            directory_path=str(finance_csv_directory),
            source_name="finance",
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert result.success, f"Load failed: {result.error}"

        staging_result = result.value
        assert staging_result is not None

        # Should have 7 tables
        assert len(staging_result.tables) == 7

        # Check that tables exist in DuckDB
        duckdb_tables = test_duckdb.execute("SHOW TABLES").fetchall()
        table_names = {t[0] for t in duckdb_tables}

        # All raw tables should exist
        for table in staging_result.tables:
            assert table.raw_table_name in table_names

    async def test_load_directory_with_pattern(
        self,
        finance_csv_directory,
        test_duckdb,
        test_session,
    ):
        """Test loading with a glob pattern (subset of files)."""
        if not finance_csv_directory.exists():
            pytest.skip("Finance CSV example directory not found")

        loader = CSVLoader()
        result = await loader.load_directory(
            directory_path=str(finance_csv_directory),
            source_name="tables_only",
            duckdb_conn=test_duckdb,
            session=test_session,
            file_pattern="*_table.csv",
        )

        assert result.success

        staging_result = result.value
        assert staging_result is not None

        # Should have loaded only tables matching pattern
        # customer, employee, master_txn, product_service, vendor
        assert len(staging_result.tables) == 5

        table_names = {t.table_name for t in staging_result.tables}
        expected = {
            "customer_table",
            "employee_table",
            "master_txn_table",
            "product_service_table",
            "vendor_table",
        }
        assert table_names == expected

    async def test_load_directory_not_found(
        self,
        test_duckdb,
        test_session,
    ):
        """Test loading from non-existent directory."""
        loader = CSVLoader()
        result = await loader.load_directory(
            directory_path="/nonexistent/path",
            source_name="test",
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert not result.success
        assert "not found" in result.error.lower() or "does not exist" in result.error.lower()

    async def test_load_directory_empty_pattern(
        self,
        finance_csv_directory,
        test_duckdb,
        test_session,
    ):
        """Test loading with a pattern that matches no files."""
        if not finance_csv_directory.exists():
            pytest.skip("Finance CSV example directory not found")

        loader = CSVLoader()
        result = await loader.load_directory(
            directory_path=str(finance_csv_directory),
            source_name="no_match",
            duckdb_conn=test_duckdb,
            session=test_session,
            file_pattern="*.nonexistent",
        )

        assert not result.success
        assert "no csv files" in result.error.lower() or "no files" in result.error.lower()


class TestCSVDirectoryPipeline:
    """Tests for unified run_pipeline() with directories."""

    async def test_pipeline_directory(
        self,
        finance_csv_directory,
        test_duckdb,
        test_session,
        mock_semantic_agent,
    ):
        """Test the unified pipeline with a directory."""
        if not finance_csv_directory.exists():
            pytest.skip("Finance CSV example directory not found")

        result = await run_pipeline(
            source=str(finance_csv_directory),
            source_name="finance_test",
            duckdb_conn=test_duckdb,
            session=test_session,
            semantic_agent=mock_semantic_agent,
        )

        assert result.success, f"Pipeline failed: {result.error}"

        pipeline_result = result.value
        assert pipeline_result is not None

        # Should have 7 tables
        assert pipeline_result.table_count == 7
        assert pipeline_result.successful_tables >= 1

        # Check that all stages completed for at least some tables
        for table_health in pipeline_result.table_health:
            if table_health.error is None:
                assert table_health.staging_completed
                assert table_health.schema_profiling_completed
                assert table_health.type_resolution_completed
                # Stats and enrichment may have issues, but should at least attempt
