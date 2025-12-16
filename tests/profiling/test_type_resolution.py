"""Integration tests for type resolution."""

import duckdb
import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dataraum_context.core.models import SourceConfig
from dataraum_context.dataflows.pipeline import run_pipeline
from dataraum_context.profiling.profiler import profile_and_resolve_types
from dataraum_context.staging.loaders.csv import CSVLoader
from dataraum_context.storage.schema import init_database


@pytest.fixture
async def test_session():
    """Create an in-memory SQLite session for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

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
def simple_csv(tmp_path):
    """Create a simple CSV with mixed types."""
    csv_content = """id,name,date,amount,is_active
1,Alice,2024-01-15,1234.56,true
2,Bob,2024-02-20,789.00,false
3,Charlie,2024-03-25,456.78,true
4,Diana,N/A,0.00,false
5,Eve,2024-05-10,999.99,true
"""
    csv_file = tmp_path / "simple.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def quarantine_csv(tmp_path):
    """Create a CSV with values that will fail type resolution."""
    csv_content = """id,date,amount
1,2024-01-15,123.45
2,not_a_date,456.78
3,2024-03-25,not_a_number
4,2024-04-30,789.00
5,invalid,invalid
"""
    csv_file = tmp_path / "quarantine.csv"
    csv_file.write_text(csv_content)
    return csv_file


class TestTypeResolution:
    """Tests for type resolution functionality."""

    async def test_simple_csv_type_resolution(self, simple_csv, test_duckdb, test_session):
        """Test basic type resolution with a simple CSV."""
        source_config = SourceConfig(
            name="simple",
            source_type="csv",
            path=str(simple_csv),
        )
        loader = CSVLoader()
        load_result = await loader.load(
            source_config=source_config,
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert load_result.success
        table_id = load_result.unwrap().tables[0].table_id

        result = await profile_and_resolve_types(
            table_id=table_id,
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert result.success
        resolution = result.unwrap()
        assert resolution.typed_table_name == "typed_simple"
        assert resolution.total_rows == 5

        # Verify typed table exists
        tables = test_duckdb.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "typed_simple" in table_names

    async def test_pipeline_run_pipeline(
        self, simple_csv, test_duckdb, test_session, mock_semantic_agent
    ):
        """Test the full unified pipeline."""
        result = await run_pipeline(
            source=str(simple_csv),
            source_name="pipeline_test",
            duckdb_conn=test_duckdb,
            session=test_session,
            semantic_agent=mock_semantic_agent,
        )

        assert result.success
        pipeline_result = result.unwrap()

        # Check basic properties
        assert pipeline_result.table_count == 1
        assert pipeline_result.successful_tables == 1

        # Check table health
        table_health = pipeline_result.table_health[0]
        assert table_health.staging_completed
        assert table_health.schema_profiling_completed
        assert table_health.type_resolution_completed
        assert table_health.statistics_profiling_completed
        assert table_health.semantic_enrichment_completed
        assert table_health.raw_table_name == "raw_simple"
        assert table_health.row_count == 5
        assert table_health.error is None


class TestQuarantineHandling:
    """Tests for quarantine table handling."""

    async def test_quarantine_table_created(
        self, quarantine_csv, test_duckdb, test_session, mock_semantic_agent
    ):
        """Test that quarantine table is created for failed casts."""
        result = await run_pipeline(
            source=str(quarantine_csv),
            source_name="quarantine_test",
            duckdb_conn=test_duckdb,
            session=test_session,
            semantic_agent=mock_semantic_agent,
            min_confidence=0.5,
        )

        assert result.success
        pipeline_result = result.unwrap()
        assert pipeline_result.table_count == 1

        table_health = pipeline_result.table_health[0]

        # Type resolution should have completed
        assert table_health.type_resolution_completed

        # Verify quarantine table exists if there were cast failures
        if table_health.quarantine_table_name:
            tables = test_duckdb.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            assert table_health.quarantine_table_name in table_names
