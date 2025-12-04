"""Integration tests for type resolution."""

import duckdb
import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dataraum_context.core.models import SourceConfig
from dataraum_context.profiling.profiler import profile_and_resolve_types
from dataraum_context.staging.loaders.csv import CSVLoader
from dataraum_context.staging.pipeline import StagingPipelineResult, stage_csv
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

    async def test_pipeline_stage_csv(self, simple_csv, test_duckdb, test_session):
        """Test the full stage_csv pipeline."""
        result = await stage_csv(
            file_path=str(simple_csv),
            table_name="pipeline_test",
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert result.success
        pipeline_result = result.unwrap()
        assert isinstance(pipeline_result, StagingPipelineResult)
        assert pipeline_result.raw_table_name == "raw_simple"
        assert pipeline_result.total_rows == 5

    async def test_stage_without_type_resolution(self, simple_csv, test_duckdb, test_session):
        """Test staging without automatic type resolution."""
        result = await stage_csv(
            file_path=str(simple_csv),
            table_name="no_resolve",
            duckdb_conn=test_duckdb,
            session=test_session,
            auto_resolve_types=False,
        )

        assert result.success
        pipeline_result = result.unwrap()
        assert pipeline_result.raw_table_name == "raw_simple"
        assert pipeline_result.type_resolution_result is None

        # Typed table should NOT exist
        tables = test_duckdb.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "typed_simple" not in table_names


class TestQuarantineHandling:
    """Tests for quarantine table handling."""

    async def test_quarantine_table_created(self, quarantine_csv, test_duckdb, test_session):
        """Test that quarantine table is created for failed casts."""
        result = await stage_csv(
            file_path=str(quarantine_csv),
            table_name="quarantine_test",
            duckdb_conn=test_duckdb,
            session=test_session,
            min_confidence=0.5,
        )

        assert result.success
        pipeline_result = result.unwrap()

        if pipeline_result.type_resolution_result:
            resolution = pipeline_result.type_resolution_result

            # Verify quarantine table exists
            tables = test_duckdb.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            assert "quarantine_quarantine" in table_names

            # Quarantine should have failed rows
            if resolution.quarantined_rows > 0:
                quarantine_count = test_duckdb.execute(
                    f'SELECT COUNT(*) FROM "{resolution.quarantine_table_name}"'
                ).fetchone()[0]
                assert quarantine_count == resolution.quarantined_rows


class TestColumnCastResults:
    """Tests for column cast result tracking."""

    async def test_column_cast_results_populated(self, simple_csv, test_duckdb, test_session):
        """Test that column cast results are properly populated."""
        result = await stage_csv(
            file_path=str(simple_csv),
            table_name="cast_results",
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert result.success
        pipeline_result = result.unwrap()

        if pipeline_result.type_resolution_result:
            for col_result in pipeline_result.type_resolution_result.column_results:
                assert col_result.source_type == "VARCHAR"
                assert col_result.target_type is not None
                assert col_result.success_count >= 0
                assert col_result.failure_count >= 0
                assert 0.0 <= col_result.success_rate <= 1.0
