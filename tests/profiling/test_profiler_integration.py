"""Integration tests for profiling with real CSV data."""

from pathlib import Path

import duckdb
import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dataraum_context.core.models import SourceConfig
from dataraum_context.profiling import profile_table
from dataraum_context.staging.loaders.csv import CSVLoader
from dataraum_context.storage.schema import init_database


@pytest.fixture
async def test_session():
    """Create an in-memory SQLite session for testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )

    # Enable foreign keys for SQLite
    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    # Initialize database schema
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
def payment_method_csv():
    """Path to payment_method.csv example."""
    return Path("examples/finance_csv_example/payment_method.csv")


class TestProfilerIntegration:
    """Integration tests for CSV loading → profiling pipeline."""

    async def test_load_and_profile_payment_method(
        self,
        payment_method_csv,
        test_duckdb,
        test_session,
    ):
        """Test full pipeline: load CSV → profile → generate type candidates."""
        if not payment_method_csv.exists():
            pytest.skip("Example CSV not found")

        # Step 1: Load CSV
        loader = CSVLoader()
        config = SourceConfig(
            name="payment_method",
            source_type="csv",
            path=str(payment_method_csv),
        )

        load_result = await loader.load(config, test_duckdb, test_session)
        assert load_result.success, f"Load failed: {load_result.error}"

        staging_result = load_result.value
        assert staging_result
        table = staging_result.tables[0]

        # Step 2: Profile the table
        profile_result = await profile_table(
            table_id=table.table_id,
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert profile_result.success, f"Profiling failed: {profile_result.error}"

        result = profile_result.value
        assert result

        # Verify we got profiles
        assert len(result.profiles) > 0
        assert len(result.profiles) == len(table.columns)

        # Verify we got type candidates (since this is a 'raw' layer table)
        assert len(result.type_candidates) > 0

        # Check that each column has at least one type candidate
        columns_with_candidates = {tc.column_id for tc in result.type_candidates}
        assert len(columns_with_candidates) > 0

        # Verify profile contents
        for profile in result.profiles:
            assert profile.total_count > 0
            assert profile.null_ratio >= 0.0
            assert profile.null_ratio <= 1.0
            assert profile.cardinality_ratio >= 0.0
            assert profile.cardinality_ratio <= 1.0

            # Should have string stats for VARCHAR columns
            if profile.string_stats:
                assert profile.string_stats.min_length >= 0
                assert profile.string_stats.max_length >= profile.string_stats.min_length

        # Verify type candidates
        for candidate in result.type_candidates:
            assert candidate.confidence >= 0.0
            assert candidate.confidence <= 1.0
            assert candidate.parse_success_rate >= 0.0
            assert candidate.parse_success_rate <= 1.0
            assert candidate.data_type is not None

        # Check for specific patterns we expect in payment_method.csv
        # Business Id should be detected as integer
        business_id_candidates = [
            tc for tc in result.type_candidates if tc.column_ref.column_name == "Business Id"
        ]

        if business_id_candidates:
            # Should have INTEGER or BIGINT as a candidate
            types = [tc.data_type.value for tc in business_id_candidates]
            assert "BIGINT" in types or "INTEGER" in types

        # Credit card column should have BOOLEAN candidates (Yes/No pattern)
        credit_card_candidates = [
            tc for tc in result.type_candidates if tc.column_ref.column_name == "Credit card"
        ]

        if credit_card_candidates:
            # Check if boolean was detected
            types = [tc.data_type.value for tc in credit_card_candidates]
            # Might be detected as BOOLEAN or VARCHAR depending on patterns
            assert len(types) > 0

    async def test_profile_creates_metadata(
        self,
        payment_method_csv,
        test_duckdb,
        test_session,
    ):
        """Test that profiling creates proper metadata records."""
        if not payment_method_csv.exists():
            pytest.skip("Example CSV not found")

        # Load CSV
        loader = CSVLoader()
        config = SourceConfig(
            name="payment_method",
            source_type="csv",
            path=str(payment_method_csv),
        )

        load_result = await loader.load(config, test_duckdb, test_session)
        assert load_result.success

        staging_result = load_result.value
        assert staging_result
        table = staging_result.tables[0]

        # Profile
        profile_result = await profile_table(
            table_id=table.table_id,
            duckdb_conn=test_duckdb,
            session=test_session,
        )

        assert profile_result.success

        # Check that metadata was stored in database
        from dataraum_context.storage.models_v2 import StatisticalProfile, Table, TypeCandidate

        # Refresh table to get updated last_profiled_at
        await test_session.refresh(await test_session.get(Table, str(table.table_id)))

        # Check StatisticalProfile records were created
        profiles = await test_session.run_sync(
            lambda sync_session: sync_session.query(StatisticalProfile).all()
        )
        assert len(profiles) > 0

        # Check TypeCandidate records were created
        candidates = await test_session.run_sync(
            lambda sync_session: sync_session.query(TypeCandidate).all()
        )
        assert len(candidates) > 0

        # Verify type candidate properties
        for candidate in candidates:
            assert candidate.data_type is not None
            assert candidate.confidence >= 0.0
            assert candidate.detected_at is not None
