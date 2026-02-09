"""Tests for statistical profiling processor."""

import duckdb
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker

from dataraum.analysis.statistics import (
    ColumnProfile,
    NumericStats,
    StatisticsProfileResult,
    StringStats,
    profile_statistics,
)
from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.core.models import SourceConfig
from dataraum.sources.csv import CSVLoader
from dataraum.storage import Table, init_database


def _get_typed_table_id(typed_table_name: str, session) -> str | None:
    """Get the table ID for a typed table by DuckDB path."""
    stmt = select(Table).where(Table.duckdb_path == typed_table_name)
    result = session.execute(stmt)
    table = result.scalar_one_or_none()
    return table.table_id if table else None


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


@pytest.fixture
def simple_csv(tmp_path):
    """Create a simple CSV with mixed types."""
    csv_content = """id,name,amount,category
1,Alice,1234.56,A
2,Bob,789.00,B
3,Charlie,456.78,A
4,Diana,0.00,C
5,Eve,999.99,B
"""
    csv_file = tmp_path / "simple.csv"
    csv_file.write_text(csv_content)
    return csv_file


class TestStatisticsProfiler:
    """Tests for profile_statistics function."""

    def test_profile_statistics_returns_result(self, simple_csv, test_duckdb, test_session):
        """Test that profile_statistics returns a valid Result."""
        # Load and type the table
        loader = CSVLoader()
        config = SourceConfig(
            name="simple",
            source_type="csv",
            path=str(simple_csv),
        )
        load_result = loader.load(config, test_duckdb, test_session)
        assert load_result.success

        staged_table = load_result.unwrap().tables[0]

        # Get the SQLAlchemy Table model from database
        raw_table = test_session.get(Table, staged_table.table_id)
        assert raw_table is not None

        # Infer types (pass Table object)
        infer_result = infer_type_candidates(raw_table, test_duckdb, test_session)
        assert infer_result.success

        # Resolve types (pass table_id string)
        resolve_result = resolve_types(staged_table.table_id, test_duckdb, test_session)
        assert resolve_result.success
        resolution = resolve_result.unwrap()

        # Get typed table ID from name
        typed_table_id = _get_typed_table_id(resolution.typed_table_name, test_session)
        assert typed_table_id is not None

        # Profile statistics
        stats_result = profile_statistics(typed_table_id, test_duckdb, test_session)

        assert stats_result.success
        result = stats_result.unwrap()
        assert isinstance(result, StatisticsProfileResult)
        assert len(result.column_profiles) > 0
        assert result.duration_seconds > 0

    def test_profile_statistics_numeric_stats(self, simple_csv, test_duckdb, test_session):
        """Test that numeric columns have numeric_stats."""
        # Load and type the table
        loader = CSVLoader()
        config = SourceConfig(
            name="simple",
            source_type="csv",
            path=str(simple_csv),
        )
        load_result = loader.load(config, test_duckdb, test_session)
        assert load_result.success

        staged_table = load_result.unwrap().tables[0]
        raw_table = test_session.get(Table, staged_table.table_id)
        assert raw_table is not None

        # Infer types (pass Table object), resolve (pass table_id string)
        infer_result = infer_type_candidates(raw_table, test_duckdb, test_session)
        assert infer_result.success

        resolve_result = resolve_types(staged_table.table_id, test_duckdb, test_session)
        assert resolve_result.success
        resolution = resolve_result.unwrap()

        # Get typed table ID from name
        typed_table_id = _get_typed_table_id(resolution.typed_table_name, test_session)
        assert typed_table_id is not None

        # Profile statistics
        stats_result = profile_statistics(typed_table_id, test_duckdb, test_session)
        assert stats_result.success
        result = stats_result.unwrap()

        # Find amount column profile (should be numeric)
        amount_profile = next(
            (p for p in result.column_profiles if p.column_ref.column_name == "amount"),
            None,
        )

        if amount_profile and amount_profile.numeric_stats:
            assert isinstance(amount_profile.numeric_stats, NumericStats)
            assert amount_profile.numeric_stats.min_value <= amount_profile.numeric_stats.max_value
            assert amount_profile.numeric_stats.mean is not None
            assert amount_profile.numeric_stats.stddev is not None

    def test_profile_statistics_string_stats(self, simple_csv, test_duckdb, test_session):
        """Test that string columns have string_stats."""
        # Load and type the table
        loader = CSVLoader()
        config = SourceConfig(
            name="simple",
            source_type="csv",
            path=str(simple_csv),
        )
        load_result = loader.load(config, test_duckdb, test_session)
        assert load_result.success

        staged_table = load_result.unwrap().tables[0]
        raw_table = test_session.get(Table, staged_table.table_id)
        assert raw_table is not None

        # Infer types (pass Table object), resolve (pass table_id string)
        infer_type_candidates(raw_table, test_duckdb, test_session)
        resolve_result = resolve_types(staged_table.table_id, test_duckdb, test_session)
        assert resolve_result.success
        resolution = resolve_result.unwrap()

        # Get typed table ID from name
        typed_table_id = _get_typed_table_id(resolution.typed_table_name, test_session)
        assert typed_table_id is not None

        # Profile statistics
        stats_result = profile_statistics(typed_table_id, test_duckdb, test_session)
        assert stats_result.success
        result = stats_result.unwrap()

        # Find name column profile (should be VARCHAR)
        name_profile = next(
            (p for p in result.column_profiles if p.column_ref.column_name == "name"),
            None,
        )

        if name_profile and name_profile.string_stats:
            assert isinstance(name_profile.string_stats, StringStats)
            assert name_profile.string_stats.min_length <= name_profile.string_stats.max_length

    def test_profile_statistics_basic_counts(self, simple_csv, test_duckdb, test_session):
        """Test that basic counts are computed correctly."""
        # Load and type the table
        loader = CSVLoader()
        config = SourceConfig(
            name="simple",
            source_type="csv",
            path=str(simple_csv),
        )
        load_result = loader.load(config, test_duckdb, test_session)
        assert load_result.success

        staged_table = load_result.unwrap().tables[0]
        raw_table = test_session.get(Table, staged_table.table_id)
        assert raw_table is not None

        # Infer types (pass Table object), resolve (pass table_id string)
        infer_type_candidates(raw_table, test_duckdb, test_session)
        resolve_result = resolve_types(staged_table.table_id, test_duckdb, test_session)
        assert resolve_result.success
        resolution = resolve_result.unwrap()

        # Get typed table ID from name
        typed_table_id = _get_typed_table_id(resolution.typed_table_name, test_session)
        assert typed_table_id is not None

        # Profile statistics
        stats_result = profile_statistics(typed_table_id, test_duckdb, test_session)
        assert stats_result.success
        result = stats_result.unwrap()

        for profile in result.column_profiles:
            assert isinstance(profile, ColumnProfile)
            assert profile.total_count == 5  # We have 5 rows
            assert profile.null_count >= 0
            assert profile.distinct_count >= 0
            assert 0.0 <= profile.null_ratio <= 1.0
            assert 0.0 <= profile.cardinality_ratio <= 1.0

    def test_profile_statistics_requires_typed_table(self, simple_csv, test_duckdb, test_session):
        """Test that profile_statistics fails on raw tables."""
        # Load but don't type the table
        loader = CSVLoader()
        config = SourceConfig(
            name="simple",
            source_type="csv",
            path=str(simple_csv),
        )
        load_result = loader.load(config, test_duckdb, test_session)
        assert load_result.success

        raw_table = load_result.unwrap().tables[0]

        # Try to profile raw table (should fail)
        stats_result = profile_statistics(raw_table.table_id, test_duckdb, test_session)

        assert not stats_result.success
        assert "typed" in stats_result.error.lower()
