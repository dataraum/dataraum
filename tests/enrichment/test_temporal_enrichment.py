"""Test temporal enrichment module."""

from datetime import datetime, timedelta

import duckdb
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dataraum_context.enrichment.models import TemporalEnrichmentResult
from dataraum_context.enrichment.temporal import enrich_temporal
from dataraum_context.quality.models import TemporalQualityResult
from dataraum_context.storage.models_v2 import Base, Column, Source, Table


@pytest.fixture
async def session():
    """Create an in-memory SQLite session for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session_factory() as session:
        yield session


@pytest.fixture
def duckdb_conn():
    """Create an in-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.mark.asyncio
async def test_enrich_temporal_daily_granularity(session: AsyncSession, duckdb_conn):
    """Test temporal enrichment with daily granularity data."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create test table
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="sales",
        layer="typed",
        row_count=30,
    )
    session.add(table1)

    # Add columns including a timestamp column
    col1 = Column(
        table_id="table1",
        column_name="sale_date",
        raw_type="DATE",
        resolved_type="DATE",
        column_position=0,
    )
    col2 = Column(
        table_id="table1",
        column_name="amount",
        raw_type="DECIMAL",
        resolved_type="DECIMAL",
        column_position=1,
    )

    session.add_all([col1, col2])
    await session.commit()

    # Create test data with daily granularity (30 consecutive days)
    duckdb_conn.execute("CREATE TABLE typed_sales (sale_date DATE, amount DECIMAL)")

    base_date = datetime(2024, 1, 1)
    for i in range(30):
        date = base_date + timedelta(days=i)
        duckdb_conn.execute(
            f"INSERT INTO typed_sales VALUES (DATE '{date.strftime('%Y-%m-%d')}', {100.0 + i})"
        )

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Verify result
    assert result.success is True
    assert result.value is not None
    assert isinstance(result.value, TemporalEnrichmentResult)

    enrichment_result = result.value
    assert len(enrichment_result.profiles) == 1

    profile = enrichment_result.profiles[0]
    assert isinstance(profile, TemporalQualityResult)
    assert profile.column_name == "sale_date"
    assert profile.table_name == "sales"
    assert profile.detected_granularity == "day"
    assert profile.granularity_confidence > 0.5
    assert profile.span_days == 29  # 30 days inclusive = 29 day span
    assert profile.completeness.completeness_ratio >= 0.95  # Should be nearly complete


@pytest.mark.asyncio
async def test_enrich_temporal_weekly_granularity(session: AsyncSession, duckdb_conn):
    """Test temporal enrichment with weekly granularity data."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create test table
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="weekly_reports",
        layer="typed",
        row_count=10,
    )
    session.add(table1)

    # Add timestamp column
    col1 = Column(
        table_id="table1",
        column_name="report_date",
        raw_type="TIMESTAMP",
        resolved_type="TIMESTAMP",
        column_position=0,
    )

    session.add(col1)
    await session.commit()

    # Create test data with weekly granularity (10 weeks)
    duckdb_conn.execute("CREATE TABLE typed_weekly_reports (report_date TIMESTAMP)")

    base_date = datetime(2024, 1, 1)
    for i in range(10):
        date = base_date + timedelta(weeks=i)
        duckdb_conn.execute(
            f"INSERT INTO typed_weekly_reports VALUES (TIMESTAMP '{date.isoformat()}')"
        )

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Verify result
    assert result.success is True
    profile = result.value.profiles[0]
    assert profile.detected_granularity == "week"
    assert profile.granularity_confidence > 0.5


@pytest.mark.asyncio
async def test_enrich_temporal_with_gaps(session: AsyncSession, duckdb_conn):
    """Test temporal enrichment detects gaps in time series."""
    # Create test source and table
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="events",
        layer="typed",
        row_count=20,
    )
    session.add(table1)

    col1 = Column(
        table_id="table1",
        column_name="event_time",
        raw_type="TIMESTAMP",
        resolved_type="TIMESTAMP",
        column_position=0,
    )

    session.add(col1)
    await session.commit()

    # Create data with gaps (daily data with a 7-day gap and a 14-day gap)
    duckdb_conn.execute("CREATE TABLE typed_events (event_time TIMESTAMP)")

    base_date = datetime(2024, 1, 1)
    # First 10 days
    for i in range(10):
        date = base_date + timedelta(days=i)
        duckdb_conn.execute(f"INSERT INTO typed_events VALUES (TIMESTAMP '{date.isoformat()}')")

    # 7-day gap (skip days 10-16)
    # Next 5 days (days 17-21)
    for i in range(17, 22):
        date = base_date + timedelta(days=i)
        duckdb_conn.execute(f"INSERT INTO typed_events VALUES (TIMESTAMP '{date.isoformat()}')")

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Verify result
    assert result.success is True
    profile = result.value.profiles[0]

    # Check gap detection
    assert profile.completeness is not None
    assert profile.completeness.gap_count > 0
    assert len(profile.completeness.gaps) > 0

    # Verify largest gap is detected
    largest_gap = profile.completeness.largest_gap_days
    assert largest_gap is not None
    assert largest_gap >= 5  # Should detect the 7-day gap


@pytest.mark.asyncio
async def test_enrich_temporal_no_temporal_columns(session: AsyncSession, duckdb_conn):
    """Test temporal enrichment with no temporal columns."""
    # Create test source and table
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="products",
        layer="typed",
        row_count=10,
    )
    session.add(table1)

    # Add only non-temporal columns
    col1 = Column(
        table_id="table1",
        column_name="product_id",
        raw_type="INTEGER",
        resolved_type="INTEGER",
        column_position=0,
    )
    col2 = Column(
        table_id="table1",
        column_name="name",
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
        column_position=1,
    )

    session.add_all([col1, col2])
    await session.commit()

    # Create test data
    duckdb_conn.execute("CREATE TABLE typed_products (product_id INTEGER, name VARCHAR)")
    duckdb_conn.execute("INSERT INTO typed_products VALUES (1, 'Widget'), (2, 'Gadget')")

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Should succeed but return no profiles
    assert result.success is True
    assert len(result.value.profiles) == 0


@pytest.mark.asyncio
async def test_enrich_temporal_multiple_columns(session: AsyncSession, duckdb_conn):
    """Test temporal enrichment with multiple temporal columns in one table."""
    # Create test source and table
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="orders",
        layer="typed",
        row_count=20,
    )
    session.add(table1)

    # Add multiple temporal columns
    col1 = Column(
        table_id="table1",
        column_name="order_date",
        raw_type="DATE",
        resolved_type="DATE",
        column_position=0,
    )
    col2 = Column(
        table_id="table1",
        column_name="ship_date",
        raw_type="DATE",
        resolved_type="DATE",
        column_position=1,
    )
    col3 = Column(
        table_id="table1",
        column_name="delivery_date",
        raw_type="DATE",
        resolved_type="DATE",
        column_position=2,
    )

    session.add_all([col1, col2, col3])
    await session.commit()

    # Create test data with multiple temporal columns
    duckdb_conn.execute(
        "CREATE TABLE typed_orders (order_date DATE, ship_date DATE, delivery_date DATE)"
    )

    base_date = datetime(2024, 1, 1)
    for i in range(20):
        order_date = base_date + timedelta(days=i)
        ship_date = order_date + timedelta(days=1)
        delivery_date = order_date + timedelta(days=3)
        duckdb_conn.execute(
            f"""
            INSERT INTO typed_orders VALUES
            (DATE '{order_date.strftime("%Y-%m-%d")}',
             DATE '{ship_date.strftime("%Y-%m-%d")}',
             DATE '{delivery_date.strftime("%Y-%m-%d")}')
        """
        )

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Verify result - should create profiles for all 3 temporal columns
    assert result.success is True
    assert len(result.value.profiles) == 3

    column_names = {p.column_name for p in result.value.profiles}
    assert column_names == {"order_date", "ship_date", "delivery_date"}

    # All should have daily granularity
    for profile in result.value.profiles:
        assert profile.detected_granularity == "day"


@pytest.mark.asyncio
async def test_enrich_temporal_stores_metrics(session: AsyncSession, duckdb_conn):
    """Test that temporal enrichment stores metrics in the database."""
    # Create test source and table
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="metrics",
        layer="typed",
        row_count=10,
    )
    session.add(table1)

    col1 = Column(
        table_id="table1",
        column_name="metric_date",
        raw_type="DATE",
        resolved_type="DATE",
        column_position=0,
    )

    session.add(col1)
    await session.commit()

    # Create test data
    duckdb_conn.execute("CREATE TABLE typed_metrics (metric_date DATE)")

    base_date = datetime(2024, 1, 1)
    for i in range(10):
        date = base_date + timedelta(days=i)
        duckdb_conn.execute(
            f"INSERT INTO typed_metrics VALUES (DATE '{date.strftime('%Y-%m-%d')}')"
        )

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    assert result.success is True

    # Check that metrics were stored in database
    from sqlalchemy import select

    from dataraum_context.enrichment.db_models import TemporalQualityMetrics

    stmt = select(TemporalQualityMetrics)
    db_result = await session.execute(stmt)
    stored_metrics = db_result.scalars().all()

    # Should have stored 1 metric (one for the temporal column)
    assert len(stored_metrics) >= 1

    # Verify stored metric has correct data
    metric = stored_metrics[0]
    assert metric.column_id == col1.column_id
    assert metric.detected_granularity == "day"
    assert metric.min_timestamp is not None
    assert metric.max_timestamp is not None


@pytest.mark.asyncio
async def test_enrich_temporal_irregular_granularity(session: AsyncSession, duckdb_conn):
    """Test temporal enrichment with irregular/random timestamps."""
    # Create test source and table
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="irregular_events",
        layer="typed",
        row_count=10,
    )
    session.add(table1)

    col1 = Column(
        table_id="table1",
        column_name="event_time",
        raw_type="TIMESTAMP",
        resolved_type="TIMESTAMP",
        column_position=0,
    )

    session.add(col1)
    await session.commit()

    # Create data with irregular timestamps (random gaps)
    duckdb_conn.execute("CREATE TABLE typed_irregular_events (event_time TIMESTAMP)")

    base_date = datetime(2024, 1, 1)
    irregular_offsets = [0, 1, 4, 5, 6, 10, 15, 16, 25, 30]  # Irregular gaps

    for offset in irregular_offsets:
        date = base_date + timedelta(days=offset)
        duckdb_conn.execute(
            f"INSERT INTO typed_irregular_events VALUES (TIMESTAMP '{date.isoformat()}')"
        )

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Verify result
    assert result.success is True
    profile = result.value.profiles[0]

    # Should detect irregular or some approximate granularity
    assert profile.detected_granularity in ["day", "irregular", "week"]
    # Confidence should be lower for irregular data
    assert profile.granularity_confidence <= 0.9


@pytest.mark.asyncio
async def test_enrich_temporal_completeness_ratio(session: AsyncSession, duckdb_conn):
    """Test that completeness ratio is calculated correctly."""
    # Create test source and table
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="incomplete_data",
        layer="typed",
        row_count=15,
    )
    session.add(table1)

    col1 = Column(
        table_id="table1",
        column_name="date",
        raw_type="DATE",
        resolved_type="DATE",
        column_position=0,
    )

    session.add(col1)
    await session.commit()

    # Create data spanning 30 days but with only 15 days of data (50% complete)
    duckdb_conn.execute("CREATE TABLE typed_incomplete_data (date DATE)")

    base_date = datetime(2024, 1, 1)
    # Insert only every other day (15 days out of 30)
    for i in range(0, 30, 2):
        date = base_date + timedelta(days=i)
        duckdb_conn.execute(
            f"INSERT INTO typed_incomplete_data VALUES (DATE '{date.strftime('%Y-%m-%d')}')"
        )

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Verify completeness is detected
    assert result.success is True
    profile = result.value.profiles[0]

    # Completeness ratio should be around 0.5 (15 out of 30 days)
    assert profile.completeness.completeness_ratio < 0.8
    assert profile.completeness.actual_periods == 15
    assert profile.completeness.expected_periods >= 15


@pytest.mark.asyncio
async def test_enrich_temporal_monthly_granularity(session: AsyncSession, duckdb_conn):
    """Test temporal enrichment with monthly granularity."""
    # Create test source and table
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="monthly_data",
        layer="typed",
        row_count=12,
    )
    session.add(table1)

    col1 = Column(
        table_id="table1",
        column_name="month",
        raw_type="DATE",
        resolved_type="DATE",
        column_position=0,
    )

    session.add(col1)
    await session.commit()

    # Create data with monthly granularity (first day of each month for 12 months)
    duckdb_conn.execute("CREATE TABLE typed_monthly_data (month DATE)")

    base_date = datetime(2024, 1, 1)
    for i in range(12):
        # Calculate first day of each month
        month = (base_date.month + i - 1) % 12 + 1
        year = base_date.year + (base_date.month + i - 1) // 12
        date = datetime(year, month, 1)
        duckdb_conn.execute(
            f"INSERT INTO typed_monthly_data VALUES (DATE '{date.strftime('%Y-%m-%d')}')"
        )

    # Run temporal enrichment
    result = await enrich_temporal(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Verify result
    assert result.success is True
    profile = result.value.profiles[0]

    # Should detect monthly granularity
    assert profile.detected_granularity == "month"
    assert profile.granularity_confidence > 0.5
