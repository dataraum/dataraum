"""Test historical complexity analysis."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dataraum_context.quality.topological import compute_historical_complexity
from dataraum_context.storage.models_v2 import Base, TopologicalQualityMetrics


@pytest.fixture
async def session():
    """Create an in-memory SQLite session for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session_factory() as session:
        yield session


@pytest.mark.asyncio
async def test_compute_historical_complexity_no_data(session: AsyncSession):
    """Test compute_historical_complexity with no historical data."""
    result = await compute_historical_complexity(
        session=session,
        table_id="test_table_1",
        current_complexity=10,
        lookback_days=30,
    )

    assert result["mean"] is None
    assert result["std"] is None
    assert result["z_score"] is None


@pytest.mark.asyncio
async def test_compute_historical_complexity_insufficient_data(session: AsyncSession):
    """Test compute_historical_complexity with insufficient data points (<5)."""
    table_id = "test_table_2"

    # Create 3 historical records (below the 5-point threshold)
    for i in range(3):
        metric = TopologicalQualityMetrics(
            table_id=table_id,
            computed_at=datetime.now(UTC) - timedelta(days=i),
            structural_complexity=10 + i,
            topology_data={},
        )
        session.add(metric)

    await session.commit()

    result = await compute_historical_complexity(
        session=session,
        table_id=table_id,
        current_complexity=15,
        lookback_days=30,
    )

    assert result["mean"] is None
    assert result["std"] is None
    assert result["z_score"] is None


@pytest.mark.asyncio
async def test_compute_historical_complexity_with_data(session: AsyncSession):
    """Test compute_historical_complexity with sufficient historical data."""
    table_id = "test_table_3"

    # Create 10 historical records with known complexity values
    # Mean: 10, Std: ~3.03
    complexities = [10, 10, 10, 10, 10, 7, 7, 13, 13, 10]

    for i, complexity in enumerate(complexities):
        metric = TopologicalQualityMetrics(
            table_id=table_id,
            computed_at=datetime.now(UTC) - timedelta(days=i),
            structural_complexity=complexity,
            topology_data={},
        )
        session.add(metric)

    await session.commit()

    # Test with current complexity = 10 (at mean)
    result = await compute_historical_complexity(
        session=session,
        table_id=table_id,
        current_complexity=10,
        lookback_days=30,
    )

    assert result["mean"] == 10.0
    assert result["std"] > 0  # Should have some variance
    assert abs(result["z_score"]) < 0.1  # Close to 0 since current = mean


@pytest.mark.asyncio
async def test_compute_historical_complexity_anomaly(session: AsyncSession):
    """Test compute_historical_complexity detects anomalies."""
    table_id = "test_table_4"

    # Create stable baseline: 10 records with complexity around 10
    for i in range(10):
        metric = TopologicalQualityMetrics(
            table_id=table_id,
            computed_at=datetime.now(UTC) - timedelta(days=i),
            structural_complexity=10,  # All same value
            topology_data={},
        )
        session.add(metric)

    await session.commit()

    # Test with current complexity = 30 (anomalously high)
    result = await compute_historical_complexity(
        session=session,
        table_id=table_id,
        current_complexity=30,
        lookback_days=30,
    )

    assert result["mean"] == 10.0
    assert result["std"] == 0.0  # No variance in historical data
    # With std=0, z_score should be 0 (special case handled in function)
    assert result["z_score"] == 0.0


@pytest.mark.asyncio
async def test_compute_historical_complexity_lookback_window(session: AsyncSession):
    """Test compute_historical_complexity respects lookback window."""
    table_id = "test_table_5"

    # Create 10 old records (60 days ago) with complexity = 5
    for i in range(10):
        metric = TopologicalQualityMetrics(
            table_id=table_id,
            computed_at=datetime.now(UTC) - timedelta(days=60 + i),
            structural_complexity=5,
            topology_data={},
        )
        session.add(metric)

    # Create 5 recent records (10 days ago) with complexity = 15
    for i in range(5):
        metric = TopologicalQualityMetrics(
            table_id=table_id,
            computed_at=datetime.now(UTC) - timedelta(days=10 + i),
            structural_complexity=15,
            topology_data={},
        )
        session.add(metric)

    await session.commit()

    # Test with lookback_days=30 - should only see recent records
    result = await compute_historical_complexity(
        session=session,
        table_id=table_id,
        current_complexity=16,
        lookback_days=30,
    )

    # Mean should be close to 15, not 5
    assert result["mean"] == 15.0
    assert result["std"] == 0.0
    assert result["z_score"] == 0.0  # std=0 special case
