"""Test historical complexity analysis."""

from datetime import UTC, datetime, timedelta

import pytest

from dataraum.analysis.topology.db_models import TopologicalQualityMetrics
from dataraum.analysis.topology.stability import compute_historical_complexity
from dataraum.storage import Source, Table


@pytest.fixture
def sample_source(session):
    """Create a sample source."""
    source = Source(
        source_id="test-source",
        name="test_data",
        source_type="csv",
    )
    session.add(source)
    session.commit()
    session.refresh(source)
    return source


def create_test_table(session, sample_source, table_id: str, table_name: str):
    """Helper to create a test table."""
    table = Table(
        table_id=table_id,
        source_id=sample_source.source_id,
        table_name=table_name,
        layer="typed",
    )
    session.add(table)
    session.commit()
    return table


def test_compute_historical_complexity_no_data(session):
    """Test compute_historical_complexity with no historical data."""
    result = compute_historical_complexity(
        session=session,
        table_id="test_table_1",
        current_complexity=10,
        window_size=10,
    )

    assert result.success
    data = result.value
    assert data["mean"] is None
    assert data["std"] is None
    assert data["z_score"] is None


def test_compute_historical_complexity_insufficient_data(session, sample_source):
    """Test compute_historical_complexity with insufficient data points (<3)."""
    table_id = "test_table_2"
    create_test_table(session, sample_source, table_id, "test_data_2")

    # Create 2 historical records (below the 3-point threshold)
    for i in range(2):
        metric = TopologicalQualityMetrics(
            table_id=table_id,
            computed_at=datetime.now(UTC) - timedelta(days=i),
            structural_complexity=10 + i,
            topology_data={},
        )
        session.add(metric)

    session.commit()

    result = compute_historical_complexity(
        session=session,
        table_id=table_id,
        current_complexity=15,
        window_size=10,
    )

    assert result.success
    data = result.value
    assert data["mean"] is None
    assert data["std"] is None
    assert data["z_score"] is None


def test_compute_historical_complexity_with_data(session, sample_source):
    """Test compute_historical_complexity with sufficient historical data."""
    table_id = "test_table_3"
    create_test_table(session, sample_source, table_id, "test_data_3")

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

    session.commit()

    # Test with current complexity = 10 (at mean)
    result = compute_historical_complexity(
        session=session,
        table_id=table_id,
        current_complexity=10,
        window_size=10,
    )

    assert result.success
    data = result.value
    assert data["mean"] == 10.0
    assert data["std"] > 0  # Should have some variance
    assert abs(data["z_score"]) < 0.1  # Close to 0 since current = mean


def test_compute_historical_complexity_anomaly(session, sample_source):
    """Test compute_historical_complexity detects anomalies."""
    table_id = "test_table_4"
    create_test_table(session, sample_source, table_id, "test_data_4")

    # Create stable baseline: 10 records with complexity around 10
    for i in range(10):
        metric = TopologicalQualityMetrics(
            table_id=table_id,
            computed_at=datetime.now(UTC) - timedelta(days=i),
            structural_complexity=10,  # All same value
            topology_data={},
        )
        session.add(metric)

    session.commit()

    # Test with current complexity = 30 (anomalously high)
    result = compute_historical_complexity(
        session=session,
        table_id=table_id,
        current_complexity=30,
        window_size=10,
    )

    assert result.success
    data = result.value
    assert data["mean"] == 10.0
    assert data["std"] == 0.0  # No variance in historical data
    # With std=0, z_score should be None (we can't compute it)
    assert data["z_score"] is None


def test_compute_historical_complexity_window_size(session, sample_source):
    """Test compute_historical_complexity respects window_size parameter."""
    table_id = "test_table_5"
    create_test_table(session, sample_source, table_id, "test_data_5")

    # Create 20 historical records
    # First 10 (older): complexity = 5
    # Last 10 (newer): complexity = 15
    for i in range(20):
        complexity = 5 if i >= 10 else 15  # i=0 is most recent
        metric = TopologicalQualityMetrics(
            table_id=table_id,
            computed_at=datetime.now(UTC) - timedelta(days=i),
            structural_complexity=complexity,
            topology_data={},
        )
        session.add(metric)

    session.commit()

    # Test with window_size=5 - should only see recent records (complexity=15)
    result = compute_historical_complexity(
        session=session,
        table_id=table_id,
        current_complexity=16,
        window_size=5,
    )

    assert result.success
    data = result.value
    # Mean should be 15.0 (only recent 5 records with complexity=15)
    assert data["mean"] == 15.0
    assert data["std"] == 0.0
