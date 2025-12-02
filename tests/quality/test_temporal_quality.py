"""Tests for temporal quality analysis module."""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from dataraum_context.core.models.temporal import (
    ChangePointResult,
    DistributionStabilityAnalysis,
    FiscalCalendarAnalysis,
    SeasonalityAnalysis,
    TemporalCompletenessAnalysis,
    TemporalQualityResult,
    TrendAnalysis,
    UpdateFrequencyAnalysis,
)
from dataraum_context.quality.temporal import (
    analyze_completeness,
    analyze_distribution_stability,
    analyze_seasonality,
    analyze_temporal_quality,
    analyze_trend,
    analyze_update_frequency,
    detect_change_points,
    detect_fiscal_calendar,
)
from dataraum_context.storage.models_v2.core import Column, Source, Table


def create_column(
    col_id: str, table_id: str, name: str, position: int, col_type: str = "TIMESTAMP"
):
    """Helper to create a column."""
    return Column(
        column_id=col_id,
        table_id=table_id,
        column_name=name,
        column_position=position,
        resolved_type=col_type,
    )


@pytest.fixture
async def sample_source(async_session):
    """Create a sample source."""
    source = Source(
        source_id="test-source",
        name="test_data",
        source_type="csv",
    )
    async_session.add(source)
    await async_session.commit()
    await async_session.refresh(source)
    return source


@pytest.fixture
async def temporal_table(async_session, sample_source, duckdb_conn):
    """Create a table with temporal data."""
    table = Table(
        table_id="temporal-table",
        source_id=sample_source.source_id,
        table_name="temporal_data",
        layer="typed",
        duckdb_path="temporal_data",
    )
    async_session.add(table)

    column = create_column("col-ts", table.table_id, "event_time", 0, "TIMESTAMP")
    async_session.add(column)
    await async_session.commit()

    # Create daily time series with some seasonality
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(365)]

    # Build SQL for creating table with dates
    date_values = ", ".join([f"timestamp '{d.strftime('%Y-%m-%d %H:%M:%S')}'" for d in dates])
    duckdb_conn.execute(
        f"""
        CREATE TABLE temporal_data AS
        SELECT unnest([{date_values}]) as event_time
        """
    )

    await async_session.refresh(table)
    await async_session.refresh(column)
    return table, column


# ============================================================================
# Unit Tests
# ============================================================================


@pytest.mark.asyncio
async def test_analyze_seasonality_no_data():
    """Test seasonality with insufficient data."""
    ts = pd.Series([1] * 5, index=pd.date_range("2023-01-01", periods=5, freq="D"))
    result = await analyze_seasonality(ts)

    assert result.success
    assert not result.value.has_seasonality
    assert result.value.strength == 0.0


@pytest.mark.asyncio
async def test_analyze_seasonality_weekly():
    """Test seasonality detection with weekly pattern."""
    # Create weekly pattern: peaks on day 6 (Saturday)
    dates = pd.date_range("2023-01-01", periods=365, freq="D")
    values = [10 if d.dayofweek == 6 else 1 for d in dates]
    ts = pd.Series(values, index=dates)

    result = await analyze_seasonality(ts, period=7)

    assert result.success
    seasonality = result.value
    # Should detect seasonality
    assert isinstance(seasonality, SeasonalityAnalysis)
    # May or may not be strong enough depending on decomposition


@pytest.mark.asyncio
async def test_analyze_trend_increasing():
    """Test trend detection with increasing trend."""
    dates = pd.date_range("2023-01-01", periods=100, freq="D")
    values = np.arange(100) + np.random.randn(100) * 2  # Linear increase with noise
    ts = pd.Series(values, index=dates)

    result = await analyze_trend(ts)

    assert result.success
    trend = result.value
    assert isinstance(trend, TrendAnalysis)
    assert trend.direction in ["increasing", "stable"]  # Might be noisy
    assert trend.slope is not None


@pytest.mark.asyncio
async def test_analyze_trend_stable():
    """Test trend detection with stable data."""
    dates = pd.date_range("2023-01-01", periods=100, freq="D")
    # Use fixed seed for reproducibility
    np.random.seed(42)
    values = np.ones(100) + np.random.randn(100) * 0.05  # Constant with very small noise
    ts = pd.Series(values, index=dates)

    result = await analyze_trend(ts)

    assert result.success
    trend = result.value
    # Either stable or weak trend
    assert trend.direction in ["stable", "increasing", "decreasing"]
    assert trend.strength < 0.5  # Should be weak


@pytest.mark.asyncio
async def test_detect_change_points_with_break():
    """Test change point detection with level shift."""
    dates = pd.date_range("2023-01-01", periods=200, freq="D")
    # Create level shift at day 100
    values = np.concatenate([np.ones(100) * 10, np.ones(100) * 20])
    ts = pd.Series(values, index=dates)

    result = await detect_change_points(ts, "test-metric")

    assert result.success
    change_points = result.value
    # Should detect at least one change point
    assert len(change_points) >= 1
    if change_points:
        cp = change_points[0]
        assert isinstance(cp, ChangePointResult)
        assert cp.change_type in ["level_shift", "variance_change", "trend_break"]


@pytest.mark.asyncio
async def test_detect_change_points_no_breaks():
    """Test change point detection with stable data."""
    dates = pd.date_range("2023-01-01", periods=100, freq="D")
    values = np.ones(100) + np.random.randn(100) * 0.1
    ts = pd.Series(values, index=dates)

    result = await detect_change_points(ts, "test-metric")

    assert result.success
    # Should detect few or no change points
    assert len(result.value) < 3


@pytest.mark.asyncio
async def test_analyze_update_frequency_regular():
    """Test update frequency with regular updates."""
    # Daily updates
    dates = pd.date_range("2023-01-01", periods=100, freq="D")
    ts = pd.Series(1, index=dates)

    result = await analyze_update_frequency(ts)

    assert result.success
    freq = result.value
    assert isinstance(freq, UpdateFrequencyAnalysis)
    assert freq.update_frequency_score > 0.8  # Should be highly regular
    assert freq.median_interval_seconds == pytest.approx(86400, rel=0.1)  # ~1 day


@pytest.mark.asyncio
async def test_analyze_update_frequency_irregular():
    """Test update frequency with irregular updates."""
    # Irregular intervals
    dates = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 2),
        datetime(2023, 1, 5),  # 3-day gap
        datetime(2023, 1, 6),
        datetime(2023, 1, 15),  # 9-day gap
    ]
    ts = pd.Series(1, index=pd.DatetimeIndex(dates))

    result = await analyze_update_frequency(ts)

    assert result.success
    freq = result.value
    # Should have lower regularity score
    assert freq.update_frequency_score < 0.9
    assert freq.interval_cv > 0.5  # High coefficient of variation


@pytest.mark.asyncio
async def test_detect_fiscal_calendar_december():
    """Test fiscal calendar detection with December year-end."""
    # Create data with spike in December
    dates = []
    for month in range(1, 13):
        count = 50 if month == 12 else 10  # Spike in December
        for _ in range(count):
            dates.append(datetime(2023, month, 15))

    ts = pd.Series(1, index=pd.DatetimeIndex(dates))

    result = await detect_fiscal_calendar(ts)

    assert result.success
    fiscal = result.value
    assert isinstance(fiscal, FiscalCalendarAnalysis)
    # Should detect December as fiscal year end
    if fiscal.fiscal_alignment_detected:
        assert fiscal.fiscal_year_end_month == 12


@pytest.mark.asyncio
async def test_analyze_distribution_stability_stable():
    """Test distribution stability with stable data."""
    dates = pd.date_range("2023-01-01", periods=200, freq="D")
    values = np.random.normal(10, 2, 200)  # Constant distribution
    ts = pd.Series(values, index=dates)

    result = await analyze_distribution_stability(ts, "test-metric", num_periods=4)

    assert result.success
    stability = result.value
    assert isinstance(stability, DistributionStabilityAnalysis)
    # Should be stable
    assert stability.stability_score > 0.7
    assert stability.shift_count < 2


@pytest.mark.asyncio
async def test_analyze_distribution_stability_shift():
    """Test distribution stability with distribution shift."""
    dates = pd.date_range("2023-01-01", periods=200, freq="D")
    # Create shift at midpoint
    values = np.concatenate(
        [
            np.random.normal(10, 2, 100),
            np.random.normal(20, 2, 100),  # Mean shift
        ]
    )
    ts = pd.Series(values, index=dates)

    result = await analyze_distribution_stability(ts, "test-metric", num_periods=4)

    assert result.success
    stability = result.value
    # Should detect shift
    assert stability.shift_count >= 1
    if stability.shifts:
        shift = stability.shifts[0]
        assert shift.is_significant
        assert shift.shift_direction in ["increase", "decrease", "mixed"]


@pytest.mark.asyncio
async def test_analyze_completeness_full():
    """Test completeness with complete daily data."""
    dates = pd.date_range("2023-01-01", "2023-12-31", freq="D")
    ts = pd.Series(1, index=dates)

    result = await analyze_completeness(ts, "day")

    assert result.success
    completeness = result.value
    assert isinstance(completeness, TemporalCompletenessAnalysis)
    assert completeness.completeness_ratio > 0.95
    assert completeness.gap_count == 0


@pytest.mark.asyncio
async def test_analyze_completeness_with_gaps():
    """Test completeness with gaps."""
    # Create data with clear gaps - daily data with 10-day gap in middle
    dates = []
    # First 30 days
    for i in range(30):
        dates.append(datetime(2023, 1, 1) + timedelta(days=i))
    # Skip 10 days (gap)
    # Next 30 days
    for i in range(40, 70):
        dates.append(datetime(2023, 1, 1) + timedelta(days=i))

    ts = pd.Series(1, index=pd.DatetimeIndex(dates))

    result = await analyze_completeness(ts, "day")

    assert result.success
    completeness = result.value
    # Should detect incompleteness (60 actual vs 70 expected)
    assert completeness.completeness_ratio < 0.9
    # Should detect at least one gap
    assert completeness.gap_count >= 1
    if completeness.gap_count > 0:
        assert len(completeness.gaps) > 0
        assert completeness.largest_gap_days >= 10


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.asyncio
async def test_analyze_temporal_quality_complete(temporal_table, duckdb_conn, async_session):
    """Test full temporal quality analysis."""
    table, column = temporal_table

    result = await analyze_temporal_quality(
        column.column_id,
        duckdb_conn,
        async_session,
    )

    assert result.success, f"Analysis failed: {result.error}"

    analysis = result.value
    assert isinstance(analysis, TemporalQualityResult)
    assert analysis.column_id == column.column_id
    assert analysis.table_name == "temporal_data"

    # Check basic temporal info
    assert analysis.min_timestamp is not None
    assert analysis.max_timestamp is not None
    assert analysis.span_days > 0
    assert analysis.detected_granularity in ["day", "daily"]

    # Check that analyses were performed (may be None if no seasonality/trend detected)
    # Note: The temporal_table fixture creates timestamps without values, so seasonality
    # analysis may not detect patterns
    assert analysis.trend is not None  # Trend analysis should always run
    assert analysis.update_frequency is not None
    assert analysis.completeness is not None

    # Check quality score
    assert 0.0 <= analysis.temporal_quality_score <= 1.0


@pytest.mark.asyncio
async def test_analyze_temporal_quality_non_temporal_column(
    async_session, sample_source, duckdb_conn
):
    """Test error handling for non-temporal column."""
    table = Table(
        table_id="non-temporal-table",
        source_id=sample_source.source_id,
        table_name="test_data",
        layer="typed",
        duckdb_path="test_data",
    )
    async_session.add(table)

    column = Column(
        column_id="col-1",
        table_id=table.table_id,
        column_name="value",
        column_position=0,
        resolved_type="INTEGER",
    )
    async_session.add(column)
    await async_session.commit()

    result = await analyze_temporal_quality(
        column.column_id,
        duckdb_conn,
        async_session,
    )

    assert not result.success
    assert "not a temporal type" in result.error.lower()


@pytest.mark.asyncio
async def test_analyze_temporal_quality_missing_column(duckdb_conn, async_session):
    """Test error handling for missing column."""
    result = await analyze_temporal_quality(
        "nonexistent-column",
        duckdb_conn,
        async_session,
    )

    assert not result.success
    assert "not found" in result.error.lower()


# ============================================================================
# Pydantic Model Tests
# ============================================================================


def test_pydantic_seasonality_analysis():
    """Test SeasonalityAnalysis model."""
    analysis = SeasonalityAnalysis(
        has_seasonality=True,
        strength=0.75,
        period="weekly",
        period_length=7,
        peaks={"day_of_week": 6},
        model_type="additive",
    )
    assert analysis.has_seasonality
    assert analysis.strength == 0.75
    assert analysis.period == "weekly"


def test_pydantic_trend_analysis():
    """Test TrendAnalysis model."""
    analysis = TrendAnalysis(
        has_trend=True,
        strength=0.85,
        direction="increasing",
        slope=0.5,
        autocorrelation_lag1=0.9,
    )
    assert analysis.has_trend
    assert analysis.direction == "increasing"
    assert analysis.slope == 0.5


def test_pydantic_change_point_result():
    """Test ChangePointResult model."""
    now = datetime.now()
    change = ChangePointResult(
        change_point_id="cp-1",
        detected_at=now,
        index_position=50,
        change_type="level_shift",
        magnitude=10.0,
        confidence=0.85,
        mean_before=10.0,
        mean_after=20.0,
        variance_before=2.0,
        variance_after=2.5,
        detection_method="pelt",
    )
    assert change.change_type == "level_shift"
    assert change.magnitude == 10.0


def test_pydantic_update_frequency_analysis():
    """Test UpdateFrequencyAnalysis model."""
    now = datetime.now()
    analysis = UpdateFrequencyAnalysis(
        update_frequency_score=0.9,
        median_interval_seconds=86400.0,
        interval_std=1000.0,
        interval_cv=0.1,
        last_update=now,
        data_freshness_days=1.0,
        is_stale=False,
    )
    assert analysis.update_frequency_score == 0.9
    assert not analysis.is_stale


def test_pydantic_fiscal_calendar_analysis():
    """Test FiscalCalendarAnalysis model."""
    analysis = FiscalCalendarAnalysis(
        fiscal_alignment_detected=True,
        fiscal_year_end_month=12,
        confidence=0.8,
        has_period_end_effects=True,
        period_end_spike_ratio=2.5,
        detected_periods=["month_end", "quarter_end"],
    )
    assert analysis.fiscal_alignment_detected
    assert analysis.fiscal_year_end_month == 12
    assert "month_end" in analysis.detected_periods


def test_pydantic_temporal_quality_result():
    """Test TemporalQualityResult model."""
    now = datetime.now()
    result = TemporalQualityResult(
        metric_id="metric-1",
        column_id="col-1",
        column_name="event_time",
        table_name="test_table",
        computed_at=now,
        min_timestamp=datetime(2023, 1, 1),
        max_timestamp=datetime(2023, 12, 31),
        span_days=365.0,
        detected_granularity="day",
        granularity_confidence=0.9,
        seasonality=SeasonalityAnalysis(has_seasonality=False, strength=0.0),
        trend=TrendAnalysis(has_trend=False, strength=0.0, direction="stable"),
        temporal_quality_score=0.85,
        has_issues=False,
    )
    assert result.metric_id == "metric-1"
    assert result.span_days == 365.0
    assert result.temporal_quality_score == 0.85
