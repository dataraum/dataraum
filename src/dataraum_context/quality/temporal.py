"""Temporal Quality Analysis (Pillar 4).

This module extracts enhanced quality metrics from temporal patterns including:
- Seasonality strength (quantified)
- Trend breaks (change points)
- Update frequency scoring
- Fiscal calendar alignment
- Distribution stability across time periods

Uses statsmodels for seasonal decomposition and ruptures for change point detection.
"""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import numpy as np
import pandas as pd
import ruptures as rpt
from scipy import stats
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from statsmodels.tsa.seasonal import seasonal_decompose

from dataraum_context.core.models.base import Result
from dataraum_context.quality.models import (
    ChangePointResult,
    DistributionShiftResult,
    DistributionStabilityAnalysis,
    FiscalCalendarAnalysis,
    SeasonalDecompositionResult,
    SeasonalityAnalysis,
    TemporalCompletenessAnalysis,
    TemporalGapInfo,
    TemporalQualityResult,
    TrendAnalysis,
    UpdateFrequencyAnalysis,
)
from dataraum_context.quality.models import (
    QualityIssue as TemporalQualityIssue,
)
from dataraum_context.storage.models_v2.core import Column, Table

# NOTE: ChangePoint and DistributionShift tables are DEPRECATED
# Data is now stored in TemporalQualityMetrics.temporal_data JSONB field

# ============================================================================
# Helper: Load Time Series Data
# ============================================================================


async def _load_time_series(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    limit: int = 10000,
) -> Result[pd.Series]:
    """Load time series data from DuckDB.

    Args:
        duckdb_conn: DuckDB connection
        table_name: DuckDB table name
        column_name: Column name
        limit: Maximum rows to load

    Returns:
        Result containing pandas Series indexed by datetime
    """
    try:
        query = f"""
        SELECT {column_name}::TIMESTAMP as ts
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        ORDER BY {column_name}
        LIMIT {limit}
        """

        df = duckdb_conn.execute(query).fetchdf()

        if df.empty:
            return Result.fail("No data found")

        # Create time series
        ts = pd.Series(
            1, index=pd.to_datetime(df["ts"])
        )  # Value doesn't matter for temporal analysis
        ts = pd.Series(
            1, index=pd.to_datetime(df["ts"])
        )  # Value doesn't matter for temporal analysis
        ts = ts.sort_index()

        return Result.ok(ts)

    except Exception as e:
        return Result.fail(f"Failed to load time series: {e}")


# ============================================================================
# Seasonality Analysis
# ============================================================================


async def analyze_seasonality(
    time_series: pd.Series,
    period: int | None = None,
) -> Result[SeasonalityAnalysis]:
    """Analyze seasonality strength using seasonal decomposition.

    Args:
        time_series: Time series data
        period: Seasonal period (auto-detected if None)

    Returns:
        Result containing SeasonalityAnalysis
    """
    try:
        if len(time_series) < 20:
            return Result.ok(
                SeasonalityAnalysis(
                    has_seasonality=False,
                    strength=0.0,
                )
            )

        # Auto-detect period if not provided
        if period is None:
            # Infer from time series frequency
            freq = pd.infer_freq(time_series)
            if freq:
                if "D" in freq:
                    period = 7  # Weekly seasonality for daily data
                elif "W" in freq:
                    period = 52  # Yearly seasonality for weekly data
                elif "M" in freq or "MS" in freq:
                    period = 12  # Yearly seasonality for monthly data
                elif "Q" in freq or "QS" in freq:
                    period = 4  # Yearly seasonality for quarterly data
                else:
                    period = min(len(time_series) // 4, 12)
            else:
                period = min(len(time_series) // 4, 12)

        # Need at least 2 full periods
        if len(time_series) < 2 * period:
            return Result.ok(
                SeasonalityAnalysis(
                    has_seasonality=False,
                    strength=0.0,
                    period_length=period,
                )
            )

        # Perform seasonal decomposition
        try:
            decomposition = seasonal_decompose(
                time_series,
                model="additive",
                period=period,
                extrapolate_trend="freq",  # type: ignore[arg-type]
            )
        except Exception:
            # Try multiplicative if additive fails
            try:
                decomposition = seasonal_decompose(
                    time_series,
                    model="multiplicative",
                    period=period,
                    extrapolate_trend="freq",  # type: ignore[arg-type]
                )
            except Exception:
                return Result.ok(
                    SeasonalityAnalysis(
                        has_seasonality=False,
                        strength=0.0,
                    )
                )

        # Calculate seasonality strength
        # Formula: 1 - Var(residual) / Var(detrended)
        detrended = time_series - decomposition.trend
        detrended = detrended.dropna()

        if len(detrended) == 0 or detrended.var() == 0:
            seasonality_strength = 0.0
        else:
            residual_var = decomposition.resid.dropna().var()
            detrended_var = detrended.var()
            seasonality_strength = max(0.0, 1.0 - (residual_var / detrended_var))

        # Determine if seasonality is significant
        has_seasonality = seasonality_strength > 0.3

        # Detect peaks in seasonal pattern
        seasonal_component = decomposition.seasonal.dropna()
        peaks = {}

        if has_seasonality and len(seasonal_component) > 0:
            # For time-based data, identify peak periods
            peak_idx = seasonal_component.idxmax()
            if isinstance(peak_idx, pd.Timestamp):
                peaks["month"] = peak_idx.month
                peaks["day_of_week"] = peak_idx.dayofweek

        period_name = _period_to_name(period)

        # Calculate trend strength
        # Formula: 1 - Var(residual) / Var(deseasonalized)
        deseasonalized = time_series - decomposition.seasonal
        deseasonalized = deseasonalized.dropna()

        if len(deseasonalized) == 0 or deseasonalized.var() == 0:
            trend_strength = 0.0
        else:
            residual_var = decomposition.resid.dropna().var()
            deseasonalized_var = deseasonalized.var()
            trend_strength = max(0.0, 1.0 - (residual_var / deseasonalized_var))

        # Build seasonal pattern summary
        seasonal_pattern_summary = None
        if has_seasonality and len(seasonal_component) > 0:
            peak_idx = seasonal_component.idxmax()
            trough_idx = seasonal_component.idxmin()
            seasonal_pattern_summary = {
                "peak_value": float(seasonal_component.max()),
                "trough_value": float(seasonal_component.min()),
                "amplitude": float(seasonal_component.max() - seasonal_component.min()),
            }
            if isinstance(peak_idx, pd.Timestamp):
                seasonal_pattern_summary["peak_month"] = int(peak_idx.month)
                seasonal_pattern_summary["peak_day_of_week"] = int(peak_idx.dayofweek)
            if isinstance(trough_idx, pd.Timestamp):
                seasonal_pattern_summary["trough_month"] = int(trough_idx.month)
                seasonal_pattern_summary["trough_day_of_week"] = int(trough_idx.dayofweek)

        # Create detailed decomposition result
        decomposition_result = SeasonalDecompositionResult(
            seasonal_component=decomposition.seasonal.fillna(0).tolist(),
            trend_component=decomposition.trend.fillna(0).tolist(),
            residual_component=decomposition.resid.fillna(0).tolist(),
            model_type="additive",
            seasonality_strength=float(seasonality_strength),
            trend_strength=float(trend_strength),
            seasonal_pattern_summary=seasonal_pattern_summary,
            period=period,
        )

        analysis = SeasonalityAnalysis(
            has_seasonality=has_seasonality,
            strength=float(seasonality_strength),
            period=period_name,
            period_length=period,
            peaks=peaks,
            model_type="additive",
            decomposition=decomposition_result,
        )

        return Result.ok(analysis)

    except Exception as e:
        return Result.fail(f"Seasonality analysis failed: {e}")


def _period_to_name(period: int) -> str:
    """Convert period length to name."""
    mapping = {
        7: "weekly",
        12: "monthly",
        4: "quarterly",
        52: "yearly",
        365: "daily",
    }
    return mapping.get(period, f"period_{period}")


# ============================================================================
# Trend Analysis
# ============================================================================


async def analyze_trend(
    time_series: pd.Series,
) -> Result[TrendAnalysis]:
    """Analyze trend strength and direction.

    Args:
        time_series: Time series data

    Returns:
        Result containing TrendAnalysis
    """
    try:
        if len(time_series) < 10:
            return Result.ok(
                TrendAnalysis(
                    has_trend=False,
                    strength=0.0,
                    direction="stable",
                )
            )

        # Calculate trend using linear regression
        x = np.arange(len(time_series))
        y = np.asarray(time_series.values, dtype=float)

        # Remove NaN values
        mask = ~np.isnan(y)
        x_clean = x[mask]
        y_clean = y[mask]

        if len(x_clean) < 10:
            return Result.ok(
                TrendAnalysis(
                    has_trend=False,
                    strength=0.0,
                    direction="stable",
                )
            )

        # Linear regression
        result = stats.linregress(x_clean, y_clean)
        slope = result.slope
        rvalue = result.rvalue
        stderr = result.stderr

        # Trend strength is R²
        trend_strength = rvalue**2

        # Determine direction
        if abs(slope) < stderr * 2:  # Not significant
            direction = "stable"
            has_trend = False
        elif slope > 0:
            direction = "increasing"
            has_trend = bool(trend_strength > 0.3)
        else:
            direction = "decreasing"
            has_trend = bool(trend_strength > 0.3)

        # Calculate autocorrelation at lag 1
        if len(time_series) > 2:
            autocorr = time_series.autocorr(lag=1)
            if np.isnan(autocorr):
                autocorr = 0.0
        else:
            autocorr = 0.0

        analysis = TrendAnalysis(
            has_trend=has_trend,
            strength=float(trend_strength),
            direction=direction,
            slope=float(slope),
            autocorrelation_lag1=float(autocorr),
        )

        return Result.ok(analysis)

    except Exception as e:
        return Result.fail(f"Trend analysis failed: {e}")


# ============================================================================
# Change Point Detection
# ============================================================================


async def detect_change_points(
    time_series: pd.Series,
    metric_id: str,
    min_size: int = 10,
    jump: int = 5,
) -> Result[list[ChangePointResult]]:
    """Detect change points using PELT algorithm.

    Args:
        time_series: Time series data
        metric_id: Parent metric ID
        min_size: Minimum segment size
        jump: Jump parameter for efficiency

    Returns:
        Result containing list of ChangePointResult
    """
    try:
        if len(time_series) < 30:
            return Result.ok([])

        # Prepare data
        signal = np.asarray(time_series.values, dtype=float)
        mask = ~np.isnan(signal)
        signal_clean = signal[mask]

        if len(signal_clean) < 30:
            return Result.ok([])

        # Detect change points using PELT
        try:
            algo = rpt.Pelt(model="rbf", min_size=min_size, jump=jump).fit(signal_clean)
            change_points_idx = algo.predict(pen=3)
        except Exception:
            # Fallback to simpler method
            try:
                algo = rpt.Pelt(model="l2", min_size=min_size).fit(signal_clean)
                change_points_idx = algo.predict(pen=5)
            except Exception:
                return Result.ok([])

        # Convert to results
        changes = []
        timestamps: pd.Index[pd.Timestamp] = time_series.index[mask]

        prev_idx = 0
        for cp_idx in change_points_idx[:-1]:  # Last point is end of series
            if cp_idx >= len(timestamps):
                continue

            # Get change point timestamp (convert to datetime)
            cp_timestamp = timestamps[cp_idx].to_pydatetime()

            # Calculate before/after statistics
            before_segment = signal_clean[prev_idx:cp_idx]
            after_segment = signal_clean[
                cp_idx : min(cp_idx + len(before_segment), len(signal_clean))
            ]

            if len(before_segment) < 2 or len(after_segment) < 2:
                prev_idx = cp_idx
                continue

            mean_before = float(np.mean(before_segment))
            mean_after = float(np.mean(after_segment))
            var_before = float(np.var(before_segment))
            var_after = float(np.var(after_segment))

            # Determine change type and magnitude
            mean_change = abs(mean_after - mean_before)
            var_change = abs(var_after - var_before)

            if mean_change > var_change:
                change_type = "level_shift"
                magnitude = mean_change
            else:
                change_type = "variance_change"
                magnitude = var_change

            # Simple confidence based on segment sizes
            confidence = min(0.9, (len(before_segment) + len(after_segment)) / 100)

            change = ChangePointResult(
                change_point_id=str(uuid4()),
                detected_at=cp_timestamp,
                index_position=int(cp_idx),
                change_type=change_type,
                magnitude=magnitude,
                confidence=confidence,
                mean_before=mean_before,
                mean_after=mean_after,
                variance_before=var_before,
                variance_after=var_after,
                detection_method="pelt",
            )

            changes.append(change)
            prev_idx = cp_idx

        return Result.ok(changes)

    except Exception as e:
        return Result.fail(f"Change point detection failed: {e}")


# ============================================================================
# Update Frequency Analysis
# ============================================================================


async def analyze_update_frequency(
    time_series: pd.Series,
) -> Result[UpdateFrequencyAnalysis]:
    """Analyze update frequency and regularity.

    Args:
        time_series: Time series data

    Returns:
        Result containing UpdateFrequencyAnalysis
    """
    try:
        if len(time_series) < 2:
            return Result.fail("Insufficient data for update frequency analysis")

        # Calculate intervals between updates
        timestamps = time_series.index
        intervals_seconds = timestamps.to_series().diff().dt.total_seconds().dropna()

        if len(intervals_seconds) == 0:
            return Result.fail("No intervals found")

        median_interval = float(intervals_seconds.median())
        interval_std = float(intervals_seconds.std())

        # Coefficient of variation (lower = more regular)
        if median_interval > 0:
            interval_cv = interval_std / median_interval
        else:
            interval_cv = 0.0

        # Regularity score (0-1, higher = more regular)
        # Based on coefficient of variation
        regularity_score = max(0.0, 1.0 - min(interval_cv, 1.0))

        # Data freshness
        last_update = timestamps[-1].to_pydatetime()
        # Make timezone-aware if needed
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=UTC)
        now = datetime.now(UTC)
        freshness_days = (now - last_update).total_seconds() / 86400

        # Determine if data is stale (more than 2x median interval)
        expected_interval_days = median_interval / 86400
        is_stale = freshness_days > (expected_interval_days * 2)

        analysis = UpdateFrequencyAnalysis(
            update_frequency_score=regularity_score,
            median_interval_seconds=median_interval,
            interval_std=interval_std,
            interval_cv=interval_cv,
            last_update=last_update,
            data_freshness_days=freshness_days,
            is_stale=is_stale,
        )

        return Result.ok(analysis)

    except Exception as e:
        return Result.fail(f"Update frequency analysis failed: {e}")


# ============================================================================
# Fiscal Calendar Detection
# ============================================================================


async def detect_fiscal_calendar(
    time_series: pd.Series,
) -> Result[FiscalCalendarAnalysis]:
    """Detect fiscal calendar alignment and period-end effects.

    Args:
        time_series: Time series data

    Returns:
        Result containing FiscalCalendarAnalysis
    """
    try:
        if len(time_series) < 90:  # Need at least ~3 months
            return Result.ok(
                FiscalCalendarAnalysis(
                    fiscal_alignment_detected=False,
                )
            )

        # Count events by month to detect fiscal year end
        timestamps = time_series.index
        month_counts = pd.Series([ts.month for ts in timestamps]).value_counts()

        # Check for anomalous month (potential fiscal year end)
        if len(month_counts) > 0:
            max_month = month_counts.idxmax()
            max_count = month_counts.max()
            mean_count = month_counts.mean()

            # Fiscal year end typically has more activity
            if max_count > mean_count * 1.5:
                fiscal_year_end = int(max_month)
                fiscal_detected = True
                confidence = min(0.9, (max_count / mean_count - 1.0) / 2.0)
            else:
                fiscal_year_end = None
                fiscal_detected = False
                confidence = 0.0
        else:
            fiscal_year_end = None
            fiscal_detected = False
            confidence = 0.0

        # Detect period-end effects (spikes at month/quarter end)
        day_of_month_counts = pd.Series([ts.day for ts in timestamps]).value_counts()

        # Days 28-31 are end of month
        end_of_month_count = sum(day_of_month_counts.get(d, 0) for d in range(28, 32))
        total_count = len(timestamps)

        # Expected: ~13% of days are end-of-month (4 days / 30 days)
        expected_ratio = 0.13
        actual_ratio = end_of_month_count / total_count if total_count > 0 else 0

        has_period_end_effects = actual_ratio > expected_ratio * 1.5
        period_end_spike_ratio = actual_ratio / expected_ratio if expected_ratio > 0 else 1.0

        detected_periods = []
        if has_period_end_effects:
            detected_periods.append("month_end")

        analysis = FiscalCalendarAnalysis(
            fiscal_alignment_detected=fiscal_detected,
            fiscal_year_end_month=fiscal_year_end,
            confidence=confidence,
            has_period_end_effects=has_period_end_effects,
            period_end_spike_ratio=float(period_end_spike_ratio),
            detected_periods=detected_periods,
        )

        return Result.ok(analysis)

    except Exception as e:
        return Result.fail(f"Fiscal calendar detection failed: {e}")


# ============================================================================
# Distribution Stability Analysis
# ============================================================================


async def analyze_distribution_stability(
    time_series: pd.Series,
    metric_id: str,
    num_periods: int = 4,
) -> Result[DistributionStabilityAnalysis]:
    """Analyze distribution stability across time periods using KS tests.

    Args:
        time_series: Time series data
        metric_id: Parent metric ID
        num_periods: Number of periods to compare

    Returns:
        Result containing DistributionStabilityAnalysis
    """
    try:
        if len(time_series) < num_periods * 10:
            return Result.ok(
                DistributionStabilityAnalysis(
                    stability_score=1.0,
                    shift_count=0,
                )
            )

        # Split into periods
        period_size = len(time_series) // num_periods
        periods = []

        for i in range(num_periods):
            start_idx = i * period_size
            end_idx = start_idx + period_size if i < num_periods - 1 else len(time_series)
            period_data = time_series.iloc[start_idx:end_idx]
            periods.append(period_data)

        # Compare adjacent periods with KS test
        shifts = []
        ks_statistics = []

        for i in range(len(periods) - 1):
            period1 = periods[i]
            period2 = periods[i + 1]

            # Kolmogorov-Smirnov test
            # Convert to numpy arrays for scipy compatibility
            values1 = np.asarray(period1.values, dtype=np.float64)
            values2 = np.asarray(period2.values, dtype=np.float64)
            ks_stat, p_value = stats.ks_2samp(values1, values2)

            is_significant = bool(p_value < 0.05)

            if is_significant:
                # Determine shift direction
                mean1 = period1.mean()
                mean2 = period2.mean()

                if mean2 > mean1 * 1.1:
                    direction = "increase"
                elif mean2 < mean1 * 0.9:
                    direction = "decrease"
                else:
                    direction = "mixed"

                magnitude = abs(mean2 - mean1) / mean1 if mean1 != 0 else 0.0

                shift = DistributionShiftResult(
                    shift_id=str(uuid4()),
                    period1_start=period1.index[0].to_pydatetime(),
                    period1_end=period1.index[-1].to_pydatetime(),
                    period2_start=period2.index[0].to_pydatetime(),
                    period2_end=period2.index[-1].to_pydatetime(),
                    test_statistic=float(ks_stat),
                    p_value=float(p_value),
                    is_significant=is_significant,
                    period1_mean=float(mean1),
                    period2_mean=float(mean2),
                    period1_std=float(period1.std()),
                    period2_std=float(period2.std()),
                    shift_direction=direction,
                    shift_magnitude=float(magnitude),
                )

                shifts.append(shift)

            ks_statistics.append(ks_stat)

        # Calculate stability score
        if ks_statistics:
            mean_ks = float(np.mean(ks_statistics))
            max_ks = float(np.max(ks_statistics))
            # Lower KS statistic = more stable
            stability_score = max(0.0, 1.0 - mean_ks)
        else:
            mean_ks = None
            max_ks = None
            stability_score = 1.0

        analysis = DistributionStabilityAnalysis(
            stability_score=stability_score,
            shift_count=len(shifts),
            shifts=shifts,
            mean_ks_statistic=mean_ks,
            max_ks_statistic=max_ks,
        )

        return Result.ok(analysis)

    except Exception as e:
        return Result.fail(f"Distribution stability analysis failed: {e}")


# ============================================================================
# Temporal Completeness Analysis
# ============================================================================


async def analyze_completeness(
    time_series: pd.Series,
    granularity: str,
) -> Result[TemporalCompletenessAnalysis]:
    """Analyze temporal completeness and gaps.

    Args:
        time_series: Time series data
        granularity: Detected granularity ('daily', 'weekly', etc.)

    Returns:
        Result containing TemporalCompletenessAnalysis
    """
    try:
        if len(time_series) < 2:
            return Result.fail("Insufficient data")

        # Calculate expected periods
        min_ts = time_series.index.min()
        max_ts = time_series.index.max()
        span = (max_ts - min_ts).total_seconds()

        granularity_seconds = {
            "second": 1,
            "minute": 60,
            "hour": 3600,
            "day": 86400,
            "weekly": 604800,
            "monthly": 2592000,
            "quarterly": 7776000,
            "yearly": 31536000,
        }

        seconds_per_period = granularity_seconds.get(granularity, 86400)
        expected_periods = int(span / seconds_per_period) + 1
        actual_periods = len(time_series)

        completeness_ratio = (
            min(1.0, actual_periods / expected_periods) if expected_periods > 0 else 1.0
        )

        # Detect gaps
        # Convert index to series and compute differences (timedeltas)
        intervals: pd.Series = time_series.index.to_series().diff()
        median_interval = intervals.median()

        # Gaps are intervals > 2x median
        gap_threshold = median_interval * 2
        large_gaps = intervals[intervals > gap_threshold].dropna()

        gaps = []
        for gap_end, gap_length in zip(large_gaps.index, large_gaps.values, strict=False):
            gap_start = gap_end - gap_length
            gap_seconds = pd.Timedelta(gap_length).total_seconds()
            gap_days = gap_seconds / 86400
            missing_periods = int(gap_seconds / seconds_per_period) - 1

            severity = "minor"
            if gap_days > 30:
                severity = "severe"
            elif gap_days > 7:
                severity = "moderate"

            gap_info = TemporalGapInfo(
                gap_start=gap_start.to_pydatetime(),
                gap_end=gap_end.to_pydatetime(),
                gap_length_days=gap_days,
                missing_periods=missing_periods,
                severity=severity,
            )
            gaps.append(gap_info)

        # Sort by length
        gaps.sort(key=lambda g: g.gap_length_days, reverse=True)

        largest_gap_days = gaps[0].gap_length_days if gaps else 0.0

        analysis = TemporalCompletenessAnalysis(
            completeness_ratio=completeness_ratio,
            expected_periods=expected_periods,
            actual_periods=actual_periods,
            gap_count=len(gaps),
            largest_gap_days=largest_gap_days,
            gaps=gaps[:10],  # Top 10 largest gaps
        )

        return Result.ok(analysis)

    except Exception as e:
        return Result.fail(f"Completeness analysis failed: {e}")


# ============================================================================
# Main Analysis Function
# ============================================================================


async def analyze_temporal_quality(
    column_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[TemporalQualityResult]:
    """Analyze temporal quality for a time column.

    This is the main entry point for temporal quality analysis.

    Args:
        column_id: Column to analyze
        duckdb_conn: DuckDB connection
        session: Database session

    Returns:
        Result containing complete temporal quality assessment
    """
    try:
        # Get column info
        stmt = select(Column, Table).join(Table).where(Column.column_id == column_id)
        result = await session.execute(stmt)
        row = result.one_or_none()

        if not row:
            return Result.fail(f"Column {column_id} not found")

        column, table = row

        # Verify it's a temporal column
        if column.resolved_type not in ["DATE", "TIMESTAMP", "TIMESTAMPTZ"]:
            return Result.fail(f"Column {column.column_name} is not a temporal type")

        # Load time series
        ts_result = await _load_time_series(
            duckdb_conn,
            table.duckdb_path,
            column.column_name,
        )

        if not ts_result.success:
            return Result.fail(ts_result.error if ts_result.error else "Unknown Error")

        time_series = ts_result.unwrap()

        # Basic temporal info
        min_timestamp = time_series.index.min().to_pydatetime()
        max_timestamp = time_series.index.max().to_pydatetime()
        span_days = (max_timestamp - min_timestamp).total_seconds() / 86400

        # Infer granularity (simple approach)
        interval: pd.Series = time_series.index.to_series().diff()
        median_interval = interval.median()

        # Handle case where median_interval might be a float (NaT case)
        if isinstance(median_interval, (int, float)):
            # If it's already a number, use it directly
            interval_seconds = float(median_interval)
        else:
            # Otherwise assume it's a Timedelta
            interval_seconds = median_interval.total_seconds()

        granularity, confidence = _infer_granularity(interval_seconds)

        # Analyze seasonality
        seasonality_result = await analyze_seasonality(time_series)
        seasonality = seasonality_result.value if seasonality_result.success else None

        # Analyze trend
        trend_result = await analyze_trend(time_series)
        trend = trend_result.value if trend_result.success else None

        # Detect change points
        metric_id = str(uuid4())
        changes_result = await detect_change_points(time_series, metric_id)
        change_points = changes_result.unwrap() if changes_result.success else []

        # Analyze update frequency
        frequency_result = await analyze_update_frequency(time_series)
        update_frequency = frequency_result.value if frequency_result.success else None

        # Detect fiscal calendar
        fiscal_result = await detect_fiscal_calendar(time_series)
        fiscal_calendar = fiscal_result.value if fiscal_result.success else None

        # Analyze distribution stability
        stability_result = await analyze_distribution_stability(time_series, metric_id)
        distribution_stability = stability_result.value if stability_result.success else None

        # Analyze completeness
        completeness_result = await analyze_completeness(time_series, granularity)
        completeness = completeness_result.value if completeness_result.success else None

        # Detect quality issues
        issues = []

        if completeness and completeness.completeness_ratio < 0.8:
            issues.append(
                TemporalQualityIssue(
                    issue_type="low_completeness",
                    severity="high" if completeness.completeness_ratio < 0.5 else "medium",
                    description=(
                        f"Only {completeness.completeness_ratio:.1%} of expected "
                        "data points present"
                    ),
                    evidence={"completeness_ratio": completeness.completeness_ratio},
                    detected_at=datetime.now(UTC),
                )
            )

        if completeness and completeness.largest_gap_days and completeness.largest_gap_days > 30:
            issues.append(
                TemporalQualityIssue(
                    issue_type="large_gap",
                    severity="high" if completeness.largest_gap_days > 90 else "medium",
                    description=f"Large gap of {completeness.largest_gap_days:.0f} days detected",
                    evidence={"gap_days": completeness.largest_gap_days},
                    detected_at=datetime.now(UTC),
                )
            )

        if update_frequency and update_frequency.is_stale:
            issues.append(
                TemporalQualityIssue(
                    issue_type="stale_data",
                    severity="medium",
                    description=f"Data is {update_frequency.data_freshness_days:.0f} days old",
                    evidence={"freshness_days": update_frequency.data_freshness_days},
                    detected_at=datetime.now(UTC),
                )
            )

        if len(change_points) > 5:
            issues.append(
                TemporalQualityIssue(
                    issue_type="many_change_points",
                    severity="medium",
                    description=f"{len(change_points)} change points detected (unstable pattern)",
                    evidence={"change_point_count": len(change_points)},
                    detected_at=datetime.now(UTC),
                )
            )

        if distribution_stability and distribution_stability.stability_score < 0.7:
            issues.append(
                TemporalQualityIssue(
                    issue_type="unstable_distribution",
                    severity="medium",
                    description=(
                        f"Distribution stability score: "
                        f"{distribution_stability.stability_score:.2f}"
                    ),
                    evidence={"stability_score": distribution_stability.stability_score},
                    detected_at=datetime.now(UTC),
                )
            )

        computed_at = datetime.now(UTC)

        # Build result
        from dataraum_context.core.models.base import ColumnRef

        column_ref = ColumnRef(
            source_id=table.source_id,
            table_name=table.table_name,
            column_name=column.column_name,
        )

        result_obj = TemporalQualityResult(
            metric_id=metric_id,
            column_id=column_id,
            column_ref=column_ref,
            column_name=column.column_name,
            table_name=table.table_name,
            computed_at=computed_at,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
            span_days=span_days,
            detected_granularity=granularity,
            granularity_confidence=confidence,
            seasonality=seasonality,
            trend=trend,
            change_points=change_points,
            update_frequency=update_frequency,
            fiscal_calendar=fiscal_calendar,
            distribution_stability=distribution_stability,
            completeness=completeness,
            quality_issues=issues,
            has_issues=len(issues) > 0,
        )

        # NOTE: Data storage happens in enrichment/temporal.py
        # This quality module only performs analysis and returns results
        # The enrichment module handles persistence using the hybrid storage approach

        return Result.ok(result_obj)

    except Exception as e:
        return Result.fail(f"Temporal quality analysis failed: {e}")


def _infer_granularity(median_gap_seconds: float | None) -> tuple[str, float]:
    """Infer time granularity from median gap."""
    if median_gap_seconds is None:
        return ("unknown", 0.0)

    granularities = [
        ("second", 1, 0.5),
        ("minute", 60, 5),
        ("hour", 3600, 300),
        ("day", 86400, 3600),
        ("weekly", 604800, 86400),
        ("monthly", 2592000, 259200),
        ("quarterly", 7776000, 777600),
        ("yearly", 31536000, 3153600),
    ]

    best_match = None
    best_distance = float("inf")

    for name, expected_seconds, tolerance in granularities:
        distance = abs(median_gap_seconds - expected_seconds)
        if distance < tolerance and distance < best_distance:
            best_match = name
            best_distance = distance

    if best_match:
        confidence = 0.9
        return (best_match, confidence)

    return ("irregular", 0.3)
