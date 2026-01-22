"""Advanced temporal pattern analysis.

Analyzes:
- Seasonality (using seasonal decomposition)
- Trends (linear regression)
- Change points (PELT algorithm)
- Update frequency and staleness
- Fiscal calendar alignment
- Distribution stability (KS tests)

Uses statsmodels for seasonal decomposition and ruptures for change point detection.
"""

from datetime import UTC, datetime
from uuid import uuid4

import numpy as np
import pandas as pd
import ruptures as rpt
from scipy import stats
from statsmodels.tsa.seasonal import seasonal_decompose

from dataraum.analysis.temporal.models import (
    ChangePointResult,
    DistributionShiftResult,
    DistributionStabilityAnalysis,
    FiscalCalendarAnalysis,
    SeasonalDecompositionResult,
    SeasonalityAnalysis,
    TrendAnalysis,
    UpdateFrequencyAnalysis,
)
from dataraum.core.models.base import Result

# =============================================================================
# Seasonality Analysis
# =============================================================================


def analyze_seasonality(
    time_series: pd.Series,
    period: int | None = None,
) -> Result[SeasonalityAnalysis]:
    """Analyze seasonality strength using seasonal decomposition.

    Args:
        time_series: Time series data (pandas Series with datetime index)
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

        # Calculate seasonality strength: 1 - Var(residual) / Var(detrended)
        detrended = time_series - decomposition.trend
        detrended = detrended.dropna()

        if len(detrended) == 0 or detrended.var() == 0:
            seasonality_strength = 0.0
        else:
            residual_var = decomposition.resid.dropna().var()
            detrended_var = detrended.var()
            seasonality_strength = max(0.0, 1.0 - (residual_var / detrended_var))

        has_seasonality = seasonality_strength > 0.3

        # Detect peaks in seasonal pattern
        seasonal_component = decomposition.seasonal.dropna()
        peaks = {}

        if has_seasonality and len(seasonal_component) > 0:
            peak_idx = seasonal_component.idxmax()
            if isinstance(peak_idx, pd.Timestamp):
                peaks["month"] = peak_idx.month
                peaks["day_of_week"] = peak_idx.dayofweek

        period_name = _period_to_name(period)

        # Calculate trend strength: 1 - Var(residual) / Var(deseasonalized)
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


# =============================================================================
# Trend Analysis
# =============================================================================


def analyze_trend(
    time_series: pd.Series,
) -> Result[TrendAnalysis]:
    """Analyze trend strength and direction using linear regression.

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


# =============================================================================
# Change Point Detection
# =============================================================================


def detect_change_points(
    time_series: pd.Series,
    min_size: int = 10,
    jump: int = 5,
    max_points: int = 1000,
) -> Result[list[ChangePointResult]]:
    """Detect change points using PELT algorithm.

    Args:
        time_series: Time series data
        min_size: Minimum segment size
        jump: Jump parameter for efficiency
        max_points: Maximum points to analyze (sample if larger)

    Returns:
        Result containing list of ChangePointResult

    TODO: Revisit change point detection sampling:
        - Currently uses uniform stride sampling for large series
        - This works for value distribution changes but may miss
          localized change points in the skipped regions
        - Consider adaptive sampling or running on time-windowed segments
        - PELT with L2 model is O(n) but RBF would be O(n²)
    """
    try:
        if len(time_series) < 30:
            return Result.ok([])

        signal = np.asarray(time_series.values, dtype=float)
        mask = ~np.isnan(signal)
        signal_clean = signal[mask]

        if len(signal_clean) < 30:
            return Result.ok([])

        # Sample if too large (PELT with rbf is O(n²))
        if len(signal_clean) > max_points:
            step = len(signal_clean) // max_points
            signal_clean = signal_clean[::step]
            mask = mask[::step]

        # Detect change points using PELT with l2 model (faster than rbf)
        try:
            algo = rpt.Pelt(model="l2", min_size=min_size, jump=jump).fit(signal_clean)
            change_points_idx = algo.predict(pen=5)
        except Exception:
            return Result.ok([])

        changes = []
        timestamps: pd.Index[pd.Timestamp] = time_series.index[mask]

        prev_idx = 0
        for cp_idx in change_points_idx[:-1]:  # Last point is end of series
            if cp_idx >= len(timestamps):
                continue

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

            mean_change = abs(mean_after - mean_before)
            var_change = abs(var_after - var_before)

            if mean_change > var_change:
                change_type = "level_shift"
                magnitude = mean_change
            else:
                change_type = "variance_change"
                magnitude = var_change

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


# =============================================================================
# Update Frequency Analysis
# =============================================================================


def analyze_update_frequency(
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

        timestamps = time_series.index
        intervals_seconds = timestamps.to_series().diff().dt.total_seconds().dropna()  # type: ignore[arg-type]

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
        regularity_score = max(0.0, 1.0 - min(interval_cv, 1.0))

        # Data freshness
        last_update = timestamps[-1].to_pydatetime()
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


# =============================================================================
# Fiscal Calendar Detection
# =============================================================================


def detect_fiscal_calendar(
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


# =============================================================================
# Distribution Stability Analysis
# =============================================================================


def analyze_distribution_stability(
    time_series: pd.Series,
    num_periods: int = 4,
) -> Result[DistributionStabilityAnalysis]:
    """Analyze distribution stability across time periods using KS tests.

    Args:
        time_series: Time series data
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

            values1 = np.asarray(period1.values, dtype=np.float64)
            values2 = np.asarray(period2.values, dtype=np.float64)
            ks_stat, p_value = stats.ks_2samp(values1, values2)

            is_significant = bool(p_value < 0.05)

            if is_significant:
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


__all__ = [
    "analyze_seasonality",
    "analyze_trend",
    "detect_change_points",
    "analyze_update_frequency",
    "detect_fiscal_calendar",
    "analyze_distribution_stability",
]
