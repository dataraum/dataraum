"""Index-derived temporal pattern analysis.

Analyzes the timestamp index of a temporal column:

- Update frequency and staleness (interval statistics)
- Fiscal calendar alignment and period-end effects (month/day-of-month activity)

The value-series analyzers (seasonality via statsmodels, change points via ruptures,
trend/distribution-stability via scipy) were removed in DAT-524: they ran on a constant
``Series(1)`` and produced data-independent, foregone-conclusion output that nobody read.
Both heavy native deps (``statsmodels``, ``ruptures``) went with them.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

from dataraum.analysis.temporal.models import (
    FiscalCalendarAnalysis,
    UpdateFrequencyAnalysis,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result

logger = get_logger(__name__)


# =============================================================================
# Update Frequency Analysis
# =============================================================================


def analyze_update_frequency(
    time_series: pd.Series,
    *,
    config: dict[str, Any],
) -> Result[UpdateFrequencyAnalysis]:
    """Analyze update frequency and regularity.

    Args:
        time_series: Time series data
        config: Temporal config dict (from config/phases/temporal.yaml)

    Returns:
        Result containing UpdateFrequencyAnalysis
    """
    try:
        stale_mult = config["staleness"]["stale_multiplier"]
        if len(time_series) < 2:
            return Result.fail("Insufficient data for update frequency analysis")

        timestamps = time_series.index
        intervals_seconds = timestamps.to_series().diff().dt.total_seconds().dropna()  # type: ignore[arg-type]

        if len(intervals_seconds) == 0:
            return Result.fail("No intervals found")

        median_interval = float(intervals_seconds.median())
        # A single interval (a 2-row column) has no sample std — pandas returns
        # NaN (ddof=1). NaN can't be serialized into the JSON profile_data column
        # (Postgres rejects the literal `NaN`), and a lone interval is trivially
        # regular, so a zero spread is the correct reading.
        interval_std = float(intervals_seconds.std())
        if np.isnan(interval_std):
            interval_std = 0.0

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

        # Determine if data is stale (more than N x median interval)
        expected_interval_days = median_interval / 86400
        is_stale = freshness_days > (expected_interval_days * stale_mult)

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
    *,
    config: dict[str, Any],
) -> Result[FiscalCalendarAnalysis]:
    """Detect fiscal calendar alignment and period-end effects.

    Args:
        time_series: Time series data
        config: Temporal config dict (from config/phases/temporal.yaml)

    Returns:
        Result containing FiscalCalendarAnalysis
    """
    try:
        cfg = config["fiscal_calendar"]
        min_points = cfg["min_data_points"]
        activity_spike_mult = cfg["activity_spike_multiplier"]
        period_end_spike_mult = cfg["period_end_spike_multiplier"]
        expected_eom_ratio = cfg["expected_end_of_month_ratio"]

        if len(time_series) < min_points:
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
            if max_count > mean_count * activity_spike_mult:
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

        actual_ratio = end_of_month_count / total_count if total_count > 0 else 0

        has_period_end_effects = actual_ratio > expected_eom_ratio * period_end_spike_mult
        period_end_spike_ratio = (
            actual_ratio / expected_eom_ratio if expected_eom_ratio > 0 else 1.0
        )

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


__all__ = [
    "analyze_update_frequency",
    "detect_fiscal_calendar",
]
