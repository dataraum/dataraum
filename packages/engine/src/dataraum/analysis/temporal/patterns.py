"""Index-derived temporal pattern analysis.

Analyzes the timestamp series of a temporal column:

- Update frequency and staleness (interval statistics)
- Fiscal calendar alignment and period-end effects (month/day-of-month activity)

The value-series analyzers (seasonality via statsmodels, change points via ruptures,
trend/distribution-stability via scipy) were removed in DAT-524: they ran on a constant
``Series(1)`` and produced data-independent, foregone-conclusion output that nobody read.
Both heavy native deps (``statsmodels``, ``ruptures``) went with them.

``time_series`` is a polars ``Datetime`` Series of the column's timestamps, **sorted
ascending** (the loader's ``ORDER BY``) — no pandas (DAT-580 migration).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import polars as pl

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
    time_series: pl.Series,
    *,
    config: dict[str, Any],
) -> Result[UpdateFrequencyAnalysis]:
    """Analyze update frequency and regularity.

    Args:
        time_series: Sorted-ascending polars Datetime series of the column's timestamps
        config: Temporal config dict (from config/phases/temporal.yaml)

    Returns:
        Result containing UpdateFrequencyAnalysis
    """
    try:
        stale_mult = config["staleness"]["stale_multiplier"]
        if len(time_series) < 2:
            return Result.fail("Insufficient data for update frequency analysis")

        # Consecutive interval seconds (the series is sorted): diff() drops the leading null.
        intervals_seconds = time_series.diff().drop_nulls().dt.total_seconds()
        if len(intervals_seconds) == 0:
            return Result.fail("No intervals found")

        median_interval = float(cast(float, intervals_seconds.median()))
        # A single interval (a 2-row column) has no sample std — polars returns None
        # (ddof=1). None/NaN can't be serialized into the JSON profile_data column
        # (Postgres rejects the literal `NaN`), and a lone interval is trivially regular,
        # so a zero spread is the correct reading.
        std = intervals_seconds.std()
        interval_std = float(cast(float, std)) if std is not None else 0.0

        # Coefficient of variation (lower = more regular)
        interval_cv = interval_std / median_interval if median_interval > 0 else 0.0

        # Regularity score (0-1, higher = more regular)
        regularity_score = max(0.0, 1.0 - min(interval_cv, 1.0))

        # Data freshness (the series is sorted, so the last element is the max timestamp)
        last_update = time_series[-1]
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
    time_series: pl.Series,
    *,
    config: dict[str, Any],
) -> Result[FiscalCalendarAnalysis]:
    """Detect fiscal calendar alignment and period-end effects.

    Args:
        time_series: Sorted-ascending polars Datetime series of the column's timestamps
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

        total_count = len(time_series)
        if total_count < min_points:
            return Result.ok(FiscalCalendarAnalysis(fiscal_alignment_detected=False))

        # Per-month activity counts (sorted by count desc → row 0 is the busiest month).
        month_counts = time_series.dt.month().alias("month").value_counts(sort=True)

        if month_counts.height > 0:
            max_month = int(month_counts["month"][0])
            max_count = int(cast(int, month_counts["count"].max()))
            mean_count = float(cast(float, month_counts["count"].mean()))
            # Fiscal year end typically has more activity
            if max_count > mean_count * activity_spike_mult:
                fiscal_year_end: int | None = max_month
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

        # Period-end effects: rows landing on days 28-31 (inclusive).
        end_of_month_count = int(time_series.dt.day().is_between(28, 31).sum())
        actual_ratio = end_of_month_count / total_count if total_count > 0 else 0.0

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
