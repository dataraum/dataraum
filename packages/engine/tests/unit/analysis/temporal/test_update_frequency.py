"""Tests for ``analyze_update_frequency`` degenerate-input handling."""

from __future__ import annotations

import math

import pandas as pd

from dataraum.analysis.temporal.patterns import analyze_update_frequency

_CONFIG = {"staleness": {"stale_multiplier": 3}}


def test_single_interval_column_yields_zero_std_not_nan() -> None:
    """A 2-row date column has one interval → pandas sample std is NaN.

    NaN can't be serialized into the JSON ``profile_data`` column (Postgres
    rejects the literal ``NaN``), and a lone interval is trivially regular, so
    the spread must read as 0.0 rather than NaN.
    """
    ts = pd.Series([1.0, 2.0], index=pd.to_datetime(["2024-01-01", "2024-01-02"]))

    result = analyze_update_frequency(ts, config=_CONFIG)

    assert result.success, result.error
    analysis = result.unwrap()
    assert analysis.interval_std == 0.0
    assert analysis.interval_cv is not None and not math.isnan(analysis.interval_cv)


def test_multiple_intervals_still_compute_a_real_std() -> None:
    """Sanity guard: the NaN coercion doesn't flatten genuine spread to 0."""
    ts = pd.Series(
        [1.0, 2.0, 3.0, 4.0],
        index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-05", "2024-01-06"]),
    )

    result = analyze_update_frequency(ts, config=_CONFIG)

    assert result.success, result.error
    assert result.unwrap().interval_std > 0.0
