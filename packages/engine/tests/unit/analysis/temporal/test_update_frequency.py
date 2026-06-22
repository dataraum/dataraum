"""Tests for ``analyze_update_frequency`` degenerate-input handling."""

from __future__ import annotations

import math
from datetime import datetime

import polars as pl

from dataraum.analysis.temporal.patterns import analyze_update_frequency

_CONFIG = {"staleness": {"stale_multiplier": 3}}


def test_single_interval_column_yields_zero_std_not_nan() -> None:
    """A 2-row date column has one interval → polars sample std is None (ddof=1).

    None/NaN can't be serialized into the JSON ``profile_data`` column (Postgres
    rejects the literal ``NaN``), and a lone interval is trivially regular, so
    the spread must read as 0.0.
    """
    ts = pl.Series("ts", [datetime(2024, 1, 1), datetime(2024, 1, 2)])

    result = analyze_update_frequency(ts, config=_CONFIG)

    assert result.success, result.error
    analysis = result.unwrap()
    assert analysis.interval_std == 0.0
    assert analysis.interval_cv is not None and not math.isnan(analysis.interval_cv)


def test_multiple_intervals_still_compute_a_real_std() -> None:
    """Sanity guard: the None coercion doesn't flatten genuine spread to 0."""
    ts = pl.Series(
        "ts",
        [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 5),
            datetime(2024, 1, 6),
        ],
    )

    result = analyze_update_frequency(ts, config=_CONFIG)

    assert result.success, result.error
    assert result.unwrap().interval_std > 0.0
