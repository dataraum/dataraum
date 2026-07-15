"""Tests for ``analyze_basic_temporal`` — the surviving temporal substrate.

DAT-783 consolidated every served temporal fact onto this single DISTINCT-timestamp
pass (span, granularity + confidence, completeness/gaps, staleness) and deleted the
duplicate-corrupted row-interval path plus the WRONG fiscal/update-frequency analyzers.
These tests lock in the behaviour the cockpit + P5 coverage now read.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb

from dataraum.analysis.temporal.detection import analyze_basic_temporal

# Real thresholds mirrored from config/phases/temporal.yaml (self-contained — no
# config bind-mount needed at unit-test time).
_CONFIG = {
    "granularity": {
        "definitions": [
            ["second", 1, 0.5],
            ["minute", 60, 5],
            ["hour", 3600, 300],
            ["day", 86400, 3600],
            ["week", 604800, 86400],
            ["month", 2592000, 259200],
            ["quarter", 7776000, 777600],
            ["year", 31536000, 3153600],
        ],
        "default_confidence": 0.7,
        "irregular_confidence": 0.3,
        "variation_divisor": 10,
    },
    "gaps": {
        "significant_gap_multiplier": 2.0,
        "severity_severe_multiplier": 10.0,
        "severity_moderate_multiplier": 5.0,
    },
    "staleness": {"stale_multiplier": 2.0},
}


def _table(conn: duckdb.DuckDBPyConnection, dates: list[datetime]) -> None:
    conn.execute("CREATE OR REPLACE TABLE t (ts TIMESTAMP)")
    conn.executemany("INSERT INTO t VALUES (?)", [(d,) for d in dates])


def test_daily_series_is_day_granularity_and_complete() -> None:
    conn = duckdb.connect()
    base = datetime(2024, 1, 1)
    _table(conn, [base + timedelta(days=i) for i in range(30)])

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()

    assert v["granularity"] == "day"
    assert v["granularity_confidence"] > 0.5
    assert v["span_days"] == 29
    assert v["completeness"].completeness_ratio >= 0.95
    assert v["completeness"].gap_count == 0


def test_planted_gap_is_detected_severe() -> None:
    conn = duckdb.connect()
    base = datetime(2024, 1, 1)
    dates = [base + timedelta(days=i) for i in range(30)]
    dates += [base + timedelta(days=50 + i) for i in range(30)]  # 20-day hole
    _table(conn, dates)

    comp = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()["completeness"]

    assert comp.gap_count == 1
    assert comp.largest_gap_days is not None and comp.largest_gap_days >= 19
    assert comp.gaps[0].severity == "severe"


def test_duplicate_heavy_fact_column_still_reads_daily() -> None:
    """A multi-row-per-day fact column stays 'day', not 'irregular'.

    Regression guard for the deleted row-interval path: with more rows than distinct
    days, the median ROW interval was 0 → granularity collapsed to 'irregular'. The
    DISTINCT pass is robust — this is why fiscal/update-frequency were deleted, not wired.
    """
    conn = duckdb.connect()
    base = datetime(2024, 1, 1)
    dates: list[datetime] = []
    for i in range(60):
        day = base + timedelta(days=i)
        dates += [day, day, day]  # three rows per day
    _table(conn, dates)

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()

    assert v["granularity"] == "day"
    assert v["completeness"].actual_periods == 60  # distinct days, not row count


def test_old_data_is_stale_recent_data_is_not() -> None:
    conn = duckdb.connect()

    old = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(30)]
    _table(conn, old)
    assert analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()["is_stale"] is True

    today = datetime.now(UTC).replace(tzinfo=None)
    recent = [today - timedelta(days=i) for i in range(30)]
    _table(conn, recent)
    assert analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()["is_stale"] is False
