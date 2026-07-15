"""Tests for ``analyze_last_period_complete`` (DAT-730 trailing-period coverage)."""

from __future__ import annotations

import duckdb
import pytest

from dataraum.analysis.temporal.detection import analyze_last_period_complete


def _table(rows_per_period: list[tuple[str, int]]) -> duckdb.DuckDBPyConnection:
    """An in-memory table ``ev(d DATE)`` with ``n`` rows in each given ``YYYY-MM`` month.

    ``rows_per_period`` is ordered as the periods should sort — the LAST entry is the
    trailing (max) period the flag judges.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE ev (d DATE)")
    for month, n in rows_per_period:
        # Day 01 of the month, repeated n times — all land in one monthly bucket.
        conn.execute(f"INSERT INTO ev SELECT DATE '{month}-01' FROM range({n})")
    return conn


def test_trailing_partial_period_flags_incomplete() -> None:
    """A last period well short of the prior periods' median is incomplete (the finance
    2026-02 snapshot: 27→4 accounts)."""
    conn = _table([("2025-01", 30), ("2025-02", 30), ("2025-03", 30), ("2025-04", 4)])
    assert analyze_last_period_complete(conn, "ev", "d", "month") is False


def test_full_last_period_is_complete() -> None:
    """A last period on par with the prior periods is complete."""
    conn = _table([("2025-01", 30), ("2025-02", 30), ("2025-03", 30), ("2025-04", 29)])
    assert analyze_last_period_complete(conn, "ev", "d", "month") is True


def test_single_period_is_undecidable() -> None:
    """One period gives no prior baseline → None, never a false 'incomplete'."""
    conn = _table([("2025-01", 30)])
    assert analyze_last_period_complete(conn, "ev", "d", "month") is None


def test_irregular_grain_is_undecidable() -> None:
    """A grain with no period boundary (irregular/unknown) is not decidable → None."""
    conn = _table([("2025-01", 30), ("2025-02", 4)])
    assert analyze_last_period_complete(conn, "ev", "d", "irregular") is None


@pytest.mark.parametrize("grain", ["day", "week", "month", "quarter", "year"])
def test_real_grains_are_accepted(grain: str) -> None:
    """Every real period unit is a valid date_trunc bucket (no SQL error)."""
    conn = _table([("2025-01", 30), ("2026-06", 30), ("2026-07", 30)])
    # Two-plus buckets at these grains → a decidable boolean, never a crash.
    assert analyze_last_period_complete(conn, "ev", "d", grain) in {True, False}
