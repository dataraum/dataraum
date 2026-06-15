"""Unit tests for the temporal slice analyzer — per-(slice, period) sums (DAT-491).

One ``GROUP BY`` over a slice table's time column yields, per populated period,
the row count and the SUM of every numeric column — the aggregation-lineage
reconciliation substrate. Real in-memory DuckDB, no mocks.
"""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from dataraum.analysis.temporal_slicing.analyzer import compute_period_sums
from dataraum.analysis.temporal_slicing.models import TimeGrain


def _conn(rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute('CREATE TABLE "slice_t" (ts DATE, debit DOUBLE, credit DOUBLE, label VARCHAR)')
    conn.executemany('INSERT INTO "slice_t" VALUES (?, ?, ?, ?)', rows)
    return conn


class TestComputePeriodSums:
    def test_monthly_buckets_and_sums(self):
        rows = [
            (date(2024, 1, 10), 100.0, 10.0, "a"),
            (date(2024, 1, 20), 200.0, 20.0, "b"),
            (date(2024, 2, 5), 50.0, 5.0, "c"),
        ]
        result = compute_period_sums("slice_t", "ts", TimeGrain.MONTHLY, _conn(rows))
        assert result.success
        periods = {p.period_label: p for p in result.value}
        assert set(periods) == {"2024-01", "2024-02"}
        jan = periods["2024-01"]
        assert jan.row_count == 2
        assert jan.column_sums["debit"] == pytest.approx(300.0)
        assert jan.column_sums["credit"] == pytest.approx(30.0)
        assert jan.period_start == date(2024, 1, 1)
        assert jan.period_end == date(2024, 2, 1)
        assert periods["2024-02"].column_sums["debit"] == pytest.approx(50.0)

    def test_only_populated_periods_appear(self):
        # Jan and Mar have data; Feb is empty and must not produce a row.
        rows = [(date(2024, 1, 10), 1.0, 0.0, "a"), (date(2024, 3, 10), 2.0, 0.0, "b")]
        result = compute_period_sums("slice_t", "ts", TimeGrain.MONTHLY, _conn(rows))
        assert {p.period_label for p in result.value} == {"2024-01", "2024-03"}

    def test_null_time_rows_excluded(self):
        rows = [(date(2024, 1, 10), 5.0, 0.0, "a"), (None, 99.0, 0.0, "b")]
        result = compute_period_sums("slice_t", "ts", TimeGrain.MONTHLY, _conn(rows))
        assert len(result.value) == 1
        assert result.value[0].row_count == 1
        assert result.value[0].column_sums["debit"] == pytest.approx(5.0)

    def test_daily_and_weekly_label_schemes(self):
        rows = [(date(2024, 1, 1), 1.0, 0.0, "a"), (date(2024, 1, 2), 1.0, 0.0, "b")]
        daily = compute_period_sums("slice_t", "ts", TimeGrain.DAILY, _conn(rows))
        assert {p.period_label for p in daily.value} == {"2024-01-01", "2024-01-02"}
        weekly = compute_period_sums("slice_t", "ts", TimeGrain.WEEKLY, _conn(rows))
        # 2024-01-01 is the Monday of ISO week 1 — both days fall in one bucket.
        assert [p.period_label for p in weekly.value] == ["2024-W01"]
        assert weekly.value[0].row_count == 2

    def test_no_numeric_columns_yields_empty_sums(self):
        conn = duckdb.connect(":memory:")
        conn.execute('CREATE TABLE "slice_t" (ts DATE, label VARCHAR)')
        conn.executemany('INSERT INTO "slice_t" VALUES (?, ?)', [(date(2024, 1, 1), "a")])
        result = compute_period_sums("slice_t", "ts", TimeGrain.MONTHLY, conn)
        assert result.success
        assert result.value[0].row_count == 1
        assert result.value[0].column_sums == {}

    def test_missing_table_fails_gracefully(self):
        result = compute_period_sums("nope", "ts", TimeGrain.MONTHLY, duckdb.connect(":memory:"))
        assert not result.success
