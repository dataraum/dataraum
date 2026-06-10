"""The shared formula-match row statistic (discovery + derived_value grading).

Pure DuckDB. ``formula_match_counts`` is the ONE source of truth for "does
``target = col1 op col2`` hold on this row": the discovery sweep and the
derived_value hypothesis grading must count identically (same tolerance, same
NULL and zero-target exclusions) or the two witnesses would disagree by
artifact rather than by data.
"""

from __future__ import annotations

import duckdb

from dataraum.analysis.correlation.within_table.derived_columns import formula_match_counts


def _table(rows: list[tuple[object, object, object]]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("CREATE TABLE t (total DOUBLE, a DOUBLE, b DOUBLE)")
    if rows:
        conn.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
    return conn


def test_counts_matches_and_total() -> None:
    conn = _table([(30.0, 20.0, 10.0), (5.0, 2.0, 3.0), (99.0, 1.0, 1.0)])
    assert formula_match_counts(conn, "t", "total", "a", "b", "+") == (2, 3)


def test_null_operands_are_excluded() -> None:
    conn = _table([(30.0, 20.0, 10.0), (30.0, None, 10.0), (None, 20.0, 10.0)])
    assert formula_match_counts(conn, "t", "total", "a", "b", "+") == (1, 1)


def test_zero_targets_are_excluded() -> None:
    # A zero target matches ANY near-zero formula within the absolute tolerance
    # — it carries no discriminative power and must not inflate the rate.
    conn = _table([(0.0, 1.0, 2.0), (30.0, 20.0, 10.0)])
    assert formula_match_counts(conn, "t", "total", "a", "b", "+") == (1, 1)


def test_empty_relation_is_zero_total() -> None:
    conn = _table([])
    assert formula_match_counts(conn, "t", "total", "a", "b", "+") == (0, 0)


def test_all_four_operations_grade() -> None:
    conn = _table([(6.0, 2.0, 3.0)])
    assert formula_match_counts(conn, "t", "total", "a", "b", "*") == (1, 1)
    assert formula_match_counts(conn, "t", "total", "a", "b", "+") == (0, 1)
    conn2 = _table([(2.0, 6.0, 3.0)])
    assert formula_match_counts(conn2, "t", "total", "a", "b", "/") == (1, 1)
    conn3 = _table([(3.0, 6.0, 3.0)])
    assert formula_match_counts(conn3, "t", "total", "a", "b", "-") == (1, 1)
