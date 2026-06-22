"""Unit tests for the SQL uniqueness ratio (DAT-580 follow-up: pandas → DuckDB SQL).

``_uniqueness_ratio`` replaced ``pd.Series.nunique()/len()`` with an in-SQL
``COUNT(DISTINCT)/NULLIF(COUNT(*),0)`` over a Bernoulli sample. These pin the three
result paths (normal, all-null, empty) and the per-(path,column) cache. Uses
``sample_percent=100`` so the assertions are deterministic (no Bernoulli draw).
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest

from dataraum.analysis.relationships.finder import _uniqueness_ratio


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(":memory:")
    try:
        yield c
    finally:
        c.close()


def test_all_distinct_column_is_one(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3), (4)) AS v(id)")
    assert _uniqueness_ratio(conn, "t", "id", 100.0, {}) == 1.0


def test_repeated_values_ratio(conn: duckdb.DuckDBPyConnection) -> None:
    # 2 distinct over 4 rows → 0.5.
    conn.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (1), (2), (2)) AS v(id)")
    assert _uniqueness_ratio(conn, "t", "id", 100.0, {}) == 0.5


def test_all_null_column_is_zero(conn: duckdb.DuckDBPyConnection) -> None:
    # COUNT(DISTINCT) ignores NULLs → 0 distinct over a non-empty sample → 0.0.
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.execute("INSERT INTO t VALUES (NULL), (NULL), (NULL)")
    assert _uniqueness_ratio(conn, "t", "id", 100.0, {}) == 0.0


def test_empty_table_is_zero(conn: duckdb.DuckDBPyConnection) -> None:
    # NULLIF(COUNT(*), 0) → NULL → 0.0 (no divide-by-zero).
    conn.execute("CREATE TABLE t (id INTEGER)")
    assert _uniqueness_ratio(conn, "t", "id", 100.0, {}) == 0.0


def test_quotes_identifiers(conn: duckdb.DuckDBPyConnection) -> None:
    # A column name needing quoting (reserved word / mixed case) must not break the SQL.
    conn.execute('CREATE TABLE t ("Select" INTEGER)')
    conn.execute('INSERT INTO t ("Select") VALUES (1), (2)')
    assert _uniqueness_ratio(conn, "t", "Select", 100.0, {}) == 1.0


def test_cache_hit_skips_requery(conn: duckdb.DuckDBPyConnection) -> None:
    # First call computes 1.0 and caches under (path, column). Mutating the table to an
    # all-null column would recompute to 0.0 — but the cached value is returned instead,
    # proving the second call never re-queried.
    conn.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (2)) AS v(id)")
    cache: dict[tuple[str, str], float] = {}
    assert _uniqueness_ratio(conn, "t", "id", 100.0, cache) == 1.0

    conn.execute("DROP TABLE t")
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.execute("INSERT INTO t VALUES (NULL), (NULL)")
    assert _uniqueness_ratio(conn, "t", "id", 100.0, cache) == 1.0  # cached, not 0.0
