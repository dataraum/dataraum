"""Surrogate-key mint helpers (DAT-277) — hash semantics proven on real DuckDB.

The hash expression is a JOIN key, so its NULL/collision semantics are the
load-bearing part: NULL must propagate (an FK with a missing component matches
nothing) and the delimiter must keep adjacent tuples apart.
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest

from dataraum.analysis.relationships.surrogate import (
    SurrogateSpec,
    amend_typed_ddl,
    is_surrogate_column,
    surrogate_column_name,
)


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    try:
        yield c
    finally:
        c.close()


def _spec(*components: str) -> SurrogateSpec:
    return SurrogateSpec(
        table_id="t1",
        column_name=surrogate_column_name(list(components)),
        component_names=components,
    )


def test_name_is_deterministic_in_component_order() -> None:
    assert surrogate_column_name(["account", "business_id"]) == "_sk__account__business_id"
    assert is_surrogate_column("_sk__account__business_id")
    assert not is_surrogate_column("account")


def test_hash_matches_across_sides_for_equal_tuples(con) -> None:
    """The same value tuple hashes identically — the join contract."""
    expr_f = _spec("account", "business_id").hash_expr
    expr_f = expr_f.replace('"account"', "'Sales'").replace('"business_id"', "'B1'")
    expr_d = _spec("account_name", "business_id").hash_expr
    expr_d = expr_d.replace('"account_name"', "'Sales'").replace('"business_id"', "'B1'")
    row = con.execute(f"SELECT ({expr_f}) = ({expr_d})").fetchone()
    assert row is not None and row[0] is True


def test_null_component_propagates_to_null_surrogate(con) -> None:
    """FK semantics: any NULL component → NULL surrogate → LEFT JOIN misses."""
    con.execute("CREATE TABLE t (a VARCHAR, b VARCHAR)")
    con.execute("INSERT INTO t VALUES ('x', NULL), (NULL, 'y'), ('x', 'y')")
    expr = _spec("a", "b").hash_expr
    rows = con.execute(f"SELECT {expr} FROM t").fetchall()
    assert rows[0][0] is None
    assert rows[1][0] is None
    assert rows[2][0] is not None


def test_delimiter_separates_adjacent_tuples(con) -> None:
    """('ab','c') and ('a','bc') must not collide (the concat trap)."""
    expr = _spec("a", "b").hash_expr
    row = con.execute(
        "SELECT ("
        + expr.replace('"a"', "'ab'").replace('"b"', "'c'")
        + ") = ("
        + expr.replace('"a"', "'a'").replace('"b"', "'bc'")
        + ")"
    ).fetchone()
    assert row is not None and row[0] is False


def test_amend_wraps_the_base_select(con) -> None:
    con.execute("CREATE SCHEMA raw_s")
    con.execute("CREATE TABLE raw_s.orders (account VARCHAR, business_id VARCHAR)")
    con.execute("INSERT INTO raw_s.orders VALUES ('Sales', 'B1')")
    base = (
        "CREATE OR REPLACE TABLE typed_orders AS "
        'SELECT TRY_CAST("account" AS VARCHAR) AS "account", '
        'TRY_CAST("business_id" AS VARCHAR) AS "business_id" FROM raw_s.orders'
    )
    amended = amend_typed_ddl(base, [_spec("account", "business_id")])
    con.execute(amended)
    cols = [r[0] for r in con.execute("DESCRIBE typed_orders").fetchall()]
    assert cols == ["account", "business_id", "_sk__account__business_id"]
    value = con.execute('SELECT "_sk__account__business_id" FROM typed_orders').fetchone()
    expected = con.execute("SELECT md5('Sales' || '|' || 'B1')").fetchone()
    assert value is not None and expected is not None and value[0] == expected[0]


def test_amend_handles_the_strongly_typed_fast_path(con) -> None:
    """Typing's fast path is ``SELECT * FROM raw`` — the wrap must survive it."""
    con.execute("CREATE TABLE raw_fast (a VARCHAR, b VARCHAR)")
    con.execute("INSERT INTO raw_fast VALUES ('x', 'y')")
    amended = amend_typed_ddl(
        "CREATE OR REPLACE TABLE typed_fast AS SELECT * FROM raw_fast", [_spec("a", "b")]
    )
    con.execute(amended)
    cols = [r[0] for r in con.execute("DESCRIBE typed_fast").fetchall()]
    assert cols == ["a", "b", "_sk__a__b"]


def test_amend_without_specs_returns_base_unchanged() -> None:
    base = "CREATE OR REPLACE TABLE t AS SELECT * FROM raw_t"
    assert amend_typed_ddl(base, []) == base


def test_amend_rejects_a_foreign_ddl_shape() -> None:
    with pytest.raises(ValueError, match="boundary"):
        amend_typed_ddl("CREATE VIEW v AS (WITH x AS (SELECT 1) SELECT * FROM x)", [_spec("a")])
