"""Composite-key rescue math (DAT-277) — proven on small, varied fixtures.

The confidence in the rescue comes from this matrix, NOT from one big dataset:
each fixture is a few rows that exercise one cardinality shape deterministically.
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest

from dataraum.analysis.relationships.composite import (
    _join_multiplication,
    rescue_fanout_to_composite,
)
from dataraum.analysis.relationships.evaluator import compute_composite_cardinality
from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    try:
        yield c
    finally:
        c.close()


def _candidate(
    table1: str, table2: str, pairs: list[tuple[str, str, float]]
) -> RelationshipCandidate:
    """A table-pair candidate carrying its value-overlap join columns (col1, col2, confidence)."""
    return RelationshipCandidate(
        table1=table1,
        table2=table2,
        join_candidates=[
            JoinCandidate(column1=a, column2=b, join_confidence=conf, cardinality="unknown")
            for a, b, conf in pairs
        ],
    )


# ---------------------------------------------------------------------------
# compute_composite_cardinality — the multi-column primitive
# ---------------------------------------------------------------------------


def test_single_pair_matches_actual_cardinality(con) -> None:
    con.execute("CREATE TABLE f (fk VARCHAR)")
    con.execute("INSERT INTO f VALUES ('a'),('a'),('b')")
    con.execute("CREATE TABLE d (pk VARCHAR)")
    con.execute("INSERT INTO d VALUES ('a'),('b')")
    # each f.fk → one d.pk; each d.pk → many f → many-to-one
    assert compute_composite_cardinality("f", "d", [("fk", "pk")], con) == "many-to-one"


def test_empty_key_is_none(con) -> None:
    assert compute_composite_cardinality("f", "d", [], con) is None


# ---------------------------------------------------------------------------
# The rescue matrix
# ---------------------------------------------------------------------------


def test_spurious_m2m_rescued_by_one_scoping_col(con) -> None:
    """The multi-tenant bookkeeping shape: account recurs across tenants; (account, business_id) is the real key."""
    con.execute("CREATE TABLE txn (account VARCHAR, business_id VARCHAR, amount INT)")
    con.execute(
        "INSERT INTO txn VALUES "
        "('Sales','B1',10),('Sales','B1',20),('COGS','B1',5),"
        "('Sales','B2',30),('COGS','B2',7),('COGS','B2',8)"
    )
    con.execute(
        "CREATE TABLE coa (account_name VARCHAR, business_id VARCHAR, account_type VARCHAR)"
    )
    con.execute(
        "INSERT INTO coa VALUES "
        "('Sales','B1','revenue'),('COGS','B1','expense'),"
        "('Sales','B2','revenue'),('COGS','B2','expense')"
    )

    # account alone fans out across tenants; the composite collapses it.
    assert (
        compute_composite_cardinality("txn", "coa", [("account", "account_name")], con)
        == "many-to-many"
    )
    assert (
        compute_composite_cardinality(
            "txn", "coa", [("account", "account_name"), ("business_id", "business_id")], con
        )
        == "many-to-one"
    )

    cand = _candidate(
        "txn", "coa", [("account", "account_name", 0.9), ("business_id", "business_id", 0.7)]
    )
    key = rescue_fanout_to_composite(cand, "txn", "coa", con)
    assert key is not None
    assert set(key.column_pairs) == {("account", "account_name"), ("business_id", "business_id")}
    assert key.cardinality == "many-to-one"


def test_spurious_m2m_needs_two_scoping_cols(con) -> None:
    """A 3-column key: neither (a) nor (a,b) collapses; (a,b,c) does."""
    con.execute("CREATE TABLE f (a VARCHAR, b VARCHAR, c VARCHAR, v INT)")
    con.execute(
        "INSERT INTO f VALUES ('X','1','P',1),('X','1','P',2),('X','1','Q',3),('Y','1','P',4)"
    )
    con.execute("CREATE TABLE d (a VARCHAR, b VARCHAR, c VARCHAR)")
    con.execute("INSERT INTO d VALUES ('X','1','P'),('X','1','Q'),('X','2','P'),('Y','1','P')")

    assert compute_composite_cardinality("f", "d", [("a", "a")], con) == "many-to-many"
    assert compute_composite_cardinality("f", "d", [("a", "a"), ("b", "b")], con) == "many-to-many"
    assert (
        compute_composite_cardinality("f", "d", [("a", "a"), ("b", "b"), ("c", "c")], con)
        == "many-to-one"
    )

    cand = _candidate("f", "d", [("a", "a", 0.9), ("b", "b", 0.6), ("c", "c", 0.5)])
    key = rescue_fanout_to_composite(cand, "f", "d", con)
    assert key is not None
    assert set(key.column_pairs) == {("a", "a"), ("b", "b"), ("c", "c")}
    assert key.cardinality == "many-to-one"


def test_junk_overlap_column_does_not_poison_the_rescue(con) -> None:
    """A coincidental column that matches NOTHING in-context must not be preferred
    over the real scope column (B1). Real candidate lists carry such decoys; without
    ranking zero-match keys as worst, the greedy fuses the junk, drives the composite
    to zero matches, and silently abandons a valid rescue."""
    con.execute("CREATE TABLE f (a VARCHAR, b VARCHAR, c VARCHAR, v INT)")
    con.execute(
        "INSERT INTO f VALUES ('X','1','K',1),('X','1','K',2),('X','2','K',3),('Y','1','K',4)"
    )
    con.execute("CREATE TABLE d (a VARCHAR, b VARCHAR, c VARCHAR)")
    con.execute("INSERT INTO d VALUES ('X','1','Z'),('X','2','Z'),('Y','1','Z')")

    # account `a` alone fans out; `(a,b)` rescues; `c` never co-occurs (K vs Z).
    assert compute_composite_cardinality("f", "d", [("a", "a")], con) == "many-to-many"
    # The decoy is even listed at higher confidence than the real scope.
    cand = _candidate("f", "d", [("a", "a", 0.9), ("c", "c", 0.8), ("b", "b", 0.5)])
    key = rescue_fanout_to_composite(cand, "f", "d", con)
    assert key is not None
    assert ("b", "b") in key.column_pairs  # real scope fused
    assert ("c", "c") not in key.column_pairs  # junk never fused
    assert key.cardinality == "many-to-one"


def test_genuine_m2m_is_not_rescued(con) -> None:
    """A real many-to-many: no composite of the shared columns collapses it → abstain (None)."""
    con.execute("CREATE TABLE a (tag VARCHAR, color VARCHAR, x INT)")
    con.execute("INSERT INTO a VALUES ('r','red',1),('r','red',2),('b','blue',3)")
    con.execute("CREATE TABLE b (tag VARCHAR, color VARCHAR, y INT)")
    con.execute("INSERT INTO b VALUES ('r','red',1),('r','red',2),('b','blue',3)")

    assert (
        compute_composite_cardinality("a", "b", [("tag", "tag"), ("color", "color")], con)
        == "many-to-many"
    )

    cand = _candidate("a", "b", [("tag", "tag", 0.9), ("color", "color", 0.6)])
    assert rescue_fanout_to_composite(cand, "a", "b", con) is None


def test_clean_m2o_anchor_is_not_rescued(con) -> None:
    """The best pair is already many-to-one → nothing to rescue, even with a fusable second column."""
    con.execute("CREATE TABLE f (cust_id VARCHAR, region VARCHAR, v INT)")
    con.execute("INSERT INTO f VALUES ('c1','N',1),('c1','N',2),('c2','S',3)")
    con.execute("CREATE TABLE d (id VARCHAR, region VARCHAR, name VARCHAR)")
    con.execute("INSERT INTO d VALUES ('c1','N','A'),('c2','S','B')")

    assert compute_composite_cardinality("f", "d", [("cust_id", "id")], con) == "many-to-one"
    cand = _candidate("f", "d", [("cust_id", "id", 0.95), ("region", "region", 0.5)])
    assert rescue_fanout_to_composite(cand, "f", "d", con) is None


def test_single_candidate_cannot_fuse(con) -> None:
    """A lone m2m candidate has nothing to fuse with → None (the caller flags the fan-trap)."""
    con.execute("CREATE TABLE a (tag VARCHAR)")
    con.execute("INSERT INTO a VALUES ('r'),('r'),('b')")
    con.execute("CREATE TABLE b (tag VARCHAR)")
    con.execute("INSERT INTO b VALUES ('r'),('r'),('b')")
    cand = _candidate("a", "b", [("tag", "tag", 0.9)])
    assert rescue_fanout_to_composite(cand, "a", "b", con) is None


def test_join_multiplication_ranks_disambiguation(con) -> None:
    """The greedy ranking: adding the scoping column drops the worst-direction multiplicity."""
    con.execute("CREATE TABLE txn (account VARCHAR, business_id VARCHAR)")
    con.execute("INSERT INTO txn VALUES ('Sales','B1'),('Sales','B1'),('Sales','B2'),('COGS','B1')")
    con.execute("CREATE TABLE coa (account_name VARCHAR, business_id VARCHAR)")
    con.execute("INSERT INTO coa VALUES ('Sales','B1'),('Sales','B2'),('COGS','B1')")

    single = _join_multiplication("txn", "coa", [("account", "account_name")], con)
    composite = _join_multiplication(
        "txn", "coa", [("account", "account_name"), ("business_id", "business_id")], con
    )
    assert composite < single  # the scoping column reduces the fan-out
