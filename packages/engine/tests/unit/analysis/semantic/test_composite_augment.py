"""The composite-rescue pre-pass surfaces hints to the LLM judge (DAT-277).

``_augment_candidates_with_composite_rescue`` probes each candidate's fan-out with
the greedy rescue and attaches a ``composite_key`` hint dict IN PLACE when a
composite collapses it. The LLM stays the sole judge — a hint is context, never a
persisted decision — and a miss/abstain attaches nothing, so the worst case for
the working single-column path is an unchanged candidate dict.
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest

from dataraum.analysis.semantic.processor import _augment_candidates_with_composite_rescue


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    """A connection whose ``lake.typed`` schema mimics the worker's DuckLake attach."""
    c = duckdb.connect()
    try:
        c.execute("ATTACH ':memory:' AS lake")
        c.execute("CREATE SCHEMA lake.typed")
        yield c
    finally:
        c.close()


def _booksql_shape(con: duckdb.DuckDBPyConnection) -> None:
    """Fact whose FK recurs across tenants; (account, business_id) is the real key."""
    con.execute('CREATE TABLE lake.typed."txn" (account VARCHAR, business_id VARCHAR, amount INT)')
    con.execute(
        'INSERT INTO lake.typed."txn" VALUES '
        "('Sales','B1',10),('Sales','B1',20),('COGS','B1',5),"
        "('Sales','B2',30),('COGS','B2',7),('COGS','B2',8)"
    )
    con.execute(
        'CREATE TABLE lake.typed."coa" '
        "(account_name VARCHAR, business_id VARCHAR, account_type VARCHAR)"
    )
    con.execute(
        'INSERT INTO lake.typed."coa" VALUES '
        "('Sales','B1','Income'),('COGS','B1','Expense'),"
        "('Sales','B2','Income'),('COGS','B2','Expense')"
    )


def _candidate(pairs: list[tuple[str, str, float]]) -> dict:
    # Exactly the shape load_relationship_candidates_for_semantic emits.
    return {
        "table1": "txn",
        "table2": "coa",
        "join_columns": [
            {"column1": a, "column2": b, "confidence": conf, "cardinality": "unknown"}
            for a, b, conf in pairs
        ],
    }


def test_rescuable_fanout_gets_a_composite_hint(con) -> None:
    _booksql_shape(con)
    cands = [_candidate([("account", "account_name", 0.9), ("business_id", "business_id", 0.5)])]

    _augment_candidates_with_composite_rescue(cands, con)

    hint = cands[0].get("composite_key")
    assert hint is not None
    assert hint["column_pairs"] == [["account", "account_name"], ["business_id", "business_id"]]
    assert hint["cardinality"] != "many-to-many"
    assert hint["coverage"] == 1.0  # every fact row matches in this fixture (DAT-695)


def test_clean_single_column_candidate_is_untouched(con) -> None:
    """A clean many-to-one anchor needs no rescue — the dict must not change."""
    con.execute('CREATE TABLE lake.typed."txn" (cust VARCHAR, region VARCHAR)')
    con.execute("INSERT INTO lake.typed.\"txn\" VALUES ('c1','EU'),('c1','EU'),('c2','US')")
    con.execute('CREATE TABLE lake.typed."coa" (cust_id VARCHAR, region VARCHAR)')
    con.execute("INSERT INTO lake.typed.\"coa\" VALUES ('c1','EU'),('c2','US')")
    cands = [_candidate([("cust", "cust_id", 0.9), ("region", "region", 0.4)])]

    _augment_candidates_with_composite_rescue(cands, con)

    assert "composite_key" not in cands[0]


def test_single_pair_candidate_cannot_fuse(con) -> None:
    """One join column = nothing to fuse with; no hint, no probe crash."""
    _booksql_shape(con)
    cands = [_candidate([("account", "account_name", 0.9)])]

    _augment_candidates_with_composite_rescue(cands, con)

    assert "composite_key" not in cands[0]


def test_probe_failure_never_breaks_synthesis(con) -> None:
    """A missing physical table (stale candidate) is tolerated, not raised."""
    cands = [_candidate([("account", "account_name", 0.9), ("business_id", "business_id", 0.5)])]

    _augment_candidates_with_composite_rescue(cands, con)  # tables never created

    assert "composite_key" not in cands[0]
