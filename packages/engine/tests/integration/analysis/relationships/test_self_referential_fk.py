"""Self-referential FK detection at Layer-A grain (DAT-763).

A self-FK (``chart_of_accounts.parent_id -> account_id``) lives inside ONE table,
so it is only ever a candidate when the finder probes a table against itself. The
finder used to iterate ``table_names[i+1:]`` — distinct cross-table pairs only — so
the self-FK never reached the judge; it entered the catalog only when the LLM
happened to propose it (nondeterministic). These tests pin the DETERMINISTIC
candidate-existence half: no LLM, real data, the diagonal probe must emit the pair
and must NOT emit a column matched against itself.
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.analysis.relationships.finder import find_relationships
from dataraum.analysis.relationships.joins import find_join_columns

COA_COLUMNS = ["account_id", "parent_id", "name", "account_type"]
COA_TYPES = {
    "account_id": "BIGINT",
    "parent_id": "BIGINT",
    "name": "VARCHAR",
    "account_type": "VARCHAR",
}


@pytest.fixture
def coa_duckdb(tmp_path):
    """A chart_of_accounts table with a self-referential ``parent_id -> account_id``.

    60 accounts; ids 1..15 are roots (NULL parent), ids 16..60 point at a parent in
    1..15. So ``parent_id`` (15 distinct, non-unique, with NULLs) is a strict subset
    of the unique ``account_id`` (60) — a textbook many-to-one self-FK.
    """
    db_path = str(tmp_path / "coa.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE chart_of_accounts AS
        SELECT
            i AS account_id,
            CASE WHEN i <= 15 THEN NULL ELSE ((i - 1) % 15) + 1 END AS parent_id,
            'account_' || i AS name,
            CASE WHEN i % 4 = 0 THEN 'Asset'
                 WHEN i % 4 = 1 THEN 'Liability'
                 WHEN i % 4 = 2 THEN 'Income'
                 ELSE 'Expense' END AS account_type
        FROM generate_series(1, 60) AS t(i)
    """)
    conn.close()
    conn = duckdb.connect(db_path, read_only=True)
    yield conn
    conn.close()


def test_self_referential_fk_is_a_candidate(coa_duckdb):
    """The finder emits the parent_id<->account_id self-FK as a Layer-A candidate."""
    results = find_relationships(
        coa_duckdb,
        {"chart_of_accounts": ("chart_of_accounts", COA_COLUMNS, COA_TYPES)},
    )
    # A single-table workspace still yields a self-referential relationship entry.
    self_rels = [r for r in results if r["table1"] == "chart_of_accounts" == r["table2"]]
    assert self_rels, "no self-referential relationship emitted for chart_of_accounts"
    pairs = {frozenset((j["column1"], j["column2"])) for r in self_rels for j in r["join_columns"]}
    assert frozenset(("parent_id", "account_id")) in pairs


def test_self_probe_excludes_identity_and_duplicate_direction(coa_duckdb):
    """A column is never matched to itself, and each unordered pair appears once."""
    candidates = find_join_columns(
        coa_duckdb,
        "chart_of_accounts",
        "chart_of_accounts",
        COA_COLUMNS,
        COA_COLUMNS,
        min_score=0.3,
        column_types1=COA_TYPES,
        column_types2=COA_TYPES,
        same_table=True,
    )
    # No diagonal identity match (account_id == account_id → jaccard 1.0 otherwise).
    assert all(c["column1"] != c["column2"] for c in candidates)
    # Each unordered column pair is tried at most once (upper triangle).
    seen = [frozenset((c["column1"], c["column2"])) for c in candidates]
    assert len(seen) == len(set(seen))
    # The self-FK pair is present.
    assert frozenset(("parent_id", "account_id")) in set(seen)


def test_cross_table_default_is_unaffected(coa_duckdb):
    """same_table defaults False — cross-table detection compares the full grid,
    including a same-named pair, exactly as before."""
    candidates = find_join_columns(
        coa_duckdb,
        "chart_of_accounts",
        "chart_of_accounts",
        ["account_id"],
        ["account_id"],
        min_score=0.3,
        column_types1={"account_id": "BIGINT"},
        column_types2={"account_id": "BIGINT"},
    )
    # With same_table left False the diagonal identity pair IS compared (the
    # cross-table contract: account_id joins account_id). Guard against a regression
    # that would make the default path skip same-named columns.
    assert any(c["column1"] == "account_id" and c["column2"] == "account_id" for c in candidates)
