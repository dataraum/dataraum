"""The quarantine-token loader query (DAT-457).

Pure DuckDB; the per-column cast-failure count that feeds the quarantine
witness. Uses an in-memory connection — no lake catalog, no session.
"""

from __future__ import annotations

import duckdb

from dataraum.entropy.detectors.loaders import rejected_token_counts


def _raw_table(rows: list[str | None]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA raw")
    conn.execute('CREATE TABLE raw."t" (amount VARCHAR)')
    conn.executemany('INSERT INTO raw."t" VALUES (?)', [(r,) for r in rows])
    return conn


def test_counts_only_cast_failures() -> None:
    conn = _raw_table(["100", "200", "N/A", "N/A", "N/A", "TBD", None])
    counts = dict(rejected_token_counts(conn, 'raw."t"', "amount", "DECIMAL"))
    # numbers parse, NULL is excluded, sentinels fail and are counted
    assert counts == {"N/A": 3, "TBD": 1}


def test_descending_by_count() -> None:
    conn = _raw_table(["x", "x", "x", "y", "N/A"])
    tokens = [tok for tok, _ in rejected_token_counts(conn, 'raw."t"', "amount", "DECIMAL")]
    assert tokens[0] == "x"  # most frequent reject first


def test_varchar_column_has_no_rejects() -> None:
    conn = _raw_table(["a", "b", "anything"])
    assert rejected_token_counts(conn, 'raw."t"', "amount", "VARCHAR") == []
