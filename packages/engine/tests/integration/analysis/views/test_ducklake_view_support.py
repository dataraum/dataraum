"""Does DuckLake support CREATE VIEW in the lake catalog? (DAT-415 de-risk).

The enriched_views revival materializes views with ``CREATE OR REPLACE VIEW`` in
the lake catalog. That phase has been dormant, and the existing
``test_enriched_views`` suite exercises only an in-memory DuckDB — so view
support against the real DuckLake-anchored connection (catalog in Postgres, data
on the lake DATA_PATH) is unproven. This pins it down: a view over a lake.typed
table must create, query, and drop. If DuckLake ever stops supporting views, this
fails loudly here rather than deep in begin_session.
"""

from __future__ import annotations

import duckdb


def test_ducklake_supports_enriched_views(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    duckdb_conn.execute(
        'CREATE OR REPLACE TABLE lake.typed."probe_fact" AS SELECT 1 AS id, 100 AS amount'
    )
    duckdb_conn.execute(
        'CREATE OR REPLACE VIEW lake.typed."probe_enriched" AS '
        'SELECT * FROM lake.typed."probe_fact"'
    )
    rows = duckdb_conn.execute('SELECT id, amount FROM lake.typed."probe_enriched"').fetchall()
    assert rows == [(1, 100)]

    duckdb_conn.execute('DROP VIEW IF EXISTS lake.typed."probe_enriched"')
    duckdb_conn.execute('DROP TABLE IF EXISTS lake.typed."probe_fact"')
