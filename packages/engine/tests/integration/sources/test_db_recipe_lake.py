"""db_recipe extraction against the real DuckLake fixture (DAT-504).

The unit tests in ``tests/unit/sources/test_extract_backend.py`` use a plain
in-memory ``lake`` alias; this file proves the lake-convergent extraction
against the actual DuckLake catalog (testcontainer Postgres + tmp DATA_PATH),
including the capability assumption it relies on:

* cross-catalog CTAS — ``CREATE OR REPLACE TABLE lake.raw.… AS SELECT`` reading
  from an ATTACHed backend while the connection sits inside the source catalog.
  (If DuckLake ever rejects this, the fallback is a temp-table hop via the
  in-memory catalog — see the DAT-504 refine notes.)

And the production wiring around it:

* the table lands in ``lake.raw`` (the live bug wrote ``lake.main`` because the
  session connection USEs ``lake.typed``), with the metadata contract
  (``layer="raw"`` + bare ``duckdb_path``) resolving to the physical table;
* a second execution over leftovers converges (CREATE OR REPLACE, no
  collision);
* the USE-restore returns the connection to the snapshotted ``lake.typed``
  pair instead of stranding it at ``lake.main``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dataraum.sources.backends import extract_backend
from dataraum.sources.db_recipe import RecipeTable


@pytest.fixture
def sqlite_source(tmp_path: Path) -> str:
    """A small sqlite database standing in for the customer backend."""
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            region TEXT
        );
        INSERT INTO customers VALUES
            (1, 'Acme Corp', 'EMEA'),
            (2, 'Globex',    'APAC'),
            (3, 'Initech',   'AMER');
        """
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_extract_lands_in_lake_raw_and_converges(integration_duckdb, sqlite_source) -> None:
    from dataraum.core.duckdb_naming import schema_for_layer
    from dataraum.server.storage import LAKE_CATALOG_ALIAS

    conn = integration_duckdb

    # Run TWICE: the second execution hits the first run's leftover raw table
    # and must overwrite it (lake convergence), not collide.
    result = None
    for attempt in (1, 2):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
            duckdb_conn=conn,
            raw_prefix="src__",
        )
        assert result.success, f"attempt {attempt}: {result.error}"
    assert result is not None
    extracted = result.unwrap().tables[0]
    assert extracted.duckdb_table == "src__customers"
    assert extracted.row_count == 3

    # The physical table lives in lake.raw and ONLY there — the DAT-504 live
    # bug parked it in lake.main while metadata claimed layer="raw".
    placements = conn.execute(
        "SELECT schema_name FROM duckdb_tables() "
        f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' AND table_name = 'src__customers'"
    ).fetchall()
    assert [r[0] for r in placements] == ["raw"]
    main_strays = conn.execute(
        "SELECT table_name FROM duckdb_tables() "
        f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' AND schema_name = 'main'"
    ).fetchall()
    assert main_strays == []

    # Metadata contract: import registers layer="raw" + bare duckdb_path; the
    # composed FQN must resolve via DESCRIBE with the harvested columns.
    fqn = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("raw")}."{extracted.duckdb_table}"'
    described = conn.execute(f"DESCRIBE {fqn}").fetchall()
    assert [(str(r[0]), str(r[1])) for r in described] == extracted.columns
    (count,) = conn.execute(f"SELECT count(*) FROM {fqn}").fetchone()
    assert count == 3

    # USE-restore: the session connection must sit back at lake.typed — the
    # old catalog-only restore stranded it at lake.main.
    pair = conn.execute("SELECT current_catalog(), current_schema()").fetchone()
    assert pair == (LAKE_CATALOG_ALIAS, "typed")


def test_failed_extract_restores_lake_typed_pair(integration_duckdb, sqlite_source) -> None:
    from dataraum.server.storage import LAKE_CATALOG_ALIAS

    conn = integration_duckdb
    result = extract_backend(
        backend="sqlite",
        url=sqlite_source,
        queries=[RecipeTable(name="bad", sql="SELECT * FROM no_such_table")],
        duckdb_conn=conn,
    )
    assert not result.success
    pair = conn.execute("SELECT current_catalog(), current_schema()").fetchone()
    assert pair == (LAKE_CATALOG_ALIAS, "typed")
