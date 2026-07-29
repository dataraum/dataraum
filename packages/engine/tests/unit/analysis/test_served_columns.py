"""Tests for the served-column seam (DAT-811, DAT-878)."""

from __future__ import annotations

from uuid import uuid4

import duckdb
import pytest

from dataraum.analysis.relationships.surrogate import SURROGATE_PREFIX, is_surrogate_column
from dataraum.analysis.served_columns import (
    describe_served,
    enriched_dimension_columns,
    quote_relation,
    served_columns,
)
from dataraum.storage import Column, Source, Table

SURROGATE_COL = f"{SURROGATE_PREFIX}account__business_id"


def test_enriched_dimension_columns_excludes_fact_passthrough(session):
    """Only ``origin='dimension'`` columns surface.

    An enriched view registers the fact's own ``f.*`` passthrough columns
    (``origin='fact'``) under the SAME ``view_table_id`` (DAT-811). The helper is the
    single home of the ``origin='dimension'`` filter that all three dims-only consumers
    (slicing, enriched derived columns, dimension_coverage) call — so a coexisting
    fact-origin sibling here proves the filter discriminates for all of them at once
    (dropping it would surface the fact column and this test would fail).
    """
    src = Source(source_id=str(uuid4()), name="csv", source_type="csv")
    session.add(src)
    session.flush()
    view_table = Table(
        table_id=str(uuid4()),
        source_id=src.source_id,
        table_name="enriched_orders",
        layer="enriched",
        duckdb_path="enriched_orders",
        row_count=3,
    )
    session.add(view_table)
    session.flush()
    session.add_all(
        [
            Column(
                column_id=str(uuid4()),
                table_id=view_table.table_id,
                column_name="customer_id__country",
                column_position=0,
                origin="dimension",
            ),
            # A fact-origin passthrough sibling under the same view_table_id.
            Column(
                column_id=str(uuid4()),
                table_id=view_table.table_id,
                column_name="amount",
                column_position=1,
                origin="fact",
            ),
        ]
    )
    session.flush()

    got = enriched_dimension_columns(session, view_table.table_id)

    assert [c.column_name for c in got] == ["customer_id__country"]


def test_served_columns_excludes_surrogates_preserving_order():
    """Mint-owned ``_sk__*`` rows drop out; everything else keeps its order."""
    cols = [
        Column(column_id="1", table_id="t", column_name="amount", column_position=0),
        Column(column_id="2", table_id="t", column_name=SURROGATE_COL, column_position=1),
        Column(column_id="3", table_id="t", column_name="booked_on", column_position=2),
    ]

    assert [c.column_name for c in served_columns(cols)] == ["amount", "booked_on"]


def test_describe_served_excludes_surrogates():
    """The physical DESCRIBE path drops surrogates the typed table really carries.

    The mint amends the typing DDL with ``SELECT *, md5(…) AS "_sk__…"``, so a
    surrogate is a genuine physical column — a raw ``DESCRIBE`` cannot see the
    difference. Building the table the way the mint does proves the filter acts on
    the real shape rather than on a hand-built fixture.
    """
    conn = duckdb.connect()
    conn.execute("CREATE TABLE ledger AS SELECT 1 AS entry_id, 2.5 AS amount")
    # Exactly the shape amend_typed_ddl produces: SELECT *, <hash> AS "_sk__…".
    conn.execute(
        'CREATE OR REPLACE TABLE ledger AS SELECT *, md5("entry_id"::VARCHAR) '
        f'AS "{SURROGATE_COL}" FROM (SELECT * FROM ledger)'
    )

    raw = [r[0] for r in conn.execute("DESCRIBE ledger").fetchall()]
    assert SURROGATE_COL in raw, "fixture must reproduce the mint's physical shape"

    got = describe_served(conn, "ledger")

    assert [name for name, _ in got] == ["entry_id", "amount"]
    assert all(t for _, t in got), "types travel with the names"


def test_describe_served_falls_loud_on_missing_relation():
    """Absence is not silently an empty column list."""
    conn = duckdb.connect()

    with pytest.raises(duckdb.Error):
        describe_served(conn, "no_such_relation")


def test_surrogate_predicate_does_not_match_the_renamed_dimension_form():
    """The prefix predicate cannot see a surrogate the view builder renamed.

    ``builder.py`` qualifies each joined dimension column as ``{fact_fk}__{col}``,
    which pushes ``_sk__`` off position 0. The predicate stays a strict prefix test
    on purpose — a substring test would be a guess about user data — so this case
    is closed by never offering a surrogate to the enrichment agent, not by
    filtering the result. This test pins WHY that prevention has to exist: if it
    is dropped, nothing downstream can detect the leak.
    """
    renamed = f"account_id__{SURROGATE_COL}"

    assert not is_surrogate_column(renamed)
    assert is_surrogate_column(SURROGATE_COL)


def test_describe_served_handles_an_embedded_quote():
    """A `"` in a relation name is reachable, so quoting must double it.

    Catalog names descend from source CSV headers under the VARCHAR-first load. The
    naive f'"{name}"' form produces a syntax error, and every caller of this helper
    treats a raise as "relation unavailable" — so the failure mode is not an error
    surfacing but a table silently vanishing from the served schema.
    """
    conn = duckdb.connect()
    weird = 'led"ger'
    conn.execute(f"CREATE TABLE {quote_relation(weird)} AS SELECT 1 AS entry_id")

    assert [name for name, _ in describe_served(conn, weird)] == ["entry_id"]


def test_quote_relation_doubles_embedded_quotes():
    assert quote_relation('led"ger') == '"led""ger"'
    assert quote_relation("ledger") == '"ledger"'
