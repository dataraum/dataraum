"""Tests for the enriched-view served-column read helpers (DAT-811)."""

from __future__ import annotations

from uuid import uuid4

from dataraum.analysis.views.served_columns import enriched_dimension_columns
from dataraum.storage import Column, Source, Table


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
