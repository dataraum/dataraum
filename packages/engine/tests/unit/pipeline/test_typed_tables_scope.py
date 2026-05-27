"""Unit tests for ``BasePhase._typed_tables`` per-table scoping (DAT-370).

The four table-local analytics phases (statistics, column_eligibility,
statistical_quality, temporal) all resolve their working set through this one
helper. Under the per-table fan-out, ``ctx.table_ids`` carries the single typed
table a child workflow is processing, so the helper must return exactly that
table; an empty filter means source-wide (direct/test invocation). These cover
that resolution directly via a constructed ``PhaseContext`` — no real profiling.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.statistics_phase import StatisticsPhase
from dataraum.storage.models import Source, Table


def _source(session: Session) -> Source:
    source = Source(name=f"src_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()
    return source


def _table(session: Session, source_id: str, name: str, layer: str) -> Table:
    table = Table(source_id=source_id, table_name=name, layer=layer, row_count=10)
    session.add(table)
    session.flush()
    return table


def _ctx(session: Session, duck: duckdb.DuckDBPyConnection, source_id: str, table_ids: list[str]):  # noqa: ANN202
    return PhaseContext(session=session, duckdb_conn=duck, source_id=source_id, table_ids=table_ids)


def test_empty_filter_returns_all_typed_tables(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    src = _source(session)
    raw = _table(session, src.source_id, "orders", layer="raw")
    t1 = _table(session, src.source_id, "orders", layer="typed")
    t2 = _table(session, src.source_id, "items", layer="typed")

    resolved = StatisticsPhase()._typed_tables(_ctx(session, duckdb_conn, src.source_id, []))

    ids = {t.table_id for t in resolved}
    assert ids == {t1.table_id, t2.table_id}, "empty filter = all typed tables"
    assert raw.table_id not in ids, "raw tables are never returned"


def test_filter_scopes_to_the_single_requested_typed_table(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    src = _source(session)
    t1 = _table(session, src.source_id, "orders", layer="typed")
    _table(session, src.source_id, "items", layer="typed")

    resolved = StatisticsPhase()._typed_tables(
        _ctx(session, duckdb_conn, src.source_id, [t1.table_id])
    )

    assert [t.table_id for t in resolved] == [t1.table_id], (
        "a per-table run must see only its own typed table, not its siblings"
    )


def test_filter_does_not_cross_source_boundaries(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    src_a = _source(session)
    src_b = _source(session)
    a1 = _table(session, src_a.source_id, "orders", layer="typed")
    _table(session, src_b.source_id, "orders", layer="typed")

    # Scoped to src_a but with no filter: only src_a's typed tables.
    resolved = StatisticsPhase()._typed_tables(_ctx(session, duckdb_conn, src_a.source_id, []))

    assert [t.table_id for t in resolved] == [a1.table_id]
