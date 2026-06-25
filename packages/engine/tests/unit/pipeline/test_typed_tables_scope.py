"""Unit tests for ``BasePhase._typed_tables`` per-table scoping (DAT-370, DAT-422).

The four table-local analytics phases (statistics, column_eligibility,
statistical_quality, temporal) all resolve their working set through this one
helper. Under the per-table fan-out, ``ctx.table_ids`` carries the single typed
table a child workflow is processing, so the helper returns exactly that table.

Source-free (DAT-506/426): the ctx carries no source at all (a run spans 1–N
per-object sources), so the helper keys purely on ``table_ids`` and resolves its
table without any source context. These exercise the resolution directly via a
constructed ``PhaseContext`` — no real profiling.
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


def _ctx(
    session: Session,
    duck: duckdb.DuckDBPyConnection,
    table_ids: list[str],
):  # noqa: ANN202
    # The ctx is source-free by construction (DAT-506/426); the helper resolves by
    # table_ids alone regardless of source.
    return PhaseContext(session=session, duckdb_conn=duck, table_ids=table_ids)


def test_filter_scopes_to_the_single_requested_typed_table(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    src = _source(session)
    t1 = _table(session, src.source_id, "orders", layer="typed")
    _table(session, src.source_id, "items", layer="typed")

    resolved = StatisticsPhase()._typed_tables(_ctx(session, duckdb_conn, [t1.table_id]))

    assert [t.table_id for t in resolved] == [t1.table_id], (
        "a per-table run must see only its own typed table, not its siblings"
    )


def test_resolves_source_free(session: Session, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """The DAT-506/426 regression guard: the ctx is source-free, so a per-table
    analytics phase must resolve its typed table by ``table_ids`` alone — never by
    a (now-removed) context source id.
    """
    src = _source(session)
    t1 = _table(session, src.source_id, "orders", layer="typed")

    resolved = StatisticsPhase()._typed_tables(_ctx(session, duckdb_conn, [t1.table_id]))

    assert [t.table_id for t in resolved] == [t1.table_id]


def test_filter_keeps_typed_tables_across_sources(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """DAT-422: the per-table scope is source-AGNOSTIC — ``table_ids`` spanning two
    sources resolve BOTH (a run is over a set of objects from 1–N sources), with
    no source boundary to cross. The two tables carry DISTINCT names because table
    identity is workspace-unique now (DAT-639): a workspace can't hold two
    ``orders``, so the cross-source set is ``orders`` + ``shipments``.
    """
    src_a = _source(session)
    src_b = _source(session)
    a1 = _table(session, src_a.source_id, "orders", layer="typed")
    b1 = _table(session, src_b.source_id, "shipments", layer="typed")

    resolved = StatisticsPhase()._typed_tables(
        _ctx(session, duckdb_conn, [a1.table_id, b1.table_id])
    )

    assert {t.table_id for t in resolved} == {a1.table_id, b1.table_id}


def test_empty_filter_resolves_nothing(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """The table-local phases always run under the per-table fan-out (a scoped
    typed table_id), so an empty filter has no unit to process → ``[]``. The
    run-wide reduce (``semantic_per_column``) overrides ``_typed_tables`` to scope
    by the session instead, so this base path never carries the run-wide case.
    """
    src = _source(session)
    _table(session, src.source_id, "orders", layer="typed")

    resolved = StatisticsPhase()._typed_tables(_ctx(session, duckdb_conn, []))

    assert resolved == []
