"""Unit tests for TypingPhase per-table filtering (DAT-342).

Covers the ``table_filter`` behavior: ``ctx.table_ids`` narrows which raw
tables get typed (per-table teach replay) without touching siblings, and
``should_skip`` honors the same filter so a targeted untyped table still runs
when its sibling tables are already typed.

These exercise the filter resolution + skip logic directly via a constructed
``PhaseContext`` — no live entry point, no real type inference needed.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.typing_phase import TypingPhase
from dataraum.storage.models import Source, Table


def _make_source(session: Session) -> Source:
    source = Source(name=f"src_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()
    return source


def _make_table(session: Session, source_id: str, name: str, layer: str = "raw") -> Table:
    table = Table(source_id=source_id, table_name=name, layer=layer, row_count=10)
    session.add(table)
    session.flush()
    return table


def _ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_id: str,
    table_ids: list[str] | None = None,
) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        source_id=source_id,
        table_ids=table_ids or [],
    )


# ---------------------------------------------------------------------------
# _resolve_target_table_ids
# ---------------------------------------------------------------------------


class TestResolveTargetTableIds:
    def test_empty_filter_returns_all_raw_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        t1 = _make_table(session, src.source_id, "t1")
        t2 = _make_table(session, src.source_id, "t2")
        t3 = _make_table(session, src.source_id, "t3")

        resolved = TypingPhase()._resolve_target_table_ids(
            _ctx(session, duckdb_conn, src.source_id)
        )

        assert set(resolved) == {t1.table_id, t2.table_id, t3.table_id}

    def test_filter_narrows_to_requested_subset(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        t1 = _make_table(session, src.source_id, "t1")
        t2 = _make_table(session, src.source_id, "t2")
        t3 = _make_table(session, src.source_id, "t3")

        resolved = TypingPhase()._resolve_target_table_ids(
            _ctx(session, duckdb_conn, src.source_id, table_ids=[t1.table_id])
        )

        # Only the targeted table is resolved; siblings are excluded so _run
        # never touches them (the per-table "siblings untouched" guarantee).
        assert resolved == [t1.table_id]
        assert t2.table_id not in resolved
        assert t3.table_id not in resolved

    def test_filter_drops_ids_that_are_not_raw_tables_of_this_source(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        t1 = _make_table(session, src.source_id, "t1")
        # A typed-layer table id and a foreign id must not survive the filter.
        typed = _make_table(session, src.source_id, "t1", layer="typed")
        other_src = _make_source(session)
        foreign = _make_table(session, other_src.source_id, "x")

        resolved = TypingPhase()._resolve_target_table_ids(
            _ctx(
                session,
                duckdb_conn,
                src.source_id,
                table_ids=[t1.table_id, typed.table_id, foreign.table_id],
            )
        )

        assert resolved == [t1.table_id]

    def test_falls_back_to_ctx_table_ids_when_no_source_raw_rows(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        # No raw rows registered under this source_id; caller carries ids.
        src = _make_source(session)
        carried = [str(uuid4()), str(uuid4())]

        resolved = TypingPhase()._resolve_target_table_ids(
            _ctx(session, duckdb_conn, src.source_id, table_ids=carried)
        )

        assert resolved == carried


# ---------------------------------------------------------------------------
# should_skip
# ---------------------------------------------------------------------------


class TestShouldSkip:
    def test_no_raw_tables_skips(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn, src.source_id))
        assert reason == "No raw tables to process"

    def test_all_typed_skips_when_unfiltered(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")
        _make_table(session, src.source_id, "t1", layer="typed")

        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn, src.source_id))
        assert reason == "All tables already typed"

    def test_one_untyped_runs_when_unfiltered(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")
        _make_table(session, src.source_id, "t1", layer="typed")
        _make_table(session, src.source_id, "t2")  # raw, no typed → must run

        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn, src.source_id))
        assert reason is None

    def test_targeted_untyped_runs_even_when_siblings_typed(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        # The core DAT-342 case: t1+t2 typed, replay targets t3 (untyped).
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")
        _make_table(session, src.source_id, "t1", layer="typed")
        _make_table(session, src.source_id, "t2")
        _make_table(session, src.source_id, "t2", layer="typed")
        t3 = _make_table(session, src.source_id, "t3")

        reason = TypingPhase().should_skip(
            _ctx(session, duckdb_conn, src.source_id, table_ids=[t3.table_id])
        )
        assert reason is None

    def test_targeted_already_typed_skips(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        t1 = _make_table(session, src.source_id, "t1")
        _make_table(session, src.source_id, "t1", layer="typed")
        _make_table(session, src.source_id, "t2")  # untyped sibling, but not targeted

        reason = TypingPhase().should_skip(
            _ctx(session, duckdb_conn, src.source_id, table_ids=[t1.table_id])
        )
        assert reason == "All tables already typed"

    def test_filter_matches_no_raw_tables_skips(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")

        reason = TypingPhase().should_skip(
            _ctx(session, duckdb_conn, src.source_id, table_ids=[str(uuid4())])
        )
        assert reason == "No raw tables match the requested table_ids filter"
