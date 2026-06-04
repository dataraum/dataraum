"""Unit tests for TypingPhase per-table filtering (DAT-342) + re-run (DAT-413).

Covers the ``table_filter`` behavior: ``ctx.table_ids`` narrows which raw
tables get typed (per-table teach replay) without touching siblings.

``should_skip`` is now a STRUCTURAL early-out only (DAT-413): it skips solely
when there are no raw tables to type (none registered, or the filter matches
none). The old output-existence bail ("typed counterpart already exists →
skip") is gone — a re-run mints a fresh ``run_id`` and must always re-derive
typing under it, so an already-typed table re-runs rather than skipping.

These exercise the filter resolution + skip logic directly via a constructed
``PhaseContext`` — no live entry point, no real type inference needed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.typing.db_models import TypeDecision
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.typing_phase import TypingPhase
from dataraum.storage.models import Column, Source, Table


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


def _make_typed_table(session: Session, source_id: str, name: str) -> Table:
    """A fully-typed table: a typed Table + one Column carrying a TypeDecision.

    Models the post-run state a re-run encounters. Under DAT-413 this state no
    longer gates ``should_skip`` (the output-existence bail is gone), so these
    rows exist only to prove the re-run is NOT short-circuited by prior output.
    """
    table = _make_table(session, source_id, name, layer="typed")
    col = Column(
        column_id=str(uuid4()),
        table_id=table.table_id,
        column_name="c0",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="BIGINT",
    )
    session.add(col)
    session.flush()
    session.add(
        TypeDecision(
            decision_id=str(uuid4()),
            column_id=col.column_id,
            decided_type="BIGINT",
            decision_source="automatic",
            decided_at=datetime.now(UTC),
        )
    )
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

    def test_keeps_raw_tables_across_sources_drops_non_raw_layer(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-422: the per-table scope is source-AGNOSTIC — a raw table from a
        DIFFERENT source is kept (a run spans per-object sources), while the
        raw-layer filter still drops a typed-layer id.
        """
        src = _make_source(session)
        t1 = _make_table(session, src.source_id, "t1")
        # A typed-layer id (wrong layer) and a raw table of ANOTHER source.
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

        # The foreign raw table IS included (source-agnostic); the typed-layer id
        # is dropped by the raw-layer filter.
        assert set(resolved) == {t1.table_id, foreign.table_id}
        assert typed.table_id not in resolved

    def test_unknown_or_non_raw_ids_are_dropped(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-422: the resolver returns only real raw tables — caller ids that are
        not raw tables (unknown uuids) are dropped. The old "trust caller ids
        verbatim when the source has no raw rows" fallback is gone; the per-table
        fan-out always hands real raw ids.
        """
        src = _make_source(session)
        carried = [str(uuid4()), str(uuid4())]  # not real tables

        resolved = TypingPhase()._resolve_target_table_ids(
            _ctx(session, duckdb_conn, src.source_id, table_ids=carried)
        )

        assert resolved == []


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

    def test_already_typed_re_runs_when_unfiltered(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A fully-typed table no longer skips — it re-types under a new run (DAT-413).

        The output-existence skip ("all tables already typed → skip") is gone:
        a re-run mints a fresh ``run_id`` and must always re-derive typing under
        it. With a raw table present, ``should_skip`` returns ``None`` even though
        the typed counterpart (with a prior run's ``TypeDecision``) already exists.
        """
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")
        _make_typed_table(session, src.source_id, "t1")

        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn, src.source_id))
        assert reason is None

    def test_one_untyped_runs_when_unfiltered(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")
        _make_typed_table(session, src.source_id, "t1")
        _make_table(session, src.source_id, "t2")  # raw, no typed → must run

        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn, src.source_id))
        assert reason is None

    def test_targeted_untyped_runs_even_when_siblings_typed(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        # The core DAT-342 case: t1+t2 typed, replay targets t3 (untyped).
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")
        _make_typed_table(session, src.source_id, "t1")
        _make_table(session, src.source_id, "t2")
        _make_typed_table(session, src.source_id, "t2")
        t3 = _make_table(session, src.source_id, "t3")

        reason = TypingPhase().should_skip(
            _ctx(session, duckdb_conn, src.source_id, table_ids=[t3.table_id])
        )
        assert reason is None

    def test_targeted_already_typed_re_runs(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A targeted already-typed table re-types under a new run (DAT-413).

        The structural early-out only checks that the *raw* table the filter
        targets still exists; the typed counterpart already existing no longer
        gates a skip, so the re-run re-derives typing under a fresh ``run_id``.
        """
        src = _make_source(session)
        t1 = _make_table(session, src.source_id, "t1")
        _make_typed_table(session, src.source_id, "t1")
        _make_table(session, src.source_id, "t2")  # untyped sibling, but not targeted

        reason = TypingPhase().should_skip(
            _ctx(session, duckdb_conn, src.source_id, table_ids=[t1.table_id])
        )
        assert reason is None

    def test_filter_matches_no_raw_tables_skips(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")

        reason = TypingPhase().should_skip(
            _ctx(session, duckdb_conn, src.source_id, table_ids=[str(uuid4())])
        )
        assert reason == "No raw tables match the requested table_ids filter"
