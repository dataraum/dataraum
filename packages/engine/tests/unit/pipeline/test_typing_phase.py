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
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.typing_phase import TypingPhase, _apply_unit_overrides
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
    table_ids: list[str] | None = None,
) -> PhaseContext:
    # Source-FREE — the production shape (DAT-422/426): typing runs in the
    # per-table fan-out children, which AddSourceWorkflow threads source_id=None
    # into. ``source_id`` above is a Table DB column (seeding), never ctx identity.
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        source_id=None,
        table_ids=table_ids or [],
    )


# ---------------------------------------------------------------------------
# _resolve_target_table_ids
# ---------------------------------------------------------------------------


class TestResolveTargetTableIds:
    def test_empty_filter_resolves_nothing(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Source-free (DAT-422): typing runs per-table under the fan-out, so an
        empty ``table_ids`` has no unit to type → ``[]``. The old "all raw tables of
        the bound ``ctx.source_id``" fallback is gone — typing never scopes by source.
        """
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")
        _make_table(session, src.source_id, "t2")

        resolved = TypingPhase()._resolve_target_table_ids(_ctx(session, duckdb_conn))

        assert resolved == []

    def test_filter_narrows_to_requested_subset(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        t1 = _make_table(session, src.source_id, "t1")
        t2 = _make_table(session, src.source_id, "t2")
        t3 = _make_table(session, src.source_id, "t3")

        resolved = TypingPhase()._resolve_target_table_ids(
            _ctx(session, duckdb_conn, table_ids=[t1.table_id])
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
        _make_source(session)  # a Source exists, but the carried ids are not its tables
        carried = [str(uuid4()), str(uuid4())]  # not real tables

        resolved = TypingPhase()._resolve_target_table_ids(
            _ctx(session, duckdb_conn, table_ids=carried)
        )

        assert resolved == []


# ---------------------------------------------------------------------------
# should_skip
# ---------------------------------------------------------------------------


class TestShouldSkip:
    def test_no_raw_tables_skips(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        _make_source(session)  # a Source exists, but it has no raw tables
        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn))
        assert reason == "No raw tables to process"

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

        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn, table_ids=[t3.table_id]))
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

        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn, table_ids=[t1.table_id]))
        assert reason is None

    def test_filter_matches_no_raw_tables_skips(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _make_source(session)
        _make_table(session, src.source_id, "t1")

        reason = TypingPhase().should_skip(_ctx(session, duckdb_conn, table_ids=[str(uuid4())]))
        assert reason == "No raw tables match the requested table_ids filter"


# ---------------------------------------------------------------------------
# _apply_unit_overrides — the column-scoped unit teach (DAT-428)
# ---------------------------------------------------------------------------


def _make_column_with_candidate(
    session: Session, table: Table, name: str = "amount", confidence: float = 0.9
) -> Column:
    col = Column(
        column_id=str(uuid4()),
        table_id=table.table_id,
        column_name=name,
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="DECIMAL",
    )
    session.add(col)
    session.flush()
    from tests.conftest import baseline_session_id

    session.add(
        TypeCandidate(
            candidate_id=str(uuid4()),
            session_id=baseline_session_id(),
            column_id=col.column_id,
            data_type="DECIMAL",
            confidence=confidence,
        )
    )
    session.flush()
    return col


class TestApplyUnitOverrides:
    """``overrides.units`` lands on the best TypeCandidate — the read half of the
    DAT-428 unit teach loop (overlay applier writes, this reader consumes)."""

    def _setup(self, session: Session, table_name: str) -> tuple[Table, Column]:
        src = _make_source(session)
        table = _make_table(session, src.source_id, table_name)
        col = _make_column_with_candidate(session, table)
        return table, col

    def _candidate(self, session: Session, col: Column) -> TypeCandidate:
        return session.execute(
            select(TypeCandidate).where(TypeCandidate.column_id == col.column_id)
        ).scalar_one()

    def test_qualified_key_patches_best_candidate(self, session: Session) -> None:
        table, col = self._setup(session, "src_abc123__bank_transactions")
        config = {"overrides": {"units": {"src_abc123__bank_transactions.amount": {"unit": "EUR"}}}}
        _apply_unit_overrides(session, config, table)
        tc = self._candidate(session, col)
        assert tc.detected_unit == "EUR"
        assert tc.unit_confidence == 1.0

    def test_raw_name_key_patches_despite_source_prefix(self, session: Session) -> None:
        # A human teaches the bare table name; the stored raw table is source-qualified.
        table, col = self._setup(session, "src_deadbeef__bank_transactions")
        config = {"overrides": {"units": {"bank_transactions.amount": {"unit": "USD"}}}}
        _apply_unit_overrides(session, config, table)
        tc = self._candidate(session, col)
        assert tc.detected_unit == "USD"
        assert tc.unit_confidence == 1.0

    def test_no_matching_key_leaves_candidate_untouched(self, session: Session) -> None:
        table, col = self._setup(session, "src_x__invoices")
        config = {"overrides": {"units": {"other.col": {"unit": "EUR"}}}}
        _apply_unit_overrides(session, config, table)
        tc = self._candidate(session, col)
        assert tc.detected_unit is None
        assert tc.unit_confidence is None

    def test_no_units_section_is_a_noop(self, session: Session) -> None:
        table, col = self._setup(session, "src_y__payments")
        _apply_unit_overrides(session, {}, table)
        tc = self._candidate(session, col)
        assert tc.detected_unit is None
