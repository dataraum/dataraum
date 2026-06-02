"""Tests for the per-phase ``replay_cleanup`` hook (DAT-343 P4).

The hook is invoked by the worker activity wrapper before ``run`` when
``replay.from_phase`` matches the phase's name. Each entry-point phase
(``import`` / ``typing`` / ``semantic_per_column``) drops its own outputs
so the rerun starts from a known-clean state and the phase's existing
``should_skip`` doesn't immediately return "already done".

Tested at the Postgres-row layer: pure SQLite fixture, no DuckLake. A
``StubDuckDB`` records the SQL the cleanup emits so we can assert the
right ``DROP TABLE IF EXISTS`` statements without standing up a real
DuckDB. The CSV / typing / quarantine SQL paths themselves are covered
by their integration tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.pipeline.phases.relationships_phase import RelationshipsPhase
from dataraum.pipeline.phases.semantic_per_column_phase import SemanticPerColumnPhase
from dataraum.pipeline.phases.semantic_per_table_phase import SemanticPerTablePhase
from dataraum.pipeline.phases.typing_phase import TypingPhase
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_session_id


class _StubDuckDB:
    """Records ``execute`` SQL so tests can assert DROP statements without DuckDB.

    Cleanup paths only call ``execute(sql)``; that's the minimum surface.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self.statements.append(sql)
        return self


def _make_ctx(session: Session, source_id: str) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=_StubDuckDB(),  # type: ignore[arg-type]
        source_id=source_id,
        session_id="00000000-0000-0000-0000-000000000001",
    )


def _seed_source_with_tables(session: Session) -> tuple[Source, Table, Table]:
    """Return a Source with one raw + one matching typed Table (one column each).

    The raw and typed Tables share ``table_name`` (typing convention) so the
    typed cleanup's name-match finds them.
    """
    source = Source(
        source_id=str(uuid4()),
        name=f"src_{uuid4().hex[:8]}",
        source_type="csv",
    )
    session.add(source)
    session.flush()

    raw = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="orders",
        layer="raw",
        duckdb_path=f"{source.name}__orders",
    )
    typed = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="orders",
        layer="typed",
        duckdb_path=f"{source.name}__orders",
    )
    session.add_all([raw, typed])
    session.flush()

    raw_col = Column(
        column_id=str(uuid4()),
        table_id=raw.table_id,
        column_name="order_id",
        column_position=0,
        raw_type="VARCHAR",
    )
    typed_col = Column(
        column_id=str(uuid4()),
        table_id=typed.table_id,
        column_name="order_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="BIGINT",
    )
    session.add_all([raw_col, typed_col])
    session.flush()

    return source, raw, typed


def _seed_semantic_annotation(session: Session, column_id: str) -> None:
    session.add(
        SemanticAnnotation(
            annotation_id=str(uuid4()),
            column_id=column_id,
            semantic_role="identifier",
            annotation_source="llm",
            annotated_at=datetime.now(UTC),
        )
    )
    session.flush()


# ---------------------------------------------------------------------------
# BasePhase default — no-op.
# ---------------------------------------------------------------------------


class TestBasePhaseDefault:
    """Phases that don't override ``replay_cleanup`` are a no-op."""

    def test_default_does_nothing(self, session: Session) -> None:
        source, _raw, _typed = _seed_source_with_tables(session)

        class _Anon(BasePhase):
            @property
            def name(self) -> str:
                return "anon"

            def _run(self, ctx: PhaseContext):  # pragma: no cover - not invoked
                raise AssertionError("should not run in this test")

        phase = _Anon()
        ctx = _make_ctx(session, source.source_id)

        # No raise; no touch to the session or duckdb.
        result = phase.replay_cleanup(ctx, [])

        assert result is None
        assert session.execute(select(Table).where(Table.source_id == source.source_id)).all()
        assert ctx.duckdb_conn.statements == []  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# ImportPhase — source-wide drop of raw/typed/quarantine + DuckDB tables.
# ---------------------------------------------------------------------------


class TestImportPhaseReplayCleanup:
    """``import.replay_cleanup`` drops every Table + DuckDB artifact for the source."""

    def test_drops_all_layers_for_source(self, session: Session) -> None:
        source, raw, typed = _seed_source_with_tables(session)
        ctx = _make_ctx(session, source.source_id)

        ImportPhase().replay_cleanup(ctx, [])

        remaining = session.execute(select(Table).where(Table.source_id == source.source_id)).all()
        assert remaining == []

    def test_emits_drop_for_each_layer(self, session: Session) -> None:
        source, raw, typed = _seed_source_with_tables(session)
        ctx = _make_ctx(session, source.source_id)

        ImportPhase().replay_cleanup(ctx, [])

        stmts = ctx.duckdb_conn.statements  # type: ignore[attr-defined]
        # Two Table rows seeded (raw + typed), each carrying a duckdb_path.
        assert len(stmts) == 2
        assert all(s.startswith("DROP TABLE IF EXISTS") for s in stmts)
        # Bare name (source__table) appears in both DROPs.
        bare = f"{source.name}__orders"
        assert all(bare in s for s in stmts)

    def test_no_op_when_source_has_no_tables(self, session: Session) -> None:
        source = Source(
            source_id=str(uuid4()),
            name=f"empty_{uuid4().hex[:8]}",
            source_type="csv",
        )
        session.add(source)
        session.flush()
        ctx = _make_ctx(session, source.source_id)

        ImportPhase().replay_cleanup(ctx, [])  # no raise

        assert ctx.duckdb_conn.statements == []  # type: ignore[attr-defined]

    def test_cascade_drops_column_rows(self, session: Session) -> None:
        source, raw, typed = _seed_source_with_tables(session)
        _seed_semantic_annotation(
            session,
            column_id=session.execute(
                select(Column.column_id).where(Column.table_id == typed.table_id)
            ).scalar_one(),
        )
        ctx = _make_ctx(session, source.source_id)

        ImportPhase().replay_cleanup(ctx, [])

        # SemanticAnnotation cascade-deletes when its Column is dropped via
        # the typed Table cascade. End state: zero rows for this source.
        assert session.execute(select(Column).where(Column.table_id == typed.table_id)).all() == []
        assert (
            session.execute(
                select(SemanticAnnotation).join(Column).where(Column.table_id == typed.table_id)
            ).all()
            == []
        )


# ---------------------------------------------------------------------------
# TypingPhase — per-raw-table-id drop of matching typed + quarantine.
# ---------------------------------------------------------------------------


class TestTypingPhaseReplayCleanup:
    """``typing.replay_cleanup`` clears typing's own state IN PLACE (DAT-373).

    The typed/quarantine ``Table`` + ``Column`` rows survive a re-type (stable
    identity); only typing-owned ``TypeCandidate`` / ``TypeDecision`` rows and the
    DuckDB typed/quarantine tables are cleared.
    """

    def test_scoped_to_table_ids_keeps_typed_table(self, session: Session) -> None:
        source, raw, typed = _seed_source_with_tables(session)
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw.table_id])

        # The typed Table row matching the raw SURVIVES with the same id —
        # ``_run`` reconciles it in place on the re-type.
        remaining_typed = session.execute(
            select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
        ).scalar_one()
        assert remaining_typed.table_id == typed.table_id
        # The raw Table row stays too.
        remaining_raw = session.execute(
            select(Table).where(Table.source_id == source.source_id, Table.layer == "raw")
        ).all()
        assert len(remaining_raw) == 1

    def test_leaves_sibling_table_decisions_alone(self, session: Session) -> None:
        """A teach on table A must not clear table B's typing state."""
        source, raw_a, _typed_a = _seed_source_with_tables(session)
        # Add a sibling pair (different table_name) with a typed column + decision.
        raw_b = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="customers",
            layer="raw",
            duckdb_path=f"{source.name}__customers",
        )
        typed_b = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="customers",
            layer="typed",
            duckdb_path=f"{source.name}__customers",
        )
        session.add_all([raw_b, typed_b])
        session.flush()
        typed_b_col = Column(
            column_id=str(uuid4()),
            table_id=typed_b.table_id,
            column_name="customer_id",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="BIGINT",
        )
        session.add(typed_b_col)
        session.flush()
        session.add(
            TypeDecision(
                decision_id=str(uuid4()),
                column_id=typed_b_col.column_id,
                decided_type="BIGINT",
                decision_source="automatic",
                decided_at=datetime.now(UTC),
            )
        )
        session.flush()
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw_a.table_id])

        # Both typed tables survive; sibling B's TypeDecision is untouched.
        remaining = {
            row.table_name
            for row in session.execute(
                select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
            ).scalars()
        }
        assert remaining == {"orders", "customers"}
        assert (
            session.execute(
                select(TypeDecision).where(TypeDecision.column_id == typed_b_col.column_id)
            ).first()
            is not None
        )

    def test_empty_table_ids_clears_source_wide_decisions(self, session: Session) -> None:
        source, raw_a, typed_a = _seed_source_with_tables(session)
        # Seed a TypeDecision on the typed column so we can prove it's cleared.
        typed_a_col_id = session.execute(
            select(Column.column_id).where(Column.table_id == typed_a.table_id)
        ).scalar_one()
        session.add(
            TypeDecision(
                decision_id=str(uuid4()),
                column_id=typed_a_col_id,
                decided_type="BIGINT",
                decision_source="automatic",
                decided_at=datetime.now(UTC),
            )
        )
        session.flush()
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [])

        # Typed table survives; its decision is cleared (source-wide re-type).
        assert (
            session.execute(
                select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
            )
            .scalar_one()
            .table_id
            == typed_a.table_id
        )
        assert (
            session.execute(
                select(TypeDecision).where(TypeDecision.column_id == typed_a_col_id)
            ).all()
            == []
        )

    def test_drops_type_candidates_for_raw_columns(self, session: Session) -> None:
        source, raw, typed = _seed_source_with_tables(session)
        raw_col_id = session.execute(
            select(Column.column_id).where(Column.table_id == raw.table_id)
        ).scalar_one()
        session.add(
            TypeCandidate(
                candidate_id=str(uuid4()),
                column_id=raw_col_id,
                detected_at=datetime.now(UTC),
                data_type="VARCHAR",
                confidence=0.5,
            )
        )
        session.add(
            TypeDecision(
                decision_id=str(uuid4()),
                column_id=raw_col_id,
                decided_type="VARCHAR",
                decision_source="inferred",
                decided_at=datetime.now(UTC),
            )
        )
        session.flush()
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw.table_id])

        # Both gone — they're FK'd to the (surviving) raw column, not the typed,
        # so cascade doesn't reach them.
        assert (
            session.execute(
                select(TypeCandidate).where(TypeCandidate.column_id == raw_col_id)
            ).all()
            == []
        )
        assert (
            session.execute(select(TypeDecision).where(TypeDecision.column_id == raw_col_id)).all()
            == []
        )

    def test_emits_drop_for_typed_and_quarantine(self, session: Session) -> None:
        source, raw, _typed = _seed_source_with_tables(session)
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw.table_id])

        stmts = ctx.duckdb_conn.statements  # type: ignore[attr-defined]
        # One raw table in scope → 2 DROPs (typed, quarantine).
        assert len(stmts) == 2
        bare = f"{source.name}__orders"
        # schema is unquoted in the SQL (``lake.typed."bare"``), table is quoted.
        assert any(f'.typed."{bare}"' in s for s in stmts)
        assert any(f'.quarantine."{bare}"' in s for s in stmts)

    def test_no_op_when_raw_table_unknown(self, session: Session) -> None:
        source, _raw, _typed = _seed_source_with_tables(session)
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, ["nonexistent-raw-id"])

        # Typed Table stays; no DuckDB statements emitted.
        remaining = session.execute(
            select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
        ).all()
        assert len(remaining) == 1
        assert ctx.duckdb_conn.statements == []  # type: ignore[attr-defined]

    def test_preserves_typed_table_and_columns(self, session: Session) -> None:
        """The typed Table + its Column rows survive a re-type (DAT-373 Option A).

        Pre-DAT-373 the cleanup dropped the typed ``Table`` and cascade-wiped
        every Column. With stable typed identity, ``replay_cleanup`` rebuilds
        only the DuckDB data + typing's own Postgres rows in place — the typed
        ``Table`` and ``Column`` rows (and their ids) are kept so other stages'
        per-Column findings stay attached.
        """
        source, raw, typed = _seed_source_with_tables(session)
        typed_col_id = session.execute(
            select(Column.column_id).where(Column.table_id == typed.table_id)
        ).scalar_one()
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw.table_id])

        # The typed Table row and its Column survive with the SAME ids.
        surviving_typed = session.execute(
            select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
        ).scalar_one()
        assert surviving_typed.table_id == typed.table_id
        surviving_col = session.execute(
            select(Column.column_id).where(Column.table_id == typed.table_id)
        ).scalar_one()
        assert surviving_col == typed_col_id

    def test_preserves_other_stage_per_column_rows(self, session: Session) -> None:
        """A type-teach replay must NOT wipe another stage's per-Column rows (DAT-373).

        The hazard: a future stage (``begin_session`` / frame-ground) attaches a
        ``SemanticAnnotation`` (here standing in for any other-stage per-Column
        finding) to a typed column. A ``type_pattern`` teach re-typing that table
        must leave the annotation intact — typing owns only its own rows, not the
        whole typed ``Table``. Pre-DAT-373 the cascade through the dropped typed
        ``Table`` wiped it; now the row survives.
        """
        source, raw, typed = _seed_source_with_tables(session)
        typed_col_id = session.execute(
            select(Column.column_id).where(Column.table_id == typed.table_id)
        ).scalar_one()
        _seed_semantic_annotation(session, typed_col_id)
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw.table_id])

        surviving = session.execute(
            select(SemanticAnnotation).where(SemanticAnnotation.column_id == typed_col_id)
        ).first()
        assert surviving is not None, (
            "type-teach replay wiped another stage's per-Column row "
            "(cross-stage data loss — DAT-373 hazard)"
        )

    def test_drops_type_candidates_for_typed_columns(self, session: Session) -> None:
        """Typing's own copies on the typed column are cleared (in-place rebuild).

        ``resolve_types`` copies ``TypeCandidate`` / ``TypeDecision`` onto the
        typed column. Because the typed ``Column`` now survives the replay, the
        cleanup must delete those copies explicitly so the re-run's fresh inserts
        don't collide with the ``uq_column_type_decision`` unique constraint.
        """
        source, raw, typed = _seed_source_with_tables(session)
        typed_col_id = session.execute(
            select(Column.column_id).where(Column.table_id == typed.table_id)
        ).scalar_one()
        session.add(
            TypeCandidate(
                candidate_id=str(uuid4()),
                column_id=typed_col_id,
                detected_at=datetime.now(UTC),
                data_type="BIGINT",
                confidence=0.9,
            )
        )
        session.add(
            TypeDecision(
                decision_id=str(uuid4()),
                column_id=typed_col_id,
                decided_type="BIGINT",
                decision_source="automatic",
                decided_at=datetime.now(UTC),
            )
        )
        session.flush()
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw.table_id])

        assert (
            session.execute(
                select(TypeCandidate).where(TypeCandidate.column_id == typed_col_id)
            ).all()
            == []
        )
        assert (
            session.execute(
                select(TypeDecision).where(TypeDecision.column_id == typed_col_id)
            ).all()
            == []
        )


# ---------------------------------------------------------------------------
# SemanticPerColumnPhase — source-wide annotation drop.
# ---------------------------------------------------------------------------


class TestSemanticPerColumnReplayCleanup:
    """``semantic_per_column.replay_cleanup`` drops every annotation for the source."""

    def test_drops_all_annotations_for_source(self, session: Session) -> None:
        source, _raw, typed = _seed_source_with_tables(session)
        typed_col_id = session.execute(
            select(Column.column_id).where(Column.table_id == typed.table_id)
        ).scalar_one()
        _seed_semantic_annotation(session, typed_col_id)
        ctx = _make_ctx(session, source.source_id)

        SemanticPerColumnPhase().replay_cleanup(ctx, [])

        remaining = session.execute(
            select(SemanticAnnotation).where(SemanticAnnotation.column_id == typed_col_id)
        ).all()
        assert remaining == []

    def test_ignores_table_ids_arg(self, session: Session) -> None:
        """The reduce is source-wide; ``table_ids`` doesn't narrow it."""
        source, _raw, typed = _seed_source_with_tables(session)
        typed_col_id = session.execute(
            select(Column.column_id).where(Column.table_id == typed.table_id)
        ).scalar_one()
        _seed_semantic_annotation(session, typed_col_id)
        ctx = _make_ctx(session, source.source_id)

        SemanticPerColumnPhase().replay_cleanup(ctx, ["pretend-table-id"])

        # Annotation deleted despite the bogus scope — proves the arg is ignored.
        assert (
            session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id == typed_col_id)
            ).all()
            == []
        )

    def test_leaves_other_sources_untouched(self, session: Session) -> None:
        source_a, _raw_a, typed_a = _seed_source_with_tables(session)
        source_b, _raw_b, typed_b = _seed_source_with_tables(session)
        col_a = session.execute(
            select(Column.column_id).where(Column.table_id == typed_a.table_id)
        ).scalar_one()
        col_b = session.execute(
            select(Column.column_id).where(Column.table_id == typed_b.table_id)
        ).scalar_one()
        _seed_semantic_annotation(session, col_a)
        _seed_semantic_annotation(session, col_b)
        ctx = _make_ctx(session, source_a.source_id)

        SemanticPerColumnPhase().replay_cleanup(ctx, [])

        # source_a's annotation gone; source_b's remains.
        assert (
            session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id == col_a)
            ).all()
            == []
        )
        assert (
            session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id == col_b)
            ).first()
            is not None
        )

    def test_no_op_when_source_has_no_typed_tables(self, session: Session) -> None:
        source = Source(
            source_id=str(uuid4()),
            name=f"empty_{uuid4().hex[:8]}",
            source_type="csv",
        )
        session.add(source)
        session.flush()
        ctx = _make_ctx(session, source.source_id)

        SemanticPerColumnPhase().replay_cleanup(ctx, [])  # no raise


# ---------------------------------------------------------------------------
# begin_session phases (DAT-401) — source-free, scoped by ``table_ids``.
# Each clears ONLY its own rows for the scoped tables; the parent ``Table``
# survives, proving the FK cascade is NOT load-bearing.
# ---------------------------------------------------------------------------


def _session_ctx(session: Session, table_ids: list[str]) -> PhaseContext:
    """Source-free ctx for the begin_session phases (DAT-401)."""
    return PhaseContext(
        session=session,
        duckdb_conn=_StubDuckDB(),  # type: ignore[arg-type]
        table_ids=table_ids,
        session_id=baseline_session_id(),
    )


def _typed_table_with_col(session: Session, name: str, n_cols: int = 1) -> tuple[Table, Column]:
    """A typed Table (under its own Source) + ``n_cols`` columns; returns the first."""
    source = Source(source_id=str(uuid4()), name=f"src_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()
    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name=name,
        layer="typed",
        duckdb_path=f"{source.name}__{name}",
    )
    session.add(table)
    session.flush()
    cols = [
        Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name=f"{name}_{i}",
            column_position=i,
            raw_type="VARCHAR",
            resolved_type="BIGINT",
        )
        for i in range(n_cols)
    ]
    session.add_all(cols)
    session.flush()
    return table, cols[0]


def _other_session(session: Session) -> str:
    """A second InvestigationSession id (distinct from the baseline fixture session)."""
    from dataraum.investigation.db_models import InvestigationSession

    sid = str(uuid4())
    session.add(InvestigationSession(session_id=sid, intent="other", status="active"))
    session.flush()
    return sid


def _rel(
    session: Session,
    from_t: Table,
    from_c: Column,
    to_t: Table,
    to_c: Column,
    detection_method: str,
    session_id: str | None = None,
) -> Relationship:
    rel = Relationship(
        relationship_id=str(uuid4()),
        session_id=session_id or baseline_session_id(),
        from_table_id=from_t.table_id,
        from_column_id=from_c.column_id,
        to_table_id=to_t.table_id,
        to_column_id=to_c.column_id,
        relationship_type="candidate" if detection_method == "candidate" else "foreign_key",
        confidence=0.8,
        detection_method=detection_method,
        is_confirmed=False,
    )
    session.add(rel)
    session.flush()
    return rel


def _remaining_rel_ids(session: Session) -> set[str]:
    return {r.relationship_id for r in session.execute(select(Relationship)).scalars()}


class TestRelationshipsReplayCleanup:
    """``relationships.replay_cleanup`` drops only its candidate rows, in scope."""

    def test_drops_only_candidate_rels_for_scoped_tables(self, session: Session) -> None:
        ta, ca = _typed_table_with_col(session, "a")
        tb, cb = _typed_table_with_col(session, "b")
        tc, cc = _typed_table_with_col(session, "c")  # out of selection
        td, cd = _typed_table_with_col(session, "d")  # out of selection

        cand_ab = _rel(session, ta, ca, tb, cb, "candidate")  # in scope, ours → gone
        llm_ab = _rel(session, ta, ca, tb, cb, "llm")  # in scope, NOT ours → survives
        cand_cd = _rel(session, tc, cc, td, cd, "candidate")  # out of scope → survives

        RelationshipsPhase().replay_cleanup(_session_ctx(session, []), [ta.table_id, tb.table_id])

        remaining = _remaining_rel_ids(session)
        assert cand_ab.relationship_id not in remaining
        assert llm_ab.relationship_id in remaining
        assert cand_cd.relationship_id in remaining
        # Cascade is NOT load-bearing: parent tables untouched by the cleanup.
        assert session.get(Table, ta.table_id) is not None
        assert session.get(Table, tb.table_id) is not None

    def test_empty_scope_is_a_noop(self, session: Session) -> None:
        ta, ca = _typed_table_with_col(session, "a")
        tb, cb = _typed_table_with_col(session, "b")
        cand_ab = _rel(session, ta, ca, tb, cb, "candidate")

        RelationshipsPhase().replay_cleanup(_session_ctx(session, []), [])

        assert cand_ab.relationship_id in _remaining_rel_ids(session)

    def test_cleanup_spares_another_sessions_candidates(self, session: Session) -> None:
        """A replay of THIS session must not delete another session's candidates.

        The unique constraint is on the column pair, so the two sessions use
        distinct column pairs over the same two tables — both in cleanup scope.
        Only the replaying session's row is deleted (DAT-401 session scoping).
        """
        ta, _ = _typed_table_with_col(session, "a", n_cols=2)
        tb, _ = _typed_table_with_col(session, "b", n_cols=2)
        ca = list(
            session.execute(
                select(Column)
                .where(Column.table_id == ta.table_id)
                .order_by(Column.column_position)
            ).scalars()
        )
        cb = list(
            session.execute(
                select(Column)
                .where(Column.table_id == tb.table_id)
                .order_by(Column.column_position)
            ).scalars()
        )
        mine = _rel(session, ta, ca[0], tb, cb[0], "candidate")  # baseline session
        other = _other_session(session)
        theirs = _rel(session, ta, ca[1], tb, cb[1], "candidate", session_id=other)

        RelationshipsPhase().replay_cleanup(_session_ctx(session, []), [ta.table_id, tb.table_id])

        remaining = _remaining_rel_ids(session)
        assert mine.relationship_id not in remaining
        assert theirs.relationship_id in remaining


class TestSemanticPerTableReplayCleanup:
    """``semantic_per_table.replay_cleanup`` drops its entities + llm rels, in scope."""

    def _entity(self, session: Session, table: Table) -> TableEntity:
        entity = TableEntity(
            session_id=baseline_session_id(),
            table_id=table.table_id,
            detected_entity_type="thing",
            confidence=0.9,
            detection_source="llm",
        )
        session.add(entity)
        session.flush()
        return entity

    def test_drops_entities_and_llm_rels_for_scoped_tables(self, session: Session) -> None:
        ta, ca = _typed_table_with_col(session, "a")
        tb, cb = _typed_table_with_col(session, "b")
        tc, cc = _typed_table_with_col(session, "c")  # out of selection
        td, cd = _typed_table_with_col(session, "d")  # out of selection

        ent_a = self._entity(session, ta)  # in scope → gone
        ent_c = self._entity(session, tc)  # out of scope → survives
        llm_ab = _rel(session, ta, ca, tb, cb, "llm")  # in scope, ours → gone
        cand_ab = _rel(session, ta, ca, tb, cb, "candidate")  # in scope, NOT ours → survives
        llm_cd = _rel(session, tc, cc, td, cd, "llm")  # out of scope → survives

        SemanticPerTablePhase().replay_cleanup(
            _session_ctx(session, []), [ta.table_id, tb.table_id]
        )

        remaining_entities = {e.entity_id for e in session.execute(select(TableEntity)).scalars()}
        assert ent_a.entity_id not in remaining_entities
        assert ent_c.entity_id in remaining_entities

        remaining = _remaining_rel_ids(session)
        assert llm_ab.relationship_id not in remaining
        assert cand_ab.relationship_id in remaining
        assert llm_cd.relationship_id in remaining
        # Cascade is NOT load-bearing: parent tables untouched by the cleanup.
        assert session.get(Table, ta.table_id) is not None
        assert session.get(Table, tb.table_id) is not None

    def test_empty_scope_is_a_noop(self, session: Session) -> None:
        ta, _ = _typed_table_with_col(session, "a")
        ent = self._entity(session, ta)

        SemanticPerTablePhase().replay_cleanup(_session_ctx(session, []), [])

        assert ent.entity_id in {
            e.entity_id for e in session.execute(select(TableEntity)).scalars()
        }

    def test_cleanup_spares_another_sessions_entities(self, session: Session) -> None:
        """A replay of THIS session must not delete another session's entities."""
        ta, _ = _typed_table_with_col(session, "a")
        mine = self._entity(session, ta)  # baseline session
        other = _other_session(session)
        theirs = TableEntity(
            session_id=other,
            table_id=ta.table_id,
            detected_entity_type="thing",
            confidence=0.9,
            detection_source="llm",
        )
        session.add(theirs)
        session.flush()

        SemanticPerTablePhase().replay_cleanup(_session_ctx(session, []), [ta.table_id])

        remaining = {e.entity_id for e in session.execute(select(TableEntity)).scalars()}
        assert mine.entity_id not in remaining
        assert theirs.entity_id in remaining
