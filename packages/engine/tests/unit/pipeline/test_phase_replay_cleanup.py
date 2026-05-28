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

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.pipeline.phases.semantic_per_column_phase import SemanticPerColumnPhase
from dataraum.pipeline.phases.typing_phase import TypingPhase
from dataraum.storage import Column, Source, Table


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
    """``typing.replay_cleanup`` drops typed/quarantine for the scoped raw tables."""

    def test_scoped_to_table_ids_drops_matching_typed(self, session: Session) -> None:
        source, raw, typed = _seed_source_with_tables(session)
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw.table_id])

        # The typed Table row matching the raw is gone.
        remaining_typed = session.execute(
            select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
        ).all()
        assert remaining_typed == []
        # The raw Table row stays — only typing's outputs are cleaned.
        remaining_raw = session.execute(
            select(Table).where(Table.source_id == source.source_id, Table.layer == "raw")
        ).all()
        assert len(remaining_raw) == 1

    def test_leaves_sibling_typed_tables_alone(self, session: Session) -> None:
        """A teach on table A must not clobber table B's typed state."""
        source, raw_a, typed_a = _seed_source_with_tables(session)
        # Add a sibling pair (different table_name).
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
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [raw_a.table_id])

        # typed_a is gone; typed_b survives.
        remaining = {
            row.table_name
            for row in session.execute(
                select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
            ).scalars()
        }
        assert remaining == {"customers"}

    def test_empty_table_ids_means_source_wide(self, session: Session) -> None:
        source, raw_a, _typed_a = _seed_source_with_tables(session)
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
        ctx = _make_ctx(session, source.source_id)

        TypingPhase().replay_cleanup(ctx, [])

        remaining_typed = session.execute(
            select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
        ).all()
        assert remaining_typed == []

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
