"""DAT-413 multi-run cut — coexisting runs + head-resolution.

This is the cut where two add_source runs over the same column coexist:

- The two add_source unique constraints are now ``(column_id, run_id)``, so a
  second run's ``TypeDecision`` / ``SemanticAnnotation`` for a column lands
  alongside the first run's row instead of colliding.
- ``head_run_id`` resolves the promoted run for one ``(table_id, stage)`` grain
  — the query-time picker an external reader uses to choose which run is current.
- ``load_persisted_readiness`` head-resolves: it returns only the promoted
  run's readiness rows per table, and treats a table with no promoted detect
  run as "no readiness" (graceful ``None``).
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, delete, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.typing.db_models import TypeDecision
from dataraum.entropy.db_models import EntropyReadinessRecord
from dataraum.entropy.views.readiness_context import load_persisted_readiness
from dataraum.storage import Column, MetadataSnapshotHead, Table, head_run_id, init_database


@pytest.fixture
def session_factory():
    """In-memory SQLite engine with all tables; FKs off so parent rows are optional."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _record):  # noqa: ANN001, ANN202
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    try:
        yield factory
    finally:
        engine.dispose()


def test_two_runs_type_decisions_for_same_column_coexist(session_factory: Any) -> None:
    """The widened ``(column_id, run_id)`` constraint lets two runs' decisions coexist."""
    with session_factory() as session:
        session.add_all(
            [
                TypeDecision(
                    session_id="sess-1",
                    column_id="col-1",
                    run_id="run-A",
                    decided_type="INTEGER",
                    decision_source="automatic",
                ),
                TypeDecision(
                    session_id="sess-1",
                    column_id="col-1",
                    run_id="run-B",
                    decided_type="BIGINT",
                    decision_source="automatic",
                ),
            ]
        )
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(select(TypeDecision).where(TypeDecision.column_id == "col-1")).scalars()
        )
    assert {r.run_id for r in rows} == {"run-A", "run-B"}
    assert len(rows) == 2


def test_typing_re_derivation_upserts_its_own_run_leaving_prior_run_intact(
    session_factory: Any,
) -> None:
    """Typing re-derivation upserts ``TypeDecision`` on ``(column_id, run_id)`` (DAT-413).

    The typing writers (``typing_phase`` strongly-typed path and
    ``resolution.resolve_types``) no longer delete-before-insert their
    ``TypeDecision`` rows — they ``upsert`` on the widened ``(column_id, run_id)``
    key, which is idempotent under a Temporal at-least-once retry (same run_id →
    the row updates in place, no duplicate) and still leaves a *prior* run's row
    untouched. Seed run-A's decision, then run run-B's writer twice (the retry)
    for the same column, and assert: run-A survives, run-B has exactly one row
    carrying the retry's value.
    """
    from dataraum.storage.upsert import upsert

    def _row(run_id: str, decided_type: str) -> dict[str, Any]:
        return {
            "session_id": "sess-1",
            "column_id": "col-1",
            "run_id": run_id,
            "decided_type": decided_type,
            "decision_source": "automatic",
        }

    with session_factory() as session:
        upsert(
            session,
            TypeDecision,
            [_row("run-A", "INTEGER")],
            index_elements=["column_id", "run_id"],
        )
        session.commit()

    # Run B's writer fires, then fires AGAIN (the at-least-once retry) with a
    # refreshed value — both via upsert, never a delete.
    with session_factory() as session:
        upsert(
            session, TypeDecision, [_row("run-B", "BIGINT")], index_elements=["column_id", "run_id"]
        )
        session.commit()
    with session_factory() as session:
        upsert(
            session, TypeDecision, [_row("run-B", "DOUBLE")], index_elements=["column_id", "run_id"]
        )
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(
                select(TypeDecision)
                .where(TypeDecision.column_id == "col-1")
                .order_by(TypeDecision.run_id)
            ).scalars()
        )
    # Two coexisting runs, one row each; run-A intact, run-B carries the retry value.
    assert [r.run_id for r in rows] == ["run-A", "run-B"]
    assert rows[0].decided_type == "INTEGER"
    assert rows[1].decided_type == "DOUBLE"


def test_two_runs_semantic_annotations_for_same_column_coexist(session_factory: Any) -> None:
    """The widened ``(column_id, run_id)`` constraint lets two runs' annotations coexist."""
    with session_factory() as session:
        session.add_all(
            [
                SemanticAnnotation(
                    session_id="sess-1",
                    column_id="col-1",
                    run_id="run-A",
                    semantic_role="measure",
                    annotation_source="llm",
                ),
                SemanticAnnotation(
                    session_id="sess-1",
                    column_id="col-1",
                    run_id="run-B",
                    semantic_role="dimension",
                    annotation_source="llm",
                ),
            ]
        )
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id == "col-1")
            ).scalars()
        )
    assert {r.run_id for r in rows} == {"run-A", "run-B"}
    assert len(rows) == 2


def test_head_run_id_returns_promoted_run(session_factory: Any) -> None:
    """``head_run_id`` resolves the promoted run for one ``(table_id, stage)``."""
    with session_factory() as session:
        session.add(
            MetadataSnapshotHead(table_id="tbl-1", stage="detect", run_id="run-B", version=3)
        )
        session.commit()

    with session_factory() as session:
        assert head_run_id(session, "tbl-1", "detect") == "run-B"
        # No head for this grain → None (the no-data fallback signal).
        assert head_run_id(session, "tbl-1", "statistics") is None
        assert head_run_id(session, "tbl-2", "detect") is None


def _seed_table_and_column(session: Session, table_id: str, column_id: str) -> None:
    """A minimal typed Table + Column so readiness records resolve to a target."""
    session.add(Table(table_id=table_id, source_id="src-1", table_name="orders", layer="typed"))
    session.add(
        Column(column_id=column_id, table_id=table_id, column_name="amount", column_position=0)
    )


def test_load_persisted_readiness_returns_only_promoted_run(session_factory: Any) -> None:
    """Two runs' readiness rows coexist; the loader head-resolves to the promoted one."""
    with session_factory() as session:
        _seed_table_and_column(session, "tbl-1", "col-1")
        # Two runs left a readiness row for the same column; only run-B is promoted.
        session.add_all(
            [
                EntropyReadinessRecord(
                    session_id="sess-1",
                    target="column:orders.amount",
                    source_id="src-1",
                    table_id="tbl-1",
                    column_id="col-1",
                    run_id="run-A",
                    band="blocked",
                ),
                EntropyReadinessRecord(
                    session_id="sess-1",
                    target="column:orders.amount",
                    source_id="src-1",
                    table_id="tbl-1",
                    column_id="col-1",
                    run_id="run-B",
                    band="ready",
                ),
                MetadataSnapshotHead(table_id="tbl-1", stage="detect", run_id="run-B"),
            ]
        )
        session.commit()

    with session_factory() as session:
        ctx = load_persisted_readiness(session, ["tbl-1"])

    # The promoted run-B row wins; the stale run-A "blocked" row is not mixed in.
    assert ctx.total_columns == 1
    col = ctx.columns["column:orders.amount"]
    assert col.readiness == "ready"
    assert ctx.overall_readiness == "ready"


def test_load_persisted_readiness_no_promoted_run_is_empty(session_factory: Any) -> None:
    """A table with readiness rows but no promoted detect head contributes nothing."""
    with session_factory() as session:
        _seed_table_and_column(session, "tbl-1", "col-1")
        session.add(
            EntropyReadinessRecord(
                session_id="sess-1",
                target="column:orders.amount",
                source_id="src-1",
                table_id="tbl-1",
                column_id="col-1",
                run_id="run-A",
                band="blocked",
            )
        )
        # No MetadataSnapshotHead row → no promoted run.
        session.commit()

    with session_factory() as session:
        ctx = load_persisted_readiness(session, ["tbl-1"])

    assert ctx.total_columns == 0
    assert ctx.overall_readiness == "ready"


def test_readiness_run_scoped_delete_leaves_prior_run_intact(session_factory: Any) -> None:
    """``persist_readiness``'s delete-before-insert is scoped to ``run_id`` (DAT-413).

    The terminal ``detect`` step clears THIS run's prior readiness rows before
    re-inserting, scoped to ``run_id`` so a re-run never wipes an earlier run's
    readiness (the head still points at the earlier run until ``promote`` flips
    it). Pins the exact run-scoped delete from ``readiness.persist_readiness`` —
    the last hole that would otherwise make ``detect`` destructive across runs.
    """
    with session_factory() as session:
        _seed_table_and_column(session, "tbl-1", "col-1")
        session.add(
            EntropyReadinessRecord(
                session_id="sess-1",
                target="column:orders.amount",
                source_id="src-1",
                table_id="tbl-1",
                column_id="col-1",
                run_id="run-A",
                band="ready",
            )
        )
        session.commit()

    # Run B's detect refresh: the run-scoped delete for the same table set.
    with session_factory() as session:
        stmt = delete(EntropyReadinessRecord).where(EntropyReadinessRecord.table_id.in_(["tbl-1"]))
        stmt = stmt.where(EntropyReadinessRecord.run_id == "run-B")
        session.execute(stmt)
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(
                select(EntropyReadinessRecord).where(EntropyReadinessRecord.table_id == "tbl-1")
            ).scalars()
        )
    # Run-A's readiness survives run-B's run-scoped delete.
    assert [r.run_id for r in rows] == ["run-A"]
    assert rows[0].band == "ready"
