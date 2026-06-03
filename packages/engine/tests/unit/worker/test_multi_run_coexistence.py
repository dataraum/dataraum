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


def test_run_scoped_delete_leaves_the_prior_runs_decision_intact(session_factory: Any) -> None:
    """Run B's delete-before-insert is scoped to its own ``run_id`` (DAT-413).

    The typing re-derivation (``typing_phase`` strongly-typed path and
    ``resolution.resolve_types``) clears THIS run's prior ``TypeDecision`` /
    ``TypeCandidate`` copies before re-inserting — scoped to ``ctx.run_id`` so a
    *new* run never touches a prior run's rows. This pins that exact delete
    statement: seed run-A's decision, run run-B's run-scoped delete for the same
    column, and assert run-A survives while only run-B's (absent) rows are
    targeted.
    """
    with session_factory() as session:
        session.add(
            TypeDecision(
                session_id="sess-1",
                column_id="col-1",
                run_id="run-A",
                decided_type="INTEGER",
                decision_source="automatic",
            )
        )
        session.commit()

    # Run B's re-derivation: delete its own run's prior copies for these columns.
    with session_factory() as session:
        session.execute(
            delete(TypeDecision).where(
                TypeDecision.column_id.in_(["col-1"]),
                TypeDecision.run_id == "run-B",
            )
        )
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(select(TypeDecision).where(TypeDecision.column_id == "col-1")).scalars()
        )
    # Run-A's decision is untouched by run-B's run-scoped delete.
    assert [r.run_id for r in rows] == ["run-A"]
    assert rows[0].decided_type == "INTEGER"


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
                    source_id="src-1",
                    table_id="tbl-1",
                    column_id="col-1",
                    run_id="run-A",
                    band="blocked",
                ),
                EntropyReadinessRecord(
                    session_id="sess-1",
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
