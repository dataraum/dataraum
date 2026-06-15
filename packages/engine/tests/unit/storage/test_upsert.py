"""Dialect-aware ``upsert`` helper — idempotency on SQLite (DAT-413).

The Temporal at-least-once contract means a one-row-per-column writer can re-run
with the SAME ``run_id`` after a worker commits-then-crashes. ``upsert`` makes
that write idempotent: a second insert on the same ``(column_id, run_id)`` key
updates the row in place (no duplicate, no raise), while a different ``run_id``
coexists. SQLite stands in for prod Postgres here — both expose
``on_conflict_do_update`` via their dialect ``insert``.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.typing.db_models import TypeDecision
from dataraum.storage import init_database
from dataraum.storage.upsert import upsert


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


def _decision(column_id: str, run_id: str, decided_type: str) -> dict[str, Any]:
    """A ``TypeDecision`` row dict with the uuid PK omitted (default applies)."""
    return {
        "column_id": column_id,
        "run_id": run_id,
        "decided_type": decided_type,
        "decision_source": "automatic",
    }


def test_upsert_applies_pk_default_when_omitted(session_factory: Any) -> None:
    """Omitting the uuid PK still applies the model's Python-side default."""
    with session_factory() as session:
        upsert(
            session,
            TypeDecision,
            [_decision("col-1", "run-A", "INTEGER")],
            index_elements=["column_id", "run_id"],
        )
        session.commit()

    with session_factory() as session:
        row = session.execute(select(TypeDecision)).scalar_one()
    assert row.decision_id is not None  # uuid default fired even though omitted
    assert row.decided_type == "INTEGER"


def test_upsert_same_key_updates_in_place(session_factory: Any) -> None:
    """A re-insert on the same ``(column_id, run_id)`` updates — no duplicate, no raise.

    This is the at-least-once retry: the activity re-runs with the SAME run_id.
    """
    with session_factory() as session:
        upsert(
            session,
            TypeDecision,
            [_decision("col-1", "run-A", "INTEGER")],
            index_elements=["column_id", "run_id"],
        )
        session.commit()

    with session_factory() as session:
        upsert(
            session,
            TypeDecision,
            [_decision("col-1", "run-A", "BIGINT")],
            index_elements=["column_id", "run_id"],
        )
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(select(TypeDecision).where(TypeDecision.column_id == "col-1")).scalars()
        )
    assert len(rows) == 1  # no duplicate
    assert rows[0].decided_type == "BIGINT"  # second call's value won


def test_upsert_different_run_id_coexists(session_factory: Any) -> None:
    """A second ``run_id`` for the same column lands alongside the first."""
    with session_factory() as session:
        upsert(
            session,
            TypeDecision,
            [_decision("col-1", "run-A", "INTEGER")],
            index_elements=["column_id", "run_id"],
        )
        upsert(
            session,
            TypeDecision,
            [_decision("col-1", "run-B", "BIGINT")],
            index_elements=["column_id", "run_id"],
        )
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(select(TypeDecision).where(TypeDecision.column_id == "col-1")).scalars()
        )
    assert {r.run_id for r in rows} == {"run-A", "run-B"}
    assert len(rows) == 2


def test_upsert_empty_rows_is_a_noop(session_factory: Any) -> None:
    """An empty row list writes nothing (and does not raise)."""
    with session_factory() as session:
        upsert(session, TypeDecision, [], index_elements=["column_id", "run_id"])
        session.commit()
        assert session.scalar(select(func.count()).select_from(TypeDecision)) == 0


def test_upsert_multi_row_insert_then_retry(session_factory: Any) -> None:
    """A multi-row insert, then a same-key retry, yields one row per key with new values."""
    with session_factory() as session:
        upsert(
            session,
            TypeDecision,
            [_decision("c1", "run-A", "INTEGER"), _decision("c2", "run-A", "VARCHAR")],
            index_elements=["column_id", "run_id"],
        )
        session.commit()

    with session_factory() as session:
        upsert(
            session,
            TypeDecision,
            [_decision("c1", "run-A", "BIGINT"), _decision("c2", "run-A", "DOUBLE")],
            index_elements=["column_id", "run_id"],
        )
        session.commit()

    with session_factory() as session:
        rows = {
            r.column_id: r.decided_type for r in session.execute(select(TypeDecision)).scalars()
        }
    assert rows == {"c1": "BIGINT", "c2": "DOUBLE"}
