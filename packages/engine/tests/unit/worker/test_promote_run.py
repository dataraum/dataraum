"""Unit tests for the DAT-413/506 ``promote_run`` terminal step.

``promote_run`` upserts ONE :class:`MetadataSnapshotHead` per table under
``(table:{id}, GENERATION_STAGE)`` for the run's tables (DAT-506 collapsed the
per-stage head axis to one generation head per table): first promote inserts the
head; a second promote (same tables, new run) re-points ``run_id`` in place. The
run's tables resolve via ``tables_for_run`` (the run-table anchor).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.storage import GENERATION_STAGE, MetadataSnapshotHead, init_database
from dataraum.worker import activity as activity_mod
from dataraum.worker.activity import promote_run
from dataraum.worker.contracts import RunRef


@pytest.fixture
def session_factory():
    """In-memory SQLite engine with all tables; FKs off so we skip parent rows."""
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


def _manager(session_factory: Any) -> Any:
    """A fake ConnectionManager exposing ``session_scope`` over the real engine."""

    class _Manager:
        @contextmanager
        def session_scope(self):  # noqa: ANN202
            session = session_factory()
            try:
                yield session
                session.commit()
            finally:
                session.close()

    return _Manager()


def _heads(session_factory: Any) -> list[MetadataSnapshotHead]:
    with session_factory() as s:
        return list(s.execute(select(MetadataSnapshotHead)).scalars().all())


def test_promote_run_upserts_one_generation_head_per_table(monkeypatch, session_factory):
    """First promote inserts one generation head per table; a re-run re-points it."""
    # Source-free, session-free RunRef — the workflow threads exactly this into the
    # terminal promote (DAT-506/426); the test feeds exactly what production feeds.
    identity = RunRef(workspace_id="ws-1", run_id="run-A")
    table_ids = ["tbl-1", "tbl-2"]

    monkeypatch.setattr(activity_mod, "tables_for_run", lambda session, run_id: list(table_ids))

    manager = _manager(session_factory)

    # First promote: ONE generation head per table, all at run-A.
    promoted = promote_run(manager, identity)
    assert promoted == len(table_ids)

    heads = _heads(session_factory)
    assert len(heads) == len(table_ids)
    assert {(h.target, h.stage) for h in heads} == {
        (f"table:{t}", GENERATION_STAGE) for t in table_ids
    }
    assert all(h.run_id == "run-A" for h in heads)

    # Second promote with a new run: no new rows, run_id re-pointed.
    identity_b = identity.model_copy(update={"run_id": "run-B"})
    promoted_again = promote_run(manager, identity_b)
    assert promoted_again == len(table_ids)

    heads_after = _heads(session_factory)
    assert len(heads_after) == len(table_ids)  # upsert, not insert
    assert all(h.run_id == "run-B" for h in heads_after)


def test_promote_run_no_tables_is_noop(monkeypatch, session_factory):
    """An empty run-table set promotes nothing (logged warning, no rows)."""
    identity = RunRef(workspace_id="ws-1", run_id="run-A")
    monkeypatch.setattr(activity_mod, "tables_for_run", lambda session, run_id: [])

    assert promote_run(_manager(session_factory), identity) == 0
    assert _heads(session_factory) == []


def test_promote_run_requires_run_id(monkeypatch, session_factory):
    """A missing run_id is a caller bug — fail loud rather than write a NULL head."""
    identity = RunRef(workspace_id="ws-1")

    with pytest.raises(RuntimeError, match="requires a stamped run.run_id"):
        promote_run(_manager(session_factory), identity)
