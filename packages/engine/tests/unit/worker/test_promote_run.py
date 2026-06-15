"""Unit tests for the DAT-413 ``promote_run`` terminal step (Phase 2).

``promote_run`` upserts one :class:`MetadataSnapshotHead` per
``(table_id, stage)`` for the run's tables: first promote inserts at
``version=0``; a second promote (same tables) re-points ``run_id`` and bumps
``version``. Behavior-preserving — nothing reads the head yet, this only pins
that the write side is a correct upsert.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.storage import MetadataSnapshotHead, init_database
from dataraum.worker import activity as activity_mod
from dataraum.worker.activity import _PROMOTE_STAGES, promote_run
from dataraum.worker.contracts import SourceIdentity


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


def test_promote_run_upserts_one_head_per_table_stage(monkeypatch, session_factory):
    """First promote inserts a v0 head per (table_id, stage); a re-run bumps + re-points."""
    # Source-free: AddSourceWorkflow threads source_id=None into the terminal
    # promote (DAT-422/426) — the test feeds exactly what production feeds.
    identity = SourceIdentity(
        workspace_id="ws-1",
        session_id="sess-1",
        run_id="run-A",
    )
    table_ids = ["tbl-1", "tbl-2"]

    monkeypatch.setattr(activity_mod, "tables_for_session", lambda session, sid: list(table_ids))

    manager = _manager(session_factory)

    # First promote: one head per (table, stage), all at run-A.
    promoted = promote_run(manager, identity)
    assert promoted == len(table_ids) * len(_PROMOTE_STAGES)

    heads = _heads(session_factory)
    assert len(heads) == len(table_ids) * len(_PROMOTE_STAGES)
    assert {(h.target, h.stage) for h in heads} == {
        (f"table:{t}", s) for t in table_ids for s in _PROMOTE_STAGES
    }
    assert all(h.run_id == "run-A" for h in heads)

    # Second promote with a new run: no new rows, run_id re-pointed.
    identity_b = identity.model_copy(update={"run_id": "run-B"})
    promoted_again = promote_run(manager, identity_b)
    assert promoted_again == len(table_ids) * len(_PROMOTE_STAGES)

    heads_after = _heads(session_factory)
    assert len(heads_after) == len(table_ids) * len(_PROMOTE_STAGES)  # upsert, not insert
    assert all(h.run_id == "run-B" for h in heads_after)


def test_promote_run_no_tables_is_noop(monkeypatch, session_factory):
    """An empty session-table set promotes nothing (logged warning, no rows)."""
    identity = SourceIdentity(
        workspace_id="ws-1",
        session_id="sess-1",
        run_id="run-A",
    )
    monkeypatch.setattr(activity_mod, "tables_for_session", lambda session, sid: [])

    assert promote_run(_manager(session_factory), identity) == 0
    assert _heads(session_factory) == []


def test_promote_run_requires_run_id(monkeypatch, session_factory):
    """A missing run_id is a caller bug — fail loud rather than write a NULL head."""
    identity = SourceIdentity(workspace_id="ws-1", session_id="sess-1")

    with pytest.raises(RuntimeError, match="requires a stamped identity.run_id"):
        promote_run(_manager(session_factory), identity)
