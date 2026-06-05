"""Unit tests for the DAT-430 run-scoped column gate (``check_run_column_limit``).

``limits.max_columns`` bounds a RUN's pipeline/LLM cost. Post-DAT-422 a run is a
SET of per-file content sources, so the gate counts the union of the run's raw
tables — the parent workflow calls it once after the import loop, before the
per-table fan-out, with the accumulated raw table ids. These tests pin that the
count is scoped to exactly the given ids (not the workspace), that a breach is a
FAILED PhaseRun (→ non-retryable ``PhaseFailed`` in the activity wrapper), and
that the workspace guard mirrors the other run-side helpers.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.storage import Column, Table, init_database
from dataraum.worker import activity as activity_mod
from dataraum.worker.activity import check_run_column_limit
from dataraum.worker.contracts import SourceIdentity

IDENTITY = SourceIdentity(workspace_id="ws-1", session_id="sess-1", run_id="run-A")


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


def _seed_raw_table(session_factory: Any, n_columns: int) -> str:
    """One raw Table row with ``n_columns`` Column rows; returns its id."""
    table_id = str(uuid4())
    with session_factory() as session:
        session.add(
            Table(
                table_id=table_id,
                source_id=str(uuid4()),
                table_name=f"src__{table_id[:8]}",
                layer="raw",
                duckdb_path=f"src__{table_id[:8]}",
                row_count=1,
            )
        )
        for pos in range(n_columns):
            session.add(
                Column(
                    table_id=table_id,
                    column_name=f"c{pos}",
                    column_position=pos,
                    raw_type="VARCHAR",
                )
            )
        session.commit()
    return table_id


def _patch_env(monkeypatch, max_columns: int) -> None:
    monkeypatch.setattr(activity_mod, "get_active_workspace_id", lambda: "ws-1")
    monkeypatch.setattr(
        activity_mod,
        "load_pipeline_config",
        lambda: {"limits": {"max_columns": max_columns}},
    )


def test_under_limit_completes(monkeypatch, session_factory):
    """A run whose union is within max_columns passes the gate."""
    _patch_env(monkeypatch, max_columns=10)
    ids = [_seed_raw_table(session_factory, 3), _seed_raw_table(session_factory, 4)]

    run = check_run_column_limit(_manager(session_factory), IDENTITY, ids)

    assert run.status == "completed"
    assert "7 columns" in run.summary


def test_over_limit_fails_loud(monkeypatch, session_factory):
    """The RUN total breaches even when every source is individually small.

    The DAT-430 case: per-file sources at 3 columns each sail under any
    per-source cap; only the union check bounds the run.
    """
    _patch_env(monkeypatch, max_columns=5)
    ids = [_seed_raw_table(session_factory, 3), _seed_raw_table(session_factory, 3)]

    run = check_run_column_limit(_manager(session_factory), IDENTITY, ids)

    assert run.status == "failed"
    assert run.error is not None
    assert "6 columns" in run.error
    assert "max_columns=5" in run.error
    assert "limits.max_columns" in run.error


def test_counts_only_the_given_tables(monkeypatch, session_factory):
    """The gate scopes to the run's id union — other workspace tables don't count."""
    _patch_env(monkeypatch, max_columns=5)
    in_run = _seed_raw_table(session_factory, 4)
    _seed_raw_table(session_factory, 400)  # another run's table; must not count

    run = check_run_column_limit(_manager(session_factory), IDENTITY, [in_run])

    assert run.status == "completed", run.error
    assert "4 columns" in run.summary


def test_workspace_mismatch_fails(monkeypatch, session_factory):
    """A payload addressed to another workspace never counts anything (DAT-364)."""
    _patch_env(monkeypatch, max_columns=5)
    foreign = IDENTITY.model_copy(update={"workspace_id": "ws-2"})

    run = check_run_column_limit(_manager(session_factory), foreign, [])

    assert run.status == "failed"
    assert "Workspace mismatch" in (run.error or "")
