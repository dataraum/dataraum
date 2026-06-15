"""Unit tests for the operating_model activity helpers (DAT-438).

``resolve_operating_model_scope`` — the pre-flight that re-reads the session's
table set and pins the ADR-0008 base-run map once per run — and
``promote_operating_model_run`` — the terminal head flip at stage
``operating_model``. Mirrors the ``test_promote_run`` fake-manager pattern.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from temporalio.exceptions import ApplicationError

from dataraum.investigation.db_models import InvestigationSession, SessionTable
from dataraum.storage import MetadataSnapshotHead, Table, init_database, session_head_target
from dataraum.worker.activity import (
    promote_operating_model_run,
    resolve_operating_model_scope,
)
from dataraum.worker.contracts import SessionIdentity

_IDENTITY = SessionIdentity(workspace_id="ws-1", session_id="sess-om", run_id="run-om-A")


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


def _seed_session(session_factory: Any, table_ids: list[str]) -> None:
    """Session + linked TYPED tables (tables_for_session joins through Table)."""
    with session_factory() as s:
        s.add(InvestigationSession(session_id=_IDENTITY.session_id, intent="test"))
        for tid in table_ids:
            s.add(Table(table_id=tid, source_id="src-1", table_name=tid, layer="typed"))
            s.add(SessionTable(session_id=_IDENTITY.session_id, table_id=tid))
        s.commit()


class TestResolveOperatingModelScope:
    def test_resolves_tables_and_pins_once(self, session_factory):
        _seed_session(session_factory, ["tbl-1", "tbl-2"])
        with session_factory() as s:
            s.add_all(
                [
                    MetadataSnapshotHead(
                        target=session_head_target(_IDENTITY.session_id),
                        stage="detect",
                        run_id="run-bs",
                    ),
                    MetadataSnapshotHead(
                        target="table:tbl-1", stage="semantic_per_column", run_id="run-sem-1"
                    ),
                ]
            )
            s.commit()

        scope = resolve_operating_model_scope(_manager(session_factory), _IDENTITY)

        assert sorted(scope.table_ids) == ["tbl-1", "tbl-2"]
        assert scope.relationship_run_id == "run-bs"
        # tbl-2 has no promoted semantic head → absent, never guessed.
        assert scope.semantic_runs == {"tbl-1": "run-sem-1"}

    def test_unpromoted_session_fails_born_loud(self, session_factory):
        """Linked tables but no promoted begin_session head = mid-flight or
        failed session (DAT-511) — refuse to ground over a partial workspace
        instead of pinning None and warning (the 2026-06-11 live-smoke bug).
        Non-retryable: deterministic until begin_session promotes."""
        _seed_session(session_factory, ["tbl-1"])

        with pytest.raises(ApplicationError, match="no promoted begin_session") as exc:
            resolve_operating_model_scope(_manager(session_factory), _IDENTITY)
        assert exc.value.non_retryable is True
        assert exc.value.type == "PhaseFailed"

    def test_no_linked_tables_fails_loud(self, session_factory):
        with session_factory() as s:
            s.add(InvestigationSession(session_id=_IDENTITY.session_id, intent="test"))
            s.commit()

        with pytest.raises(ApplicationError, match="no linked tables"):
            resolve_operating_model_scope(_manager(session_factory), _IDENTITY)

    def test_unknown_session_fails_loud(self, session_factory):

        with pytest.raises(ApplicationError, match="not found"):
            resolve_operating_model_scope(_manager(session_factory), _IDENTITY)

    def test_typod_vertical_fails_born_loud(self, session_factory):
        """A typo'd / never-framed vertical raises at run entry (DAT-480) — before
        any phase turns it into a benign no_declared_*."""
        with session_factory() as s:
            s.add(
                InvestigationSession(
                    session_id=_IDENTITY.session_id, intent="test", vertical="finanace"
                )
            )
            s.add(Table(table_id="tbl-1", source_id="src-1", table_name="tbl-1", layer="typed"))
            s.add(SessionTable(session_id=_IDENTITY.session_id, table_id="tbl-1"))
            s.commit()

        with pytest.raises(RuntimeError, match="Unknown vertical 'finanace'"):
            resolve_operating_model_scope(_manager(session_factory), _IDENTITY)

    # DAT-505: the per-activity workspace-mismatch guard was removed — the
    # per-workspace task queue + the single boot assertion enforce isolation, so
    # a misrouted payload never reaches this activity.


class TestPromoteOperatingModelRun:
    def test_upserts_the_stage_head(self, session_factory):
        manager = _manager(session_factory)

        assert promote_operating_model_run(manager, _IDENTITY) == 1

        with session_factory() as s:
            head = s.execute(select(MetadataSnapshotHead)).scalar_one()
        assert head.target == session_head_target(_IDENTITY.session_id)
        assert head.stage == "operating_model"
        assert head.run_id == "run-om-A"

        # Re-promote under a new run: re-point, never a second row.
        identity_b = _IDENTITY.model_copy(update={"run_id": "run-om-B"})
        assert promote_operating_model_run(manager, identity_b) == 1
        with session_factory() as s:
            head = s.execute(select(MetadataSnapshotHead)).scalar_one()
        assert head.run_id == "run-om-B"

    def test_coexists_with_the_begin_session_head(self, session_factory):
        """Two stages' heads share the session target without colliding."""
        with session_factory() as s:
            s.add(
                MetadataSnapshotHead(
                    target=session_head_target(_IDENTITY.session_id),
                    stage="detect",
                    run_id="run-bs",
                )
            )
            s.commit()

        promote_operating_model_run(_manager(session_factory), _IDENTITY)

        with session_factory() as s:
            heads = {
                (h.target, h.stage, h.run_id)
                for h in s.execute(select(MetadataSnapshotHead)).scalars()
            }
        target = session_head_target(_IDENTITY.session_id)
        assert heads == {(target, "detect", "run-bs"), (target, "operating_model", "run-om-A")}

    def test_requires_run_id(self, session_factory):
        identity = SessionIdentity(workspace_id="ws-1", session_id="sess-om")

        with pytest.raises(RuntimeError, match="requires a stamped identity.run_id"):
            promote_operating_model_run(_manager(session_factory), identity)
