"""Unit tests for the operating_model activity helpers (DAT-438/506).

``resolve_operating_model_scope(manager, identity, vertical)`` — the pre-flight
that reads the workspace catalog head's ``run_tables`` and pins the docs/architecture/persistence.md
base-run map AND the table set once per run (the three phases read
``scope.table_ids`` rather than each re-reading the catalog head, closing the
TOCTOU a concurrent begin_session promote would open) — and
``promote_operating_model_run`` — the terminal head flip on the catalog target
at stage ``operating_model``. Mirrors the ``test_promote_run`` fake-manager
pattern.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from temporalio.exceptions import ApplicationError

from dataraum.investigation import link_run_tables
from dataraum.storage import (
    GENERATION_STAGE,
    MetadataSnapshotHead,
    Table,
    catalog_head_target,
    init_database,
)
from dataraum.worker.activity import (
    promote_operating_model_run,
    resolve_operating_model_scope,
)
from dataraum.worker.contracts import RunRef

_IDENTITY = RunRef(workspace_id="ws-1", run_id="run-om-A")
_VERTICAL = "finance"
_CATALOG_RUN = "run-bs"


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


def _seed_catalog(
    session_factory: Any,
    table_ids: list[str],
    *,
    catalog_run: str | None = _CATALOG_RUN,
) -> None:
    """Promoted begin_session catalog run anchoring TYPED tables to ``catalog_run``.

    A promoted ``(catalog, "catalog")`` head names the run; its ``run_tables``
    anchor the workspace's composed table set (``tables_for_run`` joins through
    ``Table``). When ``catalog_run`` is ``None`` no catalog head is promoted —
    the mid-flight/failed-begin_session state.
    """
    with session_factory() as s:
        for tid in table_ids:
            s.add(Table(table_id=tid, source_id="src-1", table_name=tid, layer="typed"))
        if catalog_run is not None:
            link_run_tables(s, catalog_run, table_ids)
            s.add(
                MetadataSnapshotHead(
                    target=catalog_head_target(), stage="catalog", run_id=catalog_run
                )
            )
        s.commit()


class TestResolveOperatingModelScope:
    def test_resolves_tables_and_pins_once(self, session_factory):
        _seed_catalog(session_factory, ["tbl-1", "tbl-2"])
        # tbl-1 has a promoted per-table generation head; tbl-2 does not.
        with session_factory() as s:
            s.add(
                MetadataSnapshotHead(
                    target="table:tbl-1", stage=GENERATION_STAGE, run_id="run-sem-1"
                )
            )
            s.commit()

        scope = resolve_operating_model_scope(_manager(session_factory), _IDENTITY, _VERTICAL)

        # The scope pins the catalog head's table set ONCE (docs/architecture/persistence.md): the three
        # phases read scope.table_ids, never re-reading the head per phase.
        assert set(scope.table_ids) == {"tbl-1", "tbl-2"}
        # It also pins the base-run map once.
        assert scope.relationship_run_id == _CATALOG_RUN
        # tbl-2 has no promoted semantic head → absent, never guessed.
        assert scope.semantic_runs == {"tbl-1": "run-sem-1"}

    def test_no_promoted_catalog_run_fails_born_loud(self, session_factory):
        """Tables exist but no promoted begin_session catalog head = mid-flight or
        failed begin_session (DAT-511) — refuse to ground over a partial workspace.
        Non-retryable: deterministic until begin_session promotes."""
        _seed_catalog(session_factory, ["tbl-1"], catalog_run=None)

        with pytest.raises(ApplicationError, match="no promoted begin_session") as exc:
            resolve_operating_model_scope(_manager(session_factory), _IDENTITY, _VERTICAL)
        assert exc.value.non_retryable is True
        assert exc.value.type == "PhaseFailed"

    def test_promoted_catalog_run_with_no_tables_fails_loud(self, session_factory):
        """A promoted catalog head naming a run with no run_tables — begin_session
        must compose the workspace before operating_model runs."""
        with session_factory() as s:
            s.add(
                MetadataSnapshotHead(
                    target=catalog_head_target(), stage="catalog", run_id=_CATALOG_RUN
                )
            )
            s.commit()

        with pytest.raises(ApplicationError, match="no tables"):
            resolve_operating_model_scope(_manager(session_factory), _IDENTITY, _VERTICAL)

    def test_typod_vertical_fails_born_loud(self, session_factory):
        """A typo'd / never-framed vertical raises at run entry (DAT-480) — before
        any catalog/table resolution turns it into a benign no_declared_*."""
        with pytest.raises(RuntimeError, match="Unknown vertical 'finanace'"):
            resolve_operating_model_scope(_manager(session_factory), _IDENTITY, "finanace")

    # DAT-505: the per-activity workspace-mismatch guard was removed — the
    # per-workspace task queue + the single boot assertion enforce isolation, so
    # a misrouted payload never reaches this activity.


class TestPromoteOperatingModelRun:
    def test_upserts_the_stage_head(self, session_factory):
        manager = _manager(session_factory)

        assert promote_operating_model_run(manager, _IDENTITY) == 1

        with session_factory() as s:
            head = s.execute(select(MetadataSnapshotHead)).scalar_one()
        assert head.target == catalog_head_target()
        assert head.stage == "operating_model"
        assert head.run_id == "run-om-A"

        # Re-promote under a new run: re-point, never a second row.
        identity_b = _IDENTITY.model_copy(update={"run_id": "run-om-B"})
        assert promote_operating_model_run(manager, identity_b) == 1
        with session_factory() as s:
            head = s.execute(select(MetadataSnapshotHead)).scalar_one()
        assert head.run_id == "run-om-B"

    def test_coexists_with_the_begin_session_head(self, session_factory):
        """The begin_session (``catalog``) and operating_model heads share the
        catalog target without colliding."""
        with session_factory() as s:
            s.add(
                MetadataSnapshotHead(
                    target=catalog_head_target(), stage="catalog", run_id=_CATALOG_RUN
                )
            )
            s.commit()

        promote_operating_model_run(_manager(session_factory), _IDENTITY)

        with session_factory() as s:
            heads = {
                (h.target, h.stage, h.run_id)
                for h in s.execute(select(MetadataSnapshotHead)).scalars()
            }
        target = catalog_head_target()
        assert heads == {
            (target, "catalog", _CATALOG_RUN),
            (target, "operating_model", "run-om-A"),
        }

    def test_requires_run_id(self, session_factory):
        identity = RunRef(workspace_id="ws-1")

        with pytest.raises(RuntimeError, match="requires a stamped run.run_id"):
            promote_operating_model_run(_manager(session_factory), identity)
