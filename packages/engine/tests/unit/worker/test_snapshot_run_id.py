"""Unit tests for the DAT-413 snapshot version axis (Phase 1 substrate).

Phase 1 is behavior-preserving: it only adds a ``run_id`` axis + a head-pointer
table. These tests pin the two load-bearing facts of that substrate:

- ``MetadataSnapshotHead`` is a registered model whose table ``create_all``
  builds, and instances carry the documented columns.
- ``run_phase`` threads ``SourceIdentity.run_id`` onto ``PhaseContext.run_id``,
  so every add_source phase body sees the run's snapshot id (and a ``None``
  identity stays ``None`` — the cockpit initial-run shape).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import StaticPool

from dataraum.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum.storage import MetadataSnapshotHead
from dataraum.storage.base import init_database
from dataraum.worker import activity as activity_mod
from dataraum.worker.contracts import SourceIdentity


def test_snapshot_head_table_created_and_instantiates() -> None:
    """``init_database`` builds ``metadata_snapshot_head`` and the model instantiates."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    # ``init_database`` registers every model (including ``snapshot_head`` via the
    # storage import it adds) before ``create_all`` — so the head table is built
    # exactly as the worker bootstraps the schema.
    init_database(engine)
    assert "metadata_snapshot_head" in inspect(engine).get_table_names()

    head = MetadataSnapshotHead(
        target="table:tbl-1",
        stage="statistics",
        run_id="run-1",
    )
    assert head.target == "table:tbl-1"
    assert head.stage == "statistics"
    assert head.run_id == "run-1"


class _CapturePhase:
    """A fake phase that records the PhaseContext it is run with."""

    captured: PhaseContext | None = None

    @property
    def name(self) -> str:
        return "capture"

    def should_skip(self, ctx: PhaseContext) -> str | None:
        return None

    def run(self, ctx: PhaseContext) -> PhaseResult:
        type(self).captured = ctx
        return PhaseResult.success(summary="ok")


def _run_phase_capturing(
    monkeypatch: Any,
    identity: SourceIdentity,
) -> PhaseContext:
    """Drive ``run_phase`` with mocks, returning the ctx the phase body saw."""
    _CapturePhase.captured = None

    monkeypatch.setattr(activity_mod, "get_active_workspace_id", lambda: identity.workspace_id)
    monkeypatch.setattr(activity_mod, "get_phase_class", lambda name: _CapturePhase)
    monkeypatch.setattr(
        activity_mod,
        "_build_phase_config",
        lambda source, phase_name, ident: {},
    )

    # Source-free identity (the production post-import shape, DAT-422/426):
    # run_phase never resolves a Source row, so no session.get stub is needed.
    session = MagicMock()

    @contextmanager
    def _session_scope():  # noqa: ANN202
        yield session

    @contextmanager
    def _duckdb_cursor():  # noqa: ANN202
        yield MagicMock()

    manager = MagicMock()
    manager.session_scope = _session_scope
    manager.duckdb_cursor = _duckdb_cursor

    result = activity_mod.run_phase(manager, "capture", identity, ["tbl-1"])
    assert result.status == PhaseStatus.COMPLETED.value
    assert _CapturePhase.captured is not None
    return _CapturePhase.captured


def test_run_phase_threads_run_id_from_identity(monkeypatch: Any) -> None:
    """A stamped ``SourceIdentity.run_id`` lands on ``PhaseContext.run_id``."""
    identity = SourceIdentity(
        workspace_id="ws-1",
        session_id="sess-1",
        run_id="run-xyz",
    )
    ctx = _run_phase_capturing(monkeypatch, identity)
    assert ctx.run_id == "run-xyz"


def test_run_phase_run_id_defaults_none(monkeypatch: Any) -> None:
    """An identity with no ``run_id`` (cockpit initial-run shape) stays ``None``."""
    identity = SourceIdentity(
        workspace_id="ws-1",
        session_id="sess-1",
    )
    ctx = _run_phase_capturing(monkeypatch, identity)
    assert ctx.run_id is None
