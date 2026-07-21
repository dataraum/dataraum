"""``run_session_phase`` threads a phase's declared-artifact count up as the gate signal.

The three operating_model families (validation / business_cycles / metrics) report
``outputs["declared"]`` — ``0`` when the vertical declares none of that family.
``run_session_phase`` lifts that count onto ``PhaseRun.declared`` so it survives the
collapse to ``PhaseOutcome`` and reaches ``OperatingModelWorkflow``'s promote gate
(DAT-845). Every other phase omits the key, so the count stays ``None`` and is never
counted as "empty" (a ``None`` promotes, only an explicit ``0`` from all three
refuses). Cheap fake-phase + fake-manager drive, mirroring ``test_snapshot_run_id``.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.worker import activity as activity_mod
from dataraum.worker.contracts import RunRef


class _DeclaringPhase:
    """A fake session phase whose ``run`` returns a chosen ``outputs`` dict."""

    outputs: dict[str, Any] = {}  # noqa: RUF012

    @property
    def name(self) -> str:
        return "declaring"

    def should_skip(self, ctx: PhaseContext) -> str | None:
        return None

    def run(self, ctx: PhaseContext) -> PhaseResult:
        return PhaseResult.success(outputs=dict(type(self).outputs), summary="ok")


def _run_session_phase(monkeypatch: Any, outputs: dict[str, Any]):  # noqa: ANN202
    """Drive ``run_session_phase`` with the fake phase + a mock manager."""
    _DeclaringPhase.outputs = outputs
    monkeypatch.setattr(activity_mod, "get_phase_class", lambda name: _DeclaringPhase)
    monkeypatch.setattr(
        activity_mod,
        "_build_session_phase_config",
        lambda phase_name, vertical: {},
    )

    @contextmanager
    def _session_scope():  # noqa: ANN202
        yield MagicMock()

    @contextmanager
    def _duckdb_cursor():  # noqa: ANN202
        yield MagicMock()

    manager = MagicMock()
    manager.session_scope = _session_scope
    manager.duckdb_cursor = _duckdb_cursor

    return activity_mod.run_session_phase(
        manager, "declaring", RunRef(workspace_id="ws-1", run_id="run-om"), ["tbl-1"], "finance"
    )


def test_declared_zero_threads_through(monkeypatch: Any) -> None:
    """The empty early-return (``declared: 0``) is preserved onto ``PhaseRun``."""
    run = _run_session_phase(
        monkeypatch, {"outcome": "no_vertical", "declared": 0, "total_checks": 0}
    )
    assert run.declared == 0


def test_declared_positive_threads_through(monkeypatch: Any) -> None:
    """A real declared count (``len(artifacts)``) is preserved onto ``PhaseRun``."""
    run = _run_session_phase(monkeypatch, {"declared": 5, "grounded": 3, "executed": 3})
    assert run.declared == 5


def test_absent_declared_stays_none(monkeypatch: Any) -> None:
    """A phase that reports no ``declared`` signal leaves the count ``None`` — never
    counted as empty, so it never contributes to a promote refusal."""
    run = _run_session_phase(monkeypatch, {"drift_summaries": 2})
    assert run.declared is None
