"""Tests for ``BeginSessionWorkflow``'s replay gating (DAT-401).

The begin_session spine mirrors add_source's replay pattern but over its own
two-phase order. These pin the pure gating logic — which phases run for each
``from_phase`` value, and that an unknown ``from_phase`` fails loud — so the
workflow body's branches can be reasoned about without a Temporal worker. Real
end-to-end execution + offline determinism are covered by compose-smoke (the
project's Temporal-test convention: testcontainers/compose for exec, Replayer
for determinism), the same place the add_source spine is exercised live.
"""

from __future__ import annotations

import pytest
from temporalio.exceptions import ApplicationError

from dataraum.worker.contracts import ReplayScope
from dataraum.worker.workflows import (
    _SESSION_PHASE_ORDER,
    _SESSION_VALID_REPLAY_PHASES,
    _runs_under,
    _validate_session_replay,
)


class TestSessionPhaseOrder:
    """The constants pin the begin_session chain + its valid replay entry points."""

    def test_order_is_relationships_then_semantic(self) -> None:
        assert _SESSION_PHASE_ORDER == ("relationships", "semantic_per_table")

    def test_valid_replay_phases_are_exactly_the_order(self) -> None:
        assert _SESSION_VALID_REPLAY_PHASES == frozenset(_SESSION_PHASE_ORDER)

    def test_ingestion_phases_are_not_valid_session_replay_entrypoints(self) -> None:
        # A begin_session replay names a begin_session phase, never an add_source one.
        for phase in ("import", "typing", "semantic_per_column", "detect"):
            assert phase not in _SESSION_VALID_REPLAY_PHASES


class TestValidateSessionReplay:
    """``_validate_session_replay`` accepts the session phases, rejects the rest."""

    def test_none_is_allowed(self) -> None:
        _validate_session_replay(None)  # no raise — the initial-run shape

    @pytest.mark.parametrize("phase", ["relationships", "semantic_per_table"])
    def test_session_phases_allowed(self, phase: str) -> None:
        _validate_session_replay(ReplayScope(from_phase=phase))  # no raise

    @pytest.mark.parametrize("phase", ["import", "typing", "semantic_per_column", "bogus"])
    def test_unknown_from_phase_fails_loud(self, phase: str) -> None:
        with pytest.raises(ApplicationError) as exc:
            _validate_session_replay(ReplayScope(from_phase=phase))
        assert "begin_session replay.from_phase" in str(exc.value)


class TestRunsUnderSessionOrder:
    """``_runs_under`` over ``_SESSION_PHASE_ORDER`` decides which phases re-run."""

    def test_initial_run_runs_every_phase(self) -> None:
        assert all(_runs_under(p, None, _SESSION_PHASE_ORDER) for p in _SESSION_PHASE_ORDER)

    def test_replay_from_relationships_runs_both(self) -> None:
        replay = ReplayScope(from_phase="relationships")
        assert _runs_under("relationships", replay, _SESSION_PHASE_ORDER) is True
        assert _runs_under("semantic_per_table", replay, _SESSION_PHASE_ORDER) is True

    def test_replay_from_semantic_skips_relationships(self) -> None:
        replay = ReplayScope(from_phase="semantic_per_table")
        assert _runs_under("relationships", replay, _SESSION_PHASE_ORDER) is False
        assert _runs_under("semantic_per_table", replay, _SESSION_PHASE_ORDER) is True
