"""Tests for the ``ReplayScope`` workflow gating (DAT-343 P5).

The workflow body branches on ``replay.from_phase`` via the pure helpers
``_at_or_after`` and ``_runs_under``. These tests pin the predicate
semantics (which phases run for each ``from_phase`` value) so the
workflow body's gates can be reasoned about without spinning up a real
Temporal worker.

End-to-end "replay actually produces clean output" coverage lives in
the integration smoke that drives the running stack (P8) — these unit
tests cover only the pure-logic gating layer.
"""

from __future__ import annotations

import pytest
from temporalio.exceptions import ApplicationError

from dataraum.worker.contracts import ReplayScope
from dataraum.worker.workflows import (
    _ANALYTICS_PHASES,
    _CHILD_PHASE_ORDER,
    _PARENT_PHASE_ORDER,
    _VALID_REPLAY_PHASES,
    _at_or_after,
    _runs_under,
    _validate_replay,
)


class TestPhaseOrders:
    """The constants pin which phases each workflow body owns."""

    def test_parent_order_is_import_then_reduce(self) -> None:
        assert _PARENT_PHASE_ORDER == ("import", "semantic_per_column")

    def test_child_order_is_typing_then_analytics_then_detect_table(self) -> None:
        assert _CHILD_PHASE_ORDER == (
            "typing",
            *_ANALYTICS_PHASES,
            "detect_table",
        )


class TestAtOrAfter:
    """Pure predicate over the phase-order tuple."""

    def test_from_phase_itself_runs(self) -> None:
        assert _at_or_after("typing", "typing", _CHILD_PHASE_ORDER) is True

    def test_phases_after_from_phase_run(self) -> None:
        assert _at_or_after("statistics", "typing", _CHILD_PHASE_ORDER) is True
        assert _at_or_after("detect_table", "typing", _CHILD_PHASE_ORDER) is True

    def test_phases_before_from_phase_skip(self) -> None:
        assert _at_or_after("typing", "statistics", _CHILD_PHASE_ORDER) is False

    def test_phase_not_in_order_skipped(self) -> None:
        """``from_phase`` from a different chain (typo or wrong chain) skips all."""
        # typing is in CHILD order, not PARENT order.
        assert _at_or_after("import", "typing", _PARENT_PHASE_ORDER) is False
        assert _at_or_after("semantic_per_column", "typing", _PARENT_PHASE_ORDER) is False

    def test_unknown_phase_treated_as_not_after(self) -> None:
        assert _at_or_after("unknown_phase", "typing", _CHILD_PHASE_ORDER) is False


class TestRunsUnder:
    """Initial run runs all; replay runs only at-or-after."""

    def test_initial_run_runs_every_phase(self) -> None:
        for phase in _CHILD_PHASE_ORDER:
            assert _runs_under(phase, None, _CHILD_PHASE_ORDER) is True
        for phase in _PARENT_PHASE_ORDER:
            assert _runs_under(phase, None, _PARENT_PHASE_ORDER) is True

    def test_type_pattern_replay_runs_typing_onward(self) -> None:
        replay = ReplayScope(from_phase="typing", raw_table_ids=["raw-1"])
        # All child phases run on a typing replay.
        for phase in _CHILD_PHASE_ORDER:
            assert _runs_under(phase, replay, _CHILD_PHASE_ORDER) is True
        # Pure-predicate behavior: a child-stage from_phase ("typing") is not
        # in the parent chain → both parent phases return False from
        # ``_runs_under``. The parent's workflow body owns the "always re-run
        # the source-level reduce" decision separately (semantic_per_column +
        # detect_source aren't gated on ``_runs_under`` in the parent body).
        assert _runs_under("import", replay, _PARENT_PHASE_ORDER) is False
        assert _runs_under("semantic_per_column", replay, _PARENT_PHASE_ORDER) is False

    def test_null_value_replay_runs_import_onward(self) -> None:
        replay = ReplayScope(from_phase="import", raw_table_ids=None)
        # Both parent phases run.
        for phase in _PARENT_PHASE_ORDER:
            assert _runs_under(phase, replay, _PARENT_PHASE_ORDER) is True
        # Pure-predicate behavior: a parent-stage from_phase ("import") is
        # not in the child chain → ``_runs_under`` returns False for every
        # child phase. The parent's workflow body compensates: when
        # ``replay.from_phase`` is a parent phase, the body downgrades
        # ``child_replay`` to None before fan-out so children run every
        # phase against the freshly re-imported raw tables.
        for phase in _CHILD_PHASE_ORDER:
            assert _runs_under(phase, replay, _CHILD_PHASE_ORDER) is False

    def test_concept_property_replay_runs_only_reduce(self) -> None:
        replay = ReplayScope(from_phase="semantic_per_column", raw_table_ids=[])
        # Only the reduce runs on the parent side.
        assert _runs_under("import", replay, _PARENT_PHASE_ORDER) is False
        assert _runs_under("semantic_per_column", replay, _PARENT_PHASE_ORDER) is True

    @pytest.mark.parametrize("phase", _ANALYTICS_PHASES)
    def test_typing_replay_runs_every_analytics_phase(self, phase: str) -> None:
        replay = ReplayScope(from_phase="typing", raw_table_ids=["raw-1"])
        assert _runs_under(phase, replay, _CHILD_PHASE_ORDER) is True


class TestReplayScopeContract:
    """The ``ReplayScope`` contract — fields + Pydantic defaults."""

    def test_raw_table_ids_defaults_to_none(self) -> None:
        scope = ReplayScope(from_phase="import")
        assert scope.raw_table_ids is None

    def test_empty_raw_table_ids_means_no_children(self) -> None:
        # The workflow body distinguishes None (all children) from [] (none).
        scope = ReplayScope(from_phase="semantic_per_column", raw_table_ids=[])
        assert scope.raw_table_ids == []
        assert scope.raw_table_ids is not None


class TestValidateReplay:
    """``_validate_replay`` refuses a replay with an unknown ``from_phase``.

    Closes the silent-no-op-on-typo gap: without this guard a typo like
    ``"typ1ng"`` returns False from every ``_runs_under`` call and produces
    a partial replay with no error log. The workflow now fails loud on the
    first attempt instead.
    """

    def test_none_replay_is_noop(self) -> None:
        # Initial run — no scope to validate.
        _validate_replay(None)

    @pytest.mark.parametrize("phase", sorted(_VALID_REPLAY_PHASES))
    def test_every_known_phase_passes(self, phase: str) -> None:
        # Each phase in the parent or child chain is a valid from_phase.
        _validate_replay(ReplayScope(from_phase=phase))

    @pytest.mark.parametrize(
        "bad",
        ["typ1ng", "unknown", "TYPING", "semantic_per_table", ""],
    )
    def test_unknown_phase_raises_non_retryable(self, bad: str) -> None:
        with pytest.raises(ApplicationError) as exc:
            _validate_replay(ReplayScope(from_phase=bad))
        assert "Unknown replay.from_phase" in str(exc.value)
        assert exc.value.type == "PhaseFailed"
        assert exc.value.non_retryable is True

    def test_valid_set_is_union_of_parent_and_child_orders(self) -> None:
        assert _VALID_REPLAY_PHASES == frozenset(_PARENT_PHASE_ORDER) | frozenset(
            _CHILD_PHASE_ORDER
        )
