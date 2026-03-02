"""Tests for InteractiveCLIHandler Live display integration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dataraum.cli.gate_handler import InteractiveCLIHandler
from dataraum.pipeline.gates import (
    Gate,
    GateAction,
    GateActionType,
    GateResolution,
    GateViolation,
)


def _make_gate() -> Gate:
    """Create a minimal Gate for testing."""
    return Gate(
        gate_id="gate_test",
        gate_type="structural",
        blocked_phase="statistics",
        violations=[
            GateViolation(dimension="type_fidelity", score=0.6, threshold=0.5),
        ],
        suggested_actions=[
            GateAction(index=1, action_type=GateActionType.SKIP, label="Skip gate"),
        ],
    )


class TestSetLive:
    def test_set_live_stores_reference(self):
        handler = InteractiveCLIHandler(console=MagicMock())
        assert handler._live is None

        mock_live = MagicMock()
        handler.set_live(mock_live)
        assert handler._live is mock_live

    def test_resolve_stops_and_starts_live(self):
        """Live.stop() called before render, Live.start() called in finally."""
        handler = InteractiveCLIHandler(console=MagicMock())
        mock_live = MagicMock()
        handler.set_live(mock_live)
        gate = _make_gate()

        with (
            patch.object(handler, "_render_gate"),
            patch.object(handler, "_prompt_user", return_value=GateResolution(
                action_taken=GateActionType.SKIP,
            )),
        ):
            handler.resolve(gate)

        mock_live.stop.assert_called_once()
        mock_live.start.assert_called_once()

    def test_resolve_without_live_works(self):
        """Handler works normally without Live injected."""
        handler = InteractiveCLIHandler(console=MagicMock())
        gate = _make_gate()

        with (
            patch.object(handler, "_render_gate"),
            patch.object(handler, "_prompt_user", return_value=GateResolution(
                action_taken=GateActionType.SKIP,
            )),
        ):
            result = handler.resolve(gate)

        assert result.action_taken == GateActionType.SKIP

    def test_keyboard_interrupt_restarts_live(self):
        """Live.start() is called even when KeyboardInterrupt occurs."""
        handler = InteractiveCLIHandler(console=MagicMock())
        mock_live = MagicMock()
        handler.set_live(mock_live)
        gate = _make_gate()

        with patch.object(handler, "_render_gate", side_effect=KeyboardInterrupt):
            result = handler.resolve(gate)

        assert result.action_taken == GateActionType.SKIP
        mock_live.stop.assert_called_once()
        mock_live.start.assert_called_once()

    def test_exception_in_prompt_restarts_live(self):
        """Live.start() is called even when an unexpected error occurs in prompt."""
        handler = InteractiveCLIHandler(console=MagicMock())
        mock_live = MagicMock()
        handler.set_live(mock_live)
        gate = _make_gate()

        with patch.object(handler, "_render_gate", side_effect=EOFError):
            result = handler.resolve(gate)

        assert result.action_taken == GateActionType.SKIP
        mock_live.stop.assert_called_once()
        mock_live.start.assert_called_once()
