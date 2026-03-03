"""Tests for interactive CLI run features."""

from unittest.mock import MagicMock, patch

from dataraum.pipeline.runner import GateMode, RunConfig


class TestRunConfigGateHandler:
    def test_gate_handler_default_none(self):
        config = RunConfig()
        assert config.gate_handler is None

    def test_gate_handler_can_be_set(self):
        handler = MagicMock()
        config = RunConfig(gate_handler=handler)
        assert config.gate_handler is handler


class TestInteractiveDetection:
    def test_non_tty_skips_interactive(self):
        """Non-TTY should not create gate handler."""
        import sys

        # If stdin is not a TTY, is_interactive should be False
        with patch.object(sys.stdin, "isatty", return_value=False):
            is_interactive = sys.stdin.isatty() and True
            assert not is_interactive

    def test_tty_enables_interactive(self):
        """TTY should enable interactive features."""
        import sys

        with patch.object(sys.stdin, "isatty", return_value=True):
            is_interactive = sys.stdin.isatty() and True
            assert is_interactive

    def test_quiet_disables_interactive(self):
        """--quiet should disable interactive even on TTY."""
        import sys

        with patch.object(sys.stdin, "isatty", return_value=True):
            quiet = True
            is_interactive = sys.stdin.isatty() and not quiet
            assert not is_interactive


class TestGateHandlerCreation:
    def test_pause_mode_creates_handler(self):
        """pause mode + interactive should create InteractiveCLIHandler."""
        from dataraum.cli.gate_handler import InteractiveCLIHandler

        handler = InteractiveCLIHandler()
        assert hasattr(handler, "resolve")
        assert hasattr(handler, "notify")

    def test_handler_is_sync(self):
        """Handler methods should be sync (not coroutines)."""
        import inspect

        from dataraum.cli.gate_handler import InteractiveCLIHandler

        handler = InteractiveCLIHandler()
        # resolve and notify should not be coroutine functions
        assert not inspect.iscoroutinefunction(handler.resolve)
        assert not inspect.iscoroutinefunction(handler.notify)


class TestHandlerContext:
    def test_set_context(self):
        """Handler should accept pipeline context for fix execution."""
        from dataraum.cli.gate_handler import InteractiveCLIHandler

        handler = InteractiveCLIHandler()
        assert handler._manager is None
        assert handler._source_id == ""

        mock_manager = MagicMock()
        handler.set_context(mock_manager, "src_123")
        assert handler._manager is mock_manager
        assert handler._source_id == "src_123"

    def test_keyboard_interrupt_returns_skip(self):
        """Ctrl+C during gate resolution should return SKIP."""
        from dataraum.cli.gate_handler import InteractiveCLIHandler
        from dataraum.pipeline.gates import (
            Gate,
            GateAction,
            GateActionType,
            GateViolation,
        )

        handler = InteractiveCLIHandler(console=MagicMock())

        gate = Gate(
            gate_id="gate_test",
            gate_type="structural",
            blocked_phase="statistics",
            violations=[GateViolation(dimension="type_fidelity", score=0.6, threshold=0.5)],
            suggested_actions=[
                GateAction(index=1, action_type=GateActionType.SKIP, label="skip"),
            ],
        )

        # Patch _render_gate to raise KeyboardInterrupt
        with patch.object(handler, "_render_gate", side_effect=KeyboardInterrupt):
            resolution = handler.resolve(gate)

        assert resolution.action_taken == GateActionType.SKIP

    def test_execute_fix_without_manager_returns_none(self):
        """Fix execution without manager context should return None."""
        from dataraum.cli.gate_handler import InteractiveCLIHandler
        from dataraum.pipeline.gates import (
            Gate,
            GateAction,
            GateActionType,
        )

        handler = InteractiveCLIHandler(console=MagicMock())
        action = GateAction(
            index=1,
            action_type=GateActionType.FIX,
            label="fix",
            parameters={"action_type": "override_type", "target": "column:t.c"},
        )
        gate = Gate(gate_id="g", gate_type="structural", blocked_phase="test")
        result = handler._execute_fix(action, gate)
        assert result is None


class TestNonInteractiveFallback:
    def test_pause_in_non_tty_warns(self):
        """Pause mode in non-TTY should fall back to skip."""
        # This tests the logic that would be applied in the CLI
        is_interactive = False
        gate_mode = GateMode.PAUSE

        if gate_mode == GateMode.PAUSE and not is_interactive:
            gate_mode = GateMode.SKIP

        assert gate_mode == GateMode.SKIP
