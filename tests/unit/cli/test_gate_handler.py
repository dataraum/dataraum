"""Tests for CLI gate handler functions."""

from __future__ import annotations

from io import StringIO
from unittest.mock import patch

from rich.console import Console

from dataraum.cli.gate_handler import (
    _render_violations,
    handle_exit_check,
    render_fix_result,
)
from dataraum.entropy.fix_executor import ActionDefinition, ActionRegistry
from dataraum.pipeline.events import EventType, PipelineEvent
from dataraum.pipeline.runner import GateMode
from dataraum.pipeline.scheduler import ResolutionAction


def _make_exit_check_event(
    violations: dict[str, tuple[float, float]] | None = None,
) -> PipelineEvent:
    """Create a minimal EXIT_CHECK event."""
    return PipelineEvent(
        event_type=EventType.EXIT_CHECK,
        step=5,
        total=10,
        violations=violations
        or {
            "structural.types.type_fidelity": (0.62, 0.50),
        },
    )


def _make_registry_with_action() -> ActionRegistry:
    """Create a registry with one action that improves type_fidelity."""
    registry = ActionRegistry()
    registry.register(
        ActionDefinition(
            action_type="override_type",
            category="transform",
            description="Override column type",
            hard_verifiable=True,
            parameters_schema={"target_type": "Target SQL type"},
            improves_dimensions=["structural.types.type_fidelity"],
        )
    )
    return registry


class TestSkipMode:
    def test_returns_defer(self):
        console = Console(file=StringIO())
        event = _make_exit_check_event()
        result = handle_exit_check(console, event, GateMode.SKIP)
        assert result.action == ResolutionAction.DEFER

    def test_prints_message(self):
        output = StringIO()
        console = Console(file=output, force_terminal=True)
        event = _make_exit_check_event()
        handle_exit_check(console, event, GateMode.SKIP)
        assert "deferred" in output.getvalue().lower()


class TestFailMode:
    def test_returns_abort(self):
        console = Console(file=StringIO())
        event = _make_exit_check_event()
        result = handle_exit_check(console, event, GateMode.FAIL)
        assert result.action == ResolutionAction.ABORT


class TestInteractiveMode:
    def test_defer_choice(self):
        """PAUSE + user picks defer → Resolution(DEFER)."""
        console = Console(file=StringIO())
        event = _make_exit_check_event()

        # The defer option is at index 1 (no fix options without registry)
        with patch("dataraum.cli.gate_handler.Prompt") as mock_prompt:
            mock_prompt.ask.return_value = "1"
            result = handle_exit_check(console, event, GateMode.PAUSE)

        assert result.action == ResolutionAction.DEFER

    def test_abort_choice(self):
        """PAUSE + user picks abort → Resolution(ABORT)."""
        console = Console(file=StringIO())
        event = _make_exit_check_event()

        # Without registry: option 1=defer, option 2=abort
        with patch("dataraum.cli.gate_handler.Prompt") as mock_prompt:
            mock_prompt.ask.return_value = "2"
            result = handle_exit_check(console, event, GateMode.PAUSE)

        assert result.action == ResolutionAction.ABORT

    def test_fix_choice(self):
        """PAUSE + user picks fix → Resolution(FIX, fixes=[...])."""
        console = Console(file=StringIO())
        event = _make_exit_check_event()
        registry = _make_registry_with_action()

        # With registry: option 1=fix:override_type, option 2=defer, option 3=abort
        with patch("dataraum.cli.gate_handler.Prompt") as mock_prompt:
            # First call: choose fix (option 1)
            # Second call: target
            # Third call: target_type parameter
            mock_prompt.ask.side_effect = ["1", "column:orders.amount", "DECIMAL(10,2)"]
            result = handle_exit_check(console, event, GateMode.PAUSE, registry)

        assert result.action == ResolutionAction.FIX
        assert len(result.fixes) == 1
        assert result.fixes[0].action_type == "override_type"
        assert result.fixes[0].target == "column:orders.amount"

    def test_keyboard_interrupt_defers(self):
        """Ctrl+C during prompt → Resolution(DEFER)."""
        console = Console(file=StringIO())
        event = _make_exit_check_event()

        with patch("dataraum.cli.gate_handler.Prompt") as mock_prompt:
            mock_prompt.ask.side_effect = KeyboardInterrupt
            result = handle_exit_check(console, event, GateMode.PAUSE)

        assert result.action == ResolutionAction.DEFER

    def test_eof_defers(self):
        """EOFError during prompt → Resolution(DEFER)."""
        console = Console(file=StringIO())
        event = _make_exit_check_event()

        with patch("dataraum.cli.gate_handler.Prompt") as mock_prompt:
            mock_prompt.ask.side_effect = EOFError
            result = handle_exit_check(console, event, GateMode.PAUSE)

        assert result.action == ResolutionAction.DEFER


class TestRenderViolations:
    def test_renders_panel(self):
        """Violations panel renders without errors."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        violations = {
            "structural.types.type_fidelity": (0.62, 0.50),
            "semantic.units.unit_declaration": (0.45, 0.30),
        }
        _render_violations(console, violations)
        rendered = output.getvalue()
        assert "type_fidelity" in rendered
        assert "unit_declaration" in rendered
        assert "0.62" in rendered

    def test_renders_gap_column(self):
        """Gap column shows the difference between score and threshold."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=120)
        violations = {"dim.a": (0.75, 0.30)}
        _render_violations(console, violations)
        rendered = output.getvalue()
        assert "+0.45" in rendered

    def test_sorts_by_gap_descending(self):
        """Violations sorted by gap size, largest first."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=120)
        violations = {
            "dim.small_gap": (0.40, 0.30),
            "dim.big_gap": (0.90, 0.20),
        }
        _render_violations(console, violations)
        rendered = output.getvalue()
        # big_gap should appear before small_gap
        assert rendered.index("big_gap") < rendered.index("small_gap")

    def test_renders_column_details(self):
        """Per-column details rendered as indented sub-rows."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=120)
        violations = {"structural.types.type_fidelity": (0.65, 0.20)}
        column_details = {
            "structural.types.type_fidelity": {
                "column:orders.amount": 0.95,
                "column:orders.discount": 0.88,
            }
        }
        _render_violations(console, violations, column_details)
        rendered = output.getvalue()
        assert "orders.amount" in rendered
        assert "orders.discount" in rendered

    def test_column_details_limited_to_top_3(self):
        """Only top-3 worst columns shown per dimension."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=120)
        violations = {"dim.a": (0.80, 0.20)}
        column_details = {
            "dim.a": {
                "column:t.c1": 0.95,
                "column:t.c2": 0.90,
                "column:t.c3": 0.85,
                "column:t.c4": 0.80,  # 4th worst — should not appear
            }
        }
        _render_violations(console, violations, column_details)
        rendered = output.getvalue()
        assert "t.c1" in rendered
        assert "t.c2" in rendered
        assert "t.c3" in rendered
        assert "t.c4" not in rendered


class TestRenderFixResult:
    def test_renders_success(self):
        """Successful fix shows checkmark and before/after deltas."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=120)
        event = PipelineEvent(
            event_type=EventType.FIX_APPLIED,
            message="override_type on column:orders.amount",
            scores={"type_fidelity": 0.10},
            before_scores={"type_fidelity": 0.80},
            after_scores={"type_fidelity": 0.10},
        )
        render_fix_result(console, event)
        rendered = output.getvalue()
        assert "override_type" in rendered
        assert "orders.amount" in rendered
        assert "0.80" in rendered
        assert "0.10" in rendered
        assert "improved" in rendered

    def test_renders_failure(self):
        """Failed fix shows cross and error message."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=120)
        event = PipelineEvent(
            event_type=EventType.FIX_APPLIED,
            message="override_type on column:orders.amount",
            error="Cannot apply fix",
        )
        render_fix_result(console, event)
        rendered = output.getvalue()
        assert "Cannot apply fix" in rendered
