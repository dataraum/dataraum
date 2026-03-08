"""Tests for the CLI gate handler — PAUSE mode and fix flow."""

from __future__ import annotations

import re
from io import StringIO
from unittest.mock import MagicMock, patch

from rich.console import Console

from dataraum.cli.gate_handler import (
    _collect_fix_actions,
    _handle_pause,
    build_gate_context,
    handle_exit_check,
)
from dataraum.pipeline.events import EventType, PipelineEvent
from dataraum.pipeline.runner import GateMode
from dataraum.pipeline.scheduler import ResolutionAction


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", text)


def _make_exit_check_event(
    violations: dict[str, tuple[float, float]] | None = None,
    fixable_actions: dict[str, list[dict[str, str]]] | None = None,
    column_details: dict[str, dict[str, float]] | None = None,
) -> PipelineEvent:
    """Create an EXIT_CHECK event with common defaults."""
    return PipelineEvent(
        event_type=EventType.EXIT_CHECK,
        step=1,
        total=5,
        phase="quality_review",
        violations=violations or {"structural.types.type_fidelity": (0.62, 0.50)},
        fixable_actions=fixable_actions or {},
        column_details=column_details or {},
    )


class TestHandleExitCheckModes:
    def test_skip_returns_defer(self) -> None:
        console = Console(file=StringIO())
        event = _make_exit_check_event()

        result = handle_exit_check(console, event, GateMode.SKIP)

        assert result.action == ResolutionAction.DEFER

    def test_fail_returns_abort(self) -> None:
        console = Console(file=StringIO())
        event = _make_exit_check_event()

        result = handle_exit_check(console, event, GateMode.FAIL)

        assert result.action == ResolutionAction.ABORT

    def test_pause_delegates_to_handle_pause(self) -> None:
        """PAUSE mode without fixable actions defers."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(fixable_actions={})

        result = handle_exit_check(console, event, GateMode.PAUSE)

        assert result.action == ResolutionAction.DEFER
        assert "no fix actions" in _strip_ansi(output.getvalue()).lower()


class TestCollectFixActions:
    def test_collects_actions_from_event(self) -> None:
        event = _make_exit_check_event(
            fixable_actions={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                ],
            }
        )

        actions = _collect_fix_actions(event)

        assert len(actions) == 1
        assert actions[0]["action_name"] == "override_type"
        assert actions[0]["phase_name"] == "typing"
        assert actions[0]["dimension"] == "structural.types.type_fidelity"

    def test_deduplicates_by_action_name(self) -> None:
        event = _make_exit_check_event(
            fixable_actions={
                "dim1": [{"action_name": "fix_a", "phase_name": "p1"}],
                "dim2": [{"action_name": "fix_a", "phase_name": "p1"}],
            }
        )

        actions = _collect_fix_actions(event)

        assert len(actions) == 1

    def test_multiple_distinct_actions(self) -> None:
        event = _make_exit_check_event(
            fixable_actions={
                "dim1": [
                    {"action_name": "fix_a", "phase_name": "p1"},
                    {"action_name": "fix_b", "phase_name": "p2"},
                ],
            }
        )

        actions = _collect_fix_actions(event)

        assert len(actions) == 2
        names = {a["action_name"] for a in actions}
        assert names == {"fix_a", "fix_b"}


class TestHandlePause:
    def test_no_fixable_actions_defers(self) -> None:
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(fixable_actions={})

        result = _handle_pause(console, event, None, None, None)

        assert result.action == ResolutionAction.DEFER

    def test_user_chooses_defer(self) -> None:
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(
            fixable_actions={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                ],
            }
        )

        with patch.object(console, "input", return_value="d"):
            result = _handle_pause(console, event, None, None, None)

        assert result.action == ResolutionAction.DEFER

    def test_user_chooses_abort(self) -> None:
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(
            fixable_actions={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                ],
            }
        )

        with patch.object(console, "input", return_value="a"):
            result = _handle_pause(console, event, None, None, None)

        assert result.action == ResolutionAction.ABORT

    def test_invalid_choice_defers(self) -> None:
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(
            fixable_actions={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                ],
            }
        )

        with patch.object(console, "input", return_value="xyz"):
            result = _handle_pause(console, event, None, None, None)

        assert result.action == ResolutionAction.DEFER

    def test_no_session_defers(self) -> None:
        """Selecting a fix action without session falls back to DEFER."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(
            fixable_actions={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                ],
            }
        )

        with patch.object(console, "input", return_value="1"):
            result = _handle_pause(console, event, None, None, None)

        assert result.action == ResolutionAction.DEFER
        rendered = _strip_ansi(output.getvalue())
        assert "session not available" in rendered.lower()

    def test_displays_action_menu(self) -> None:
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(
            fixable_actions={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                ],
            }
        )

        with patch.object(console, "input", return_value="d"):
            _handle_pause(console, event, None, None, None)

        rendered = _strip_ansi(output.getvalue())
        assert "override_type" in rendered
        assert "typing" in rendered
        assert "defer" in rendered.lower()
        assert "abort" in rendered.lower()


class TestRunFixFlow:
    def test_full_fix_flow(self) -> None:
        """End-to-end: agent generates questions, user answers, fix applied."""
        from dataraum.cli.gate_handler import _run_fix_flow
        from dataraum.core.models.base import Result
        from dataraum.documentation.agent import (
            ClarifyingQuestion,
            ConfigFixInterpretation,
            ConfigFixQuestions,
        )

        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        session = MagicMock()
        session.execute.return_value.first.return_value = None
        source_id = "test-source"

        event = _make_exit_check_event(
            violations={"structural.types.type_fidelity": (0.62, 0.50)},
            column_details={
                "structural.types.type_fidelity": {
                    "column:orders.order_date": 0.85,
                }
            },
        )
        action_info = {
            "action_name": "override_type",
            "phase_name": "typing",
            "dimension": "structural.types.type_fidelity",
        }

        mock_questions = ConfigFixQuestions(
            questions=[
                ClarifyingQuestion(
                    question="Override orders.order_date to DATE?",
                    question_type="multiple_choice",
                    choices=["DATE", "TIMESTAMP", "Keep VARCHAR"],
                )
            ],
            context_summary="order_date looks like dates.",
            suggested_action="override_type",
        )

        mock_interp = ConfigFixInterpretation(
            parameters={"resolved_type": "DATE"},
            config_action="override_type",
            affected_columns=["orders.order_date"],
            interpretation="Override orders.order_date type to DATE.",
            confidence="high",
            summary="Type override to DATE",
        )

        mock_agent = MagicMock()
        mock_agent.generate_config_questions.return_value = Result.ok(mock_questions)
        mock_agent.interpret_config_answers.return_value = Result.ok(mock_interp)

        # User: select choice 1 ("DATE"), then confirm "y"
        input_values = iter(["1", "y"])

        with (
            patch("dataraum.cli.commands.fix._create_document_agent", return_value=mock_agent),
            patch.object(console, "input", side_effect=lambda _: next(input_values)),
        ):
            result = _run_fix_flow(console, session, source_id, action_info, event)

        assert result.action == ResolutionAction.FIX
        assert len(result.fix_inputs) == 1
        fix = result.fix_inputs[0]
        assert fix.action_name == "override_type"
        assert fix.parameters == {"resolved_type": "DATE"}
        assert fix.affected_columns == ["orders.order_date"]

    def test_user_cancels_at_confirmation(self) -> None:
        """User answers questions but declines at confirmation step."""
        from dataraum.cli.gate_handler import _run_fix_flow
        from dataraum.core.models.base import Result
        from dataraum.documentation.agent import (
            ClarifyingQuestion,
            ConfigFixInterpretation,
            ConfigFixQuestions,
        )

        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        event = _make_exit_check_event()
        action_info = {
            "action_name": "override_type",
            "phase_name": "typing",
            "dimension": "structural.types.type_fidelity",
        }

        mock_questions = ConfigFixQuestions(
            questions=[
                ClarifyingQuestion(
                    question="Override type?",
                    question_type="free_text",
                )
            ],
            context_summary="",
        )

        mock_interp = ConfigFixInterpretation(
            parameters={},
            config_action="override_type",
            affected_columns=[],
            interpretation="Override type.",
            confidence="low",
            summary="Override",
        )

        mock_agent = MagicMock()
        mock_agent.generate_config_questions.return_value = Result.ok(mock_questions)
        mock_agent.interpret_config_answers.return_value = Result.ok(mock_interp)

        input_values = iter(["DATE", "n"])  # answer, then decline

        with (
            patch("dataraum.cli.commands.fix._create_document_agent", return_value=mock_agent),
            patch.object(console, "input", side_effect=lambda _: next(input_values)),
        ):
            result = _run_fix_flow(
                console, MagicMock(), "src", action_info, event,
            )

        assert result.action == ResolutionAction.DEFER

    def test_agent_failure_defers(self) -> None:
        """LLM failure during question generation defers gracefully."""
        from dataraum.cli.gate_handler import _run_fix_flow
        from dataraum.core.models.base import Result

        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        event = _make_exit_check_event()
        action_info = {
            "action_name": "override_type",
            "phase_name": "typing",
            "dimension": "structural.types.type_fidelity",
        }

        mock_agent = MagicMock()
        mock_agent.generate_config_questions.return_value = Result.fail("API error")

        with patch(
            "dataraum.cli.commands.fix._create_document_agent",
            return_value=mock_agent,
        ):
            result = _run_fix_flow(
                console, MagicMock(), "src", action_info, event,
            )

        assert result.action == ResolutionAction.DEFER
        assert "error" in _strip_ansi(output.getvalue()).lower()


class TestBuildGateContext:
    def test_includes_action_details(self) -> None:
        session = MagicMock()
        session.execute.return_value = MagicMock(first=MagicMock(return_value=None))

        event = _make_exit_check_event(
            violations={"structural.types.type_fidelity": (0.62, 0.50)},
            column_details={
                "structural.types.type_fidelity": {
                    "column:orders.order_date": 0.85,
                }
            },
        )
        action_info = {
            "action_name": "override_type",
            "phase_name": "typing",
            "dimension": "structural.types.type_fidelity",
        }

        context = build_gate_context(session, "src-1", action_info, event)

        assert "<action_details>" in context
        assert "override_type" in context
        assert "0.62" in context

    def test_includes_entropy_evidence(self) -> None:
        session = MagicMock()
        session.execute.return_value = MagicMock(first=MagicMock(return_value=None))

        event = _make_exit_check_event(
            violations={"structural.types.type_fidelity": (0.62, 0.50)},
            column_details={
                "structural.types.type_fidelity": {
                    "column:orders.order_date": 0.85,
                    "column:orders.created_at": 0.40,
                }
            },
        )
        action_info = {
            "action_name": "override_type",
            "phase_name": "typing",
            "dimension": "structural.types.type_fidelity",
        }

        context = build_gate_context(session, "src-1", action_info, event)

        assert "<entropy_evidence>" in context
        assert "type_fidelity" in context
        assert "0.85" in context
        assert "Worst targets" in context
