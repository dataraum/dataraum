"""Tests for the CLI gate handler — interactive fix flow."""

from __future__ import annotations

import re
from io import StringIO
from unittest.mock import MagicMock, patch

from rich.console import Console

from dataraum.cli.gate_handler import (
    _collect_fix_groups,
    _DimensionFixGroup,
    _handle_pause,
    build_gate_context,
    handle_exit_check_interactive,
)
from dataraum.pipeline.events import EventType, PipelineEvent
from dataraum.pipeline.scheduler import ResolutionAction


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", text)


def _make_exit_check_event(
    violations: dict[str, tuple[float, float]] | None = None,
    available_fixes: dict[str, list[dict[str, str]]] | None = None,
    column_details: dict[str, dict[str, float]] | None = None,
) -> PipelineEvent:
    """Create an EXIT_CHECK event with common defaults."""
    return PipelineEvent(
        event_type=EventType.EXIT_CHECK,
        step=1,
        total=5,
        phase="quality_review",
        violations=violations or {"structural.types.type_fidelity": (0.62, 0.50)},
        available_fixes=available_fixes or {},
        column_details=column_details or {},
    )


def _make_group(
    dimension: str = "structural.types.type_fidelity",
    score: float = 0.62,
    threshold: float = 0.50,
    actions: list[dict[str, str]] | None = None,
) -> _DimensionFixGroup:
    """Create a dimension fix group for testing."""
    if actions is None:
        actions = [
            {
                "action_name": "override_type",
                "phase_name": "typing",
                "dimension": dimension,
            }
        ]
    return _DimensionFixGroup(
        dimension=dimension,
        score=score,
        threshold=threshold,
        actions=actions,
    )


class TestHandleExitCheckInteractive:
    def test_no_fixes_defers(self) -> None:
        """Interactive handler without fixable actions defers."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(available_fixes={})

        result = handle_exit_check_interactive(console, event)

        assert result.action == ResolutionAction.DEFER
        assert "no fix actions" in _strip_ansi(output.getvalue()).lower()


class TestCollectFixGroups:
    def test_groups_actions_by_dimension(self) -> None:
        event = _make_exit_check_event(
            available_fixes={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                    {"action_name": "accept_finding", "phase_name": "quality_review"},
                ],
            }
        )

        groups = _collect_fix_groups(event)

        assert len(groups) == 1
        assert groups[0].dimension == "structural.types.type_fidelity"
        assert len(groups[0].actions) == 2
        names = {a["action_name"] for a in groups[0].actions}
        assert names == {"override_type", "accept_finding"}

    def test_separate_groups_per_dimension(self) -> None:
        event = _make_exit_check_event(
            violations={
                "dim1": (0.7, 0.5),
                "dim2": (0.6, 0.5),
            },
            available_fixes={
                "dim1": [{"action_name": "fix_a", "phase_name": "p1"}],
                "dim2": [{"action_name": "fix_a", "phase_name": "p1"}],
            },
        )

        groups = _collect_fix_groups(event)

        assert len(groups) == 2
        dims = {g.dimension for g in groups}
        assert dims == {"dim1", "dim2"}

    def test_deduplicates_within_dimension(self) -> None:
        event = _make_exit_check_event(
            available_fixes={
                "dim1": [
                    {"action_name": "fix_a", "phase_name": "p1"},
                    {"action_name": "fix_a", "phase_name": "p1"},
                ],
            }
        )

        groups = _collect_fix_groups(event)

        assert len(groups) == 1
        assert len(groups[0].actions) == 1

    def test_sorted_by_gap_descending(self) -> None:
        event = _make_exit_check_event(
            violations={
                "small_gap": (0.55, 0.50),
                "big_gap": (0.90, 0.50),
            },
            available_fixes={
                "small_gap": [{"action_name": "fix_a", "phase_name": "p1"}],
                "big_gap": [{"action_name": "fix_b", "phase_name": "p2"}],
            },
        )

        groups = _collect_fix_groups(event)

        assert groups[0].dimension == "big_gap"
        assert groups[1].dimension == "small_gap"


class TestHandlePause:
    def test_no_available_fixes_defers(self) -> None:
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(available_fixes={})

        result = _handle_pause(console, event, None, None, None)

        assert result.action == ResolutionAction.DEFER

    def test_user_chooses_defer(self) -> None:
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(
            available_fixes={
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
            available_fixes={
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
            available_fixes={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                ],
            }
        )

        with patch.object(console, "input", return_value="xyz"):
            result = _handle_pause(console, event, None, None, None)

        assert result.action == ResolutionAction.DEFER

    def test_no_session_defers(self) -> None:
        """Selecting a fix group without session falls back to DEFER."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(
            available_fixes={
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

    def test_displays_dimension_menu(self) -> None:
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)
        event = _make_exit_check_event(
            available_fixes={
                "structural.types.type_fidelity": [
                    {"action_name": "override_type", "phase_name": "typing"},
                    {"action_name": "accept_finding", "phase_name": "quality_review"},
                ],
            }
        )

        with patch.object(console, "input", return_value="d"):
            _handle_pause(console, event, None, None, None)

        rendered = _strip_ansi(output.getvalue())
        assert "type_fidelity" in rendered
        assert "2 actions" in rendered
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
        group = _make_group(
            actions=[
                {
                    "action_name": "override_type",
                    "phase_name": "typing",
                    "dimension": "structural.types.type_fidelity",
                },
                {
                    "action_name": "accept_finding",
                    "phase_name": "quality_review",
                    "dimension": "structural.types.type_fidelity",
                },
            ]
        )

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
            patch("dataraum.cli.gate_handler._create_document_agent", return_value=mock_agent),
            patch.object(console, "input", side_effect=lambda _: next(input_values)),
        ):
            result = _run_fix_flow(console, session, source_id, group, event)

        assert result.action == ResolutionAction.FIX
        assert len(result.fix_inputs) == 1
        fix = result.fix_inputs[0]
        assert fix.action_name == "override_type"
        assert fix.parameters["resolved_type"] == "DATE"
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
        group = _make_group()

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
            patch("dataraum.cli.gate_handler._create_document_agent", return_value=mock_agent),
            patch.object(console, "input", side_effect=lambda _: next(input_values)),
        ):
            result = _run_fix_flow(
                console,
                MagicMock(),
                "src",
                group,
                event,
            )

        assert result.action == ResolutionAction.DEFER

    def test_agent_failure_defers(self) -> None:
        """LLM failure during question generation defers gracefully."""
        from dataraum.cli.gate_handler import _run_fix_flow
        from dataraum.core.models.base import Result

        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        event = _make_exit_check_event()
        group = _make_group()

        mock_agent = MagicMock()
        mock_agent.generate_config_questions.return_value = Result.fail("API error")

        with patch(
            "dataraum.cli.gate_handler._create_document_agent",
            return_value=mock_agent,
        ):
            result = _run_fix_flow(
                console,
                MagicMock(),
                "src",
                group,
                event,
            )

        assert result.action == ResolutionAction.DEFER
        assert "error" in _strip_ansi(output.getvalue()).lower()

    def test_not_applicable_defers(self) -> None:
        """When LLM says no action fits, defers gracefully."""
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
        group = _make_group()

        mock_questions = ConfigFixQuestions(
            questions=[ClarifyingQuestion(question="Q?", question_type="free_text")],
            context_summary="",
        )
        mock_interp = ConfigFixInterpretation(
            parameters={},
            config_action="override_type",
            affected_columns=[],
            interpretation="None of the actions fit.",
            confidence="high",
            summary="Not applicable",
            applicable=False,
        )

        mock_agent = MagicMock()
        mock_agent.generate_config_questions.return_value = Result.ok(mock_questions)
        mock_agent.interpret_config_answers.return_value = Result.ok(mock_interp)

        input_values = iter(["answer"])

        with (
            patch("dataraum.cli.gate_handler._create_document_agent", return_value=mock_agent),
            patch.object(console, "input", side_effect=lambda _: next(input_values)),
        ):
            result = _run_fix_flow(console, MagicMock(), "src", group, event)

        assert result.action == ResolutionAction.DEFER
        rendered = _strip_ansi(output.getvalue())
        assert "no available action" in rendered.lower()


class TestBuildGateContext:
    def test_includes_all_actions(self) -> None:
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
        group = _make_group(
            actions=[
                {
                    "action_name": "override_type",
                    "phase_name": "typing",
                    "dimension": "structural.types.type_fidelity",
                },
                {
                    "action_name": "accept_finding",
                    "phase_name": "quality_review",
                    "dimension": "structural.types.type_fidelity",
                },
            ]
        )

        context = build_gate_context(session, "src-1", group, event)

        assert "<available_actions>" in context
        assert "override_type" in context
        assert "accept_finding" in context
        assert "Action 1:" in context
        assert "Action 2:" in context
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
        group = _make_group()

        context = build_gate_context(session, "src-1", group, event)

        assert "<entropy_evidence>" in context
        assert "type_fidelity" in context
        assert "0.85" in context
        assert "Per-column breakdown:" in context
