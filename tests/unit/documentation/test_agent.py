"""Tests for the document agent."""

from __future__ import annotations

from unittest.mock import MagicMock

from dataraum.core.models.base import Result
from dataraum.documentation.agent import (
    ConfigFixInterpretation,
    ConfigFixQuestions,
    DocumentAgent,
    DocumentFixInterpretation,
    DocumentFixQuestions,
)
from dataraum.llm.providers.base import ConversationResponse, ToolCall


def _make_agent() -> tuple[DocumentAgent, MagicMock]:
    """Create a DocumentAgent with a mocked provider."""
    provider = MagicMock()
    renderer = MagicMock()

    # Default renderer behavior: return system, user, temperature
    renderer.render_split.return_value = ("system prompt", "user prompt", 0.2)

    agent = DocumentAgent(
        provider=provider,
        renderer=renderer,
        model="test-model",
    )
    return agent, provider


SAMPLE_CONTEXT = """<action_details>
Action: document_unit
Description: Document the unit of measure for amount columns
Priority: low (score: 0.10)
Effort: low
</action_details>

<data_profile>
  Column: transactions.amount
    Table rows: 1000
    Distinct values: 450
    Sample values: 100.00, 250.50, 1200.00
</data_profile>"""


class TestGenerateQuestions:
    def test_returns_questions(self) -> None:
        agent, provider = _make_agent()

        questions_data = {
            "questions": [
                {
                    "question": "What currency is the amount column in?",
                    "question_type": "multiple_choice",
                    "choices": ["EUR", "USD", "GBP", "Other"],
                },
            ],
            "context_summary": "The amount column needs a currency unit.",
        }

        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="",
                tool_calls=[
                    ToolCall(id="tc1", name="document_fix_questions", input=questions_data)
                ],
                stop_reason="tool_use",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        result = agent.generate_questions(SAMPLE_CONTEXT)
        assert result.success
        questions = result.unwrap()
        assert isinstance(questions, DocumentFixQuestions)
        assert len(questions.questions) == 1
        assert questions.questions[0].question_type == "multiple_choice"
        assert "EUR" in questions.questions[0].choices

    def test_llm_failure(self) -> None:
        agent, provider = _make_agent()
        provider.converse.return_value = Result.fail("API error")

        result = agent.generate_questions(SAMPLE_CONTEXT)
        assert not result.success
        assert "API error" in (result.error or "")

    def test_no_tool_call(self) -> None:
        agent, provider = _make_agent()
        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="I don't know",
                tool_calls=[],
                stop_reason="end_turn",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        result = agent.generate_questions(SAMPLE_CONTEXT)
        assert not result.success
        assert "did not use the tool" in (result.error or "")


class TestInterpretAnswers:
    def test_returns_interpretation(self) -> None:
        agent, provider = _make_agent()

        interp_data = {
            "interpretation": "Column transactions.amount uses EUR as fixed currency unit.",
            "confidence": "high",
            "summary": "amount is always in EUR",
        }

        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="",
                tool_calls=[
                    ToolCall(id="tc1", name="document_fix_interpretation", input=interp_data)
                ],
                stop_reason="tool_use",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        result = agent.interpret_answers(SAMPLE_CONTEXT, "Q: Currency?\nA: EUR")
        assert result.success
        interp = result.unwrap()
        assert isinstance(interp, DocumentFixInterpretation)
        assert interp.confidence == "high"
        assert "EUR" in interp.interpretation


# --- Config fix mode tests ---

CONFIG_CONTEXT = """<action_details>
Action: override_type
Description: Override column type from VARCHAR to DATE
Priority: high (score: 0.85)
Affected columns: orders.order_date
</action_details>

<entropy_evidence>
Detector: type_fidelity
Score: 0.85
Evidence: Column typed as VARCHAR but 98% of values match DATE pattern
</entropy_evidence>

<data_profile>
  Column: orders.order_date
    Table rows: 5000
    Distinct values: 365
    Sample values: 2024-01-15, 2024-02-28, 2024-03-01
    Current type: VARCHAR
    Semantic role: timestamp
</data_profile>"""


class TestGenerateConfigQuestions:
    def test_returns_config_questions(self) -> None:
        agent, provider = _make_agent()

        questions_data = {
            "questions": [
                {
                    "question": "Override orders.order_date from VARCHAR to DATE?",
                    "question_type": "multiple_choice",
                    "choices": ["DATE", "TIMESTAMP", "Keep VARCHAR"],
                },
            ],
            "context_summary": "order_date is VARCHAR but contains dates.",
            "suggested_action": "override_type",
        }

        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="",
                tool_calls=[ToolCall(id="tc1", name="config_fix_questions", input=questions_data)],
                stop_reason="tool_use",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        result = agent.generate_config_questions(CONFIG_CONTEXT)
        assert result.success
        questions = result.unwrap()
        assert isinstance(questions, ConfigFixQuestions)
        assert len(questions.questions) == 1
        assert questions.suggested_action == "override_type"
        assert "DATE" in questions.questions[0].choices

    def test_uses_config_fix_template(self) -> None:
        """Verify config mode uses the config_fix prompt template."""
        agent, provider = _make_agent()

        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="",
                tool_calls=[
                    ToolCall(
                        id="tc1",
                        name="config_fix_questions",
                        input={
                            "questions": [],
                            "context_summary": "",
                            "suggested_action": "",
                        },
                    )
                ],
                stop_reason="tool_use",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        agent.generate_config_questions(CONFIG_CONTEXT)

        # Verify render_split was called with "config_fix" template
        call_args = agent.renderer.render_split.call_args
        assert call_args[0][0] == "config_fix"
        assert call_args[0][1]["mode"] == "questions"

    def test_llm_failure(self) -> None:
        agent, provider = _make_agent()
        provider.converse.return_value = Result.fail("API error")

        result = agent.generate_config_questions(CONFIG_CONTEXT)
        assert not result.success

    def test_no_tool_call(self) -> None:
        agent, provider = _make_agent()
        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="I can't help",
                tool_calls=[],
                stop_reason="end_turn",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        result = agent.generate_config_questions(CONFIG_CONTEXT)
        assert not result.success
        assert "did not use the tool" in (result.error or "")


class TestInterpretConfigAnswers:
    def test_returns_structured_parameters(self) -> None:
        agent, provider = _make_agent()

        interp_data = {
            "parameters": {"resolved_type": "DATE"},
            "config_action": "override_type",
            "affected_columns": ["orders.order_date"],
            "interpretation": "Override orders.order_date type from VARCHAR to DATE.",
            "confidence": "high",
            "summary": "Type override to DATE",
        }

        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="",
                tool_calls=[
                    ToolCall(id="tc1", name="config_fix_interpretation", input=interp_data)
                ],
                stop_reason="tool_use",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        result = agent.interpret_config_answers(CONFIG_CONTEXT, "Q: Override type?\nA: DATE")
        assert result.success
        interp = result.unwrap()
        assert isinstance(interp, ConfigFixInterpretation)
        assert interp.parameters == {"resolved_type": "DATE"}
        assert interp.config_action == "override_type"
        assert interp.affected_columns == ["orders.order_date"]
        assert interp.confidence == "high"

    def test_uses_config_fix_template(self) -> None:
        """Verify interpret mode uses the config_fix prompt template."""
        agent, provider = _make_agent()

        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="",
                tool_calls=[
                    ToolCall(
                        id="tc1",
                        name="config_fix_interpretation",
                        input={
                            "parameters": {},
                            "config_action": "",
                            "affected_columns": [],
                            "interpretation": "",
                            "confidence": "low",
                            "summary": "",
                        },
                    )
                ],
                stop_reason="tool_use",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        agent.interpret_config_answers(CONFIG_CONTEXT, "A: yes")

        call_args = agent.renderer.render_split.call_args
        assert call_args[0][0] == "config_fix"
        assert call_args[0][1]["mode"] == "interpret"
        assert call_args[0][1]["user_answers"] == "A: yes"

    def test_llm_failure(self) -> None:
        agent, provider = _make_agent()
        provider.converse.return_value = Result.fail("API error")

        result = agent.interpret_config_answers(CONFIG_CONTEXT, "A: DATE")
        assert not result.success

    def test_no_tool_call(self) -> None:
        agent, provider = _make_agent()
        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="I can't help",
                tool_calls=[],
                stop_reason="end_turn",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        result = agent.interpret_config_answers(CONFIG_CONTEXT, "A: DATE")
        assert not result.success
        assert "did not use the tool" in (result.error or "")
