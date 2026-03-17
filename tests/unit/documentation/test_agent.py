"""Tests for the document agent."""

from __future__ import annotations

from unittest.mock import MagicMock

from dataraum.core.models.base import Result
from dataraum.documentation.agent import (
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


