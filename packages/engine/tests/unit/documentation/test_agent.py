"""Tests for the batch plan agent."""

from __future__ import annotations

from unittest.mock import MagicMock

from dataraum.core.models.base import Result
from dataraum.documentation.agent import BatchActionPlan, BatchPlanAgent
from dataraum.llm.providers.base import ConversationResponse, ToolCall


def _make_agent() -> tuple[BatchPlanAgent, MagicMock]:
    """Create a BatchPlanAgent with a mocked provider."""
    provider = MagicMock()
    renderer = MagicMock()

    # Default renderer behavior: return system, user, temperature
    renderer.render_split.return_value = ("system prompt", "user prompt", 0.2)

    agent = BatchPlanAgent(
        provider=provider,
        renderer=renderer,
        model="test-model",
    )
    return agent, provider


SAMPLE_CONTEXT = """<available_actions>
Dimension: value.nulls.null_ratio
Score: 0.70 (threshold: 0.30)
Affected columns: column:orders.amount, column:orders.name

--- Action 1: concept_property ---
Guidance: Accept null ratio as expected.
</available_actions>"""


class TestGenerateBatchPlan:
    def test_returns_plan(self) -> None:
        agent, provider = _make_agent()

        plan_data = {
            "summary": "Accept nulls for both columns",
            "items": [
                {
                    "target": "column:orders.amount",
                    "recommended_action": "concept_property",
                    "reason": "Optional field",
                    "parameters": {},
                },
            ],
            "follow_up_questions": [],
        }

        provider.converse.return_value = Result.ok(
            ConversationResponse(
                content="",
                tool_calls=[ToolCall(id="tc1", name="batch_action_plan", input=plan_data)],
                stop_reason="tool_use",
                model="test-model",
                input_tokens=100,
                output_tokens=50,
            )
        )

        result = agent.generate_batch_plan(SAMPLE_CONTEXT)
        assert result.success
        plan = result.unwrap()
        assert isinstance(plan, BatchActionPlan)
        assert len(plan.items) == 1
        assert plan.items[0].recommended_action == "concept_property"

    def test_llm_failure(self) -> None:
        agent, provider = _make_agent()
        provider.converse.return_value = Result.fail("API error")

        result = agent.generate_batch_plan(SAMPLE_CONTEXT)
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

        result = agent.generate_batch_plan(SAMPLE_CONTEXT)
        assert not result.success
        assert "did not use the tool" in (result.error or "")
