"""Tests for CycleInductionAgent."""

from unittest.mock import MagicMock, patch

from dataraum.analysis.cycles.induction import CycleInductionAgent
from dataraum.core.models.base import Result


def _make_agent() -> CycleInductionAgent:
    mock_config = MagicMock()
    mock_config.limits.max_output_tokens_per_request = 8000
    return CycleInductionAgent(
        config=mock_config,
        provider=MagicMock(),
        prompt_renderer=MagicMock(),
    )


class TestCycleInductionAgent:
    @patch("dataraum.analysis.cycles.induction._build_induction_context")
    def test_no_tables_returns_fail(self, mock_ctx: MagicMock) -> None:
        agent = _make_agent()
        mock_ctx.return_value = ([], "", "")
        result = agent.induce(MagicMock(), table_ids=["t1"])
        assert not result.success

    @patch("dataraum.analysis.cycles.induction._build_induction_context")
    def test_successful_induction(self, mock_ctx: MagicMock) -> None:
        agent = _make_agent()
        mock_ctx.return_value = (
            [{"table_name": "orders", "columns": []}],
            "annotations",
            "relationships",
        )
        agent.renderer.render_split.return_value = ("system", "user", 0.0)
        agent.provider.get_model_for_tier.return_value = "claude-sonnet-4-5"

        expected = {
            "cycle_types": {
                "order_to_payment": {
                    "description": "Order to payment flow",
                    "business_value": "high",
                    "typical_stages": [{"name": "Order", "order": 1, "indicators": ["ordered"]}],
                    "participating_entities": ["order", "payment"],
                    "completion_indicators": ["paid"],
                    "feeds_into": [],
                }
            }
        }
        tool_call = MagicMock()
        tool_call.input = expected
        response = MagicMock()
        response.tool_calls = [tool_call]
        agent.provider.converse.return_value = Result.ok(response)

        result = agent.induce(MagicMock(), table_ids=["t1"])
        assert result.success
        assert "order_to_payment" in result.value["cycle_types"]

    @patch("dataraum.analysis.cycles.induction._build_induction_context")
    def test_llm_failure(self, mock_ctx: MagicMock) -> None:
        agent = _make_agent()
        mock_ctx.return_value = ([{"table_name": "t", "columns": []}], "", "")
        agent.renderer.render_split.return_value = ("system", "user", 0.0)
        agent.provider.get_model_for_tier.return_value = "claude-sonnet-4-5"
        agent.provider.converse.return_value = Result.fail("API error")

        result = agent.induce(MagicMock(), table_ids=["t1"])
        assert not result.success
