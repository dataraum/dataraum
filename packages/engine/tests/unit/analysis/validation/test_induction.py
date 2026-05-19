"""Tests for ValidationInductionAgent."""

from unittest.mock import MagicMock, patch

from dataraum.analysis.validation.induction import ValidationInductionAgent
from dataraum.core.models.base import Result


def _make_agent() -> ValidationInductionAgent:
    mock_config = MagicMock()
    mock_config.limits.max_output_tokens_per_request = 8000
    return ValidationInductionAgent(
        config=mock_config,
        provider=MagicMock(),
        prompt_renderer=MagicMock(),
    )


class TestValidationInductionAgent:
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
            [{"table_name": "journal_lines", "columns": []}],
            "annotations",
            "relationships",
        )
        agent.renderer.render_split.return_value = ("system", "user", 0.0)
        agent.provider.get_model_for_tier.return_value = "claude-sonnet-4-5"

        expected = {
            "validations": [
                {
                    "validation_id": "double_entry_balance",
                    "name": "Double Entry Balance",
                    "description": "Debits must equal credits",
                    "category": "financial",
                    "severity": "critical",
                    "check_type": "balance",
                    "sql_hints": "Compare SUM(debit) to SUM(credit)",
                    "expected_outcome": "Difference is zero",
                    "tags": ["accounting"],
                    "relevant_cycles": [],
                }
            ]
        }
        tool_call = MagicMock()
        tool_call.input = expected
        response = MagicMock()
        response.tool_calls = [tool_call]
        agent.provider.converse.return_value = Result.ok(response)

        result = agent.induce(MagicMock(), table_ids=["t1"])
        assert result.success
        assert len(result.value) == 1
        assert result.value[0]["validation_id"] == "double_entry_balance"

    @patch("dataraum.analysis.cycles.induction._build_induction_context")
    def test_llm_failure(self, mock_ctx: MagicMock) -> None:
        agent = _make_agent()
        mock_ctx.return_value = ([{"table_name": "t", "columns": []}], "", "")
        agent.renderer.render_split.return_value = ("system", "user", 0.0)
        agent.provider.get_model_for_tier.return_value = "claude-sonnet-4-5"
        agent.provider.converse.return_value = Result.fail("API error")

        result = agent.induce(MagicMock(), table_ids=["t1"])
        assert not result.success
