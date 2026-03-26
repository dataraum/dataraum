"""Tests for VerticalAgent — LLM-powered ontology generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from dataraum.analysis.semantic.ontology import OntologyDefinition
from dataraum.analysis.semantic.vertical_agent import VerticalAgent


def _make_agent(
    mock_provider: Any, mock_renderer: Any, verticals_dir: Path | None = None
) -> VerticalAgent:
    """Build a VerticalAgent with mocked LLM dependencies."""
    mock_config = MagicMock()
    mock_config.limits.max_output_tokens_per_request = 4000
    mock_config.features.vertical_creation = MagicMock(enabled=True, model_tier="balanced")
    mock_provider.get_model_for_tier.return_value = "test-model"
    mock_renderer.render_split.return_value = ("System prompt", "User prompt", 0.0)
    return VerticalAgent(
        config=mock_config,
        provider=mock_provider,
        prompt_renderer=mock_renderer,
        verticals_dir=verticals_dir,
    )


def _make_tool_call_response(concepts: list[dict]) -> Any:
    """Build a mock provider response that returns a valid VerticalCreationOutput."""
    tool_call = MagicMock()
    tool_call.name = "create_ontology"
    tool_call.input = {
        "name": "shopify_datev",
        "description": "Test vertical",
        "concepts": concepts,
    }
    response = MagicMock()
    response.tool_calls = [tool_call]
    response.model = "test-model"
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.value = response
    return mock_result


class TestVerticalAgentGenerate:
    def test_generate_returns_ontology_definition(self) -> None:
        """Happy path: LLM returns valid output, Result is success with OntologyDefinition."""
        mock_provider = MagicMock()
        mock_renderer = MagicMock()
        agent = _make_agent(mock_provider, mock_renderer)

        mock_provider.converse.return_value = _make_tool_call_response(
            [
                {
                    "name": "datev_revenue_standard",
                    "description": "Revenue at 19% VAT",
                    "indicators": ["total", "subtotal"],
                    "temporal_behavior": "additive",
                    "typical_role": "measure",
                },
                {
                    "name": "datev_gift_card_liability",
                    "description": "Gift card sold — liability",
                    "indicators": ["gift_card"],
                    "temporal_behavior": "point_in_time",
                    "typical_role": "measure",
                },
            ]
        )

        result = agent.generate(
            vertical_name="shopify_datev",
            domain_description="Shopify to DATEV",
            source_columns=["total", "subtotal", "gift_card"],
            target_taxonomy=[
                {"code": "4400", "name": "Erlöse 19%", "type": "revenue"},
                {"code": "3480", "name": "Gutscheine", "type": "liability"},
            ],
        )

        assert result.success
        ontology = result.unwrap()
        assert isinstance(ontology, OntologyDefinition)
        assert len(ontology.concepts) == 2
        assert ontology.concepts[0].name == "datev_revenue_standard"
        assert ontology.concepts[1].name == "datev_gift_card_liability"

    def test_generate_with_base_vertical_loads_concepts(self, tmp_path: Path) -> None:
        """When base_vertical is set, existing concepts are passed as context."""
        # Write a minimal finance vertical
        vertical_dir = tmp_path / "finance"
        vertical_dir.mkdir()
        (vertical_dir / "ontology.yaml").write_text(
            "name: financial_reporting\nversion: '1.0.0'\nconcepts:\n  - name: revenue\n    indicators: [revenue]\n    temporal_behavior: additive\n    typical_role: measure\n"
        )

        mock_provider = MagicMock()
        mock_renderer = MagicMock()
        agent = _make_agent(mock_provider, mock_renderer, verticals_dir=tmp_path)

        mock_provider.converse.return_value = _make_tool_call_response(
            [
                {
                    "name": "datev_revenue_standard",
                    "description": "Revenue",
                    "indicators": ["total"],
                    "temporal_behavior": "additive",
                    "typical_role": "measure",
                }
            ]
        )

        result = agent.generate(
            vertical_name="shopify_datev",
            domain_description="Test",
            source_columns=["total"],
            target_taxonomy=[],
            base_vertical="finance",
        )

        assert result.success
        # Verify the base concepts section was passed to the renderer
        render_call_kwargs = mock_renderer.render_split.call_args
        context = render_call_kwargs[0][1]  # second positional arg is context dict
        assert "base_vertical" in context["base_concepts_section"]

    def test_generate_with_unknown_base_vertical_returns_fail(self, tmp_path: Path) -> None:
        """Unknown base_vertical → Result.fail, no LLM call."""
        mock_provider = MagicMock()
        mock_renderer = MagicMock()
        agent = _make_agent(mock_provider, mock_renderer, verticals_dir=tmp_path)

        result = agent.generate(
            vertical_name="new_vertical",
            domain_description="Test",
            source_columns=[],
            target_taxonomy=[],
            base_vertical="nonexistent",
        )

        assert not result.success
        assert "nonexistent" in result.error
        mock_provider.converse.assert_not_called()

    def test_generate_provider_failure_returns_result_fail(self) -> None:
        """LLM API failure propagates as Result.fail."""
        mock_provider = MagicMock()
        mock_renderer = MagicMock()
        agent = _make_agent(mock_provider, mock_renderer)

        failed = MagicMock()
        failed.success = False
        failed.error = "API timeout"
        failed.value = None
        mock_provider.converse.return_value = failed

        result = agent.generate(
            vertical_name="shopify_datev",
            domain_description="Test",
            source_columns=[],
            target_taxonomy=[],
        )

        assert not result.success
        assert "API timeout" in result.error

    def test_generate_llm_no_tool_call_returns_fail(self) -> None:
        """If LLM doesn't use the tool, return a clear error."""
        mock_provider = MagicMock()
        mock_renderer = MagicMock()
        agent = _make_agent(mock_provider, mock_renderer)

        response = MagicMock()
        response.tool_calls = []
        response.content = "I can't do that"
        response.model = "test-model"
        provider_result = MagicMock()
        provider_result.success = True
        provider_result.value = response
        mock_provider.converse.return_value = provider_result

        result = agent.generate(
            vertical_name="shopify_datev",
            domain_description="Test",
            source_columns=[],
            target_taxonomy=[],
        )

        assert not result.success
        assert "create_ontology tool" in result.error

    def test_generate_disabled_feature_returns_fail(self) -> None:
        """When feature is disabled in config, return error without LLM call."""
        mock_provider = MagicMock()
        mock_renderer = MagicMock()
        mock_config = MagicMock()
        mock_config.limits.max_output_tokens_per_request = 4000
        mock_config.features.vertical_creation = MagicMock(enabled=False, model_tier="balanced")
        mock_provider.get_model_for_tier.return_value = "test-model"
        mock_renderer.render_split.return_value = ("System", "User", 0.0)

        agent = VerticalAgent(
            config=mock_config,
            provider=mock_provider,
            prompt_renderer=mock_renderer,
        )

        result = agent.generate(
            vertical_name="test",
            domain_description="Test",
            source_columns=[],
            target_taxonomy=[],
        )

        assert not result.success
        assert "disabled" in result.error
        mock_provider.converse.assert_not_called()
