"""Tests for OntologyInductionAgent."""

from unittest.mock import MagicMock, patch

from dataraum.analysis.semantic.induction import OntologyInductionAgent
from dataraum.analysis.semantic.ontology import OntologyConcept, OntologyDefinition
from dataraum.core.models.base import Result


def _make_agent() -> OntologyInductionAgent:
    """Create an OntologyInductionAgent with mocked dependencies."""
    mock_config = MagicMock()
    mock_config.limits.max_output_tokens_per_request = 8000
    mock_config.privacy = MagicMock()

    mock_provider = MagicMock()
    mock_renderer = MagicMock()

    return OntologyInductionAgent(
        config=mock_config,
        provider=mock_provider,
        prompt_renderer=mock_renderer,
    )


def _mock_llm_response(agent: OntologyInductionAgent, definition: OntologyDefinition) -> None:
    """Configure the mocked provider to return a tool call with the given definition."""
    tool_call = MagicMock()
    tool_call.input = definition.model_dump()

    response = MagicMock()
    response.tool_calls = [tool_call]

    agent.provider.converse.return_value = Result.ok(response)
    agent.provider.get_model_for_tier.return_value = "claude-sonnet-4-5"
    agent.renderer.render_split.return_value = ("system", "user", 0.0)


def _make_mock_profile() -> MagicMock:
    """Create a minimal mock ColumnProfile."""
    p = MagicMock()
    p.column_ref.table_name = "orders"
    p.column_ref.column_name = "amount"
    p.total_count = 100
    p.null_ratio = 0.0
    p.null_count = 0
    p.distinct_count = 50
    p.cardinality_ratio = 0.5
    p.original_name = None
    return p


class TestOntologyInductionAgent:
    """Test OntologyInductionAgent."""

    @patch("dataraum.analysis.semantic.agent.SemanticAgent._load_profiles")
    def test_no_profiles_returns_fail(self, mock_load: MagicMock) -> None:
        agent = _make_agent()
        mock_load.return_value = Result.ok([])

        result = agent.induce(MagicMock(), table_ids=["t1"])

        assert not result.success
        assert "profiles" in (result.error or "").lower()

    @patch("dataraum.analysis.semantic.agent.SemanticAgent._build_tables_json")
    @patch("dataraum.analysis.semantic.agent.SemanticAgent._load_profiles")
    @patch("dataraum.analysis.semantic.induction.DataSampler")
    def test_successful_induction(
        self, mock_sampler_cls: MagicMock, mock_load: MagicMock, mock_build: MagicMock
    ) -> None:
        agent = _make_agent()

        mock_load.return_value = Result.ok([_make_mock_profile()])
        mock_sampler_cls.return_value.prepare_samples.return_value = {}
        mock_build.return_value = [{"table_name": "orders", "columns": []}]

        expected = OntologyDefinition(
            name="induced",
            description="Auto-generated ontology",
            concepts=[
                OntologyConcept(
                    name="order_amount",
                    description="Transaction amount",
                    indicators=["amount", "total"],
                    typical_role="measure",
                    temporal_behavior="additive",
                ),
            ],
        )
        _mock_llm_response(agent, expected)

        result = agent.induce(MagicMock(), table_ids=["t1"])

        assert result.success
        assert result.value is not None
        assert len(result.value.concepts) == 1
        assert result.value.concepts[0].name == "order_amount"

    @patch("dataraum.analysis.semantic.agent.SemanticAgent._build_tables_json")
    @patch("dataraum.analysis.semantic.agent.SemanticAgent._load_profiles")
    @patch("dataraum.analysis.semantic.induction.DataSampler")
    def test_llm_failure_returns_fail(
        self, mock_sampler_cls: MagicMock, mock_load: MagicMock, mock_build: MagicMock
    ) -> None:
        agent = _make_agent()

        mock_load.return_value = Result.ok([_make_mock_profile()])
        mock_sampler_cls.return_value.prepare_samples.return_value = {}
        mock_build.return_value = []

        agent.provider.converse.return_value = Result.fail("API error")
        agent.provider.get_model_for_tier.return_value = "claude-sonnet-4-5"
        agent.renderer.render_split.return_value = ("system", "user", 0.0)

        result = agent.induce(MagicMock(), table_ids=["t1"])

        assert not result.success
        assert "failed" in (result.error or "").lower()

    @patch("dataraum.analysis.semantic.agent.SemanticAgent._build_tables_json")
    @patch("dataraum.analysis.semantic.agent.SemanticAgent._load_profiles")
    @patch("dataraum.analysis.semantic.induction.DataSampler")
    def test_no_tool_calls_returns_fail(
        self, mock_sampler_cls: MagicMock, mock_load: MagicMock, mock_build: MagicMock
    ) -> None:
        agent = _make_agent()

        mock_load.return_value = Result.ok([_make_mock_profile()])
        mock_sampler_cls.return_value.prepare_samples.return_value = {}
        mock_build.return_value = []

        response = MagicMock()
        response.tool_calls = []
        agent.provider.converse.return_value = Result.ok(response)
        agent.provider.get_model_for_tier.return_value = "claude-sonnet-4-5"
        agent.renderer.render_split.return_value = ("system", "user", 0.0)

        result = agent.induce(MagicMock(), table_ids=["t1"])

        assert not result.success
        assert "tool" in (result.error or "").lower()
