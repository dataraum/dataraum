"""The agent→request seam, per agent, against the REAL config (DAT-807).

``test_request_shape_contract.py`` pins what the provider CHOKEPOINT builds from
a request. This file pins the other half: that each agent actually builds that
request — reading its model tier and effort from ``llm/config.yaml``, carrying
its output schema, and forcing no tool.

That seam is where the bug this slice fixed lived: ``slicing_analysis`` never
passed ``model=``, so it silently ran on the provider's ``default_model`` and
ignored its configured tier. A contract test that assembles the request itself
cannot catch that class — only driving the real agent can.

The agents whose seam is pinned elsewhere are deliberately absent:
``semantic_per_table`` (test_synthesis_output.py), ``dimension_conform`` /
``dimension_alias`` (test_judge.py), ``graph_sql_generation``
(test_contract_repair.py — the one agent that DOES ship a tool).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dataraum.llm.config import LLMConfig, load_llm_config
from dataraum.llm.providers.base import ConversationRequest


@pytest.fixture(scope="module")
def config() -> LLMConfig:
    return load_llm_config()


def _provider(config: LLMConfig) -> MagicMock:
    """A provider that records the request and resolves tiers like the real one."""
    models = config.providers[config.active_provider].models
    provider = MagicMock()
    provider.get_model_for_tier.side_effect = lambda tier: models.get(
        tier, config.providers[config.active_provider].default_model
    )
    # Returning content the output model cannot parse is fine — every assertion
    # here is about the REQUEST, and the agents fail closed afterwards.
    response = MagicMock()
    response.content = "{}"
    response.tool_calls = []
    response.model = "recorded"
    response.stop_reason = "end_turn"
    response.output_tokens = 1
    provider.converse.return_value = MagicMock(unwrap=MagicMock(return_value=response))
    return provider


def _renderer() -> MagicMock:
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user", 0.0)
    return renderer


def _captured(provider: MagicMock) -> ConversationRequest:
    provider.converse.assert_called_once()
    request: ConversationRequest = provider.converse.call_args.args[0]
    return request


def _assert_shape(
    request: ConversationRequest,
    config: LLMConfig,
    *,
    label: str,
    feature_key: str,
    schema_title: str,
) -> None:
    """Every agent's request must carry its CONFIGURED tier + effort, and no tool."""
    feature = getattr(config.features, feature_key)
    models = config.providers[config.active_provider].models
    assert request.label == label
    assert request.model == models[feature.model_tier], (
        f"{label}: must pass model= from its configured tier — omitting it falls "
        "back to the provider default and silently ignores the config"
    )
    assert request.effort == feature.effort
    assert request.max_tokens == config.limits.max_output_tokens_per_request
    assert request.output_schema is not None
    assert request.output_schema["title"] == schema_title
    assert request.tools == []
    assert request.tool_choice is None


def test_slicing_analysis(config: LLMConfig) -> None:
    from dataraum.analysis.slicing.agent import SlicingAgent

    provider = _provider(config)
    agent = SlicingAgent(config, provider, _renderer())
    context_data: dict[str, Any] = {"tables": [{"table_name": "t", "columns": []}]}

    agent.analyze(MagicMock(), ["t1"], context_data)

    _assert_shape(
        _captured(provider),
        config,
        label="slicing_analysis",
        feature_key="slicing_analysis",
        schema_title="SlicingAnalysisOutput",
    )


def test_enrichment_analysis(config: LLMConfig) -> None:
    from dataraum.analysis.views.enrichment_agent import EnrichmentAgent

    provider = _provider(config)
    agent = EnrichmentAgent(config, provider, _renderer())

    agent.analyze(MagicMock(), {"tables": [], "annotations": []})

    _assert_shape(
        _captured(provider),
        config,
        label="enrichment_analysis",
        feature_key="enrichment_analysis",
        schema_title="EnrichmentAnalysisOutput",
    )


def test_validation_sql(config: LLMConfig) -> None:
    from dataraum.analysis.validation.agent import ValidationAgent
    from dataraum.analysis.validation.models import ValidationSpec

    provider = _provider(config)
    agent = ValidationAgent(config, provider, _renderer())
    spec = ValidationSpec(
        validation_id="v1",
        name="V",
        description="d",
        category="c",
        check_type="balance",
    )

    schema = {"tables": [{"table_name": "t", "duckdb_path": "t", "columns": []}]}
    agent._generate_sql(spec, schema)

    _assert_shape(
        _captured(provider),
        config,
        label="validation_sql",
        feature_key="validation",
        schema_title="ValidationSQLOutput",
    )


def test_column_annotation(config: LLMConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent

    ontology = MagicMock()
    ontology.concepts = [MagicMock()]
    monkeypatch.setattr(
        "dataraum.analysis.semantic.column_agent.load_workspace_concepts", lambda s, o: ontology
    )
    provider = _provider(config)
    agent = ColumnAnnotationAgent(config, provider, _renderer())
    agent._ontology_loader = MagicMock()  # type: ignore[method-assign]
    agent._build_tables_json = MagicMock(return_value=[])  # type: ignore[method-assign]
    monkeypatch.setattr(
        "dataraum.analysis.semantic.column_agent.DataSampler",
        MagicMock(return_value=MagicMock(prepare_samples=MagicMock(return_value={}))),
    )

    agent.annotate(MagicMock(), ["t1"], profiles=[MagicMock()])

    _assert_shape(
        _captured(provider),
        config,
        label="column_annotation",
        feature_key="column_annotation",
        schema_title="ColumnAnnotationOutput",
    )


def test_business_cycles(config: LLMConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    from dataraum.analysis.cycles.agent import BusinessCycleAgent
    from dataraum.lifecycle import BaseRunMap

    monkeypatch.setattr(
        "dataraum.analysis.cycles.agent.build_cycle_detection_context",
        lambda *a, **k: {"tables": [], "summary": {}},
    )
    monkeypatch.setattr("dataraum.analysis.cycles.agent.format_context_for_prompt", lambda c: "CTX")
    provider = _provider(config)
    agent = BusinessCycleAgent(config, provider, _renderer())

    agent.ground_cycles(
        MagicMock(),
        MagicMock(),
        ["t1"],
        vertical="general",
        base_runs=BaseRunMap(),
    )

    _assert_shape(
        _captured(provider),
        config,
        label="business_cycles",
        feature_key="business_cycles",
        schema_title="BusinessCycleAnalysisOutput",
    )
