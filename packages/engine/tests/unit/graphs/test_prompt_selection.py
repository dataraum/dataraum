"""The LLM authoring surface is a single leaf EXTRACT (DAT-643).

``_generate_sql`` is reached ONLY for an EXTRACT node: a FORMULA/CONSTANT is composed
deterministically and can never call the LLM. So there is no longer a per-node prompt
*selection* — the one prompt is the full grounding prompt (``graph_sql_generation``) on
the balanced/Sonnet tier, fed the dataset context + field mappings.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dataraum.graphs.agent import ExecutionContext, GraphAgent
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepSource,
    StepType,
    TransformationGraph,
)


def _graph(graph_id: str, steps: dict[str, GraphStep]) -> TransformationGraph:
    return TransformationGraph(
        graph_id=graph_id,
        version="1.0",
        metadata=GraphMetadata(
            name=graph_id, description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
    )


def _agent_with(
    renderer: MagicMock,
    provider: MagicMock,
    feature_config: object | None = None,
) -> GraphAgent:
    agent = GraphAgent.__new__(GraphAgent)
    agent.renderer = renderer  # type: ignore[attr-defined]
    agent.provider = provider  # type: ignore[attr-defined]
    config = MagicMock()
    config.limits.max_output_tokens_per_request = 4000
    # A real value (or None), never a bare MagicMock — a mock here would leak a
    # mock `effort`/`model_tier` into ConversationRequest (the PR #432 lesson).
    config.features.graph_sql_generation = feature_config
    agent.config = config  # type: ignore[attr-defined]
    return agent


def _mocks() -> tuple[MagicMock, MagicMock]:
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user", 0.0)
    provider = MagicMock()
    provider.get_model_for_tier.side_effect = lambda tier: f"model-{tier}"
    # No tool_calls → _generate_sql returns Result.fail AFTER prompt selection, which
    # is all this test asserts.
    provider.converse.return_value.unwrap.return_value = MagicMock(tool_calls=[])
    return renderer, provider


def test_extract_node_uses_the_grounding_prompt_on_balanced_tier(monkeypatch) -> None:
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr(
        "dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "MAPS"
    )
    ext = GraphStep(
        step_id="revenue",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="revenue", statement="income_statement"),
        aggregation="sum",
        output_step=True,
    )
    graph = _graph("revenue", {"revenue": ext})
    renderer, provider = _mocks()
    agent = _agent_with(renderer, provider)
    agent._build_schema_info = MagicMock(return_value={})  # type: ignore[method-assign]
    agent._build_prior_context = MagicMock(return_value="")  # type: ignore[method-assign]
    rich = MagicMock()
    rich.field_mappings.mappings = {"revenue": object()}
    rich.conventions = "CREDIT-NORMAL → credit - debit"  # DAT-645: vertical conventions
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws", rich_context=rich)

    agent._generate_sql(MagicMock(), graph, ctx, {})

    name, prompt_ctx = renderer.render_split.call_args.args
    assert name == "graph_sql_generation"
    provider.get_model_for_tier.assert_called_with("balanced")
    assert prompt_ctx["rich_context"] == "META" and prompt_ctx["field_mappings"] == "MAPS"
    # DAT-645: the vertical's conventions are piped verbatim into the prompt context.
    assert prompt_ctx["vertical_conventions"] == "CREDIT-NORMAL → credit - debit"
    # No feature-config entry → API-default effort (None), never a leaked mock.
    request = provider.converse.call_args.args[0]
    assert request.effort is None


def test_feature_config_sets_tier_and_effort(monkeypatch) -> None:
    """DAT-603: the graph_sql_generation feature entry reaches the request."""
    from dataraum.llm.config import FeatureConfig

    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr(
        "dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "MAPS"
    )
    ext = GraphStep(
        step_id="revenue",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="revenue", statement="income_statement"),
        aggregation="sum",
        output_step=True,
    )
    graph = _graph("revenue", {"revenue": ext})
    renderer, provider = _mocks()
    agent = _agent_with(
        renderer, provider, feature_config=FeatureConfig(model_tier="fast", effort="low")
    )
    agent._build_schema_info = MagicMock(return_value={})  # type: ignore[method-assign]
    agent._build_prior_context = MagicMock(return_value="")  # type: ignore[method-assign]
    rich = MagicMock()
    rich.field_mappings.mappings = {"revenue": object()}
    rich.conventions = ""
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws", rich_context=rich)

    agent._generate_sql(MagicMock(), graph, ctx, {})

    provider.get_model_for_tier.assert_called_with("fast")
    request = provider.converse.call_args.args[0]
    assert request.effort == "low"
    # thinking not set on the feature → forced tool_choice, thinking off.
    assert request.thinking is False
    assert request.tool_choice == {"type": "tool", "name": "generate_sql"}


def test_thinking_feature_uses_auto_tool_choice(monkeypatch) -> None:
    """DAT-603: thinking is API-incompatible with a forced tool_choice — a
    thinking feature offers the tool on auto and the prompt mandates the call."""
    from dataraum.llm.config import FeatureConfig

    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr(
        "dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "MAPS"
    )
    ext = GraphStep(
        step_id="revenue",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="revenue", statement="income_statement"),
        aggregation="sum",
        output_step=True,
    )
    graph = _graph("revenue", {"revenue": ext})
    renderer, provider = _mocks()
    agent = _agent_with(renderer, provider, feature_config=FeatureConfig(thinking=True))
    agent._build_schema_info = MagicMock(return_value={})  # type: ignore[method-assign]
    agent._build_prior_context = MagicMock(return_value="")  # type: ignore[method-assign]
    rich = MagicMock()
    rich.field_mappings.mappings = {"revenue": object()}
    rich.conventions = ""
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws", rich_context=rich)

    agent._generate_sql(MagicMock(), graph, ctx, {})

    request = provider.converse.call_args.args[0]
    assert request.thinking is True
    assert request.tool_choice == {"type": "auto"}


def test_multi_step_graph_fails_loud_before_any_llm_call() -> None:
    """DAT-603: authoring takes a single-extract mini-graph ONLY — the full-graph
    authoring path is retired (metrics are assembled from the binding map)."""
    ext_a = GraphStep(
        step_id="revenue",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="revenue", statement="income_statement"),
        aggregation="sum",
    )
    ext_b = GraphStep(
        step_id="cogs",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="cost_of_goods_sold", statement="income_statement"),
        aggregation="sum",
        output_step=True,
    )
    graph = _graph("gross_profit", {"revenue": ext_a, "cogs": ext_b})
    renderer, provider = _mocks()
    agent = _agent_with(renderer, provider)
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws")

    result = agent._generate_sql(MagicMock(), graph, ctx, {})

    assert result.success is False
    assert "single-extract mini-graph" in (result.error or "")
    provider.converse.assert_not_called()


def test_generated_sql_binds_to_the_graphs_own_leaf_id(monkeypatch) -> None:
    """DAT-603: the model returns bare sql + description; THIS code binds it to
    the leaf's step_id — the DAT-664 id-paraphrase class is gone by construction."""
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr(
        "dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "MAPS"
    )
    ext = GraphStep(
        step_id="accounts_receivable",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="accounts_receivable", statement="balance_sheet"),
        aggregation="end_of_period",
        output_step=True,
    )
    graph = _graph("accounts_receivable", {"accounts_receivable": ext})
    renderer, provider = _mocks()
    tool_call = MagicMock()
    tool_call.name = "generate_sql"
    tool_call.input = {
        "grounding": "accounts_receivable via account_id__name IN ('Accounts Receivable')",
        "sql": "SELECT SUM(amount) AS value FROM enriched_gl",
        "description": "AR at latest period",
        "column_mappings": {"accounts_receivable": "enriched_gl.amount"},
        "assumptions": [],
        "provenance": None,
    }
    provider.converse.return_value.unwrap.return_value = MagicMock(tool_calls=[tool_call])
    agent = _agent_with(renderer, provider)
    agent._build_schema_info = MagicMock(return_value={})  # type: ignore[method-assign]
    agent._build_prior_context = MagicMock(return_value="")  # type: ignore[method-assign]
    rich = MagicMock()
    rich.field_mappings.mappings = {"accounts_receivable": object()}
    rich.conventions = ""
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws", rich_context=rich)

    result = agent._generate_sql(MagicMock(), graph, ctx, {})

    assert result.success is True
    code = result.value
    assert code is not None
    assert code.steps == [
        {
            "step_id": "accounts_receivable",
            "sql": "SELECT SUM(amount) AS value FROM enriched_gl",
            "description": "AR at latest period",
        }
    ]
    assert code.final_sql == "SELECT * FROM accounts_receivable"
    assert code.summary == "AR at latest period"
