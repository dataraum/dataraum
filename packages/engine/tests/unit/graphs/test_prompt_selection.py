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


def _agent_with(renderer: MagicMock, provider: MagicMock) -> GraphAgent:
    agent = GraphAgent.__new__(GraphAgent)
    agent.renderer = renderer  # type: ignore[attr-defined]
    agent.provider = provider  # type: ignore[attr-defined]
    config = MagicMock()
    config.limits.max_output_tokens_per_request = 4000
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
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws", rich_context=rich)

    agent._generate_sql(MagicMock(), graph, ctx, {})

    name, prompt_ctx = renderer.render_split.call_args.args
    assert name == "graph_sql_generation"
    provider.get_model_for_tier.assert_called_with("balanced")
    assert prompt_ctx["rich_context"] == "META" and prompt_ctx["field_mappings"] == "MAPS"
