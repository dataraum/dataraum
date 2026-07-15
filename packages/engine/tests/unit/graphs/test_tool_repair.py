"""Tool-boundary schema enforcement with a repair turn (DAT-699).

The grounding output schema is ENFORCED, never coerced: when the model's
generate_sql arguments fail validation (live kill: `provenance` emitted as a
JSON-encoded string), the agent re-prompts the model with its own output plus
the exact validation error under a forced tool choice, and validates again.
One finished, correct grounding must never be discarded whole on a
serialization slip — and the repair is the model's, not a silent
``json.loads`` behind its back.
"""

from __future__ import annotations

import json
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

_VALID_INPUT = {
    "grounding": "revenue via account_type = 'Income' (complete value set)",
    "relation": "t",
    "where": ["account_type IN ('Income')"],
    "select_expr": "SUM(amount)",
    "description": "Total revenue",
    "provenance": {
        "column_mappings_basis": {"revenue": {"column": "amount", "filter": "Income"}},
    },
}

# What compose_extract_sql renders from _VALID_INPUT's parts — the step's `sql`.
_VALID_RENDERED = "SELECT SUM(amount) AS value\nFROM t\nWHERE account_type IN ('Income')"

# The live failure shape: the nested provenance object emitted as a JSON string.
_STRINGIFIED_INPUT = {**_VALID_INPUT, "provenance": json.dumps(_VALID_INPUT["provenance"])}


def _graph() -> TransformationGraph:
    ext = GraphStep(
        step_id="revenue",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="revenue", statement="income_statement"),
        aggregation="sum",
        output_step=True,
    )
    return TransformationGraph(
        graph_id="revenue",
        version="1.0",
        metadata=GraphMetadata(
            name="revenue", description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={"revenue": ext},
    )


def _agent_with(provider: MagicMock) -> GraphAgent:
    agent = GraphAgent.__new__(GraphAgent)
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user", 0.0)
    agent.renderer = renderer  # type: ignore[attr-defined]
    agent.provider = provider  # type: ignore[attr-defined]
    config = MagicMock()
    config.limits.max_output_tokens_per_request = 4000
    config.features.graph_sql_generation = None
    agent.config = config  # type: ignore[attr-defined]
    agent._build_schema_info = MagicMock(return_value={})  # type: ignore[method-assign]
    agent._build_prior_context = MagicMock(return_value="")  # type: ignore[method-assign]
    return agent


def _tool_response(tool_input: dict | None) -> MagicMock:
    response = MagicMock()
    if tool_input is None:
        response.tool_calls = []
    else:
        call = MagicMock()
        call.name = "generate_sql"
        call.input = tool_input
        response.tool_calls = [call]
    return response


def _provider(*responses: MagicMock) -> MagicMock:
    provider = MagicMock()
    provider.get_model_for_tier.side_effect = lambda tier: f"model-{tier}"
    results = [MagicMock(unwrap=MagicMock(return_value=r)) for r in responses]
    provider.converse.side_effect = results
    return provider


def _ctx() -> ExecutionContext:
    rich = MagicMock()
    rich.field_mappings.mappings = {"revenue": object()}
    rich.conventions = ""
    return ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws", rich_context=rich)


def _generate(agent: GraphAgent) -> object:
    return agent._generate_sql(MagicMock(), _graph(), _ctx(), {})


def test_valid_output_needs_no_repair(monkeypatch) -> None:
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "M")
    provider = _provider(_tool_response(_VALID_INPUT))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    assert provider.converse.call_count == 1


def test_stringified_field_is_repaired_by_the_model(monkeypatch) -> None:
    """The live kill: provenance as a JSON string. The repair turn carries the
    invalid input + the validation error, forces the tool, and the repaired
    output grounds the extract — the finished SQL is never discarded."""
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "M")
    provider = _provider(_tool_response(_STRINGIFIED_INPUT), _tool_response(_VALID_INPUT))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    generated = result.unwrap()
    assert generated.steps[0]["sql"] == _VALID_RENDERED
    assert provider.converse.call_count == 2

    repair_request = provider.converse.call_args_list[1].args[0]
    assert repair_request.tool_choice == {"type": "tool", "name": "generate_sql"}
    assert repair_request.label == "graph_sql_generation_repair"
    content = repair_request.messages[0].content
    assert "Validation error" in content
    assert "column_mappings_basis" in content  # the model's own output rides along


def test_second_validation_failure_fails_loud_with_both_errors(monkeypatch) -> None:
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "M")
    provider = _provider(_tool_response(_STRINGIFIED_INPUT), _tool_response(_STRINGIFIED_INPUT))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert not result.success
    assert "after a repair turn" in result.error
    assert "original error" in result.error
    assert provider.converse.call_count == 2


def test_repair_turn_without_tool_call_fails_loud(monkeypatch) -> None:
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "M")
    provider = _provider(_tool_response(_STRINGIFIED_INPUT), _tool_response(None))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert not result.success
    assert "no tool call" in result.error
