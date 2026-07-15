"""Tool-boundary enforcement with a repair turn — schema (DAT-699) + contract (DAT-727).

The grounding output schema is ENFORCED, never coerced: when the model's
generate_sql arguments fail validation (live kill: `provenance` emitted as a
JSON-encoded string), the agent re-prompts the model with its own output plus
the exact validation error under a forced tool choice, and validates again.
One finished, correct grounding must never be discarded whole on a
serialization slip — and the repair is the model's, not a silent
``json.loads`` behind its back.

The provenance contract v2 (DAT-727) rides the same mechanics one level up:
a schema-valid output whose column enumeration violates the served schema
(membership) or its own SQL parts (completeness) gets a contract-repair turn;
a still-invalid output falls loud into the failed-snippet path.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import duckdb

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
        "field_resolution": "direct",
        "column_mappings_basis": {
            "revenue": {
                "measure_columns": ["amount"],
                "filter_columns": ["account_type"],
                "filter": "Income",
                "resolution": "direct",
            }
        },
    },
}

# What compose_extract_sql renders from _VALID_INPUT's parts — the step's `sql`.
_VALID_RENDERED = "SELECT SUM(amount) AS value\nFROM t\nWHERE account_type IN ('Income')"

# The live failure shape: the nested provenance object emitted as a JSON string.
_STRINGIFIED_INPUT = {**_VALID_INPUT, "provenance": json.dumps(_VALID_INPUT["provenance"])}

# The relation's SERVED schema — what _build_schema_info renders into the prompt
# and what the contract enforcement validates against.
_SCHEMA_INFO = {
    "tables": [
        {
            "table_name": "t",
            "columns": [
                {"name": "amount", "type": "DECIMAL"},
                {"name": "account_type", "type": "VARCHAR"},
            ],
            "row_count": 3,
        }
    ]
}


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
    # The served schema feeds the prompt AND the contract-v2 enforcement.
    agent._build_schema_info = MagicMock(return_value=_SCHEMA_INFO)  # type: ignore[method-assign]
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
    # A REAL in-memory DuckDB: the contract enforcement parses the SQL parts
    # through json_serialize_sql (validator only) on this connection.
    return ExecutionContext(
        duckdb_conn=duckdb.connect(":memory:"), schema_mapping_id="ws", rich_context=rich
    )


def _generate(agent: GraphAgent) -> object:
    return agent._generate_sql(MagicMock(), _graph(), _ctx(), {}, workspace_id="ws")


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
    assert "field_resolution" in content  # the model's own output rides along


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


# --- Provenance contract v2 enforcement (DAT-727) ----------------------------------


def _with_basis(basis: dict) -> dict:
    return {
        **_VALID_INPUT,
        "provenance": {"field_resolution": "direct", "column_mappings_basis": basis},
    }


def test_membership_violation_gets_a_contract_repair_turn(monkeypatch) -> None:
    """A schema-valid output enumerating a column the served relation does not
    have ('amout') is contract-repaired — the repair prompt names the violation
    and the repaired enumeration grounds the extract."""
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "M")
    bad = _with_basis(
        {
            "revenue": {
                "measure_columns": ["amout"],
                "filter_columns": ["account_type"],
                "resolution": "direct",
            }
        }
    )
    provider = _provider(_tool_response(bad), _tool_response(_VALID_INPUT))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    assert provider.converse.call_count == 2
    repair_request = provider.converse.call_args_list[1].args[0]
    assert repair_request.tool_choice == {"type": "tool", "name": "generate_sql"}
    content = repair_request.messages[0].content
    assert "Contract violations" in content
    assert "'amout'" in content


def test_completeness_violation_gets_a_contract_repair_turn(monkeypatch) -> None:
    """The SQL parts filter on account_type but the enumeration omits it — the
    completeness net (parts cross-check) catches it and the repair turn fixes
    the enumeration, not the SQL."""
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "M")
    incomplete = _with_basis({"revenue": {"measure_columns": ["amount"], "resolution": "direct"}})
    provider = _provider(_tool_response(incomplete), _tool_response(_VALID_INPUT))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    assert provider.converse.call_count == 2
    content = provider.converse.call_args_list[1].args[0].messages[0].content
    assert "account_type" in content
    assert "does not enumerate" in content


def test_contract_violation_after_repair_falls_into_failed_snippet_path(monkeypatch) -> None:
    """A still-violating repaired output falls loud (DAT-543): the authored SQL is
    retained flagged with mode='provenance_invalid' so prior_context can feed the
    exact violations back to the next authoring."""
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "M")
    incomplete = _with_basis({"revenue": {"measure_columns": ["amount"], "resolution": "direct"}})
    provider = _provider(_tool_response(incomplete), _tool_response(incomplete))
    agent = _agent_with(provider)
    agent._save_failed_snippet = MagicMock()  # type: ignore[method-assign]

    result = _generate(agent)

    assert not result.success
    assert "grounding contract violated after repair" in result.error
    assert "account_type" in result.error
    agent._save_failed_snippet.assert_called_once()
    assert agent._save_failed_snippet.call_args.kwargs["mode"] == "provenance_invalid"
    assert agent._save_failed_snippet.call_args.kwargs["workspace_id"] == "ws"


def test_fall_loud_grounding_is_exempt_from_the_contract(monkeypatch) -> None:
    """The fall-loud shape (relation null, select NULL) carries no columns —
    no contract check, no repair turn, the honest abstention passes through."""
    monkeypatch.setattr("dataraum.graphs.context.format_metadata_document", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_mappings_for_prompt", lambda f: "M")
    fall_loud = {
        "grounding": "revenue cannot be grounded: no served value names it",
        "relation": None,
        "where": [],
        "select_expr": "NULL",
        "description": "ungroundable",
        "assumptions": [
            {
                "dimension": "semantic.grounding",
                "target": "concept:revenue",
                "assumption": "revenue is not grounded",
                "basis": "inferred",
                "confidence": 0.1,
            }
        ],
    }
    provider = _provider(_tool_response(fall_loud))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    assert provider.converse.call_count == 1
    assert result.value.steps[0]["sql"] == "SELECT NULL AS value"
