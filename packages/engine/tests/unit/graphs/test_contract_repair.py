"""Grounding-output enforcement: structured output (DAT-807) + contract v2 (DAT-727).

The grounding arrives as message CONTENT the API constrained to
``ExtractGroundingOutput``'s schema, so the malformation class the DAT-699/710
schema-repair turn existed for — ``provenance`` emitted as a JSON-encoded string
— is structurally unreachable and that repair is gone.

The CONTRACT repair is not: constrained decoding guarantees SHAPE, never
semantics. A schema-valid output whose column enumeration violates the served
schema (membership) or its own SQL parts (completeness) still gets one
contract-repair turn, and a still-invalid output falls loud into the
failed-snippet path.
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
from dataraum.llm.providers.base import ToolCall

_VALID_OUTPUT = {
    "grounding": "revenue via account_type = 'Income' (complete value set)",
    "relation": "t",
    "where": ["account_type IN ('Income')"],
    "select_expr": "SUM(amount)",
    "description": "Total revenue",
    "assumptions": [],
    # Contract v2 (DAT-727): the enumeration covers exactly the columns the
    # parts touch, by role. A LIST of {concept, basis} since DAT-807 — an open
    # map cannot be expressed under constrained decoding.
    "provenance": {
        "column_mappings_basis": [
            {
                "concept": "revenue",
                "basis": {
                    "measure_columns": ["amount"],
                    "filter_columns": ["account_type"],
                    "filter": "Income",
                    "filter_members": [{"column": "account_type", "value": "Income"}],
                },
            }
        ],
    },
}

# What compose_extract_sql renders from _VALID_OUTPUT's parts — the step's `sql`.
_VALID_RENDERED = "SELECT SUM(amount) AS value\nFROM t\nWHERE account_type IN ('Income')"

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


def _output_response(payload: dict) -> MagicMock:
    """A finished turn: structured-output content, no tool call."""
    response = MagicMock()
    response.content = json.dumps(payload)
    response.tool_calls = []
    return response


def _search_response() -> MagicMock:
    """A turn that calls the ONE real tool instead of finishing."""
    response = MagicMock()
    response.content = ""
    response.raw_content = None
    response.tool_calls = [
        ToolCall(
            id="tu-1",
            name="search_values",
            input={"table": "t", "column": "account_type", "pattern": "inc"},
        )
    ]
    return response


def _provider(*responses: MagicMock) -> MagicMock:
    provider = MagicMock()
    provider.get_model_for_tier.side_effect = lambda tier: f"model-{tier}"
    results = [MagicMock(unwrap=MagicMock(return_value=r)) for r in responses]
    provider.converse.side_effect = results
    return provider


def _ctx() -> ExecutionContext:
    rich = MagicMock()
    rich.field_mappings = [object()]
    rich.conventions = ""
    # A REAL in-memory DuckDB: the contract enforcement parses the SQL parts
    # through json_serialize_sql (validator only) on this connection.
    return ExecutionContext(
        duckdb_conn=duckdb.connect(":memory:"), schema_mapping_id="ws", rich_context=rich
    )


def _generate(agent: GraphAgent) -> object:
    return agent._generate_sql(MagicMock(), _graph(), _ctx(), {}, workspace_id="ws")


def _patch_context(monkeypatch) -> None:
    monkeypatch.setattr("dataraum.graphs.context.format_served_context", lambda c: "META")
    monkeypatch.setattr("dataraum.graphs.field_mapping.format_meanings_for_prompt", lambda f: "M")


def test_valid_output_grounds_in_one_turn(monkeypatch) -> None:
    _patch_context(monkeypatch)
    provider = _provider(_output_response(_VALID_OUTPUT))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    assert result.unwrap().steps[0]["sql"] == _VALID_RENDERED
    assert provider.converse.call_count == 1


def test_request_carries_the_output_schema_and_only_the_real_tool(monkeypatch) -> None:
    """``search_values`` is a tool the model genuinely calls; the typed grounding
    is a structured OUTPUT, so tool_choice stays auto (DAT-807)."""
    _patch_context(monkeypatch)
    provider = _provider(_output_response(_VALID_OUTPUT))
    agent = _agent_with(provider)

    _generate(agent)

    request = provider.converse.call_args_list[0].args[0]
    assert [t.name for t in request.tools] == ["search_values"]
    assert request.tools[0].strict is True
    assert request.tool_choice == {"type": "auto", "disable_parallel_tool_use": True}
    assert request.output_schema["title"] == "ExtractGroundingOutput"


def test_ending_on_a_tool_call_is_a_bind_error(monkeypatch) -> None:
    """The agent must FINISH with the grounding output. Burning the search budget
    without ever grounding is a loud bind error, never a guessed SQL (DAT-439)."""
    _patch_context(monkeypatch)
    agent = _agent_with(_provider(*[_search_response() for _ in range(6)]))
    agent._run_value_search = MagicMock(return_value="no matches")  # type: ignore[method-assign]

    result = _generate(agent)

    assert not result.success
    assert "search_values" in result.error


# --- Provenance contract v2 enforcement (DAT-727) ----------------------------------


def _with_basis(basis: list[dict]) -> dict:
    return {**_VALID_OUTPUT, "provenance": {"column_mappings_basis": basis}}


_BAD_MEMBERSHIP = _with_basis(
    [
        {
            "concept": "revenue",
            "basis": {
                "measure_columns": ["amout"],
                "filter_columns": ["account_type"],
                "filter": "Income",
                "filter_members": [{"column": "account_type", "value": "Income"}],
            },
        }
    ]
)
_INCOMPLETE = _with_basis(
    [
        {
            "concept": "revenue",
            "basis": {
                "measure_columns": ["amount"],
                "filter_columns": [],
                "filter": "",
                "filter_members": [],
            },
        }
    ]
)


_BAD_MEMBER = _with_basis(
    [
        {
            "concept": "revenue",
            "basis": {
                "measure_columns": ["amount"],
                "filter_columns": ["account_type"],
                "filter": "Income",
                # DAT-787: a member whose column is NOT one of filter_columns — the
                # value side of the contract, routed through the SAME repair turn.
                "filter_members": [{"column": "amount", "value": "Income"}],
            },
        }
    ]
)


def test_filter_member_violation_gets_a_contract_repair_turn(monkeypatch) -> None:
    """DAT-787: a filter_members violation (a member on a column outside
    filter_columns) rides the SAME repair path as the column-contract checks — the
    repair prompt names the violation and the corrected members ground the extract."""
    _patch_context(monkeypatch)
    provider = _provider(_output_response(_BAD_MEMBER), _output_response(_VALID_OUTPUT))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    assert provider.converse.call_count == 2
    content = provider.converse.call_args_list[1].args[0].messages[0].content
    assert "Contract violations" in content
    assert "filter_members" in content and "not in filter_columns" in content


def test_membership_violation_gets_a_contract_repair_turn(monkeypatch) -> None:
    """A schema-valid output enumerating a column the served relation does not
    have ('amout') is contract-repaired — the repair prompt names the violation
    and the repaired enumeration grounds the extract."""
    _patch_context(monkeypatch)
    provider = _provider(_output_response(_BAD_MEMBERSHIP), _output_response(_VALID_OUTPUT))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    assert provider.converse.call_count == 2
    repair_request = provider.converse.call_args_list[1].args[0]
    assert repair_request.output_schema["title"] == "ExtractGroundingOutput"
    assert repair_request.label == "graph_sql_generation_repair"
    content = repair_request.messages[0].content
    assert "Contract violations" in content
    assert "'amout'" in content


def test_completeness_violation_gets_a_contract_repair_turn(monkeypatch) -> None:
    """The SQL parts filter on account_type but the enumeration omits it — the
    completeness net (parts cross-check) catches it and the repair turn fixes
    the enumeration, not the SQL."""
    _patch_context(monkeypatch)
    provider = _provider(_output_response(_INCOMPLETE), _output_response(_VALID_OUTPUT))
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
    _patch_context(monkeypatch)
    provider = _provider(_output_response(_INCOMPLETE), _output_response(_INCOMPLETE))
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
    """The fall-loud shape (relation "", select NULL) carries no columns — no
    contract check, no repair turn, the honest abstention passes through."""
    _patch_context(monkeypatch)
    fall_loud = {
        "grounding": "revenue cannot be grounded: no served value names it",
        "relation": "",
        "where": [],
        "select_expr": "NULL",
        "description": "ungroundable",
        "provenance": {"column_mappings_basis": []},
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
    provider = _provider(_output_response(fall_loud))
    agent = _agent_with(provider)

    result = _generate(agent)

    assert result.success
    assert provider.converse.call_count == 1
    assert result.value.steps[0]["sql"] == "SELECT NULL AS value"
