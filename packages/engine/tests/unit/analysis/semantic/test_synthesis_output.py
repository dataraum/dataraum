"""semantic_per_table schema enforcement under structured outputs (DAT-807).

The typed result arrives as message CONTENT the API constrained to
``TableSynthesisOutput``'s schema, so the malformation class the DAT-710 repair
turn existed for — a lazy relationship entry missing ``to_column``, a payload
stringified into one field — is structurally unreachable and the repair turn is
gone. What constrained decoding does NOT guarantee is a CROSS-FIELD contract
expressed as a Pydantic validator (DAT-780's anchor invariant): the shape is
legal, the content is not. Those still fail, and they fail LOUD in one turn.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from dataraum.analysis.semantic.agent import SemanticAgent

_COMPLETE_REL = {
    "from_table": "payments",
    "from_column": "invoice_id",
    "to_table": "invoices",
    "to_column": "id",
    "relationship_type": "foreign_key",
    "confidence": 0.9,
    "reasoning": "payments.invoice_id references invoices.id",
    "key_columns": [],
}

_VALID_OUTPUT = {"tables": [], "relationships": [_COMPLETE_REL], "column_concepts": []}

# DAT-780: a table with event dates but no anchor is schema-legal — the
# violation is the TableEntityOutput cross-field validator, which constrained
# decoding cannot enforce.
_TABLE_NO_ANCHOR = {
    "table_name": "orders",
    "entity_type": "orders",
    "description": "Customer orders.",
    "is_fact_table": True,
    "grain": ["order_id"],
    "time_columns": [
        {
            "column": "order_date",
            "aspect": "order",
            "role": "event",
            "is_anchor": False,
            "note": "When placed.",
        }
    ],
    "identity_columns": [],
}
_TABLE_WITH_ANCHOR = {
    **_TABLE_NO_ANCHOR,
    "time_columns": [{**_TABLE_NO_ANCHOR["time_columns"][0], "is_anchor": True}],
}
_ANCHORLESS_OUTPUT = {"tables": [_TABLE_NO_ANCHOR], "relationships": [], "column_concepts": []}
_ANCHORED_OUTPUT = {"tables": [_TABLE_WITH_ANCHOR], "relationships": [], "column_concepts": []}


def _response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.model = "model-x"
    response.content = json.dumps(payload)
    response.tool_calls = []
    return response


def _provider(*responses: MagicMock) -> MagicMock:
    provider = MagicMock()
    provider.get_model_for_tier.side_effect = lambda tier: "model-x"
    provider.converse.side_effect = [MagicMock(unwrap=MagicMock(return_value=r)) for r in responses]
    return provider


def _agent_with(provider: MagicMock, monkeypatch) -> SemanticAgent:
    # Everything before the converse call is mocked so the test drives only the
    # converse → validate → build seam.
    monkeypatch.setattr("dataraum.analysis.semantic.agent.DataSampler", MagicMock())
    monkeypatch.setattr(
        "dataraum.analysis.semantic.agent.load_persisted_annotations", lambda s, t: []
    )

    agent = SemanticAgent.__new__(SemanticAgent)
    agent.provider = provider  # type: ignore[attr-defined]
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user", 0.0)
    agent.renderer = renderer  # type: ignore[attr-defined]

    config = MagicMock()
    config.features.semantic_analysis.enabled = True
    config.features.semantic_analysis.model_tier = "balanced"
    config.features.semantic_analysis.effort = None
    config.limits.max_output_tokens_per_request = 4000
    agent.config = config  # type: ignore[attr-defined]

    # Concepts now come from the typed table via load_workspace_concepts (DAT-728);
    # stub it non-empty so synthesis proceeds. The loader survives only as the
    # prompt formatter.
    ontology_def = MagicMock()
    ontology_def.concepts = [MagicMock()]
    monkeypatch.setattr(
        "dataraum.analysis.semantic.agent.load_workspace_concepts", lambda s, v: ontology_def
    )
    ontology_loader = MagicMock()
    ontology_loader.format_concepts_for_prompt.return_value = ""
    agent._ontology_loader = ontology_loader  # type: ignore[attr-defined]

    agent._load_profiles = MagicMock(  # type: ignore[method-assign]
        return_value=MagicMock(success=True, value=[MagicMock()], error=None)
    )
    agent._build_tables_json = MagicMock(return_value=[])  # type: ignore[method-assign]
    agent._format_relationship_candidates = MagicMock(return_value="")  # type: ignore[method-assign]
    agent._format_persisted_annotations = MagicMock(return_value="")  # type: ignore[method-assign]
    return agent


def test_request_carries_the_output_schema_and_no_tool(monkeypatch) -> None:
    """The typed result is a structured OUTPUT, not a forced tool (DAT-807)."""
    provider = _provider(_response(_VALID_OUTPUT))
    agent = _agent_with(provider, monkeypatch)

    agent.synthesize_tables(MagicMock(), ["t1"])

    request = provider.converse.call_args_list[0].args[0]
    assert request.tools == []
    assert request.tool_choice is None
    assert request.output_schema["title"] == "TableSynthesisOutput"


def test_structured_output_is_parsed_from_content(monkeypatch) -> None:
    provider = _provider(_response(_VALID_OUTPUT))
    agent = _agent_with(provider, monkeypatch)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert result.success
    assert provider.converse.call_count == 1
    assert result.value.relationships[0].to_column == "id"


def test_anchored_tables_pass(monkeypatch) -> None:
    provider = _provider(_response(_ANCHORED_OUTPUT))
    agent = _agent_with(provider, monkeypatch)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert result.success
    axes = result.value.entity_detections[0].time_columns
    assert [tc.is_anchor for tc in axes] == [True]


def test_cross_field_contract_violation_fails_loud_in_one_turn(monkeypatch) -> None:
    """DAT-780's anchor invariant is a cross-field validator, not a shape rule.

    Constrained decoding cannot enforce it, and there is no repair turn any more
    (DAT-807) — so it fails loud on the FIRST turn rather than costing a second
    call. No retry, no silent degrade.
    """
    provider = _provider(_response(_ANCHORLESS_OUTPUT))
    agent = _agent_with(provider, monkeypatch)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert not result.success
    assert "is_anchor" in result.error
    assert provider.converse.call_count == 1
