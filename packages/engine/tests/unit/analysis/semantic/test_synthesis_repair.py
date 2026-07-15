"""semantic_per_table schema enforcement with a repair turn (DAT-710).

One lazy relationship entry (a missing ``to_column``, a ``"placeholder"``
reasoning) used to fail ``begin_session`` whole — strict Pydantic validation →
non-retryable ``PhaseFailed`` with a whole-cascade blast radius. The
``analyze_tables`` output now gets the same one-turn schema repair as
``generate_sql`` (DAT-699): the model fixes its own tool output instead of the
phase dying. strict grammar is the wrong lever here — a large batched extraction
makes the model legally under-produce under strict — so this is repair, not
strict.
"""

from __future__ import annotations

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
}
# The live DAT-710 failure shape: a relationship entry missing `to_column`.
_MALFORMED_REL = {k: v for k, v in _COMPLETE_REL.items() if k != "to_column"}

_VALID_INPUT = {"tables": [], "relationships": [_COMPLETE_REL], "column_concepts": []}
_MALFORMED_INPUT = {"tables": [], "relationships": [_MALFORMED_REL], "column_concepts": []}


def _response(tool_input: dict | None) -> MagicMock:
    response = MagicMock()
    response.model = "model-x"
    if tool_input is None:
        response.tool_calls = []
    else:
        call = MagicMock()
        call.name = "analyze_tables"
        call.input = tool_input
        response.tool_calls = [call]
    return response


def _provider(*responses: MagicMock) -> MagicMock:
    provider = MagicMock()
    provider.get_model_for_tier.side_effect = lambda tier: "model-x"
    provider.converse.side_effect = [MagicMock(unwrap=MagicMock(return_value=r)) for r in responses]
    return provider


def _agent_with(provider: MagicMock, monkeypatch, annotations: list | None = None) -> SemanticAgent:
    # Everything before the converse call is mocked so the test drives only the
    # validate → repair → build seam (mirrors tests/unit/graphs/test_tool_repair.py).
    # ``annotations`` feeds the DAT-768 measure-presence check (empty = no measures).
    monkeypatch.setattr("dataraum.analysis.semantic.agent.DataSampler", MagicMock())
    monkeypatch.setattr(
        "dataraum.analysis.semantic.agent.load_persisted_annotations",
        lambda s, t: annotations or [],
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


def test_valid_output_needs_no_repair(monkeypatch) -> None:
    provider = _provider(_response(_VALID_INPUT))
    agent = _agent_with(provider, monkeypatch)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert result.success
    assert provider.converse.call_count == 1
    assert result.value.relationships[0].to_column == "id"


def test_missing_field_is_repaired_by_the_model(monkeypatch) -> None:
    """The DAT-710 kill: a relationship missing `to_column`. The repair turn
    carries the invalid input + validation error, forces the tool, and the
    repaired output builds the relationship — begin_session is never failed."""
    provider = _provider(_response(_MALFORMED_INPUT), _response(_VALID_INPUT))
    agent = _agent_with(provider, monkeypatch)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert result.success
    assert result.value.relationships[0].to_column == "id"
    assert provider.converse.call_count == 2

    repair_request = provider.converse.call_args_list[1].args[0]
    assert repair_request.tool_choice == {"type": "tool", "name": "analyze_tables"}
    assert repair_request.label == "semantic_per_table_repair"
    content = repair_request.messages[0].content
    assert "Validation error" in content
    assert "to_column" in content  # the model's own broken output rides along


def test_second_validation_failure_fails_loud(monkeypatch) -> None:
    provider = _provider(_response(_MALFORMED_INPUT), _response(_MALFORMED_INPUT))
    agent = _agent_with(provider, monkeypatch)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert not result.success
    assert "after a repair turn" in result.error
    assert provider.converse.call_count == 2


def test_repair_turn_without_tool_call_fails_loud(monkeypatch) -> None:
    provider = _provider(_response(_MALFORMED_INPUT), _response(None))
    agent = _agent_with(provider, monkeypatch)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert not result.success
    assert "no tool call" in result.error
    assert provider.converse.call_count == 2


# --- DAT-768: empty column_concepts is schema-legal → a targeted re-prompt ---

_CONCEPT = {
    "table_name": "trial_balance",
    "column_name": "debit_balance",
    "business_concept": "account_balance",
}
_EMPTY_CONCEPTS = {"tables": [], "relationships": [], "column_concepts": []}
# Turn 1 carries a confirmed relationship but no concepts; the retry recovers the
# concept but (as a blind fresh turn) drops the relationship — the merge must keep
# turn 1's relationship, not swap the whole object.
_REL_NO_CONCEPTS = {"tables": [], "relationships": [_COMPLETE_REL], "column_concepts": []}
_CONCEPT_NO_REL = {"tables": [], "relationships": [], "column_concepts": [_CONCEPT]}
_MEASURE_ANNS = [{"semantic_role": "measure", "column_name": "debit_balance"}]


def test_empty_concepts_with_measures_triggers_reprompt(monkeypatch) -> None:
    """Measures present + zero column_concepts is an implausible whole-field omission
    (schema-legal, so the repair turn never fires). One corrective re-prompt recovers
    the bindings — and grafts in ONLY column_concepts, so turn 1's vetted
    relationships survive the blind retry (they are not re-derivable there)."""
    provider = _provider(_response(_REL_NO_CONCEPTS), _response(_CONCEPT_NO_REL))
    agent = _agent_with(provider, monkeypatch, annotations=_MEASURE_ANNS)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert result.success
    assert len(result.value.column_concepts) == 1  # recovered from the retry
    assert len(result.value.relationships) == 1  # turn 1's relationship NOT clobbered
    assert result.value.relationships[0].to_column == "id"
    assert provider.converse.call_count == 2
    reprompt = provider.converse.call_args_list[1].args[0]
    assert reprompt.label == "semantic_per_table_concepts"
    assert "empty column_concepts" in reprompt.messages[1].content


def test_empty_concepts_without_measures_no_reprompt(monkeypatch) -> None:
    """No measure columns → an empty column_concepts is a legitimate judgment; the
    agent must NOT waste a re-prompt."""
    provider = _provider(_response(_EMPTY_CONCEPTS))
    agent = _agent_with(provider, monkeypatch, annotations=[])

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert result.success
    assert result.value.column_concepts == []
    assert provider.converse.call_count == 1


def test_reprompt_still_empty_returns_empty_for_phase_backstop(monkeypatch) -> None:
    """Measures present but the re-prompt ALSO returns empty → the agent returns the
    empty surface (one attempt only); the phase's loud backstop fails begin_session."""
    provider = _provider(_response(_EMPTY_CONCEPTS), _response(_EMPTY_CONCEPTS))
    agent = _agent_with(provider, monkeypatch, annotations=_MEASURE_ANNS)

    result = agent.synthesize_tables(MagicMock(), ["t1"])

    assert result.success  # the agent doesn't fail — the phase does
    assert result.value.column_concepts == []
    assert provider.converse.call_count == 2
