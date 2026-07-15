"""DimensionIdentityJudge plumbing (DAT-762 Phase A).

Scripted-provider tests (the test_synthesis_repair pattern): the judge's
forced-tool turn, the DAT-710 schema repair, the empty-input short-circuit,
and the from_config lane-off contract. No LLM calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dataraum.analysis.hierarchies.judge import (
    ConformBatchOutput,
    DimensionIdentityJudge,
    VetoBatchOutput,
)

_VETO_INPUT = {
    "verdicts": [
        {"structure_ref": "s1", "verdict": "veto", "reason": "id-shaped determinant"}
    ]
}
# Live failure shape: a verdict entry missing its reason.
_MALFORMED_VETO_INPUT = {"verdicts": [{"structure_ref": "s1", "verdict": "veto"}]}

_CONFORM_INPUT = {
    "verdicts": [
        {
            "pair_ref": "p1",
            "verdict": "conform",
            "concept_label": "account",
            "reason": "same key and attribute set, meanings agree",
        }
    ]
}


def _response(tool_name: str, tool_input: dict | None) -> MagicMock:
    response = MagicMock()
    response.model = "model-x"
    if tool_input is None:
        response.tool_calls = []
    else:
        call = MagicMock()
        call.name = tool_name
        call.input = tool_input
        response.tool_calls = [call]
    return response


def _provider(*responses: MagicMock) -> MagicMock:
    provider = MagicMock()
    provider.get_model_for_tier.side_effect = lambda tier: "model-x"
    provider.converse.side_effect = [
        MagicMock(unwrap=MagicMock(return_value=r)) for r in responses
    ]
    return provider


def _config(enabled: bool = True) -> MagicMock:
    config = MagicMock()
    feature = MagicMock()
    feature.enabled = enabled
    feature.model_tier = "balanced"
    feature.effort = None
    config.features.dimension_identity_judgment = feature if enabled else None
    config.limits.max_output_tokens_per_request = 24000
    return config


def _renderer() -> MagicMock:
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user", 0.1)
    return renderer


_STRUCTURES = [
    {"ref": "s1", "kind": "alias", "members": ["entry_key", "desc_entry"],
     "routed_class": "proxy-bijection"}
]

_CANDIDATES = [
    {
        "ref": "p1",
        "left": {"fact_table": "ledger", "key": "acct_id",
                 "attributes": ["acct_name"], "meanings": {"acct_id": "the entity key"}},
        "right": {"fact_table": "balances", "key": "account_id",
                  "attributes": ["account_name"], "meanings": {}},
    }
]


def test_veto_happy_path() -> None:
    provider = _provider(_response("review_structures", _VETO_INPUT))
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.veto(table_name="ledger", all_columns=["a", "b"], structures=_STRUCTURES)

    assert result.success
    (verdict,) = result.unwrap()
    assert verdict.verdict == "veto"
    assert verdict.structure_ref == "s1"


def test_veto_empty_structures_short_circuits() -> None:
    provider = _provider()  # converse would raise StopIteration if called
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.veto(table_name="t", all_columns=[], structures=[])

    assert result.success and result.unwrap() == []
    provider.converse.assert_not_called()


def test_veto_malformed_output_is_repaired(monkeypatch) -> None:
    provider = _provider(_response("review_structures", _MALFORMED_VETO_INPUT))
    repaired = VetoBatchOutput.model_validate(_VETO_INPUT)
    repair = MagicMock(return_value=MagicMock(success=True, unwrap=lambda: repaired))
    monkeypatch.setattr("dataraum.analysis.hierarchies.judge.repair_tool_output", repair)
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.veto(table_name="t", all_columns=["a"], structures=_STRUCTURES)

    assert result.success
    assert result.unwrap()[0].verdict == "veto"
    repair.assert_called_once()


def test_veto_wrong_tool_fails_closed() -> None:
    provider = _provider(_response("other_tool", _VETO_INPUT))
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.veto(table_name="t", all_columns=["a"], structures=_STRUCTURES)

    assert not result.success


def test_conform_happy_path() -> None:
    provider = _provider(_response("judge_exposures", _CONFORM_INPUT))
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.conform(candidates=_CANDIDATES)

    assert result.success
    (verdict,) = result.unwrap()
    assert verdict.verdict == "conform"
    assert verdict.concept_label == "account"


def test_conform_batch_output_validates_abstain() -> None:
    out = ConformBatchOutput.model_validate(
        {"verdicts": [{"pair_ref": "p", "verdict": "abstain", "reason": "no evidence"}]}
    )
    assert out.verdicts[0].concept_label is None


def test_from_config_returns_none_when_feature_absent(monkeypatch) -> None:
    config = _config(enabled=False)
    monkeypatch.setattr(
        "dataraum.analysis.hierarchies.judge.load_llm_config", lambda: config
    )
    assert DimensionIdentityJudge.from_config() is None


def test_evidence_formatting_is_deterministic() -> None:
    text = DimensionIdentityJudge._format_candidates(_CANDIDATES)
    assert "ref=p1" in text
    assert "fact=ledger" in text
    assert "acct_id: the entity key" in text
    veto_text = DimensionIdentityJudge._format_structures(_STRUCTURES)
    assert "entry_key -> desc_entry" in veto_text
    assert "routed_class=proxy-bijection" in veto_text
