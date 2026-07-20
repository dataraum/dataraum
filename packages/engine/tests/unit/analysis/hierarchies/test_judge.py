"""DimensionIdentityJudge plumbing (DAT-762 Phase A).

Scripted-provider tests: the judge's structured-output turn (DAT-807), the
cross-field contract that constrained decoding cannot enforce, and the
empty-input short-circuit. No LLM calls.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from dataraum.analysis.hierarchies.judge import ConformBatchOutput, DimensionIdentityJudge

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
# The one violation constrained decoding cannot prevent: a schema-valid conform
# verdict whose concept_label is the documented empty value.
_CONTRACT_VIOLATING_CONFORM = {
    "verdicts": [
        {"pair_ref": "p1", "verdict": "conform", "concept_label": "", "reason": "same thing"}
    ]
}


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


def _config() -> MagicMock:
    config = MagicMock()
    feature = MagicMock()
    feature.model_tier = "balanced"
    feature.effort = None
    config.features.dimension_identity_judgment = feature
    config.limits.max_output_tokens_per_request = 24000
    return config


def _renderer() -> MagicMock:
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user", 0.1)
    return renderer


_CANDIDATES = [
    {
        "ref": "p1",
        "left": {
            "fact_table": "ledger",
            "key": "acct_id",
            "attributes": ["acct_name"],
            "meanings": {"acct_id": "the entity key"},
        },
        "right": {
            "fact_table": "balances",
            "key": "account_id",
            "attributes": ["account_name"],
            "meanings": {},
        },
    }
]


def test_conform_happy_path() -> None:
    provider = _provider(_response(_CONFORM_INPUT))
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.conform(candidates=_CANDIDATES)

    assert result.success
    (verdict,) = result.unwrap()
    assert verdict.verdict == "conform"
    assert verdict.concept_label == "account"


def test_conform_empty_candidates_short_circuits() -> None:
    provider = _provider()  # converse would raise StopIteration if called
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.conform(candidates=[])

    assert result.success and result.unwrap() == []
    provider.converse.assert_not_called()


def test_conform_request_carries_the_output_schema_and_no_tool() -> None:
    """The verdicts are a structured OUTPUT, not a forced tool (DAT-807)."""
    provider = _provider(_response(_CONFORM_INPUT))
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    judge.conform(candidates=_CANDIDATES)

    request = provider.converse.call_args_list[0].args[0]
    assert request.tools == []
    assert request.tool_choice is None
    assert request.output_schema["title"] == "ConformBatchOutput"


def test_conform_contract_violation_fails_loud() -> None:
    """A conform verdict without its label is the ONE thing the grammar cannot
    catch — it is a cross-field contract, so the batch fails loud (no repair
    turn since DAT-807) and the lane is skipped: absence of judgment is not a
    judgment."""
    provider = _provider(_response(_CONTRACT_VIOLATING_CONFORM))
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.conform(candidates=_CANDIDATES)

    assert not result.success
    assert provider.converse.call_count == 1


def test_conform_batch_output_validates_abstain() -> None:
    out = ConformBatchOutput.model_validate(
        {
            "verdicts": [
                {"pair_ref": "p", "verdict": "abstain", "concept_label": "", "reason": "none"}
            ]
        }
    )
    assert out.verdicts[0].concept_label == ""


def test_conform_without_label_is_malformed() -> None:
    """A conform verdict with an empty label FAILS validation — never a
    deterministic fill-in by the consumer (a column name is not a concept
    label). Constrained decoding guarantees the KEY is present; only this
    validator can require it to carry a judgment."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ConformBatchOutput.model_validate(
            {
                "verdicts": [
                    {"pair_ref": "p", "verdict": "conform", "concept_label": "", "reason": "x"}
                ]
            }
        )


def test_evidence_formatting_is_deterministic() -> None:
    text = DimensionIdentityJudge._format_candidates(_CANDIDATES)
    assert "ref=p1" in text
    assert "fact=ledger" in text
    assert "acct_id: the entity key" in text


_ALIAS_INPUT = {
    "verdicts": [
        {
            "pair_ref": "0",
            "confidence": 0.95,
            "reason": "an id and its name for the same entity",
        }
    ]
}
_ALIAS_CANDIDATES = [
    {
        "ref": "0",
        "table": "facts",
        "a": {"name": "account_id", "distinct": 3, "samples": ["A0", "A1", "A2"]},
        "b": {"name": "account_name", "distinct": 3, "samples": ["Cash", "Receivable", "Payable"]},
        "meanings": {"account_id": "the account entity key"},
    }
]


def test_alias_identity_happy_path() -> None:
    provider = _provider(_response(_ALIAS_INPUT))
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.alias_identity(candidates=_ALIAS_CANDIDATES)

    assert result.success
    (verdict,) = result.unwrap()
    assert verdict.confidence == 0.95


def test_alias_identity_empty_candidates_short_circuits() -> None:
    provider = _provider()  # converse would raise StopIteration if called
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.alias_identity(candidates=[])

    assert result.success and result.unwrap() == []
    provider.converse.assert_not_called()


def test_alias_confidence_out_of_range_is_malformed() -> None:
    """A confidence outside [0,1] fails validation — loud, never coerced."""
    import pytest
    from pydantic import ValidationError

    from dataraum.analysis.hierarchies.judge import AliasIdentityBatchOutput

    with pytest.raises(ValidationError):
        AliasIdentityBatchOutput.model_validate(
            {"verdicts": [{"pair_ref": "0", "confidence": 1.7, "reason": "x"}]}
        )


def test_alias_evidence_formatting_is_deterministic() -> None:
    text = DimensionIdentityJudge._format_alias_candidates(_ALIAS_CANDIDATES)
    assert "ref=0 table=facts" in text
    assert "account_id — 3 distinct" in text
    assert "meaning[account_id]: the account entity key" in text
