"""DimensionIdentityJudge plumbing (DAT-762 Phase A).

Scripted-provider tests (the test_synthesis_repair pattern): the judge's
forced-tool turn, the DAT-710 schema repair, and the empty-input
short-circuit. No LLM calls.
"""

from __future__ import annotations

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
# Live failure shape: a conform verdict missing its required concept_label.
_MALFORMED_CONFORM_INPUT = {
    "verdicts": [{"pair_ref": "p1", "verdict": "conform", "reason": "same thing"}]
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
    provider = _provider(_response("judge_exposures", _CONFORM_INPUT))
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


def test_conform_malformed_output_is_repaired(monkeypatch) -> None:
    provider = _provider(_response("judge_exposures", _MALFORMED_CONFORM_INPUT))
    repaired = ConformBatchOutput.model_validate(_CONFORM_INPUT)
    repair = MagicMock(return_value=MagicMock(success=True, unwrap=lambda: repaired))
    monkeypatch.setattr("dataraum.analysis.hierarchies.judge.repair_tool_output", repair)
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.conform(candidates=_CANDIDATES)

    assert result.success
    assert result.unwrap()[0].concept_label == "account"
    repair.assert_called_once()


def test_conform_wrong_tool_fails_closed() -> None:
    provider = _provider(_response("other_tool", _CONFORM_INPUT))
    judge = DimensionIdentityJudge(_config(), provider, _renderer())

    result = judge.conform(candidates=_CANDIDATES)

    assert not result.success


def test_conform_batch_output_validates_abstain() -> None:
    out = ConformBatchOutput.model_validate(
        {"verdicts": [{"pair_ref": "p", "verdict": "abstain", "reason": "no evidence"}]}
    )
    assert out.verdicts[0].concept_label is None


def test_conform_without_label_is_malformed() -> None:
    """A conform verdict missing its required label FAILS validation — it feeds
    the DAT-710 repair loop (re-ask the judge), never a deterministic fill-in
    by the consumer (a column name is not a concept label)."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ConformBatchOutput.model_validate(
            {"verdicts": [{"pair_ref": "p", "verdict": "conform", "reason": "same thing"}]}
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
    provider = _provider(_response("judge_aliases", _ALIAS_INPUT))
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
    """A confidence outside [0,1] fails validation → the DAT-710 repair loop."""
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


def test_conform_empty_batch_is_malformed() -> None:
    """A zero-verdict batch FAILS validation — absence must fall loud.

    The judge is only called with a non-empty candidate batch, so an empty
    verdict list is a malformed response, not "nothing conforms" (``abstain``
    is the verdict for "I cannot decide"). Validation routes it into the
    DAT-710 repair loop and, if the judge still cannot answer, to a failed
    Result — never to a silent zero, which would blank cross-fact identity and
    every aggregation lineage riding on it (DAT-725 run #5).
    """
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ConformBatchOutput.model_validate({"verdicts": []})


def test_alias_empty_batch_is_malformed() -> None:
    """Same contract on the alias judge: an empty batch is malformed, not 'no aliases'."""
    import pytest
    from pydantic import ValidationError

    from dataraum.analysis.hierarchies.judge import AliasIdentityBatchOutput

    with pytest.raises(ValidationError):
        AliasIdentityBatchOutput.model_validate({"verdicts": []})
