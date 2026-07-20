"""The structured-output parse boundary and its diagnosis (DAT-807).

Constrained decoding guarantees the payload matches the schema — but only for a
payload the model was allowed to FINISH. A turn cut off at ``max_tokens``
returns a valid-prefix JSON document that does not parse, and lands in the same
``ValidationError`` branch as a genuine API contract break. These pin that the
failure names the real cause instead of blaming Anthropic.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel, model_validator

from dataraum.llm.providers.base import ConversationResponse
from dataraum.llm.structured_output import parse_structured_output


class _Out(BaseModel):
    answer: str
    count: int


class _Contracted(BaseModel):
    """A model whose CROSS-FIELD contract the grammar cannot express."""

    verdict: str
    label: str

    @model_validator(mode="after")
    def _label_required_on_yes(self) -> _Contracted:
        if self.verdict == "yes" and not self.label:
            raise ValueError("label is required on a yes verdict")
        return self


def _response(content: str, *, stop_reason: str = "end_turn") -> ConversationResponse:
    return ConversationResponse(
        content=content,
        stop_reason=stop_reason,
        model="claude-x",
        input_tokens=10,
        output_tokens=5,
    )


def test_valid_payload_parses() -> None:
    result = parse_structured_output(
        _response('{"answer": "x", "count": 2}'), _Out, label="some_label"
    )
    assert result.success
    assert result.unwrap().count == 2


def test_truncated_turn_blames_truncation_not_the_api() -> None:
    """A max_tokens cut-off is the LIKELY cause of an unparseable payload on the
    large batched extractors. Saying "the API contract broke" would send the next
    debugger to the wrong place."""
    result = parse_structured_output(
        _response('{"answer": "x", "cou', stop_reason="max_tokens"), _Out, label="semantic"
    )
    assert not result.success
    assert "stop_reason=max_tokens" in result.error
    assert "raise max_tokens or reduce the batch" in result.error


@pytest.mark.parametrize("stop_reason", ["refusal", "pause_turn"])
def test_other_unfinished_turns_are_named_too(stop_reason: str) -> None:
    result = parse_structured_output(_response("", stop_reason=stop_reason), _Out, label="l")
    assert not result.success
    assert f"stop_reason={stop_reason}" in result.error
    assert "did not finish" in result.error


def test_finished_turn_reports_its_stop_reason_without_a_truncation_claim() -> None:
    """An end_turn that still does not parse IS a surprise — say so plainly, and
    do not offer the truncation remedy."""
    result = parse_structured_output(_response("not json at all"), _Out, label="l")
    assert not result.success
    assert "stop_reason=end_turn" in result.error
    assert "did not finish" not in result.error


def test_cross_field_contract_violation_surfaces_on_a_finished_turn() -> None:
    """The one failure constrained decoding cannot prevent — shape is legal, the
    contract is not. It reports end_turn, which is exactly the signal that this
    is the model's judgment at fault rather than a truncation."""
    result = parse_structured_output(
        _response('{"verdict": "yes", "label": ""}'), _Contracted, label="judge"
    )
    assert not result.success
    assert "stop_reason=end_turn" in result.error
    assert "label is required" in result.error
