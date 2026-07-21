"""Tests for entropy models."""

import pytest

from dataraum.entropy.models import (
    ABSTAIN_NOT_APPLICABLE,
    STATUS_ABSTAINED,
    EntropyObject,
)


class TestEntropyObject:
    """Tests for EntropyObject."""

    def test_dimension_path(self, sample_entropy_object: EntropyObject):
        """Test dimension path property."""
        assert sample_entropy_object.dimension_path == "structural.types.type_fidelity"


class TestAbstentionVocabulary:
    """DAT-853: the status/score/reason pairing is enforced at construction."""

    def test_default_is_measured(self):
        obj = EntropyObject(score=0.4)
        assert obj.status == "measured"
        assert obj.abstain_reason is None
        assert obj.measured_score == 0.4

    def test_abstained_object_valid(self):
        obj = EntropyObject(
            score=None, status=STATUS_ABSTAINED, abstain_reason=ABSTAIN_NOT_APPLICABLE
        )
        assert obj.status == "abstained"

    def test_unknown_status_rejected(self):
        with pytest.raises(ValueError, match="status"):
            EntropyObject(status="skipped")

    def test_measured_without_score_rejected(self):
        with pytest.raises(ValueError, match="requires a score"):
            EntropyObject(score=None)

    def test_measured_with_reason_rejected(self):
        with pytest.raises(ValueError, match="abstain_reason"):
            EntropyObject(score=0.1, abstain_reason=ABSTAIN_NOT_APPLICABLE)

    def test_abstained_with_score_rejected(self):
        with pytest.raises(ValueError, match="must not carry a score"):
            EntropyObject(score=0.1, status=STATUS_ABSTAINED, abstain_reason=ABSTAIN_NOT_APPLICABLE)

    def test_abstained_without_reason_rejected(self):
        with pytest.raises(ValueError, match="abstain_reason"):
            EntropyObject(score=None, status=STATUS_ABSTAINED)

    def test_abstained_with_unknown_reason_rejected(self):
        with pytest.raises(ValueError, match="abstain_reason"):
            EntropyObject(score=None, status=STATUS_ABSTAINED, abstain_reason="because")

    def test_measured_score_raises_on_abstention(self):
        obj = EntropyObject(
            score=None, status=STATUS_ABSTAINED, abstain_reason=ABSTAIN_NOT_APPLICABLE
        )
        with pytest.raises(ValueError, match="no score"):
            _ = obj.measured_score
