"""Tests for entropy models."""

import pytest

from dataraum.entropy.models import (
    EntropyObject,
    ResolutionOption,
)


class TestEntropyObject:
    """Tests for EntropyObject."""

    def test_is_high_entropy(self, sample_entropy_object: EntropyObject):
        """Test high entropy detection."""
        assert sample_entropy_object.is_high_entropy(threshold=0.3) is True
        assert sample_entropy_object.is_high_entropy(threshold=0.5) is False

    def test_is_critical(self, sample_entropy_object: EntropyObject):
        """Test critical entropy detection."""
        assert sample_entropy_object.is_critical(threshold=0.8) is False

        critical_obj = EntropyObject(
            score=0.85, layer="value", dimension="nulls", sub_dimension="null_ratio", target="test"
        )
        assert critical_obj.is_critical() is True

    def test_dimension_path(self, sample_entropy_object: EntropyObject):
        """Test dimension path property."""
        assert sample_entropy_object.dimension_path == "structural.types.type_fidelity"


class TestResolutionOption:
    """Tests for ResolutionOption."""

    def test_priority_score_low_effort(self):
        """Test priority score calculation for low effort."""
        opt = ResolutionOption(
            action="test",
            parameters={},
            expected_entropy_reduction=0.5,
            effort="low",
        )
        assert opt.priority_score() == 0.5  # 0.5 / 1.0

    def test_priority_score_high_effort(self):
        """Test priority score calculation for high effort."""
        opt = ResolutionOption(
            action="test",
            parameters={},
            expected_entropy_reduction=0.8,
            effort="high",
        )
        assert opt.priority_score() == 0.2  # 0.8 / 4.0
