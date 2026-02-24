"""Tests for entropy models."""


from dataraum.entropy.models import (
    EntropyObject,
    ResolutionOption,
)


class TestEntropyObject:
    """Tests for EntropyObject."""

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
