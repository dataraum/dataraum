"""Tests for entropy models."""

from dataraum.entropy.models import (
    CompoundRisk,
    EntropyObject,
    ResolutionCascade,
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


class TestCompoundRisk:
    """Tests for CompoundRisk."""

    def test_from_scores(self):
        """Test creating compound risk from scores."""
        risk = CompoundRisk.from_scores(
            target="column:test.col",
            dimensions=["semantic.units", "computational.aggregations"],
            scores={
                "semantic.units": 0.7,
                "computational.aggregations": 0.6,
            },
            risk_level="critical",
            impact="Test impact",
            multiplier=2.0,
        )

        assert risk.risk_level == "critical"
        assert risk.multiplier == 2.0
        # Combined: avg(0.7, 0.6) * 2.0 = 0.65 * 2.0 = 1.3, capped at 1.0
        assert risk.combined_score == 1.0


class TestResolutionCascade:
    """Tests for ResolutionCascade."""

    def test_calculate_priority(self):
        """Test priority score calculation."""
        cascade = ResolutionCascade(
            action="declare_unit",
            parameters={"unit": "USD"},
            entropy_reductions={
                "semantic.units": 0.5,
                "computational.aggregations": 0.3,
            },
            effort="low",
        )
        priority = cascade.calculate_priority()

        assert cascade.total_reduction == 0.8
        assert cascade.dimensions_improved == 2
        assert priority == 0.8  # 0.8 / 1.0 (low effort factor)
