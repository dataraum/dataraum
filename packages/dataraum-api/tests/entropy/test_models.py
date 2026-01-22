"""Tests for entropy models."""

import pytest

from dataraum.entropy.models import (
    ColumnEntropyProfile,
    CompoundRisk,
    EntropyContext,
    EntropyObject,
    ResolutionCascade,
    ResolutionOption,
    TableEntropyProfile,
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


class TestColumnEntropyProfile:
    """Tests for ColumnEntropyProfile."""

    def test_calculate_composite(self, sample_column_profile: ColumnEntropyProfile):
        """Test composite score calculation."""
        # Profile already has calculate_composite called in fixture
        # structural: 0.25, semantic: 0.40, value: 0.15, computational: 0.10
        # Weights: 0.25, 0.30, 0.30, 0.15
        expected = (0.25 * 0.25) + (0.40 * 0.30) + (0.15 * 0.30) + (0.10 * 0.15)
        assert sample_column_profile.composite_score == pytest.approx(expected, abs=0.01)

    def test_update_high_entropy_dimensions(self):
        """Test identifying high entropy dimensions."""
        profile = ColumnEntropyProfile(
            column_name="test",
            table_name="test",
            dimension_scores={
                "a": 0.3,
                "b": 0.6,
                "c": 0.8,
            },
        )
        profile.update_high_entropy_dimensions(threshold=0.5)

        assert "b" in profile.high_entropy_dimensions
        assert "c" in profile.high_entropy_dimensions
        assert "a" not in profile.high_entropy_dimensions

    def test_update_readiness_ready(self):
        """Test readiness classification for low entropy."""
        profile = ColumnEntropyProfile(
            column_name="test",
            table_name="test",
        )
        profile.composite_score = 0.2
        profile.update_readiness()

        assert profile.readiness == "ready"

    def test_update_readiness_investigate(self):
        """Test readiness classification for medium entropy."""
        profile = ColumnEntropyProfile(
            column_name="test",
            table_name="test",
        )
        profile.composite_score = 0.45
        profile.update_readiness()

        assert profile.readiness == "investigate"

    def test_update_readiness_blocked(self):
        """Test readiness classification for high entropy."""
        profile = ColumnEntropyProfile(
            column_name="test",
            table_name="test",
        )
        profile.composite_score = 0.75
        profile.update_readiness()

        assert profile.readiness == "blocked"


class TestTableEntropyProfile:
    """Tests for TableEntropyProfile."""

    def test_calculate_aggregates(self, sample_column_profile: ColumnEntropyProfile):
        """Test aggregate calculation from column profiles."""
        table_profile = TableEntropyProfile(
            table_name="orders",
            column_profiles=[sample_column_profile],
        )
        table_profile.calculate_aggregates()

        assert table_profile.avg_structural_entropy == sample_column_profile.structural_entropy
        assert table_profile.max_structural_entropy == sample_column_profile.structural_entropy

    def test_identify_blocked_columns(self, high_entropy_column_profile: ColumnEntropyProfile):
        """Test identification of blocked columns."""
        # Adjust profile to be blocked
        high_entropy_column_profile.composite_score = 0.85
        high_entropy_column_profile.update_readiness()

        table_profile = TableEntropyProfile(
            table_name="transactions",
            column_profiles=[high_entropy_column_profile],
        )
        table_profile.calculate_aggregates()

        assert "value" in table_profile.blocked_columns
        assert table_profile.readiness == "blocked"


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


class TestEntropyContext:
    """Tests for EntropyContext."""

    def test_get_column_entropy(self, sample_column_profile: ColumnEntropyProfile):
        """Test getting column entropy from context."""
        context = EntropyContext()
        context.column_profiles["orders.amount"] = sample_column_profile

        result = context.get_column_entropy("orders", "amount")
        assert result is sample_column_profile

    def test_get_high_entropy_columns(
        self,
        sample_column_profile: ColumnEntropyProfile,
        high_entropy_column_profile: ColumnEntropyProfile,
    ):
        """Test getting high entropy columns."""
        context = EntropyContext()
        context.column_profiles["orders.amount"] = sample_column_profile
        context.column_profiles["transactions.value"] = high_entropy_column_profile

        high_cols = context.get_high_entropy_columns(threshold=0.5)
        assert "transactions.value" in high_cols
        # sample profile has lower composite score
        assert "orders.amount" not in high_cols

    def test_has_critical_risks(self):
        """Test critical risk detection."""
        context = EntropyContext()
        assert context.has_critical_risks() is False

        context.compound_risks.append(
            CompoundRisk(
                risk_level="critical",
                dimensions=["a", "b"],
                target="test",
            )
        )
        assert context.has_critical_risks() is True

    def test_update_summary_stats(self, high_entropy_column_profile: ColumnEntropyProfile):
        """Test summary stats update."""
        context = EntropyContext()
        context.column_profiles["test.col"] = high_entropy_column_profile

        context.update_summary_stats()

        assert context.high_entropy_count >= 1
