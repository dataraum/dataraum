"""Tests for entropy context builder integration.

Tests the entropy/context.py module which bridges database data
to the EntropyProcessor.
"""

import pytest

from dataraum.entropy.context import (
    build_entropy_context,
    get_column_entropy_summary,
    get_table_entropy_summary,
)
from dataraum.entropy.models import (
    ColumnEntropyProfile,
    EntropyContext,
    TableEntropyProfile,
)


class TestGetColumnEntropySummary:
    """Tests for get_column_entropy_summary helper."""

    def test_returns_dict_with_key_fields(self) -> None:
        """Summary should include all key entropy fields."""
        profile = ColumnEntropyProfile(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            structural_entropy=0.3,
            semantic_entropy=0.5,
            value_entropy=0.2,
            computational_entropy=0.1,
            composite_score=0.35,
            readiness="investigate",
            high_entropy_dimensions=["semantic.business_meaning"],
        )

        summary = get_column_entropy_summary(profile)

        assert summary["composite_score"] == 0.35
        assert summary["readiness"] == "investigate"
        assert summary["structural_entropy"] == 0.3
        assert summary["semantic_entropy"] == 0.5
        assert summary["value_entropy"] == 0.2
        assert summary["computational_entropy"] == 0.1
        assert summary["high_entropy_dimensions"] == ["semantic.business_meaning"]
        assert "resolution_hints" in summary

    def test_limits_resolution_hints_to_3(self) -> None:
        """Resolution hints should be limited to top 3."""
        from dataraum.entropy.models import ResolutionOption

        profile = ColumnEntropyProfile(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
        )
        # Add 5 resolution hints
        for i in range(5):
            profile.top_resolution_hints.append(
                ResolutionOption(
                    action=f"action_{i}",
                    parameters={},
                    expected_entropy_reduction=0.1 * (i + 1),
                    effort="low",
                    description=f"Fix {i}",
                )
            )

        summary = get_column_entropy_summary(profile)

        assert len(summary["resolution_hints"]) == 3

    def test_resolution_hints_format(self) -> None:
        """Resolution hints should have required fields."""
        from dataraum.entropy.models import ResolutionOption

        profile = ColumnEntropyProfile(
            column_id="col-1",
            column_name="amount",
            table_name="orders",
            top_resolution_hints=[
                ResolutionOption(
                    action="add_description",
                    parameters={"column": "amount"},
                    expected_entropy_reduction=0.5,
                    effort="low",
                    description="Add business description",
                )
            ],
        )

        summary = get_column_entropy_summary(profile)

        assert len(summary["resolution_hints"]) == 1
        hint = summary["resolution_hints"][0]
        assert hint["action"] == "add_description"
        assert hint["description"] == "Add business description"
        assert hint["expected_reduction"] == 0.5
        assert hint["effort"] == "low"


class TestGetTableEntropySummary:
    """Tests for get_table_entropy_summary helper."""

    def test_returns_dict_with_key_fields(self) -> None:
        """Summary should include all key table entropy fields."""
        profile = TableEntropyProfile(
            table_id="tbl-1",
            table_name="orders",
            avg_composite_score=0.4,
            max_composite_score=0.8,
            readiness="investigate",
            high_entropy_columns=["amount", "status"],
            blocked_columns=["status"],
        )
        profile.compound_risks = [object(), object()]  # type: ignore

        summary = get_table_entropy_summary(profile)

        assert summary["avg_composite_score"] == 0.4
        assert summary["max_composite_score"] == 0.8
        assert summary["readiness"] == "investigate"
        assert summary["high_entropy_columns"] == ["amount", "status"]
        assert summary["blocked_columns"] == ["status"]
        assert summary["compound_risk_count"] == 2


class TestBuildEntropyContext:
    """Tests for build_entropy_context function."""

    def test_empty_table_ids_returns_empty_context(
        self,
        mock_session: MockSession,  # noqa: F821
    ) -> None:
        """Empty table_ids should return empty EntropyContext."""
        context = build_entropy_context(mock_session, [])  # type: ignore

        assert isinstance(context, EntropyContext)
        assert len(context.column_profiles) == 0
        assert len(context.table_profiles) == 0
        assert len(context.relationship_profiles) == 0


class TestColumnEntropyProfileInterpretation:
    """Tests for interpretation field on ColumnEntropyProfile."""

    def test_default_interpretation_is_none(self) -> None:
        """Interpretation should default to None."""
        profile = ColumnEntropyProfile(
            column_name="amount",
            table_name="orders",
        )
        assert profile.interpretation is None

    def test_can_set_interpretation(self) -> None:
        """Should be able to set interpretation."""
        from dataraum.entropy.interpretation import (
            EntropyInterpretation,
        )

        profile = ColumnEntropyProfile(
            column_name="amount",
            table_name="orders",
        )

        interpretation = EntropyInterpretation(
            column_name="amount",
            table_name="orders",
            assumptions=[],
            resolution_actions=[],
            explanation="Test explanation",
            composite_score=0.5,
            readiness="investigate",
        )

        profile.interpretation = interpretation

        assert profile.interpretation is not None
        assert profile.interpretation.explanation == "Test explanation"


class TestEntropyContextInterpretations:
    """Tests for column_interpretations in EntropyContext."""

    def test_default_interpretations_is_empty(self) -> None:
        """column_interpretations should default to empty dict."""
        context = EntropyContext()
        assert context.column_interpretations == {}

    def test_can_add_interpretations(self) -> None:
        """Should be able to add interpretations to context."""
        from dataraum.entropy.interpretation import (
            EntropyInterpretation,
        )

        context = EntropyContext()

        interpretation = EntropyInterpretation(
            column_name="amount",
            table_name="orders",
            assumptions=[],
            resolution_actions=[],
            explanation="Test explanation",
            composite_score=0.5,
            readiness="investigate",
        )

        context.column_interpretations["orders.amount"] = interpretation

        assert len(context.column_interpretations) == 1
        assert "orders.amount" in context.column_interpretations


class TestBuildEntropyContextWithInterpretation:
    """Tests for build_entropy_context with interpretation support."""

    def test_no_interpreter_means_no_interpretations(
        self,
        mock_session: MockSession,  # noqa: F821
    ) -> None:
        """When no interpreter is provided, no interpretations are generated."""
        context = build_entropy_context(
            mock_session,  # type: ignore
            [],
        )

        assert isinstance(context, EntropyContext)
        assert len(context.column_interpretations) == 0


# Fixtures for mocking database session
@pytest.fixture
def mock_session():
    """Create a mock session that returns no data."""
    from unittest.mock import MagicMock

    session = MagicMock()
    session.execute = MagicMock(return_value=MagicMock(scalars=lambda: MagicMock(all=lambda: [])))
    return session
