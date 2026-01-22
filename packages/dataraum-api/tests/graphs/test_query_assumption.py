"""Tests for QueryAssumption model and entropy tracking in GraphExecution."""

from __future__ import annotations

from dataraum.graphs.models import (
    AssumptionBasis,
    GraphExecution,
    GraphSource,
    GraphType,
    QueryAssumption,
)


class TestQueryAssumption:
    """Tests for QueryAssumption dataclass."""

    def test_create_assumption(self) -> None:
        """Create an assumption using the factory method."""
        assumption = QueryAssumption.create(
            execution_id="exec-123",
            dimension="semantic.units",
            target="column:orders.amount",
            assumption="Currency is EUR",
            basis=AssumptionBasis.SYSTEM_DEFAULT,
            confidence=0.8,
        )

        assert assumption.execution_id == "exec-123"
        assert assumption.dimension == "semantic.units"
        assert assumption.target == "column:orders.amount"
        assert assumption.assumption == "Currency is EUR"
        assert assumption.basis == AssumptionBasis.SYSTEM_DEFAULT
        assert assumption.confidence == 0.8
        assert assumption.can_promote is True
        assert assumption.promoted_at is None
        assert assumption.assumption_id is not None  # Auto-generated

    def test_assumption_bases(self) -> None:
        """All assumption bases are available."""
        assert AssumptionBasis.SYSTEM_DEFAULT.value == "system_default"
        assert AssumptionBasis.INFERRED.value == "inferred"
        assert AssumptionBasis.USER_SPECIFIED.value == "user_specified"

    def test_assumption_with_low_confidence(self) -> None:
        """Assumption with low confidence."""
        assumption = QueryAssumption.create(
            execution_id="exec-123",
            dimension="value.nulls",
            target="column:orders.status",
            assumption="Excluding null values",
            basis=AssumptionBasis.INFERRED,
            confidence=0.3,
        )

        assert assumption.confidence == 0.3
        assert assumption.basis == AssumptionBasis.INFERRED


class TestGraphExecutionWithEntropy:
    """Tests for entropy-related fields in GraphExecution."""

    def test_execution_has_entropy_fields(self) -> None:
        """GraphExecution should have entropy-related fields."""
        execution = GraphExecution(
            execution_id="exec-123",
            graph_id="metric-1",
            graph_type=GraphType.METRIC,
            graph_version="1.0.0",
            source=GraphSource.SYSTEM,
            parameters={},
        )

        # Check entropy fields exist with defaults
        assert execution.assumptions == []
        assert execution.max_entropy_score == 0.0
        assert execution.entropy_warnings == []

    def test_execution_with_assumptions(self) -> None:
        """GraphExecution with assumptions populated."""
        assumption = QueryAssumption.create(
            execution_id="exec-123",
            dimension="semantic.units",
            target="column:orders.amount",
            assumption="Currency is EUR",
            basis=AssumptionBasis.SYSTEM_DEFAULT,
            confidence=0.8,
        )

        execution = GraphExecution(
            execution_id="exec-123",
            graph_id="metric-1",
            graph_type=GraphType.METRIC,
            graph_version="1.0.0",
            source=GraphSource.SYSTEM,
            parameters={},
            assumptions=[assumption],
            max_entropy_score=0.65,
            entropy_warnings=["High uncertainty in currency data"],
        )

        assert len(execution.assumptions) == 1
        assert execution.assumptions[0].dimension == "semantic.units"
        assert execution.max_entropy_score == 0.65
        assert len(execution.entropy_warnings) == 1

    def test_execution_with_multiple_assumptions(self) -> None:
        """GraphExecution with multiple assumptions."""
        assumptions = [
            QueryAssumption.create(
                execution_id="exec-123",
                dimension="semantic.units",
                target="column:orders.amount",
                assumption="Currency is EUR",
                basis=AssumptionBasis.SYSTEM_DEFAULT,
                confidence=0.8,
            ),
            QueryAssumption.create(
                execution_id="exec-123",
                dimension="value.nulls",
                target="column:orders.status",
                assumption="Excluding null values",
                basis=AssumptionBasis.SYSTEM_DEFAULT,
                confidence=0.7,
            ),
        ]

        execution = GraphExecution(
            execution_id="exec-123",
            graph_id="metric-1",
            graph_type=GraphType.METRIC,
            graph_version="1.0.0",
            source=GraphSource.SYSTEM,
            parameters={},
            assumptions=assumptions,
            max_entropy_score=0.75,
        )

        assert len(execution.assumptions) == 2
        dimensions = [a.dimension for a in execution.assumptions]
        assert "semantic.units" in dimensions
        assert "value.nulls" in dimensions
