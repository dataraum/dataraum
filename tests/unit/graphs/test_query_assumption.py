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


class TestGraphExecutionAssumptions:
    """Tests for assumption tracking in GraphExecution."""

    def test_execution_defaults(self) -> None:
        """GraphExecution should have empty assumptions by default."""
        execution = GraphExecution(
            execution_id="exec-123",
            graph_id="metric-1",
            graph_type=GraphType.METRIC,
            graph_version="1.0.0",
            source=GraphSource.SYSTEM,
            parameters={},
        )

        assert execution.assumptions == []

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
        )

        assert len(execution.assumptions) == 1
        assert execution.assumptions[0].dimension == "semantic.units"

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
        )

        assert len(execution.assumptions) == 2
        dimensions = [a.dimension for a in execution.assumptions]
        assert "semantic.units" in dimensions
        assert "value.nulls" in dimensions
