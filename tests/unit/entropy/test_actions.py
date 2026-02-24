"""Tests for merge_actions with network context integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dataraum.entropy.actions import merge_actions
from dataraum.entropy.views.network_context import (
    ColumnNetworkResult,
    ColumnNodeEvidence,
    EntropyForNetwork,
    IntentReadiness,
)

# ---------------------------------------------------------------------------
# Stubs for LLM inputs
# ---------------------------------------------------------------------------


@dataclass
class FakeInterp:
    table_name: str = "orders"
    column_name: str = "amount"
    resolution_actions_json: list[dict[str, Any]] | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_network_context(
    columns: dict[str, ColumnNetworkResult] | None = None,
) -> EntropyForNetwork:
    """Build a minimal EntropyForNetwork for testing."""
    cols = columns or {}
    return EntropyForNetwork(
        columns=cols,
        total_columns=len(cols),
    )


def _make_column_result(
    target: str = "column:orders.amount",
    node_evidence: list[ColumnNodeEvidence] | None = None,
    readiness: str = "investigate",
) -> ColumnNetworkResult:
    evidence = node_evidence or []
    return ColumnNetworkResult(
        target=target,
        node_evidence=evidence,
        intents=[IntentReadiness(
            intent_name="aggregation_intent",
            posterior={"low": 0.3, "medium": 0.4, "high": 0.3},
            p_high=0.3,
            readiness=readiness,
        )],
        nodes_observed=len(evidence),
        nodes_high=sum(1 for ne in evidence if ne.state == "high"),
        worst_intent_p_high=0.3,
        readiness=readiness,
    )


def _make_node_evidence(
    node_name: str = "unit_declaration",
    state: str = "high",
    score: float = 0.8,
    impact_delta: float = 0.15,
    resolution_options: list[dict[str, Any]] | None = None,
) -> ColumnNodeEvidence:
    return ColumnNodeEvidence(
        node_name=node_name,
        dimension_path=f"structural.types.{node_name}",
        state=state,
        score=score,
        impact_delta=impact_delta,
        resolution_options=resolution_options or [],
    )


# ---------------------------------------------------------------------------
# Tests: backward compatibility (no network_context)
# ---------------------------------------------------------------------------


class TestMergeActionsBackwardCompat:
    """merge_actions without network_context should behave as before."""

    def test_empty_inputs(self):
        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
        )
        assert result == []

    def test_none_network_context_is_default(self):
        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=None,
        )
        assert result == []


# ---------------------------------------------------------------------------
# Tests: network enriches existing actions
# ---------------------------------------------------------------------------


class TestNetworkEnrichesExistingActions:
    """Network adds causal impact to actions from LLM."""

    def test_network_adds_impact_to_llm_action(self):
        # LLM produces declare_unit action
        interp = FakeInterp(
            resolution_actions_json=[{
                "action": "declare_unit",
                "description": "Add unit declaration",
                "effort": "low",
                "expected_impact": "Reduces semantic.units entropy",
            }],
        )

        # Network also sees unit_declaration node with resolution_option "declare_unit"
        ne = _make_node_evidence(
            node_name="unit_declaration",
            state="high",
            impact_delta=0.25,
            resolution_options=[{
                "action": "declare_unit",
                "description": "Add unit declaration",
                "effort": "low",
                "parameters": {},
                "expected_entropy_reduction": 0.3,
                "cascade_dimensions": [],
            }],
        )
        network_ctx = _make_network_context(
            columns={"column:orders.amount": _make_column_result(
                node_evidence=[ne],
            )},
        )

        result = merge_actions(
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        assert len(result) == 1
        action = result[0]
        assert action["action"] == "declare_unit"
        assert action["from_llm"] is True
        assert action["from_network"] is True
        assert action["network_impact"] == 0.25
        assert action["network_columns"] == 1

    def test_network_impact_aggregates_across_columns(self):
        """Same action across two columns sums impact_delta."""
        ne1 = _make_node_evidence(
            node_name="unit_declaration",
            state="high",
            impact_delta=0.25,
            resolution_options=[{"action": "declare_unit", "description": "", "effort": "low",
                                 "parameters": {}, "expected_entropy_reduction": 0.3,
                                 "cascade_dimensions": []}],
        )
        ne2 = _make_node_evidence(
            node_name="unit_declaration",
            state="medium",
            impact_delta=0.10,
            resolution_options=[{"action": "declare_unit", "description": "", "effort": "low",
                                 "parameters": {}, "expected_entropy_reduction": 0.3,
                                 "cascade_dimensions": []}],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(
                target="column:orders.amount", node_evidence=[ne1],
            ),
            "column:orders.price": _make_column_result(
                target="column:orders.price", node_evidence=[ne2],
            ),
        })

        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        assert len(result) == 1
        assert result[0]["network_impact"] == 0.35  # 0.25 + 0.10
        assert result[0]["network_columns"] == 2

    def test_low_state_nodes_are_excluded(self):
        """Nodes with state='low' should not contribute network impact."""
        ne = _make_node_evidence(
            node_name="unit_declaration",
            state="low",
            impact_delta=0.0,
            resolution_options=[{"action": "declare_unit", "description": "", "effort": "low",
                                 "parameters": {}, "expected_entropy_reduction": 0.3,
                                 "cascade_dimensions": []}],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        # No action created since node is low
        assert result == []


# ---------------------------------------------------------------------------
# Tests: network creates new actions
# ---------------------------------------------------------------------------


class TestNetworkCreatesNewActions:
    """Network resolution_options not in LLM sources create new actions."""

    def test_network_only_action(self):
        ne = _make_node_evidence(
            node_name="outlier_rate",
            state="high",
            impact_delta=0.20,
            resolution_options=[{
                "action": "transform_winsorize",
                "description": "Cap extreme values at percentile boundaries",
                "effort": "medium",
                "parameters": {"percentile": 0.99},
                "expected_entropy_reduction": 0.4,
                "cascade_dimensions": ["statistical.outliers"],
            }],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        assert len(result) == 1
        action = result[0]
        assert action["action"] == "transform_winsorize"
        assert action["from_network"] is True
        assert action["from_detector"] is False
        assert action["from_llm"] is False
        assert action["description"] == "Cap extreme values at percentile boundaries"
        assert action["effort"] == "medium"
        assert action["parameters"] == {"percentile": 0.99}
        assert action["max_reduction"] == 0.4
        assert action["cascade_dimensions"] == ["statistical.outliers"]
        assert action["network_impact"] == 0.20
        assert action["network_columns"] == 1

    def test_node_without_resolution_options_creates_no_action(self):
        """Node evidence with empty resolution_options should be ignored."""
        ne = _make_node_evidence(
            node_name="outlier_rate",
            state="high",
            impact_delta=0.20,
            resolution_options=[],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        assert result == []


# ---------------------------------------------------------------------------
# Tests: priority scoring with network impact
# ---------------------------------------------------------------------------


class TestNetworkImpactPriorityScoring:
    """Network impact should boost priority_score."""

    def test_network_impact_increases_score(self):
        """Action with network impact should score higher than LLM-only action."""
        interp = FakeInterp(
            resolution_actions_json=[{
                "action": "declare_unit",
                "description": "Add unit declaration",
                "effort": "low",
                "expected_impact": "Reduces semantic.units entropy",
            }],
        )

        # LLM-only (no network)
        result_no_net = merge_actions(
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={},
        )

        # Same LLM action plus network impact
        ne = _make_node_evidence(
            node_name="unit_declaration",
            state="high",
            impact_delta=0.50,
            resolution_options=[{"action": "declare_unit", "description": "", "effort": "low",
                                 "parameters": {}, "expected_entropy_reduction": 0.3,
                                 "cascade_dimensions": []}],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result_with_net = merge_actions(
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        score_no_net = result_no_net[0]["priority_score"]
        score_with_net = result_with_net[0]["priority_score"]
        assert score_with_net > score_no_net

    def test_network_impact_formula(self):
        """Verify exact priority_score calculation with network impact.

        priority_score = (total_reduction + affected_cols * 0.1 + network_impact) / effort_factor
        """
        ne = _make_node_evidence(
            node_name="outlier_rate",
            state="high",
            impact_delta=0.40,
            resolution_options=[{
                "action": "winsorize",
                "description": "Cap outliers",
                "effort": "low",  # effort_factor = 1.0
                "parameters": {},
                "expected_entropy_reduction": 0.0,
                "cascade_dimensions": [],
            }],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        # total_reduction=0.0, affected_columns=0 (network doesn't add to affected_columns),
        # network_impact=0.40, effort_factor=1.0
        # score = (0.0 + 0 * 0.1 + 0.40) / 1.0 = 0.40
        assert len(result) == 1
        assert abs(result[0]["priority_score"] - 0.40) < 1e-6

    def test_network_reranks_actions(self):
        """Action with high network impact should rank above one without."""
        # Action A: from LLM interpretation, no network impact
        interp = FakeInterp(
            resolution_actions_json=[{
                "action": "action_a",
                "description": "LLM suggested fix",
                "effort": "low",
                "expected_impact": "Some improvement",
            }],
        )
        # Action B: from network only, high impact
        ne = _make_node_evidence(
            node_name="some_node",
            state="high",
            impact_delta=0.80,
            resolution_options=[{
                "action": "action_b",
                "description": "Network fix",
                "effort": "low",
                "parameters": {},
                "expected_entropy_reduction": 0.1,
                "cascade_dimensions": [],
            }],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = merge_actions(
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        # Both are medium priority, so ordering is by priority_score
        # action_a: (0.0 + 1*0.1 + 0.0) / 1.0 = 0.10 (LLM only, no reduction)
        # action_b: (0.0 + 0*0.1 + 0.80) / 1.0 = 0.80 (from network only)
        assert len(result) == 2
        assert result[0]["action"] == "action_b"
        assert result[1]["action"] == "action_a"


# ---------------------------------------------------------------------------
# Tests: _build_network_impact helper
# ---------------------------------------------------------------------------


class TestBuildNetworkImpact:
    """Test the _build_network_impact helper directly."""

    def test_empty_network_context(self):
        from dataraum.entropy.actions import _build_network_impact

        ctx = _make_network_context()
        result = _build_network_impact(ctx)
        assert result == {}

    def test_multiple_resolution_options_per_node(self):
        """One node with multiple resolution_options creates multiple action entries."""
        from dataraum.entropy.actions import _build_network_impact

        ne = _make_node_evidence(
            node_name="outlier_rate",
            state="high",
            impact_delta=0.30,
            resolution_options=[
                {"action": "winsorize", "description": "Cap outliers", "effort": "low",
                 "parameters": {}, "expected_entropy_reduction": 0.3, "cascade_dimensions": []},
                {"action": "remove_outliers", "description": "Remove rows", "effort": "high",
                 "parameters": {}, "expected_entropy_reduction": 0.5, "cascade_dimensions": []},
            ],
        )
        ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = _build_network_impact(ctx)
        assert "winsorize" in result
        assert "remove_outliers" in result
        # Both get the same impact_delta since they come from the same node
        assert result["winsorize"]["total_delta"] == 0.30
        assert result["remove_outliers"]["total_delta"] == 0.30


# ---------------------------------------------------------------------------
# Tests: score-derived priority labels
# ---------------------------------------------------------------------------


class TestScoreDerivedPriority:
    """Priority labels are derived from priority_score thresholds."""

    def test_high_priority_from_high_score(self):
        """Actions with priority_score > 1.0 get 'high' priority."""
        ne = _make_node_evidence(
            node_name="unit_declaration",
            state="high",
            impact_delta=2.0,
            resolution_options=[{
                "action": "declare_unit",
                "description": "Add unit",
                "effort": "low",
                "parameters": {},
                "expected_entropy_reduction": 0.5,
                "cascade_dimensions": [],
            }],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        assert len(result) == 1
        assert result[0]["priority"] == "high"
        assert result[0]["priority_score"] > 1.0

    def test_medium_priority_from_medium_score(self):
        """Actions with 0.3 < priority_score <= 1.0 get 'medium' priority."""
        ne = _make_node_evidence(
            node_name="unit_declaration",
            state="high",
            impact_delta=0.50,
            resolution_options=[{
                "action": "declare_unit",
                "description": "Add unit",
                "effort": "low",
                "parameters": {},
                "expected_entropy_reduction": 0.0,
                "cascade_dimensions": [],
            }],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        assert len(result) == 1
        assert result[0]["priority"] == "medium"
        assert 0.3 < result[0]["priority_score"] <= 1.0

    def test_low_priority_from_low_score(self):
        """Actions with priority_score <= 0.3 get 'low' priority."""
        ne = _make_node_evidence(
            node_name="unit_declaration",
            state="medium",
            impact_delta=0.10,
            resolution_options=[{
                "action": "declare_unit",
                "description": "Add unit",
                "effort": "high",  # effort_factor = 4.0
                "parameters": {},
                "expected_entropy_reduction": 0.0,
                "cascade_dimensions": [],
            }],
        )
        network_ctx = _make_network_context(columns={
            "column:orders.amount": _make_column_result(node_evidence=[ne]),
        })

        result = merge_actions(
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
            network_context=network_ctx,
        )

        assert len(result) == 1
        # priority_score = (0.0 + 0*0.1 + 0.10) / 4.0 = 0.025
        assert result[0]["priority"] == "low"
        assert result[0]["priority_score"] <= 0.3

    def test_llm_priority_field_is_ignored(self):
        """LLM resolution_actions_json with 'priority' field should be ignored."""
        interp = FakeInterp(
            resolution_actions_json=[{
                "action": "document_unit",
                "description": "Add unit declaration",
                "priority": "high",  # This should be ignored
                "effort": "high",
                "expected_impact": "Reduces semantic.units entropy",
            }],
        )

        result = merge_actions(
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={},
        )

        assert len(result) == 1
        # With effort=high (factor 4.0), total_reduction=0.0, affected_columns=1
        # score = (0.0 + 1*0.1) / 4.0 = 0.025 -> low priority
        assert result[0]["priority"] == "low"
        assert result[0]["priority_score"] <= 0.3
