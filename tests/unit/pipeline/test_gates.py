"""Tests for gate model and builder."""

from dataraum.pipeline.gates import (
    Gate,
    GateAction,
    GateActionType,
    GateResolution,
    GateViolation,
    build_gate,
)


class TestGateViolation:
    def test_basic_violation(self):
        v = GateViolation(dimension="type_fidelity", score=0.6, threshold=0.5)
        assert v.dimension == "type_fidelity"
        assert v.score == 0.6
        assert v.threshold == 0.5
        assert v.affected_targets == []

    def test_violation_with_targets(self):
        v = GateViolation(
            dimension="type_fidelity",
            score=0.6,
            threshold=0.5,
            affected_targets=["column:orders.amount"],
            evidence_summary="VARCHAR should be DECIMAL(10,2)",
        )
        assert len(v.affected_targets) == 1
        assert v.evidence_summary


class TestGateAction:
    def test_skip_action(self):
        a = GateAction(
            index=1,
            action_type=GateActionType.SKIP,
            label="skip: continue with warnings",
        )
        assert a.action_type == GateActionType.SKIP
        assert a.index == 1

    def test_fix_action_with_confidence(self):
        a = GateAction(
            index=2,
            action_type=GateActionType.FIX,
            label="fix: cast to DECIMAL",
            confidence=0.94,
            parameters={"column": "orders.amount", "target_type": "DECIMAL(10,2)"},
        )
        assert a.confidence == 0.94
        assert a.parameters["column"] == "orders.amount"


class TestGate:
    def test_gate_properties(self):
        gate = Gate(
            gate_id="gate_statistics",
            gate_type="structural",
            blocked_phase="statistics",
            violations=[GateViolation(dimension="type_fidelity", score=0.6, threshold=0.5)],
            suggested_actions=[
                GateAction(
                    index=1,
                    action_type=GateActionType.SKIP,
                    label="skip",
                ),
            ],
        )
        assert gate.action_count == 1
        assert gate.blocked_phase == "statistics"
        assert len(gate.violations) == 1


class TestGateResolution:
    def test_skip_resolution(self):
        r = GateResolution(action_taken=GateActionType.SKIP)
        assert r.action_taken == GateActionType.SKIP
        assert r.action_index is None

    def test_numbered_resolution(self):
        r = GateResolution(
            action_taken=GateActionType.FIX,
            action_index=2,
            parameters={"column": "orders.amount"},
        )
        assert r.action_index == 2

    def test_question_resolution(self):
        r = GateResolution(
            action_taken=GateActionType.QUESTION,
            user_input="What columns are affected?",
        )
        assert r.user_input == "What columns are affected?"


class TestBuildGate:
    def test_structural_gate(self):
        gate = build_gate(
            blocked_phase="statistics",
            violations={"type_fidelity": (0.6, 0.5)},
            entropy_state={"type_fidelity": 0.6, "null_ratio": 0.1},
        )
        assert gate.gate_id == "gate_statistics"
        assert gate.gate_type == "structural"
        assert gate.blocked_phase == "statistics"
        assert len(gate.violations) == 1
        assert gate.violations[0].dimension == "type_fidelity"
        assert gate.violations[0].score == 0.6
        assert gate.violations[0].threshold == 0.5
        # Skip action always included
        assert any(a.action_type == GateActionType.SKIP for a in gate.suggested_actions)

    def test_semantic_gate(self):
        gate = build_gate(
            blocked_phase="graph_execution",
            violations={"naming_clarity": (0.7, 0.4)},
            entropy_state={"naming_clarity": 0.7},
        )
        assert gate.gate_type == "semantic"

    def test_value_gate(self):
        gate = build_gate(
            blocked_phase="test",
            violations={"null_ratio": (0.8, 0.3)},
            entropy_state={"null_ratio": 0.8},
        )
        assert gate.gate_type == "value"

    def test_multiple_violations(self):
        gate = build_gate(
            blocked_phase="semantic",
            violations={
                "type_fidelity": (0.5, 0.3),
                "join_path_determinism": (0.7, 0.5),
            },
            entropy_state={"type_fidelity": 0.5, "join_path_determinism": 0.7},
        )
        assert len(gate.violations) == 2
        assert gate.gate_type == "structural"

    def test_entropy_state_preserved(self):
        state = {"type_fidelity": 0.6, "null_ratio": 0.1, "naming_clarity": 0.2}
        gate = build_gate(
            blocked_phase="test",
            violations={"type_fidelity": (0.6, 0.5)},
            entropy_state=state,
        )
        assert gate.entropy_state == state

    def test_fix_actions_for_type_fidelity(self):
        gate = build_gate(
            blocked_phase="statistics",
            violations={"type_fidelity": (0.6, 0.5)},
            entropy_state={"type_fidelity": 0.6},
        )
        fix_actions = [a for a in gate.suggested_actions if a.action_type == GateActionType.FIX]
        assert len(fix_actions) == 1
        assert fix_actions[0].parameters["action_type"] == "override_type"
        # Skip should still be last
        assert gate.suggested_actions[-1].action_type == GateActionType.SKIP

    def test_fix_actions_for_null_ratio(self):
        gate = build_gate(
            blocked_phase="test",
            violations={"null_ratio": (0.8, 0.3)},
            entropy_state={"null_ratio": 0.8},
        )
        fix_actions = [a for a in gate.suggested_actions if a.action_type == GateActionType.FIX]
        action_types = {a.parameters["action_type"] for a in fix_actions}
        assert "declare_null_meaning" in action_types
        assert "create_filtered_view" in action_types

    def test_fix_actions_for_naming_clarity(self):
        gate = build_gate(
            blocked_phase="test",
            violations={"naming_clarity": (0.7, 0.4)},
            entropy_state={"naming_clarity": 0.7},
        )
        fix_actions = [a for a in gate.suggested_actions if a.action_type == GateActionType.FIX]
        assert len(fix_actions) == 1
        assert fix_actions[0].parameters["action_type"] == "add_business_name"

    def test_fix_actions_deduplicated(self):
        """Multiple dimensions mapping to the same action type should not duplicate."""
        gate = build_gate(
            blocked_phase="test",
            violations={
                "join_path_determinism": (0.8, 0.5),
                "relationship_quality": (0.7, 0.4),
            },
            entropy_state={},
        )
        fix_actions = [a for a in gate.suggested_actions if a.action_type == GateActionType.FIX]
        # confirm_relationship appears once despite two dimensions mapping to it
        assert len(fix_actions) == 1
        assert fix_actions[0].parameters["action_type"] == "confirm_relationship"

    def test_fix_action_indices_sequential(self):
        gate = build_gate(
            blocked_phase="test",
            violations={"type_fidelity": (0.6, 0.5), "null_ratio": (0.8, 0.3)},
            entropy_state={},
        )
        indices = [a.index for a in gate.suggested_actions]
        assert indices == list(range(1, len(indices) + 1))

    def test_fix_all_when_multiple_fixes(self):
        """FIX_ALL action should appear when there are 2+ fix actions."""
        gate = build_gate(
            blocked_phase="test",
            violations={"null_ratio": (0.8, 0.3)},
            entropy_state={},
        )
        fix_actions = [a for a in gate.suggested_actions if a.action_type == GateActionType.FIX]
        fix_all_actions = [
            a for a in gate.suggested_actions if a.action_type == GateActionType.FIX_ALL
        ]
        assert len(fix_actions) == 2  # declare_null_meaning + create_filtered_view
        assert len(fix_all_actions) == 1
        # Skip is still last
        assert gate.suggested_actions[-1].action_type == GateActionType.SKIP

    def test_no_fix_all_with_single_fix(self):
        """FIX_ALL should not appear when there's only one fix action."""
        gate = build_gate(
            blocked_phase="test",
            violations={"type_fidelity": (0.6, 0.5)},
            entropy_state={},
        )
        fix_all = [a for a in gate.suggested_actions if a.action_type == GateActionType.FIX_ALL]
        assert len(fix_all) == 0

    def test_unknown_dimension_no_fix_actions(self):
        gate = build_gate(
            blocked_phase="test",
            violations={"temporal_drift": (0.9, 0.5)},
            entropy_state={"temporal_drift": 0.9},
        )
        fix_actions = [a for a in gate.suggested_actions if a.action_type == GateActionType.FIX]
        assert len(fix_actions) == 0
        # Still has skip
        assert gate.suggested_actions[-1].action_type == GateActionType.SKIP
