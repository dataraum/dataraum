"""Tests for MCP gate handler."""

from dataraum.mcp.gate_handler import MCPGateHandler
from dataraum.pipeline.gates import (
    Gate,
    GateAction,
    GateActionType,
    GateViolation,
)


def _make_gate(**kwargs):
    defaults = {
        "gate_id": "gate_statistics",
        "gate_type": "structural",
        "blocked_phase": "statistics",
        "violations": [GateViolation(dimension="type_fidelity", score=0.6, threshold=0.5)],
        "suggested_actions": [
            GateAction(
                index=1,
                action_type=GateActionType.SKIP,
                label="skip: continue",
            )
        ],
        "entropy_state": {"type_fidelity": 0.6},
    }
    defaults.update(kwargs)
    return Gate(**defaults)


class TestMCPGateHandler:
    def test_resolve_returns_skip(self):
        handler = MCPGateHandler()
        gate = _make_gate()
        resolution = handler.resolve(gate)
        assert resolution.action_taken == GateActionType.SKIP

    def test_resolve_stores_current_gate(self):
        handler = MCPGateHandler()
        gate = _make_gate()
        handler.resolve(gate)
        assert handler.current_gate is gate

    def test_has_pending_gate_after_resolve(self):
        handler = MCPGateHandler()
        assert not handler.has_pending_gate
        gate = _make_gate()
        handler.resolve(gate)
        assert handler.has_pending_gate

    def test_notify_stores_messages(self):
        handler = MCPGateHandler()
        handler.notify("Gate cleared: type_fidelity improved")
        handler.notify("Pipeline continuing")
        assert len(handler._notifications) == 2

    def test_format_gate_status_none(self):
        handler = MCPGateHandler()
        assert handler.format_gate_status() is None

    def test_format_gate_status(self):
        handler = MCPGateHandler()
        gate = _make_gate()
        handler.resolve(gate)

        status = handler.format_gate_status()
        assert status is not None
        assert status["gate_id"] == "gate_statistics"
        assert status["gate_type"] == "structural"
        assert status["blocked_phase"] == "statistics"
        assert len(status["violations"]) == 1
        assert status["violations"][0]["dimension"] == "type_fidelity"
        assert status["violations"][0]["score"] == 0.6
        assert len(status["suggested_actions"]) == 1

    def test_clear(self):
        handler = MCPGateHandler()
        gate = _make_gate()
        handler.resolve(gate)
        handler.notify("test")

        handler.clear()
        assert handler.current_gate is None
        assert not handler.has_pending_gate
        assert handler.format_gate_status() is None
