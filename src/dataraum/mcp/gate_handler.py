"""MCP gate handler for AI agent gate resolution.

When the pipeline is running with gate_mode=pause and is triggered via MCP,
this handler formats gate information for the LLM and accepts resolution
via the apply_fix tool.
"""

from __future__ import annotations

import logging
from typing import Any

from dataraum.pipeline.gates import (
    Gate,
    GateActionType,
    GateResolution,
)

_log = logging.getLogger(__name__)


class MCPGateHandler:
    """Gate handler that stores gate state for MCP tool resolution.

    Unlike the interactive CLI handler, the MCP handler does not block
    on user input. Instead it stores the current gate and exposes it
    for resolution via the apply_fix tool.
    """

    def __init__(self) -> None:
        self._current_gate: Gate | None = None
        self._resolution: GateResolution | None = None
        self._notifications: list[str] = []

    @property
    def current_gate(self) -> Gate | None:
        """The gate currently awaiting resolution."""
        return self._current_gate

    @property
    def has_pending_gate(self) -> bool:
        """Whether there is a gate waiting for resolution."""
        return self._current_gate is not None and self._resolution is None

    def resolve(self, gate: Gate) -> GateResolution:
        """Store gate for external resolution and return skip.

        In the MCP flow, gates are not resolved interactively.
        The handler records the gate and immediately returns a skip
        resolution so the pipeline can continue. The LLM can then
        use apply_fix to address issues.
        """
        self._current_gate = gate
        _log.info(
            "Gate fired: %s (type=%s, phase=%s, violations=%d)",
            gate.gate_id,
            gate.gate_type,
            gate.blocked_phase,
            len(gate.violations),
        )
        # Auto-skip in MCP mode — the LLM sees gate info via get_actions
        return GateResolution(action_taken=GateActionType.SKIP)

    def notify(self, message: str) -> None:
        """Record notification for later retrieval."""
        self._notifications.append(message)
        _log.info("Gate notification: %s", message)

    def format_gate_status(self) -> dict[str, Any] | None:
        """Format the current gate as a dict for tool responses."""
        if self._current_gate is None:
            return None

        gate = self._current_gate
        return {
            "gate_id": gate.gate_id,
            "gate_type": gate.gate_type,
            "blocked_phase": gate.blocked_phase,
            "violations": [
                {
                    "dimension": v.dimension,
                    "score": v.score,
                    "threshold": v.threshold,
                    "affected_targets": v.affected_targets,
                    "evidence_summary": v.evidence_summary,
                }
                for v in gate.violations
            ],
            "suggested_actions": [
                {
                    "index": a.index,
                    "action_type": a.action_type.value,
                    "label": a.label,
                    "confidence": a.confidence,
                    "parameters": a.parameters,
                }
                for a in gate.suggested_actions
            ],
            "entropy_state": gate.entropy_state,
        }

    def clear(self) -> None:
        """Clear gate state after resolution or pipeline completion."""
        self._current_gate = None
        self._resolution = None
        self._notifications.clear()
