"""Gate model and handler protocol for entropy-gated pipeline.

Gates are checkpoints between pipeline phases where entropy preconditions
are checked. When a gate fires, the pipeline can pause and present
the user (or an agent) with options to resolve the issue.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol


class GateActionType(str, Enum):
    """Types of actions available at a gate."""

    FIX = "fix"  # Apply a specific fix
    FIX_ALL = "fix_all"  # Apply all suggested fixes
    SKIP = "skip"  # Continue with warnings
    INSPECT = "inspect"  # Show affected rows/details
    QUESTION = "question"  # Free-text question for LLM


@dataclass
class GateViolation:
    """A single entropy dimension that violates a gate threshold."""

    dimension: str  # Sub-dimension name (e.g., "type_fidelity")
    score: float  # Current score
    threshold: float  # Maximum allowed score
    affected_targets: list[str] = field(default_factory=list)  # e.g., ["column:orders.amount"]
    evidence_summary: str = ""


@dataclass
class GateAction:
    """A numbered option presented at a gate."""

    index: int  # Display number (1-based)
    action_type: GateActionType
    label: str  # e.g., "fix: cast orders.amount to DECIMAL(10,2)"
    description: str = ""
    confidence: float = 0.0  # 0.0-1.0
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass
class Gate:
    """A fired gate blocking pipeline progress."""

    gate_id: str
    gate_type: str  # "structural", "semantic", "value", "contract"
    blocked_phase: str  # Phase that can't proceed
    violations: list[GateViolation] = field(default_factory=list)
    suggested_actions: list[GateAction] = field(default_factory=list)
    entropy_state: dict[str, float] = field(default_factory=dict)  # Current hard scores

    @property
    def action_count(self) -> int:
        return len(self.suggested_actions)


@dataclass
class GateResolution:
    """Result of resolving a gate."""

    action_taken: GateActionType
    action_index: int | None = None  # Which numbered action was chosen
    parameters: dict[str, Any] = field(default_factory=dict)
    user_input: str = ""  # Free-text input if action_type == QUESTION


class GateHandler(Protocol):
    """Protocol for gate resolution handlers.

    Implementations:
    - InteractiveCLIHandler: Rich prompts in terminal
    - MCPGateHandler: Resolve via MCP tool calls
    - AutoFixHandler: Automatic resolution (future)
    """

    async def resolve(self, gate: Gate) -> GateResolution:
        """Present a gate to the user/agent and return their resolution."""
        ...

    async def notify(self, message: str) -> None:
        """Send a non-blocking notification about gate status."""
        ...


def build_gate(
    blocked_phase: str,
    violations: dict[str, tuple[float, float]],
    entropy_state: dict[str, float],
) -> Gate:
    """Build a Gate from precondition violations.

    Args:
        blocked_phase: Name of the phase that can't proceed
        violations: sub_dimension -> (current_score, threshold)
        entropy_state: Full current hard scores dict

    Returns:
        Gate with violations and basic actions (skip always available)
    """
    gate_violations = [
        GateViolation(
            dimension=dim,
            score=current,
            threshold=threshold,
        )
        for dim, (current, threshold) in violations.items()
    ]

    # Determine gate type from the blocked dimensions
    structural_dims = {"type_fidelity", "join_path_determinism", "relationship_quality"}
    semantic_dims = {"naming_clarity", "unit_declaration", "time_role"}
    value_dims = {"null_ratio", "outlier_rate", "benford_compliance", "temporal_drift"}

    violated_dims = set(violations.keys())
    if violated_dims & structural_dims:
        gate_type = "structural"
    elif violated_dims & semantic_dims:
        gate_type = "semantic"
    elif violated_dims & value_dims:
        gate_type = "value"
    else:
        gate_type = "general"

    # Always include skip action
    actions = [
        GateAction(
            index=1,
            action_type=GateActionType.SKIP,
            label="skip: continue with warnings",
            description="Accept current entropy levels and continue the pipeline",
        ),
    ]

    return Gate(
        gate_id=f"gate_{blocked_phase}",
        gate_type=gate_type,
        blocked_phase=blocked_phase,
        violations=gate_violations,
        suggested_actions=actions,
        entropy_state=entropy_state,
    )
