"""Fix execution framework for entropy-gated pipeline.

Provides:
- ActionDefinition: Declarative action specification
- ActionRegistry: Registry of available fix actions
- FixRequest/FixResult: Request and result types
- FixExecutor: Executes fixes with before/after hard verification
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.entropy.decisions import Decision, DecisionRecord


class ActionCategory:
    """Categories of fix actions."""

    TRANSFORM = "transform"  # Modifies data (type casts, views)
    ANNOTATE = "annotate"  # Writes metadata (units, names, null semantics)


@dataclass
class ActionDefinition:
    """Declarative specification for a fix action."""

    action_type: str  # e.g., "override_type", "declare_unit"
    category: str  # ActionCategory.TRANSFORM or .ANNOTATE
    description: str
    hard_verifiable: bool  # Whether before/after hard scores can be compared
    parameters_schema: dict[str, str] = field(default_factory=dict)  # param_name -> description

    # The actual executor function
    # Signature: (session, duckdb_conn, target, parameters) -> dict[str, Any]
    executor: Callable[..., dict[str, Any]] | None = None


@dataclass
class FixRequest:
    """Request to execute a fix action."""

    action_type: str
    target: str  # e.g., "column:orders.amount"
    parameters: dict[str, Any] = field(default_factory=dict)
    actor: str = "user"  # "user", "auto_fix", "mcp_agent"
    gate_type: str = ""
    blocked_phase: str = ""
    source_id: str = ""
    run_id: str = ""


@dataclass
class FixResult:
    """Result of executing a fix action."""

    success: bool
    improved: bool = False
    before_scores: dict[str, float] = field(default_factory=dict)
    after_scores: dict[str, float] = field(default_factory=dict)
    decision: Decision | None = None
    error: str | None = None

    @property
    def score_deltas(self) -> dict[str, float]:
        """Compute score changes (negative = improvement)."""
        return {
            dim: self.after_scores.get(dim, 0) - self.before_scores.get(dim, 0)
            for dim in set(self.before_scores) | set(self.after_scores)
        }


class ActionRegistry:
    """Registry of available fix actions."""

    def __init__(self) -> None:
        self._actions: dict[str, ActionDefinition] = {}

    def register(self, definition: ActionDefinition) -> None:
        """Register a fix action."""
        self._actions[definition.action_type] = definition

    def get(self, action_type: str) -> ActionDefinition | None:
        """Look up an action by type."""
        return self._actions.get(action_type)

    def list_actions(self) -> list[ActionDefinition]:
        """Get all registered actions."""
        return list(self._actions.values())

    def has(self, action_type: str) -> bool:
        """Check if an action type is registered."""
        return action_type in self._actions


class FixExecutor:
    """Executes fix actions with before/after hard verification.

    Flow:
    1. Take hard snapshot (before)
    2. Execute the action
    3. Take hard snapshot (after)
    4. Create Decision with before/after scores
    5. Persist DecisionRecord
    6. Return FixResult
    """

    def __init__(self, registry: ActionRegistry):
        self.registry = registry

    def execute(
        self,
        request: FixRequest,
        session: Session,
        duckdb_conn: Any = None,
    ) -> FixResult:
        """Execute a fix action with verification.

        Args:
            request: The fix to execute
            session: SQLAlchemy session for DB writes
            duckdb_conn: DuckDB connection for data operations

        Returns:
            FixResult with before/after scores and decision record
        """
        definition = self.registry.get(request.action_type)
        if definition is None:
            return FixResult(
                success=False,
                error=f"Unknown action type: {request.action_type}",
            )

        if definition.executor is None:
            return FixResult(
                success=False,
                error=f"No executor registered for: {request.action_type}",
            )

        try:
            from dataraum.entropy.hard_snapshot import take_hard_snapshot

            # Before snapshot (if action is hard-verifiable)
            before_scores: dict[str, float] = {}
            if definition.hard_verifiable:
                before = take_hard_snapshot(
                    target=request.target,
                    session=session,
                    duckdb_conn=duckdb_conn,
                )
                before_scores = before.scores

            # Execute the action
            action_result = definition.executor(
                session=session,
                duckdb_conn=duckdb_conn,
                target=request.target,
                parameters=request.parameters,
            )

            # After snapshot (if action is hard-verifiable)
            after_scores: dict[str, float] = {}
            if definition.hard_verifiable:
                # Flush so the snapshot sees the action's writes
                session.flush()
                after = take_hard_snapshot(
                    target=request.target,
                    session=session,
                    duckdb_conn=duckdb_conn,
                )
                after_scores = after.scores

            # Determine improvement from hard scores (if available),
            # otherwise fall back to action_result
            if before_scores and after_scores:
                improved = any(
                    after_scores.get(d, 1.0) < before_scores.get(d, 1.0)
                    for d in before_scores
                )
            else:
                improved = action_result.get("improved", False)

            # Create decision record
            decision = Decision(
                decision_id=str(uuid4()),
                gate_type=request.gate_type,
                blocked_phase=request.blocked_phase,
                action_type=request.action_type,
                target=request.target,
                parameters=request.parameters,
                actor=request.actor,
                before_scores=before_scores,
                after_scores=after_scores,
                improved=improved,
                evidence_summary=action_result.get("evidence", ""),
                decided_at=datetime.now(UTC),
            )

            # Persist decision
            record = DecisionRecord(
                decision_id=decision.decision_id,
                run_id=request.run_id or None,
                source_id=request.source_id or None,
                gate_type=decision.gate_type,
                blocked_phase=decision.blocked_phase,
                action_type=decision.action_type,
                target=decision.target,
                parameters=decision.parameters,
                actor=decision.actor,
                before_scores=decision.before_scores,
                after_scores=decision.after_scores,
                improved=decision.improved,
                evidence_summary=decision.evidence_summary,
                decided_at=decision.decided_at,
            )
            session.add(record)

            return FixResult(
                success=True,
                improved=decision.improved,
                before_scores=decision.before_scores,
                after_scores=decision.after_scores,
                decision=decision,
            )

        except Exception as e:
            return FixResult(
                success=False,
                error=str(e),
            )


# Global registry
_default_registry: ActionRegistry | None = None


def get_default_action_registry() -> ActionRegistry:
    """Get the default action registry with seed actions registered."""
    global _default_registry
    if _default_registry is None:
        _default_registry = ActionRegistry()
        _register_seed_actions(_default_registry)
    return _default_registry


def _register_seed_actions(registry: ActionRegistry) -> None:
    """Register seed actions from action_executors module."""
    from dataraum.entropy.action_executors import get_seed_actions

    for definition in get_seed_actions():
        registry.register(definition)
