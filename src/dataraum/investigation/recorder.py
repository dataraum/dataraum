"""Investigation session recording.

Business logic for creating, updating, and querying investigation sessions
and their steps. Used by MCP tool dispatch to build audit trails.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from dataraum.investigation.db_models import InvestigationSession, InvestigationStep

# Maximum length for result_summary to avoid DB bloat
_MAX_SUMMARY_LENGTH = 2000


def begin_session(
    session: Session,
    source_id: str,
    intent: str,
    *,
    contract: str | None = None,
) -> InvestigationSession:
    """Create a new investigation session.

    Args:
        session: SQLAlchemy session.
        source_id: Data source being investigated.
        intent: Free-text description of what the agent is trying to do.
        contract: Active contract name.

    Returns:
        The created InvestigationSession.
    """
    investigation = InvestigationSession(
        source_id=source_id,
        intent=intent,
        contract=contract,
        status="active",
    )
    session.add(investigation)
    session.flush()
    return investigation


def record_step(
    session: Session,
    session_id: str,
    tool_name: str,
    arguments: dict[str, Any],
    *,
    status: str = "success",
    result: dict[str, Any] | str | None = None,
    error: str | None = None,
    started_at: datetime | None = None,
    duration_seconds: float = 0.0,
) -> InvestigationStep:
    """Record a tool invocation within an investigation session.

    Args:
        session: SQLAlchemy session.
        session_id: Parent session ID.
        tool_name: MCP tool name (e.g., "look", "measure").
        arguments: Full tool arguments.
        status: "success" or "error".
        result: Tool result (will be summarized for storage).
        error: Error message if status is "error".
        started_at: When the tool call started.
        duration_seconds: How long the call took.

    Returns:
        The created InvestigationStep.
    """
    # Determine ordinal from current step count
    inv = session.get(InvestigationSession, session_id)
    if inv is None:
        raise ValueError(f"No session with id {session_id!r}")
    if inv.status != "active":
        raise ValueError(f"Session {session_id!r} is {inv.status!r}, cannot record steps")

    ordinal = inv.step_count
    inv.step_count = ordinal + 1

    # Extract target and dimension from arguments
    target = _extract_target(arguments)
    dimension = arguments.get("dimension")

    step = InvestigationStep(
        session_id=session_id,
        ordinal=ordinal,
        tool_name=tool_name,
        arguments=arguments,
        status=status,
        result_summary=summarize_result(tool_name, result) if result else None,
        error=error,
        started_at=started_at or datetime.now(UTC),
        duration_seconds=duration_seconds,
        target=target,
        dimension=dimension,
    )
    session.add(step)
    session.flush()
    return step


def end_session(
    session: Session,
    session_id: str,
    outcome: str,
    *,
    summary: str | None = None,
    payload: dict[str, Any] | None = None,
) -> InvestigationSession:
    """Close an investigation session with an outcome.

    Args:
        session: SQLAlchemy session.
        session_id: Session to close.
        outcome: One of "delivered", "refused", "escalated", "abandoned".
        summary: Agent's justification for the outcome.
        payload: Structured outcome data.

    Returns:
        The updated InvestigationSession.
    """
    _TERMINAL_STATUSES = {"delivered", "refused", "escalated", "abandoned"}

    inv = session.get(InvestigationSession, session_id)
    if inv is None:
        raise ValueError(f"No session with id {session_id!r}")
    if inv.status != "active":
        raise ValueError(f"Session {session_id!r} is already {inv.status!r}, cannot end")
    if outcome not in _TERMINAL_STATUSES:
        raise ValueError(f"Invalid outcome {outcome!r}, must be one of {_TERMINAL_STATUSES}")

    now = datetime.now(UTC)
    inv.status = outcome
    inv.ended_at = now
    inv.duration_seconds = (now - inv.started_at).total_seconds()
    inv.outcome_summary = summary
    inv.outcome_payload = payload
    session.flush()
    return inv


def get_session_trace(
    session: Session,
    session_id: str,
) -> dict[str, Any]:
    """Load a complete session with all steps for replay/review.

    Returns a dict with session metadata and ordered steps.
    """
    inv = session.get(InvestigationSession, session_id)
    if inv is None:
        raise ValueError(f"No session with id {session_id!r}")

    return {
        "session_id": inv.session_id,
        "source_id": inv.source_id,
        "status": inv.status,
        "intent": inv.intent,
        "contract": inv.contract,
        "started_at": inv.started_at.isoformat(),
        "ended_at": inv.ended_at.isoformat() if inv.ended_at else None,
        "duration_seconds": inv.duration_seconds,
        "outcome_summary": inv.outcome_summary,
        "outcome_payload": inv.outcome_payload,
        "step_count": inv.step_count,
        "steps": [
            {
                "ordinal": step.ordinal,
                "tool_name": step.tool_name,
                "arguments": step.arguments,
                "status": step.status,
                "result_summary": step.result_summary,
                "error": step.error,
                "started_at": step.started_at.isoformat(),
                "duration_seconds": step.duration_seconds,
                "target": step.target,
                "dimension": step.dimension,
            }
            for step in inv.steps
        ],
    }


def summarize_result(tool_name: str, result: dict[str, Any] | str | None) -> str:
    """Extract a storage-friendly summary from a tool result.

    Compact tools (measure, why, hypothesize, fix) store full results.
    Large tools (query, run_sql, get_context) are truncated.
    """
    if result is None:
        return ""

    if isinstance(result, str):
        text = result
    else:
        text = json.dumps(result, default=str, ensure_ascii=False)

    if len(text) <= _MAX_SUMMARY_LENGTH:
        return text

    return text[:_MAX_SUMMARY_LENGTH] + "…"


def _extract_target(arguments: dict[str, Any]) -> str | None:
    """Extract a target reference from tool arguments.

    Looks for common argument patterns across tools.
    """
    # Explicit target field (e.g., "column:orders.amount")
    target = arguments.get("target")
    if isinstance(target, str) and target:
        return target

    # table + column → "table.column"
    table = arguments.get("table") or arguments.get("table_name")
    column = arguments.get("column")
    if isinstance(table, str) and isinstance(column, str):
        return f"{table}.{column}"
    if isinstance(table, str):
        return table

    return None
