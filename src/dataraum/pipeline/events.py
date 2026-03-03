"""Structured pipeline events.

Replaces the flat (int, int, str) progress callback with typed events
that carry entropy scores, gate results, and parallel-phase info.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum


class EventType(str, Enum):
    """Types of events emitted during pipeline execution."""

    PHASE_STARTED = "phase_started"
    PHASE_COMPLETED = "phase_completed"
    PHASE_FAILED = "phase_failed"
    PHASE_SKIPPED = "phase_skipped"
    POST_VERIFICATION = "post_verification"
    GATE_EVALUATED = "gate_evaluated"
    GATE_BLOCKED = "gate_blocked"
    GATE_RESOLVED = "gate_resolved"
    FIX_APPLIED = "fix_applied"
    EXIT_CHECK = "exit_check"
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"


@dataclass(frozen=True)
class PipelineEvent:
    """A single structured event emitted during pipeline execution."""

    event_type: EventType
    phase: str = ""
    step: int = 0
    total: int = 0
    message: str = ""
    scores: dict[str, float] = field(default_factory=dict)
    gate_status: str = ""  # "passed" | "blocked" | "skipped"
    violations: dict[str, tuple[float, float]] = field(default_factory=dict)
    duration_seconds: float = 0.0
    error: str = ""
    parallel_phases: list[str] = field(default_factory=list)


# Callback type for structured events
EventCallback = Callable[[PipelineEvent], None]
