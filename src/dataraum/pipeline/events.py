"""Structured pipeline events.

Replaces the flat (int, int, str) progress callback with typed events
that carry entropy scores, gate results, and parallel-phase info.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """Types of events emitted during pipeline execution."""

    PHASE_STARTED = "phase_started"
    PHASE_COMPLETED = "phase_completed"
    PHASE_FAILED = "phase_failed"
    PHASE_SKIPPED = "phase_skipped"
    POST_VERIFICATION = "post_verification"
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
    violations: dict[str, tuple[float, float]] = field(default_factory=dict)
    duration_seconds: float = 0.0
    error: str = ""
    parallel_phases: list[str] = field(default_factory=list)
    # EXIT_CHECK / POST_VERIFICATION: dimension_path -> {target -> score}
    column_details: dict[str, dict[str, float]] = field(default_factory=dict)
    # PHASE_COMPLETED: observability metrics from PhaseResult
    records_processed: int = 0
    records_created: int = 0
    warnings: list[str] = field(default_factory=list)
    outputs: dict[str, Any] = field(default_factory=dict)
    summary: str = ""
    # FIX_APPLIED only: before/after scores keyed by dimension
    before_scores: dict[str, float] = field(default_factory=dict)
    after_scores: dict[str, float] = field(default_factory=dict)


# Callback type for structured events
EventCallback = Callable[[PipelineEvent], None]
