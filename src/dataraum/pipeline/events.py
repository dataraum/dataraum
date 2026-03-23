"""Structured pipeline events.

Typed events emitted during pipeline execution carrying phase status,
timing, and observability metrics.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from dataraum.entropy.dimensions import _StrValueMixin


class EventType(_StrValueMixin):
    """Types of events emitted during pipeline execution."""

    PHASE_STARTED = "phase_started"
    PHASE_COMPLETED = "phase_completed"
    PHASE_FAILED = "phase_failed"
    PHASE_SKIPPED = "phase_skipped"
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
    duration_seconds: float = 0.0
    error: str = ""
    parallel_phases: list[str] = field(default_factory=list)
    # PHASE_COMPLETED: observability metrics from PhaseResult
    records_processed: int = 0
    records_created: int = 0
    warnings: list[str] = field(default_factory=list)
    outputs: dict[str, Any] = field(default_factory=dict)
    summary: str = ""


# Callback type for structured events
EventCallback = Callable[[PipelineEvent], None]
