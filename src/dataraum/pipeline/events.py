"""Structured pipeline events.

Replaces the flat (int, int, str) progress callback with typed events
that carry entropy scores, gate results, and parallel-phase info.
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
    POST_VERIFICATION = "post_verification"
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
    table_details: dict[str, dict[str, float]] = field(default_factory=dict)
    view_details: dict[str, dict[str, float]] = field(default_factory=dict)
    # PHASE_COMPLETED: observability metrics from PhaseResult
    records_processed: int = 0
    records_created: int = 0
    warnings: list[str] = field(default_factory=list)
    outputs: dict[str, Any] = field(default_factory=dict)
    summary: str = ""
    # EXIT_CHECK: available fixes per violating dimension
    # dim_path -> [{"action_name": str, "phase_name": str, "guidance": str}]
    available_fixes: dict[str, list[dict[str, str]]] = field(default_factory=dict)
    # POST_VERIFICATION: detectors that could not run
    # [{"detector_id": str, "reason": str}]
    skipped_detectors: list[dict[str, str]] = field(default_factory=list)
    # EXIT_CHECK: per-target component evidence for smart context
    # dim_path -> target -> {component_key: value}
    column_evidence: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)


# Callback type for structured events
EventCallback = Callable[[PipelineEvent], None]
