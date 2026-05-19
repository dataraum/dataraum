"""Pipeline orchestration module.

Provides a scheduler-based pipeline that executes phases in dependency
order, with contract-driven exit checks and fix replay.

Usage:
    from dataraum.pipeline import Phase, PhaseContext, PhaseResult

    # Check status
    status = get_pipeline_status(session, source_id="my_source")
"""

from dataraum.pipeline.base import (
    Phase,
    PhaseContext,
    PhaseResult,
    PhaseStatus,
)
from dataraum.pipeline.status import get_pipeline_status

__all__ = [
    # Base types
    "Phase",
    "PhaseContext",
    "PhaseResult",
    "PhaseStatus",
    # Status
    "get_pipeline_status",
]
