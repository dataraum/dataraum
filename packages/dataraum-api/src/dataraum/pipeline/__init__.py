"""Pipeline orchestration module.

Replaces ad-hoc scripts with a testable, parallel DAG orchestrator.

Usage:
    from dataraum.pipeline import Pipeline, run_pipeline

    # Run full pipeline
    results = await run_pipeline(source_id="my_source")

    # Run specific phase (+ dependencies)
    results = await run_pipeline(source_id="my_source", target_phase="semantic")

    # Check status
    status = await get_pipeline_status(source_id="my_source")
"""

from dataraum.pipeline.base import (
    Phase,
    PhaseContext,
    PhaseResult,
    PhaseStatus,
)
from dataraum.pipeline.orchestrator import Pipeline, run_pipeline
from dataraum.pipeline.status import get_pipeline_status

__all__ = [
    # Base types
    "Phase",
    "PhaseContext",
    "PhaseResult",
    "PhaseStatus",
    # Orchestrator
    "Pipeline",
    "run_pipeline",
    # Status
    "get_pipeline_status",
]
