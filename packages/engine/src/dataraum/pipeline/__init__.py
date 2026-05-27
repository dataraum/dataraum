"""Pipeline phase types.

The Phase protocol + per-phase data structures (PhaseContext / PhaseResult).
Execution is driven by the Temporal worker (``dataraum.worker``); there is no
in-tree scheduler (retired in DAT-369).
"""

from dataraum.pipeline.base import (
    Phase,
    PhaseContext,
    PhaseResult,
    PhaseStatus,
)

__all__ = [
    "Phase",
    "PhaseContext",
    "PhaseResult",
    "PhaseStatus",
]
