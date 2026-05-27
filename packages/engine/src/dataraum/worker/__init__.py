"""Temporal activity worker (DAT-344).

The engine runs as a pure Python Temporal worker: it bootstraps the substrate
once (:mod:`dataraum.worker.bootstrap`) and serves both the Python workflows
(:mod:`dataraum.worker.workflows`) and the pipeline-phase activities
(:mod:`dataraum.worker.activity`) on one task queue. The cockpit triggers
workflows by name via the Temporal Client. No Starlette shell.
"""

from __future__ import annotations

from dataraum.worker.activity import run_phase_activity
from dataraum.worker.bootstrap import (
    bootstrap_worker_substrate,
    shutdown_worker_substrate,
)
from dataraum.worker.contracts import PhaseActivityInput, PhaseActivityResult
from dataraum.worker.workflows import AddSourceWorkflow

__all__ = [
    "AddSourceWorkflow",
    "PhaseActivityInput",
    "PhaseActivityResult",
    "bootstrap_worker_substrate",
    "run_phase_activity",
    "shutdown_worker_substrate",
]
