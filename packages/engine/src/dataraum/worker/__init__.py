"""Temporal activity worker (DAT-344).

The engine runs as a pure Python Temporal *activity* worker: it bootstraps the
substrate once (:mod:`dataraum.worker.bootstrap`) and exposes pipeline phases as
activities via :mod:`dataraum.worker.activity`. TS workflows (cockpit) drive
these activities by name. No Starlette shell.
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
