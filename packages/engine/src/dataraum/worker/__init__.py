"""Temporal activity worker (DAT-344; per-table fan-out DAT-370).

The engine runs as a pure Python Temporal worker: it bootstraps the substrate
once (:mod:`dataraum.worker.bootstrap`) and serves both the Python workflows
(:mod:`dataraum.worker.workflows`) and the pipeline-phase activities
(:mod:`dataraum.worker.activities`) on one task queue. The cockpit triggers
workflows by name via the Temporal Client. No Starlette shell.
"""

from __future__ import annotations

from dataraum.worker.activity import (
    raw_table_ids,
    run_phase,
    run_source_detectors,
    run_table_detectors,
    typed_table_id_for_raw,
)
from dataraum.worker.bootstrap import (
    bootstrap_worker_substrate,
    shutdown_worker_substrate,
)
from dataraum.worker.contracts import (
    AddSourceInput,
    AddSourceResult,
    ImportResult,
    PhaseOutcome,
    ProcessTableInput,
    ProcessTableResult,
    SourceIdentity,
    TableScopedInput,
    TypingResult,
)
from dataraum.worker.workflows import AddSourceWorkflow, ProcessTableWorkflow

__all__ = [
    "AddSourceInput",
    "AddSourceResult",
    "AddSourceWorkflow",
    "ImportResult",
    "PhaseOutcome",
    "ProcessTableInput",
    "ProcessTableResult",
    "ProcessTableWorkflow",
    "SourceIdentity",
    "TableScopedInput",
    "TypingResult",
    "bootstrap_worker_substrate",
    "raw_table_ids",
    "run_phase",
    "run_source_detectors",
    "run_table_detectors",
    "shutdown_worker_substrate",
    "typed_table_id_for_raw",
]
