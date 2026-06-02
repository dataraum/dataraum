"""Temporal activity worker (DAT-344; per-table fan-out DAT-370).

The engine runs as a pure Python Temporal worker: it bootstraps the substrate
once (:mod:`dataraum.worker.bootstrap`) and serves both the Python workflows
(:mod:`dataraum.worker.workflows`) and the pipeline-phase activities
(:mod:`dataraum.worker.activities`) on one task queue. The cockpit triggers
workflows by name via the Temporal Client. No Starlette shell.
"""

from __future__ import annotations

from dataraum.worker.activity import (
    begin_session_select,
    raw_table_ids,
    run_detectors,
    run_phase,
    run_session_phase,
    run_session_replay_cleanup,
    typed_table_id_for_raw,
)
from dataraum.worker.bootstrap import (
    bootstrap_worker_substrate,
    shutdown_worker_substrate,
)
from dataraum.worker.contracts import (
    AddSourceInput,
    AddSourceResult,
    BeginSessionInput,
    BeginSessionResult,
    ImportResult,
    PhaseOutcome,
    ProcessTableInput,
    ProcessTableResult,
    ProgressSnapshot,
    SessionIdentity,
    SessionReplayCleanupInput,
    SessionScopedInput,
    SourceIdentity,
    TableScopedInput,
    TypingResult,
)
from dataraum.worker.workflows import (
    AddSourceWorkflow,
    BeginSessionWorkflow,
    ProcessTableWorkflow,
)

__all__ = [
    "AddSourceInput",
    "AddSourceResult",
    "AddSourceWorkflow",
    "BeginSessionInput",
    "BeginSessionResult",
    "BeginSessionWorkflow",
    "ImportResult",
    "PhaseOutcome",
    "ProcessTableInput",
    "ProcessTableResult",
    "ProcessTableWorkflow",
    "ProgressSnapshot",
    "SessionIdentity",
    "SessionReplayCleanupInput",
    "SessionScopedInput",
    "SourceIdentity",
    "TableScopedInput",
    "TypingResult",
    "begin_session_select",
    "bootstrap_worker_substrate",
    "raw_table_ids",
    "run_detectors",
    "run_phase",
    "run_session_phase",
    "run_session_replay_cleanup",
    "shutdown_worker_substrate",
    "typed_table_id_for_raw",
]
