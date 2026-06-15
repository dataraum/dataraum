"""``import`` is the ONE source-bound activity (DAT-422/426) — pin its guard.

Past the import loop the whole spine runs source-free (the suite now feeds that
shape everywhere), so this is the counterpart assertion: the per-source
``import`` activity must REFUSE a source-free identity loudly — a ``None``
``source_id`` is a caller bug, and silently loading nothing would be worse than
failing the run.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from temporalio.exceptions import ApplicationError

from dataraum.worker.activities import PhaseActivities
from dataraum.worker.contracts import SourceIdentity, SourcePhaseInput


def test_import_refuses_a_source_free_identity() -> None:
    """A source-free identity fails the import activity loud + non-retryable.

    The guard sits before any substrate work, so a bare MagicMock manager
    proves it raises without touching connections.
    """
    activities = PhaseActivities(MagicMock())
    identity = SourceIdentity(workspace_id="ws-1", session_id="sess-1", run_id="run-A")

    with pytest.raises(ApplicationError) as excinfo:
        activities.run_import(SourcePhaseInput(identity=identity, vertical="finance"))

    assert excinfo.value.type == "PhaseFailed"
    assert excinfo.value.non_retryable
    assert "source_id" in str(excinfo.value)
