"""Guard the two table-local phase lists that must stay in sync (DAT-370).

The child workflow schedules the analytics phases from
``workflows._ANALYTICS_PHASES``; the stage-level detect step aggregates
detectors from ``activity._TABLE_LOCAL_PHASES``. They describe the same set of
table-local phases from two angles: ``_TABLE_LOCAL_PHASES`` is ``typing`` (which
mints the typed id and owns the ``type_fidelity`` detector) plus the analytics
phases. If a new table-local phase is added to the workflow but not here, its
pipeline.yaml detectors would silently never run at ``detect_table`` — so pin the
relationship with a test rather than a comment.
"""

from __future__ import annotations

from dataraum.worker.activity import _TABLE_LOCAL_PHASES
from dataraum.worker.workflows import _ANALYTICS_PHASES


def test_table_local_phases_are_typing_plus_the_analytics_chain() -> None:
    assert _TABLE_LOCAL_PHASES == ("typing", *_ANALYTICS_PHASES)
