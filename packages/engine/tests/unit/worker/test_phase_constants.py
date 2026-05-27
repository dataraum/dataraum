"""Guard the phase-list constants that the detect steps depend on (DAT-370).

The child workflow schedules the analytics phases from
``workflows._ANALYTICS_PHASES``; the stage-level detect steps run detectors for
``activity._TABLE_LOCAL_PHASES`` (``detect_table``, per typed table) and
``activity._SOURCE_LEVEL_PHASES`` (``detect_source``, once after the reduce).

Two invariants are pinned here:
1. ``_TABLE_LOCAL_PHASES`` is ``("typing", *_ANALYTICS_PHASES)``.
2. **No detector a chain phase declares in pipeline.yaml is orphaned** — every
   such detector runs in one of the two detect steps. This is the regression the
   original DAT-370 cut introduced and eval caught: ``semantic_per_column``
   declared five detectors but, having moved off the per-phase path, was in no
   detect step, so they never executed.
"""

from __future__ import annotations

from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.worker.activity import (
    _SOURCE_LEVEL_PHASES,
    _TABLE_LOCAL_PHASES,
    declared_detector_ids,
)
from dataraum.worker.workflows import _ANALYTICS_PHASES

# The analysis phases the workflows actually execute: import + the child's
# typing/analytics chain + the parent's source-level reduce. Source of truth is
# workflows.py (parent + child ``run`` bodies); kept here independently so a
# detector-bearing chain phase that isn't wired into a detect step is caught.
_CHAIN_PHASES = (
    "import",
    "typing",
    *_ANALYTICS_PHASES,
    "semantic_per_column",
)


def test_table_local_phases_are_typing_plus_the_analytics_chain() -> None:
    assert _TABLE_LOCAL_PHASES == ("typing", *_ANALYTICS_PHASES)


def test_no_chain_phase_detector_is_orphaned() -> None:
    """Every detector a chain phase declares runs in detect_table or detect_source."""
    detect_step_phases = set(_TABLE_LOCAL_PHASES) | set(_SOURCE_LEVEL_PHASES)
    declarations = load_phase_declarations()

    for phase in _CHAIN_PHASES:
        decl = declarations.get(phase)
        if not decl or not decl.detectors:
            continue
        assert phase in detect_step_phases, (
            f"phase '{phase}' declares detectors {decl.detectors} but is in no "
            "detect step (detect_table / detect_source) — they would never run"
        )


def test_source_level_detectors_resolve_to_the_semantic_reduce() -> None:
    """detect_source picks up semantic_per_column's declared detectors."""
    assert _SOURCE_LEVEL_PHASES == ("semantic_per_column",)
    assert set(declared_detector_ids(_SOURCE_LEVEL_PHASES)) == {
        "business_meaning",
        "unit_entropy",
        "temporal_entropy",
        "outlier_rate",
        "benford",
    }
