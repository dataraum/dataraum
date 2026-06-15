"""Guard the phase-list constant the terminal detect step depends on (DAT-394).

The single terminal ``detect`` activity runs the union of the detectors declared
by ``activity._DETECTOR_PHASES`` — the executed-chain phases (the child's
``typing`` + ``_ANALYTICS_PHASES`` and the parent's ``semantic_per_column``
reduce), source-wide, once, after the fan-out + reduce.

Two invariants are pinned here:
1. ``_DETECTOR_PHASES`` covers the executed chain: ``typing`` + the analytics
   phases + ``semantic_per_column``.
2. **No detector a chain phase declares in pipeline.yaml is orphaned** — every
   such detector runs in the terminal detect step. This is the regression the
   original DAT-370 cut introduced and eval caught: ``semantic_per_column``
   declared five detectors but, having moved off the per-phase path, was in no
   detect step, so they never executed. (DAT-394 collapsed the two detect steps
   into one, but the no-orphan property must still hold.)
"""

from __future__ import annotations

from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.worker.activity import (
    _DETECTOR_PHASES,
    SESSION_DETECTOR_PHASES,
    declared_detector_ids,
)
from dataraum.worker.workflows import (
    _ANALYTICS_PHASES,
    _SESSION_PHASE_ORDER,
    _SESSION_VALUE_PHASE_ORDER,
)

# The analysis phases the workflows actually execute: import + the child's
# typing/analytics chain + the parent's source-level reduce. Source of truth is
# workflows.py (parent + child ``run`` bodies); kept here independently so a
# detector-bearing chain phase that isn't wired into the terminal detect step is
# caught.
_CHAIN_PHASES = (
    "import",
    "typing",
    *_ANALYTICS_PHASES,
    "semantic_per_column",
)

# The begin_session chain the beginSessionWorkflow executes, in order. Kept
# independently from workflows.py so a detector-bearing session phase that isn't
# wired into the terminal ``session_detect`` (SESSION_DETECTOR_PHASES) is caught —
# the regression DAT-403's value layer would have been (detectors declared, phase
# unwired). The interleaved non-phase steps (``session_materialize_overlays``,
# ``session_write_keepers``, ``session_promote_to_latest``) declare no detectors and
# are intentionally omitted — the orphan guard only needs the detector-bearing phases.
_SESSION_CHAIN_PHASES = (
    "begin_session_select",
    *_SESSION_PHASE_ORDER,
    "enriched_views",
    *_SESSION_VALUE_PHASE_ORDER,
)


def test_detector_phases_cover_the_executed_chain() -> None:
    assert _DETECTOR_PHASES == ("typing", *_ANALYTICS_PHASES, "semantic_per_column")


def test_no_chain_phase_detector_is_orphaned() -> None:
    """Every detector a chain phase declares runs in the terminal detect step."""
    detect_step_phases = set(_DETECTOR_PHASES)
    declarations = load_phase_declarations()

    for phase in _CHAIN_PHASES:
        decl = declarations.get(phase)
        if not decl or not decl.detectors:
            continue
        assert phase in detect_step_phases, (
            f"phase '{phase}' declares detectors {decl.detectors} but is not in "
            "the terminal detect step (_DETECTOR_PHASES) — they would never run"
        )


def test_no_session_chain_phase_detector_is_orphaned() -> None:
    """Every detector a begin_session phase declares runs in the terminal session detect.

    The begin_session analogue of the above: a value phase (slice_analysis,
    temporal_slice_analysis, correlations) declaring a detector but absent from
    ``SESSION_DETECTOR_PHASES`` would never measure it (DAT-403).
    """
    detect_step_phases = set(SESSION_DETECTOR_PHASES)
    declarations = load_phase_declarations()

    for phase in _SESSION_CHAIN_PHASES:
        decl = declarations.get(phase)
        if not decl or not decl.detectors:
            continue
        assert phase in detect_step_phases, (
            f"begin_session phase '{phase}' declares detectors {decl.detectors} but is "
            "not in SESSION_DETECTOR_PHASES — they would never run"
        )


def test_terminal_detect_step_runs_every_wired_detector() -> None:
    """The terminal step picks up the union of the executed chain's detectors."""
    assert set(declared_detector_ids(_DETECTOR_PHASES)) == {
        "type_fidelity",
        "null_semantics",
        "null_ratio",
        "slice_conditional_null",
        "business_meaning",
        "unit_entropy",
        "temporal_entropy",
        "benford",
        "temporal_behavior",
    }
