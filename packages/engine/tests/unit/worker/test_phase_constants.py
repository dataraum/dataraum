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
from dataraum.worker.activity import _DETECTOR_PHASES, declared_detector_ids
from dataraum.worker.workflows import _ANALYTICS_PHASES

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


def test_terminal_detect_step_runs_every_wired_detector() -> None:
    """The terminal step picks up the union of the executed chain's detectors."""
    assert set(declared_detector_ids(_DETECTOR_PHASES)) == {
        "type_fidelity",
        "null_ratio",
        "business_meaning",
        "unit_entropy",
        "temporal_entropy",
        "outlier_rate",
        "benford",
    }
