"""No-orphan guard for detector phases vs detect paths (review wave-1).

Every ``pipeline.yaml`` phase that DECLARES detectors must be covered by some
detect path — ``_DETECTOR_PHASES`` (add_source ``detect``),
``SESSION_DETECTOR_PHASES`` (begin_session ``session_detect``), or
``OPERATING_MODEL_DETECTOR_PHASES`` (operating_model's terminal detect). The
gap this pins: ``cross_table_consistency`` was declared on the ``validation``
phase while no detect path ran that phase — silently scoreless for weeks, and
the regression class has now bitten three times.
"""

from __future__ import annotations

import yaml

from dataraum.core.config import get_config_file
from dataraum.worker.activity import (
    _DETECTOR_PHASES,
    OPERATING_MODEL_DETECTOR_PHASES,
    SESSION_DETECTOR_PHASES,
)


def test_every_detector_declaring_phase_is_on_a_detect_path() -> None:
    pipeline = yaml.safe_load(get_config_file("pipeline.yaml").read_text())
    declaring = {
        name
        for name, spec in (pipeline.get("phases") or {}).items()
        if (spec or {}).get("detectors")
    }
    covered = (
        set(_DETECTOR_PHASES) | set(SESSION_DETECTOR_PHASES) | set(OPERATING_MODEL_DETECTOR_PHASES)
    )
    orphaned = declaring - covered
    assert not orphaned, (
        f"phases declare detectors but no detect path runs them: {sorted(orphaned)} — "
        "wire the phase into a detect path or remove the declaration"
    )
