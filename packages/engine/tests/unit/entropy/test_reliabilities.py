"""Witness reliabilities config — the shipped, provenance-bearing trust weights.

ADR-0009 (DAT-457/450): reliabilities are estimated-with-provenance artifacts, not
inline constants. This proves the loader contract (per-measurement lookup, the
calibrated flag) and that the shipped artifact actually carries the three
null_semantics witnesses the pooled measurement expects.
"""

from __future__ import annotations

from dataraum.entropy.measurements.null_semantics import DEFAULT_RELIABILITIES
from dataraum.entropy.reliabilities import (
    ReliabilityConfig,
    get_reliability_config,
    reset_reliability_config_cache,
)

_CONFIG = ReliabilityConfig(
    witnesses={"null_semantics": {"quarantine_clustering": 0.8, "type_claim": 0.7}},
    provenance={"calibrated": True, "sample_size": 500},
)


def test_for_measurement_returns_witness_weights() -> None:
    assert _CONFIG.for_measurement("null_semantics") == {
        "quarantine_clustering": 0.8,
        "type_claim": 0.7,
    }


def test_unlisted_measurement_is_empty_so_detector_falls_back() -> None:
    assert _CONFIG.for_measurement("unit_consistency") == {}


def test_calibrated_flag_reads_provenance() -> None:
    assert _CONFIG.calibrated is True
    assert ReliabilityConfig(provenance={"calibrated": False}).calibrated is False
    assert ReliabilityConfig().calibrated is False  # absent → uncalibrated


def test_shipped_artifact_carries_the_null_semantics_witnesses() -> None:
    reset_reliability_config_cache()
    try:
        cfg = get_reliability_config()
        shipped = cfg.for_measurement("null_semantics")
        # The artifact must cover exactly the witnesses the measurement pools, so
        # the detector never silently drops one to its fallback.
        assert set(shipped) == set(DEFAULT_RELIABILITIES)
        assert all(0.0 <= r <= 1.0 for r in shipped.values())
        # Provenance is present (calibrated flag is a real bool either way).
        assert isinstance(cfg.calibrated, bool)
    finally:
        reset_reliability_config_cache()
