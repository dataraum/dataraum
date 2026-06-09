"""Witness reliabilities — the calibrated trust weights for pooled measurements.

ADR-0009 (DAT-457/450): a pooled measurement adjudicates a claim by pooling
several WITNESSES, each weighted by a reliability ``r ∈ [0, 1]`` (the log-linear
pooling exponent in :mod:`dataraum.entropy.pooling`). Per the design these ``r``
are *estimated quantities with provenance, never inline constants*: the eval
reliability rig runs each witness over the generative injection families against
ground truth and ships the measured values + provenance in
``dataraum-config/entropy/reliabilities.yaml``.

This module is the engine-side loader (a sibling of :mod:`dataraum.entropy.loss`).
The detector loads the config and threads ``for_measurement(...)`` into the pure
measurement; the measurement keeps a neutral uncalibrated fallback only for
direct/test callers, so the SHIPPED values always come from the artifact.

Reliability is the *resolution* weight only — conflict ``C`` is weight-robust, so
an uncalibrated ``r`` never hides a disagreement (it only mis-resolves the
posterior + ignorance). That is what makes cold-start on placeholder priors safe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from dataraum.core.config import get_config_file
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

RELIABILITY_CONFIG_PATH = "entropy/reliabilities.yaml"


@dataclass(frozen=True)
class ReliabilityConfig:
    """Per-measurement, per-witness reliabilities + the provenance of the values.

    ``witnesses[measurement_id][witness_id] = r``. ``provenance`` records whether
    the values were measured by the rig (``calibrated``), the corpus version,
    sample size, seed range, and date — so a consumer can tell a shipped
    calibration from a placeholder prior.
    """

    witnesses: dict[str, dict[str, float]] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)

    def for_measurement(self, measurement_id: str) -> dict[str, float]:
        """The witness reliabilities for one measurement, or ``{}`` if unlisted.

        A measurement absent here has no shipped reliabilities; its detector
        falls back to the measurement module's neutral default.
        """
        return dict(self.witnesses.get(measurement_id, {}))

    @property
    def calibrated(self) -> bool:
        """True once the rig has overwritten the placeholder priors with measured values."""
        return bool(self.provenance.get("calibrated", False))


_cache: ReliabilityConfig | None = None


def get_reliability_config() -> ReliabilityConfig:
    """Load + cache the reliability artifact from config."""
    global _cache  # noqa: PLW0603
    if _cache is None:
        raw = yaml.safe_load(get_config_file(RELIABILITY_CONFIG_PATH).read_text()) or {}
        _cache = ReliabilityConfig(
            witnesses=raw.get("witnesses", {}),
            provenance=raw.get("provenance", {}),
        )
    return _cache


def reset_reliability_config_cache() -> None:
    """Drop the cached config (tests / config reload)."""
    global _cache  # noqa: PLW0603
    _cache = None
