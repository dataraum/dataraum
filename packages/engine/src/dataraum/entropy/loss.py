"""Loss layer — severity as expected loss per intent (docs/architecture/entropy.md, DAT-442/457).

For a modeled measurement, readiness per intent is EXPECTED LOSS, not the detector
score:

    risk(intent) = clamp01( Σ_signal  weight[signal] · value(signal) )

with the per-intent weights in ``dataraum-config/entropy/loss.yaml``. A weight
named ``conflict`` / ``surprise`` / ``score`` scores the measurement's PRIMARY
value (``obj.score`` — the pooled conflict ``C`` for an adjudication measurement,
a KL surprise ``D_KL(observed ‖ reference)`` for a statistical one); any other
name scores a secondary signal from evidence (e.g. ``ignorance``). One generic
rule, so the SAME loss layer scores both paradigms (DAT-442 second wave). Severity
moves OUT of the score and INTO the loss table — the same value is catastrophic
for aggregation (you'd aggregate mishandled values) but mild for an exploratory
query that can hedge on a caveat.

The loss is driven by the DISAGREEMENT (conflict ``C`` + ignorance ``U``), NOT
the point belief: the log-linear posterior stays confident even under conflict
(two witnesses agree, one dissents), so ``E_q`` over it would read ~0 for a
contested column — exactly backwards. This is "entropy as disagreement" applied
to severity.

The loss tables ARE the whole readiness rollup (the Bayesian network and
``network.yaml`` were deleted in DAT-442); a pooled measurement (one with a loss table here)
feeds ``risk(intent)`` directly, with no hand-set edge weights. Per-measurement
weights are PLACEHOLDER priors, calibrated later (DAT-450), never tuned to a metric.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import yaml

from dataraum.core.config import get_config_file
from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    from dataraum.entropy.models import EntropyObject

logger = get_logger(__name__)

LOSS_CONFIG_PATH = "entropy/loss.yaml"


@dataclass(frozen=True)
class LossConfig:
    """Per-measurement, per-intent ``(conflict, ignorance)`` loss weights.

    ``measurements[detector_id][intent] = {"conflict": w_c, "ignorance": w_u}``.
    A detector present here is a *loss-path* (pooled) measurement; the readiness
    assembler feeds its ``risk(intent)`` straight into the per-intent bands.
    """

    measurements: dict[str, dict[str, dict[str, float]]] = field(default_factory=dict)
    # Readiness bands (relocated from the deleted network.yaml discretization): a
    # per-intent risk ≤ low_upper is ready, ≤ medium_upper is investigate, else blocked.
    readiness_bands: dict[str, float] = field(
        default_factory=lambda: {"low_upper": 0.3, "medium_upper": 0.6}
    )

    def is_loss_measurement(self, detector_id: str) -> bool:
        return detector_id in self.measurements

    def intents(self) -> list[str]:
        """Every intent name across the loss table — replaces the network intent nodes."""
        seen: set[str] = set()
        for table in self.measurements.values():
            seen.update(table)
        return sorted(seen)

    def band(self, risk: float) -> str:
        """Readiness band for a per-intent risk: the whole banding, no network.

        ``risk ≤ low_upper`` → ready · ``≤ medium_upper`` → investigate · else blocked.
        """
        if risk > self.readiness_bands["medium_upper"]:
            return "blocked"
        if risk > self.readiness_bands["low_upper"]:
            return "investigate"
        return "ready"


_cache: LossConfig | None = None


def get_loss_config() -> LossConfig:
    """Load + cache the loss table from config."""
    global _cache  # noqa: PLW0603
    if _cache is None:
        raw = yaml.safe_load(get_config_file(LOSS_CONFIG_PATH).read_text()) or {}
        bands = raw.get("readiness_bands") or {"low_upper": 0.3, "medium_upper": 0.6}
        _cache = LossConfig(measurements=raw.get("measurements", {}), readiness_bands=bands)
    return _cache


def reset_loss_config_cache() -> None:
    """Drop the cached config (tests / config reload)."""
    global _cache  # noqa: PLW0603
    _cache = None


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


# A loss weight named one of these scores the object's PRIMARY measure
# (``obj.score``) — "conflict" for an adjudication (pooled) measurement, "surprise"
# (a KL divergence) for a statistical one, "score" the neutral alias. Any OTHER
# weight name scores the worst value of that key across the object's per-token /
# per-row evidence (e.g. "ignorance"). The worst signal drives the column's risk.
_PRIMARY_SIGNALS = frozenset({"score", "conflict", "surprise"})


def _signal_value(obj: EntropyObject, signal: str) -> float:
    """The measurement's value for one named loss signal."""
    if signal in _PRIMARY_SIGNALS:
        return obj.score
    return max((float(e.get(signal, 0.0)) for e in obj.evidence), default=0.0)


def loss_risk_for_object(obj: EntropyObject, config: LossConfig) -> dict[str, float]:
    """Per-intent expected-loss risk for one measurement object."""
    table = config.measurements.get(obj.detector_id)
    if not table:
        return {}
    return {
        intent: _clamp01(
            sum(weight * _signal_value(obj, signal) for signal, weight in weights.items())
        )
        for intent, weights in table.items()
    }


def compute_loss_risk(objects: list[EntropyObject], config: LossConfig) -> dict[str, float]:
    """Per-intent risk for a column's pooled objects — the worst (max) across them."""
    merged: dict[str, float] = {}
    for obj in objects:
        for intent, risk in loss_risk_for_object(obj, config).items():
            if risk > merged.get(intent, 0.0):
                merged[intent] = risk
    return merged
