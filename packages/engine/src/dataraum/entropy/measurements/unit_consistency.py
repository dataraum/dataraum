"""Unit-consistency adjudication — the second pooled measurement (ADR-0009, DAT-428).

Does a numeric column secretly mix SCALES under one declared unit (kEUR amounts
among EUR amounts)? Two witnesses vote over the claim space {consistent, mixed} and
the pooling engine (:mod:`dataraum.entropy.pooling`) returns the posterior plus
conflict ``C`` and ignorance ``U`` — exactly the null_semantics shape (DAT-457),
reusing the same engine with a different claim space and witness extractors.

* **magnitude modality** — a single-unit column's values cluster in one decade
  band; a column mixing scales is BIMODAL in log-magnitude. The grounded statistic
  is Pearson's bimodality coefficient ``(skew²+1)/kurtosis`` on ``log10|v|`` — a
  cited measure, NOT a boost curve, with the uniform-distribution reference
  ``5/9 ≈ 0.555`` as its pivot.
* **declared unit** — the column declares a single unit (Pint/LLM unit confidence)
  → it CLAIMS consistency. It abstains when no unit was declared.

The novel-scale case is the point: magnitude reads BIMODAL while the declared unit
insists SINGLE → conflict ``C`` rises → ``investigate`` + a unit teach. A small fx
mix (``×1.1`` currency) is deliberately OUT of scope — a 10% shift is undetectable
from values (the old unit_entropy misalignment); the generative mixed-units family
(DAT-450) injects SCALE mixes that this can actually see.

Pure module: no DB, no config, no LLM. Reliabilities are documented placeholder
priors, calibrated later from the families (DAT-450) — not tuned to a metric.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dataraum.entropy.pooling import PoolResult, Witness, pool

# The canonical claim space (identity comparison, ADR-0009). Order fixes the tuple
# layout passed to the pooling engine; index 1 is the P(mixed) coordinate.
CLAIM_SPACE: tuple[str, str] = ("consistent", "mixed")

# Placeholder reliability priors (DAT-428). Calibrated from generative families by
# DAT-450; deliberately NOT tuned to pass a metric.
DEFAULT_RELIABILITIES: dict[str, float] = {
    "magnitude_modality": 0.7,
    "declared_unit": 0.6,
}

_MIN_SAMPLE = 30  # below this, the log-magnitude shape is too noisy to judge


def _distribution(p_mixed: float) -> dict[str, float]:
    """A claim-space distribution from P(mixed), clamped to [0, 1]."""
    p = min(1.0, max(0.0, p_mixed))
    return {"consistent": 1.0 - p, "mixed": p}


def _witness(witness_id: str, distribution: Mapping[str, float], reliability: float) -> Witness:
    return Witness(
        witness_id=witness_id,
        distribution=tuple(distribution[label] for label in CLAIM_SPACE),
        reliability=reliability,
    )


def bimodality_coefficient(xs: Sequence[float]) -> float:
    """Pearson's bimodality coefficient ``(skewness² + 1) / kurtosis``.

    A standardised measure of bimodality: ``≈0.33`` for a unimodal normal,
    ``5/9 ≈ 0.555`` for a uniform, ``→1`` for a clean two-point split. ``0.0`` for
    degenerate input (``n < 4`` or zero variance). ``skewness`` and ``kurtosis`` are
    the population moments (kurtosis non-excess, i.e. ``3`` for a normal).
    """
    n = len(xs)
    if n < 4:
        return 0.0
    mean = math.fsum(xs) / n
    m2 = math.fsum((x - mean) ** 2 for x in xs) / n
    if m2 <= 0.0:
        return 0.0
    m3 = math.fsum((x - mean) ** 3 for x in xs) / n
    m4 = math.fsum((x - mean) ** 4 for x in xs) / n
    skew = m3 / m2**1.5
    kurt = m4 / m2**2  # non-excess kurtosis (3.0 for a normal)
    return (skew**2 + 1.0) / kurt if kurt > 0.0 else 0.0


def magnitude_modality_distribution(
    values: Sequence[Any], *, min_sample: int = _MIN_SAMPLE
) -> dict[str, float]:
    """How strongly the value MAGNITUDES imply mixed scales — via log-bimodality.

    Bimodal ``log10|v|`` (two decade clusters) reads as ``mixed``; a single cluster
    as ``consistent``. P(mixed) is the bimodality coefficient itself (clamped): it
    crosses 0.5 exactly as BC passes its uniform reference, so there is no tuned
    threshold. Abstains (``0.5``) below ``min_sample`` non-zero numeric values.
    """
    logs = [
        math.log10(abs(float(v)))
        for v in values
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v not in (0, None)
    ]
    if len(logs) < min_sample:
        return _distribution(0.5)
    return _distribution(bimodality_coefficient(logs))


def declared_unit_distribution(unit_confidence: float | None) -> dict[str, float]:
    """The column's CLAIM to a single unit — strength = the declared confidence.

    A declared unit asserts consistency; its absence abstains (``0.5``). The conflict
    is born when this insists ``consistent`` but magnitude reads ``mixed`` — a column
    confidently labelled one unit whose values span scales.
    """
    if unit_confidence is None or unit_confidence <= 0.0:
        return _distribution(0.5)
    return _distribution(0.5 - 0.5 * min(1.0, unit_confidence))


@dataclass(frozen=True)
class UnitAdjudication:
    """The pooled verdict for a column's unit consistency + the witnesses behind it."""

    witnesses: tuple[Witness, ...]
    result: PoolResult


def measure_unit_consistency(
    values: Sequence[Any],
    unit_confidence: float | None,
    *,
    reliabilities: Mapping[str, float] | None = None,
) -> UnitAdjudication:
    """Adjudicate one numeric column into ``(C, U)`` + posterior over {consistent, mixed}.

    Args:
        values: the column's numeric cells (non-numeric / empty are ignored).
        unit_confidence: the declared-unit confidence (Pint/LLM); ``None`` → the
            declared-unit witness abstains.
        reliabilities: per-witness overrides; defaults to :data:`DEFAULT_RELIABILITIES`.
    """
    rel = reliabilities or DEFAULT_RELIABILITIES
    witnesses = (
        _witness(
            "magnitude_modality",
            magnitude_modality_distribution(values),
            rel["magnitude_modality"],
        ),
        _witness(
            "declared_unit", declared_unit_distribution(unit_confidence), rel["declared_unit"]
        ),
    )
    return UnitAdjudication(witnesses=witnesses, result=pool(witnesses))
