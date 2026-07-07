"""Surprise — ``D_KL(observed ‖ reference)`` as a sample-size-invariant score.

The SECOND of the two entropy kinds (docs/architecture/entropy.md). The pooling engine
(:mod:`dataraum.entropy.pooling`) measures ADJUDICATION entropy — witnesses
disagree about a *claim*, yielding conflict ``C``. A statistical detector measures
SURPRISE — an observed distribution sits far from the reference the data is
*expected* to follow (Benford's leading-digit law, a uniform null share, a
historical baseline). That distance is the Kullback–Leibler divergence

    D_KL(observed ‖ reference) = Σ_i observed[i] · log2( observed[i] / reference[i] )   [bits]

the extra bits per observation needed to encode the data under the reference —
exactly "how surprised should we be". Unlike chi-square, KL is INTENSIVE (a
per-observation average), so it does NOT grow with sample size: 8000 rows that
follow Benford score ~0, the same as 100 do. This is why the surprise path needs
no Cramér's-V / effect-size correction — those boost curves existed only to undo
chi-square's n-inflation, under which a clean column at n=8000 *fails* a chi-square
compliance test (the test gains power to reject any deviation) and lands at the
non-compliant floor, while its KL stays ~0.

The score squashes KL with the parameter-free

    surprise = 1 - 2^{-D_KL}  ∈ [0, 1)

— one minus the typicality ``2^{-D_KL}`` (the per-observation likelihood the
reference assigns relative to the observed). 0 bits → 0, 1 bit → 0.5, ~3.3 bits
(a 10× surprise) → 0.9. No hand-tuned knob; per-intent severity lives in the loss
table (:mod:`dataraum.entropy.loss`), never here.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

# A reference probability below this floor is lifted to it, so an observed outcome
# the reference deems (near-)impossible yields large-but-finite surprise rather
# than +inf. Mirrors the pooling engine's log floor.
_REF_FLOOR = 1e-9


def _normalize(probs: Sequence[float]) -> list[float]:
    """Clamp negatives to 0 and scale to sum 1. Raises on zero/negative mass."""
    clamped = [x if x > 0.0 else 0.0 for x in probs]
    total = math.fsum(clamped)
    if total <= 0.0:
        raise ValueError("distribution must have positive probability mass")
    return [x / total for x in clamped]


def kl_divergence(
    observed: Sequence[float],
    reference: Sequence[float],
    *,
    base: float = 2.0,
) -> float:
    """``D_KL(observed ‖ reference)`` in ``base`` units (bits by default).

    Both arguments are normalized to sum 1 first, so counts and proportions give
    the same result (the score is sample-size-invariant). Outcomes with
    ``observed[i] == 0`` contribute 0 (the ``x·log x → 0`` limit); a
    ``reference[i]`` below :data:`_REF_FLOOR` is lifted to it so a real-but-rare
    outcome scores high-finite, not infinite. The result is clamped at 0 (KL ≥ 0;
    only float error can push it slightly negative).
    """
    if len(observed) != len(reference):
        raise ValueError("observed and reference must share length")
    p = _normalize(observed)
    q = _normalize(reference)
    div = 0.0
    for pi, qi in zip(p, q, strict=True):
        if pi > 0.0:
            div += pi * math.log(pi / max(qi, _REF_FLOOR), base)
    return max(0.0, div)


def surprise_score(
    observed: Sequence[float],
    reference: Sequence[float],
    *,
    base: float = 2.0,
) -> float:
    """Entropy score ``1 - base^{-D_KL}`` ∈ [0, 1) for observed vs reference.

    A parameter-free squash of the KL surprise: 0 when the data matches the
    reference, → 1 as it departs. Severity per intent is applied downstream by the
    loss table, never here (per docs/architecture/entropy.md — severity lives in the loss, not the
    score).
    """
    div = kl_divergence(observed, reference, base=base)
    return 1.0 - math.pow(base, -div)
