"""The generic opinion-pooling engine (ADR-0009, DAT-457).

Pure functions — no DB, no LLM, no config. Given a set of :class:`Witness`
opinions over one shared canonical claim space, compute:

* **posterior** ``q`` — a *log-linear* pool ``q ∝ Π pᵢ^{wᵢ}`` with ``wᵢ`` the
  witness reliability. This is the actionable belief (what the resolved layer
  reads), and it is the pool whose mode shifts with the weights.
* **conflict** ``C`` — a generalized Jensen–Shannon divergence between the
  witness distributions, normalized by the entropy of the weight distribution
  so it reads as *the fraction of the maximum disagreement these witnesses
  could express*. ``C`` is weight-robust at full disagreement: two disjoint
  one-hot witnesses give ``C = 1`` regardless of their relative reliabilities
  (only the *resolution* depends on weights, not the *flagging*).
* **ignorance** ``U`` — evidence thinness ``κ / (κ + m)`` where the effective
  informative mass ``m = Σ rᵢ · certaintyᵢ`` discounts each witness both by its
  reliability and by how sharp its distribution is (a uniform witness, however
  reliable, adds no certainty). ``U`` is high when nobody informative has
  weighed in; it is the "collect more evidence" signal, distinct from ``C``.

The JSD is computed on the *linear* mixture (that is where the identity
``H(mixture) = Σ wᵢ H(pᵢ) + JSD`` lives); the posterior is the *log-linear*
pool. Two pools, two jobs — by design.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from dataraum.entropy.pooling.models import PoolResult, Witness

_EPS = 1e-12
# Smoothing floor applied to probabilities before log-linear pooling, so a
# witness assigning exactly 0 to a claim cannot veto it to -inf. Small enough
# to be negligible against any real opinion.
_LOG_FLOOR = 1e-9


def _normalize(probs: Sequence[float]) -> list[float]:
    """Clamp negatives to 0 and scale to sum 1. Raises on zero/negative mass."""
    clamped = [x if x > 0.0 else 0.0 for x in probs]
    total = math.fsum(clamped)
    if total <= 0.0:
        raise ValueError("distribution must have positive probability mass")
    return [x / total for x in clamped]


def shannon_entropy(probs: Sequence[float], *, base: float = 2.0) -> float:
    """Shannon entropy of a probability distribution, in ``base`` units.

    Assumes ``probs`` is already normalized; ignores zero-probability outcomes.
    """
    h = 0.0
    for x in probs:
        if x > 0.0:
            h -= x * math.log(x, base)
    return h


def _linear_mixture(dists: Sequence[Sequence[float]], weights: Sequence[float]) -> list[float]:
    """Weighted linear mixture ``Σ wᵢ pᵢ`` (``weights`` must sum to 1)."""
    k = len(dists[0])
    mixture = [0.0] * k
    for w, dist in zip(weights, dists, strict=True):
        for i in range(k):
            mixture[i] += w * dist[i]
    return mixture


def jensen_shannon_divergence(
    dists: Sequence[Sequence[float]],
    weights: Sequence[float],
    *,
    base: float = 2.0,
) -> float:
    """Generalized JSD ``= H(Σ wᵢ pᵢ) - Σ wᵢ H(pᵢ)``.

    ``weights`` are the (normalized) mixing weights. Result is in ``base`` units
    and is non-negative (clamped against float error).
    """
    mixture = _linear_mixture(dists, weights)
    h_mixture = shannon_entropy(mixture, base=base)
    h_conditional = math.fsum(
        w * shannon_entropy(dist, base=base) for w, dist in zip(weights, dists, strict=True)
    )
    return max(0.0, h_mixture - h_conditional)


def log_linear_pool(
    dists: Sequence[Sequence[float]],
    weights: Sequence[float],
    *,
    floor: float = _LOG_FLOOR,
) -> list[float]:
    """Log-linear (logarithmic) opinion pool ``q ∝ Π pᵢ^{wᵢ}``.

    Computed in log space with a smoothing ``floor`` and a max-subtraction for
    numerical stability. ``weights`` need not be normalized (a global scale only
    sharpens/flattens ``q``; here we pass raw reliabilities).
    """
    k = len(dists[0])
    log_q = [0.0] * k
    for w, dist in zip(weights, dists, strict=True):
        for i in range(k):
            log_q[i] += w * math.log(dist[i] if dist[i] > floor else floor)
    offset = max(log_q)
    return _normalize([math.exp(lq - offset) for lq in log_q])


def pool(witnesses: Sequence[Witness], *, prior_strength: float = 1.0) -> PoolResult:
    """Pool ``witnesses`` into a posterior and the ``(conflict, ignorance)`` split.

    Args:
        witnesses: Opinions over one shared claim space. May be empty.
        prior_strength: The ``κ`` in ``U = κ / (κ + m)`` — the pseudo-count mass
            of the "we know nothing" prior. Larger ``κ`` means more corroborating
            evidence is needed before ignorance falls. Documented knob, not tuned
            against a metric (per ADR-0009).

    Returns:
        A :class:`PoolResult`. With no witnesses: empty posterior, ``C = 0``,
        ``U = 1`` (total ignorance).
    """
    if not witnesses:
        return PoolResult(
            posterior=(),
            conflict=0.0,
            ignorance=1.0,
            n_witnesses=0,
            evidence_mass=0.0,
        )

    k = len(witnesses[0].distribution)
    if k == 0:
        raise ValueError("claim space must be non-empty")
    if any(len(w.distribution) != k for w in witnesses):
        raise ValueError("all witnesses must share the same claim-space size")

    dists = [_normalize(w.distribution) for w in witnesses]
    rels = [min(1.0, max(0.0, w.reliability)) for w in witnesses]
    rel_total = math.fsum(rels)

    # --- posterior q: log-linear pool, reliability as exponent ---
    if rel_total <= _EPS:
        posterior = [1.0 / k] * k  # nobody is trusted → fall back to uniform
    else:
        posterior = log_linear_pool(dists, rels)

    # --- conflict C: weighted JSD normalized by the weight entropy ---
    if rel_total <= _EPS or len(witnesses) == 1:
        conflict = 0.0
    else:
        mix_weights = [r / rel_total for r in rels]
        jsd = jensen_shannon_divergence(dists, mix_weights)
        weight_entropy = shannon_entropy(mix_weights)
        conflict = jsd / weight_entropy if weight_entropy > _EPS else 0.0
    conflict = min(1.0, max(0.0, conflict))

    # --- ignorance U: thinness of informative evidence ---
    log2_k = math.log2(k) if k > 1 else 1.0
    evidence_mass = 0.0
    for rel, dist in zip(rels, dists, strict=True):
        certainty = 1.0 - shannon_entropy(dist) / log2_k
        evidence_mass += rel * max(0.0, certainty)
    ignorance = prior_strength / (prior_strength + evidence_mass)

    return PoolResult(
        posterior=tuple(posterior),
        conflict=conflict,
        ignorance=ignorance,
        n_witnesses=len(witnesses),
        evidence_mass=evidence_mass,
    )
