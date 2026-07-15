"""Structural reconciliation — the grounded stock/flow discriminator (DAT-491).

Ported from the DAT-459 redirect probe (eval ``dat459_structural_reconciliation``),
which grounded this statistic before any engine code was written. For a
period-keyed measure series ``y[1..T]`` (per entity) and the INDEPENDENT
per-period net movement ``m[1..T]`` aggregated from event rows (NOT from ``y``):

    FLOW hypothesis : y[t]  ≈ m[t]          (the column IS the period's movement)
    STOCK hypothesis: Δy[t] ≈ m[t]  (t≥2)   (the column carries forward)

Scale-free residuals — no tuning, no boost curve:

    R_flow  = Σ|y[t] − m[t]| / Σ|m[t]|
    R_stock = Σ|Δy[t] − m[t][1:]| / Σ|m[t][1:]|

Classify STOCK iff ``R_stock < R_flow``. This is robust exactly where the
falsified persistence statistic (rho1/VR) broke: a trending/seasonal flow still
equals its movement (R_flow≈0) and a mean-reverting stock still carries forward
(R_stock≈0).

The ABSTAIN gate is the probe's wrong-anchor guardrail: with a misaligned anchor
(wrong entity, wrong join, wrong period bridge) BOTH residuals stay large —
measured median min-residual ≈ 1.0 vs ≈ 0.0–0.1 for a correct anchor, holding
through reconciliation noise up to ~0.25–0.5 of the movement scale. An entity
therefore only VOTES when its winning residual is ≤ ``FIRE_RESIDUAL_MAX``; a
candidate only fires when enough entities vote and they agree. These constants
are separation-derived from the probe (provenance above), not fitted to a metric.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from statistics import median

from dataraum.analysis.lineage.models import (
    PATTERN_CUMULATIVE,
    PATTERN_PER_PERIOD,
    CandidateDisposal,
)

# Probe scope: residuals on fewer than 4 periods are not meaningful (DAT-459 fixture
# filter ``T >= 4``); shorter entity series abstain.
MIN_PERIODS = 4

# Wrong-anchor guardrail (see module docstring): correct-anchor winning residual
# ≈ 0.0–0.1 (≤ ~0.5 under heavy reconciliation noise); wrong-anchor min-residual
# ≈ 1.0. The gate sits at the measured separation midpoint.
FIRE_RESIDUAL_MAX = 0.5

# A candidate's verdict needs at least this many voting entities and this much
# agreement among them — a lone entity or a split vote is ignorance, not lineage.
MIN_ENTITIES_FIRED = 2
AGREEMENT_MIN = 0.8


@dataclass(frozen=True)
class EntityReconciliation:
    """One entity's residual pair + its vote (``None`` = abstained)."""

    r_flow: float
    r_stock: float
    label: str | None  # PATTERN_PER_PERIOD / PATTERN_CUMULATIVE / None


def reconcile(y: Sequence[float], m: Sequence[float]) -> tuple[float, float]:
    """Return ``(R_flow, R_stock)`` for one entity's series against its anchor.

    A residual is ``inf`` when its hypothesis' normalizer is zero (a dead
    anchor for flow; a dead anchor TAIL for stock): the residual is scale-free
    by construction, and silently degrading to an absolute scale can mint a
    spurious perfect fit — the hypothesis abstains instead.
    """
    if len(y) != len(m):
        raise ValueError(f"series/anchor length mismatch: {len(y)} != {len(m)}")
    denom_flow = sum(abs(v) for v in m)
    r_flow = (
        sum(abs(yv - mv) for yv, mv in zip(y, m, strict=True)) / denom_flow
        if denom_flow
        else float("inf")
    )
    dy = [y[t] - y[t - 1] for t in range(1, len(y))]
    m_tail = list(m[1:])
    denom_stock = sum(abs(v) for v in m_tail)
    r_stock = (
        sum(abs(dv - mv) for dv, mv in zip(dy, m_tail, strict=True)) / denom_stock
        if denom_stock
        else float("inf")
    )
    return r_flow, r_stock


def classify_entity(y: Sequence[float], m: Sequence[float]) -> EntityReconciliation:
    """Classify one entity, abstaining on short/dead series, dead anchors, or bad fits.

    A dead MEASURE (identically zero) abstains symmetrically with the dead
    anchor: a series that never moves has no stock/flow nature to detect.
    """
    if len(y) < MIN_PERIODS or not any(m) or not any(y):
        return EntityReconciliation(r_flow=float("inf"), r_stock=float("inf"), label=None)
    r_flow, r_stock = reconcile(y, m)
    if min(r_flow, r_stock) > FIRE_RESIDUAL_MAX:
        # Wrong-anchor guardrail: neither hypothesis fits — abstain, never guess.
        return EntityReconciliation(r_flow=r_flow, r_stock=r_stock, label=None)
    label = PATTERN_CUMULATIVE if r_stock < r_flow else PATTERN_PER_PERIOD
    return EntityReconciliation(r_flow=r_flow, r_stock=r_stock, label=label)


def classify_series(
    series: Mapping[str, tuple[Sequence[float], Sequence[float]]],
) -> dict[str, EntityReconciliation]:
    """Classify every entity's aligned ``(y, m)`` series (DAT-759 split).

    Exposed separately from :func:`dispose` so the selection layer can read the
    per-entity residuals (support counting, ΔBIC arity tie-break) without
    re-running the arithmetic.
    """
    return {k: classify_entity(y, m) for k, (y, m) in series.items()}


def wilson_lcb(successes: int, n: int, z: float = 1.96) -> float:
    """Wilson score interval lower bound for a ``successes / n`` rate (DAT-759).

    The support statistic for convention selection: because the reconciliation
    residual carries no fitted per-entity coefficient, leave-one-entity-out CV
    degenerates to the vote count — the vote RATE is an out-of-sample
    generalization estimate, and its Wilson lower bound (Wilson 1927) is the
    parameter-free way to rank it under small n. ``n`` MUST be the common
    entity denominator of the pairing, never a convention's own aligned subset
    (the support-gameability trap — DAT-759 probe leg b2).
    """
    if n <= 0:
        return 0.0
    p = successes / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, (centre - margin) / denom)


def dispose(
    series: Mapping[str, tuple[Sequence[float], Sequence[float]]],
) -> CandidateDisposal | None:
    """Aggregate per-entity votes into a candidate verdict; ``None`` = no lineage.

    Args:
        series: entity key → ``(y, m)`` aligned period series (measure, anchor).

    Returns:
        A :class:`CandidateDisposal` when enough entities vote and agree
        (``match_rate`` = voting fraction × agreement), else ``None`` — the
        candidate did not reconcile and the witness must abstain.
    """
    return dispose_classified(classify_series(series))


def dispose_classified(
    results: Mapping[str, EntityReconciliation],
) -> CandidateDisposal | None:
    """:func:`dispose` over pre-classified entities (see :func:`classify_series`)."""
    if not results:
        return None
    voted = [r for r in results.values() if r.label is not None]
    if len(voted) < MIN_ENTITIES_FIRED:
        return None
    counts = {
        PATTERN_PER_PERIOD: sum(1 for r in voted if r.label == PATTERN_PER_PERIOD),
        PATTERN_CUMULATIVE: sum(1 for r in voted if r.label == PATTERN_CUMULATIVE),
    }
    pattern = max(counts, key=lambda p: counts[p])
    agreement = counts[pattern] / len(voted)
    if agreement < AGREEMENT_MIN:
        return None  # split vote — ambiguous lineage is ignorance, not a verdict
    # Medians over the WINNING-label voters only — a dissenting minority's
    # residuals would contaminate the diagnostics the witness later surfaces.
    winners = [r for r in voted if r.label == pattern]
    return CandidateDisposal(
        pattern=pattern,
        match_rate=(len(voted) / len(results)) * agreement,
        r_flow_median=median(r.r_flow for r in winners),
        r_stock_median=median(r.r_stock for r in winners),
        n_entities=len(results),
        n_entities_fired=len(voted),
    )
