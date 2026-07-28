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

Classify STOCK iff ``R_stock`` beats ``R_flow`` BY A MARGIN. This is robust
exactly where the falsified persistence statistic (rho1/VR) broke: a
trending/seasonal flow still equals its movement (R_flow≈0) and a mean-reverting
stock still carries forward (R_stock≈0).

Two ABSTAIN gates, both refusing to convert ignorance into a verdict:

- *Wrong anchor* — the probe's guardrail: with a misaligned anchor (wrong
  entity, wrong join, wrong period bridge) BOTH residuals stay large — measured
  median min-residual ≈ 1.0 vs ≈ 0.0–0.1 for a correct anchor, holding through
  reconciliation noise up to ~0.25–0.5 of the movement scale. An entity only
  VOTES when its winning residual is ≤ ``FIRE_RESIDUAL_MAX``.
- *Near tie* — a fit that passes the first gate can still beat its rival by
  nothing at all; a bare ``<`` would then mint a verdict out of the last
  significant digit. The winner must lead by ``MIN_SEPARATION`` on the
  scale-free separation index (see :func:`separation`).

A candidate then only fires when enough entities vote and they agree. These
constants are separation-derived from the probe (provenance above), not fitted
to a metric.
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

# Near-tie margin (DAT-847) — DERIVED from the two levels the discriminator is
# already built on. It introduces no NEW fitted constant: it inherits
# ``FIRE_RESIDUAL_MAX``'s probe-measured provenance (module docstring) and adds
# no number of its own.
#
# Under a CORRECT anchor the false hypothesis sits at R ≈ 1: a perfect flow
# (y == m) gives R_stock = Σ|m[t-1]| / Σ|m[t]|, and a perfect stock gives R_flow
# the mirror ratio — both ≈ 1 for a STATIONARY movement scale, the same ≈ 1.0
# level the wrong-anchor guardrail is calibrated against. Under a strongly
# drifting scale the ratio sags (over 12 periods: m[t] = t → 0.86, m[t] = 2^t
# → 0.50, against exactly 1.00 for a constant scale), which
# shrinks the observed separation and so errs toward ABSTENTION — the safe
# direction. The true hypothesis sits at the reconciliation noise, which
# ``FIRE_RESIDUAL_MAX`` already declares admissible out to 0.5. So the WEAKEST
# separation this module still accepts as a fit is ``FIRE_RESIDUAL_MAX`` against
# 1; a pair closer than that is inside the band the module itself calls
# non-discriminating, and picking a side there reports the noise. On the
# separation index (see :func:`separation`) that level is:
MIN_SEPARATION = (1.0 - FIRE_RESIDUAL_MAX) / (1.0 + FIRE_RESIDUAL_MAX)
"""Minimum lead the winning hypothesis must hold — 1/3, i.e. the loser's
residual must be at least twice the winner's, at any residual magnitude.

The two gates are COUPLED by this derivation: moving ``FIRE_RESIDUAL_MAX`` to
1.0 drives ``MIN_SEPARATION`` to 0 and silently disables the tie gate, while
moving it to 0.0 drives the margin to 1 and lets nothing fire. Retune the fit
gate only with that in view.
"""

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


def separation(r_flow: float, r_stock: float) -> float:
    """Scale-free contrast between the two hypotheses' residuals, in ``[0, 1]``.

    ``(R_lose - R_win) / (R_lose + R_win)`` — 1 when one hypothesis fits and the
    other does not, 0 when the two are indistinguishable. Symmetric and
    dimensionless, so it reads the same at any residual magnitude: it asks which
    hypothesis is RELATIVELY better, never how good either one is (that is
    ``FIRE_RESIDUAL_MAX``'s question).

    An infinite loser — a hypothesis whose normalizer died, see
    :func:`reconcile` — is total separation; an infinite winner means both died
    and there is nothing to compare. Two identically zero residuals are NO
    separation: that is a series whose whole movement is one terminal period,
    which fits flow and stock equally, and is evidence for neither.
    """
    r_win, r_lose = min(r_flow, r_stock), max(r_flow, r_stock)
    if math.isinf(r_win):
        return 0.0
    if math.isinf(r_lose):
        return 1.0
    total = r_win + r_lose
    return (r_lose - r_win) / total if total else 0.0


def classify_entity(y: Sequence[float], m: Sequence[float]) -> EntityReconciliation:
    """Classify one entity, abstaining on short/dead series, dead anchors, bad fits, or ties.

    A dead MEASURE (identically zero) abstains symmetrically with the dead
    anchor: a series that never moves has no stock/flow nature to detect.
    """
    if len(y) < MIN_PERIODS or not any(m) or not any(y):
        return EntityReconciliation(r_flow=float("inf"), r_stock=float("inf"), label=None)
    r_flow, r_stock = reconcile(y, m)
    if min(r_flow, r_stock) > FIRE_RESIDUAL_MAX:
        # Wrong-anchor guardrail: neither hypothesis fits — abstain, never guess.
        return EntityReconciliation(r_flow=r_flow, r_stock=r_stock, label=None)
    if separation(r_flow, r_stock) < MIN_SEPARATION:
        # Near-tie guardrail (DAT-847): one hypothesis fits, but not measurably
        # better than the other — the winner is the noise, not the structure.
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
