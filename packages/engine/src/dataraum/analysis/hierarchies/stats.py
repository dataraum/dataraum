"""Statistical measures for dimension-identity discovery (DAT-761, from DAT-757).

Every measure is a named, established method — proven against the DAT-757
adversarial matrix (32/32) and three RelBench databases folded by their own FK
metadata (100% recoverable-truth recall; verdicts on DAT-757):

- **row-g3** (Kivinen–Mannila): the minimum fraction of rows to delete for the
  FD ``a → b`` to hold exactly — the effect-size screen for edges. The
  distinct-count ratio it replaces is NOT row-g3 and mass-asserts on wide data.
- **FI** (fraction of information, Cavallo–Pittarelli): ``1 − H(b|a)/H(b)`` —
  the dependence statistic the permutation null is computed on.
- **permutation p-value** (add-one corrected, early-stop): where the observed
  FI sits in the shuffled-b null DISTRIBUTION. Replaces RFI's mean-centering
  (variance-blind, leaked skew×heavy-tail FPs) and the analytic χ²/G null
  (invalid on our sparse contingency tables).
- **Benjamini–Hochberg (1995)**: FDR q ≤ 0.05 over one view's effect-screened
  candidate family — what gets ASSERTED from discovery.
- **Goodman–Kruskal λ (1954)**: PRE over the majority baseline,
  ``λ = 1 − g3/minority_mass``. Kills the vacuous-skew class (≥98%-dominant
  dependents satisfy g3 ≤ 0.01 vacuously; neither perm-p nor RFI blocks them).
  Exact FDs have λ = 1, so true edges onto skewed flags survive.
- **disagreement-set role tests** (DAT-757 gate #2): a near-copy pair (A, B) is
  classified ROLE vs dirty-copy from the only rows carrying information — the
  disagreement set ``dis = 1{A≠B}``. T1: perm-p of dis vs a context column
  (membership systematicity — the role-specific signal). T2: dis vs B (value
  concentration — fires on roles AND value-concentrated dirt, so alone it only
  escalates). Bonferroni over the family. Wild-validated 9/9 on SAP SALT's
  role-playing customer FKs.

All functions are pure (numpy in, scalars out); the caller owns caching and the
seeded RNG (a fixed seed keeps the phase deterministic across redeliveries).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import polars as pl

# Permutation budget: p_min = 1/(reps+1) ≈ 3.3e-4 — resolvable under BH even for
# a single true hit in a family of ~100 candidates.
PERM_REPS = 2999
# Early-stop: once this many null exceedances are seen, no BH threshold at
# q ≤ 0.05 could ever reject — stop permuting (conservative, exact).
_EARLY_STOP_COUNT = 20
_PERM_BLOCK = 100


def codes_of(series: pl.Series) -> np.ndarray:
    """Dense integer codes for one column, NULL as its own category.

    Null-as-category is the row-stat policy (DAT-757 null lane): a null-coded
    binary like ``{1, NULL}`` carries real structure that SQL ``COUNT(DISTINCT)``
    is blind to. The sentinel is a non-colliding unicode NUL symbol.
    """
    import polars as pl  # noqa: PLC0415 — keep the module importable without polars typing

    return series.cast(pl.Utf8).fill_null("␀").rank("dense").cast(pl.Int64).to_numpy() - 1


def _grouped_sorted(a: np.ndarray, b: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """(pair_counts, a-group boundaries) over the (a, b) contingency, sorted by a."""
    packed = a.astype(np.int64) * (int(b.max()) + 1) + b
    uniq, counts = np.unique(packed, return_counts=True)
    a_of_pair = uniq // (int(b.max()) + 1)
    bounds = np.flatnonzero(np.diff(a_of_pair)) + 1
    return counts, bounds


def row_g3(a: np.ndarray, b: np.ndarray) -> float:
    """Classic g3(a → b): 1 − (Σ over a-groups of the majority-b count)/n."""
    counts, bounds = _grouped_sorted(a, b)
    kept = np.maximum.reduceat(counts, np.r_[0, bounds])
    return float(1.0 - kept.sum() / len(a))


def _entropy_from_counts(counts: np.ndarray) -> float:
    p = counts / counts.sum()
    return float(-(p * np.log(p)).sum())


def fi(a: np.ndarray, b: np.ndarray) -> float:
    """FI(a → b) = 1 − H(b|a)/H(b), in [0, 1]; 0 when H(b) = 0."""
    _, b_counts = np.unique(b, return_counts=True)
    h_b = _entropy_from_counts(b_counts)
    if h_b == 0.0:
        return 0.0
    counts, bounds = _grouped_sorted(a, b)
    n = len(a)
    group_tot = np.add.reduceat(counts, np.r_[0, bounds])
    with np.errstate(divide="ignore", invalid="ignore"):
        rep = np.repeat(group_tot, np.diff(np.r_[0, bounds, len(counts)]))
        p = counts / rep
        plogp = p * np.log(p)
    h_b_given_a = float(-(np.add.reduceat(plogp, np.r_[0, bounds]) * group_tot / n).sum())
    return 1.0 - h_b_given_a / h_b


def perm_pvalue(
    a: np.ndarray, b: np.ndarray, rng: np.random.Generator, reps: int = PERM_REPS
) -> float:
    """P_perm(FI(a, shuffled b) ≥ FI_obs), add-one corrected, early-stopping.

    Early-stops in blocks once the exceedance count is high enough that no BH
    threshold (q ≤ 0.05) could ever reject — the stopped p is an underestimate
    of the true p only on the never-rejected side, so the stop is conservative.
    """
    obs = fi(a, b)
    bc = b.copy()
    count = done = 0
    while done < reps:
        block = min(_PERM_BLOCK, reps - done)
        for _ in range(block):
            rng.shuffle(bc)
            if fi(a, bc) >= obs:
                count += 1
        done += block
        if count >= _EARLY_STOP_COUNT:
            break
    return (1 + count) / (1 + done)


def bh_reject[K](pvals: dict[K, float], m_family: int, q: float = 0.05) -> set[K]:
    """Benjamini–Hochberg over a family of size ``m_family``.

    Untested family members implicitly carry p = 1. Returns the keys whose
    hypotheses are rejected (i.e. the dependence is significant).
    """
    ranked = sorted(pvals.items(), key=lambda kv: kv[1])
    cut = 0
    for k, (_, p) in enumerate(ranked, start=1):
        if p <= q * k / m_family:
            cut = k
    return {key for key, _ in ranked[:cut]}


def minority_mass(codes: np.ndarray) -> float:
    """1 − (majority value share): the baseline error of always-predicting the mode."""
    _, counts = np.unique(codes, return_counts=True)
    return 1.0 - counts.max() / len(codes)


def gk_lambda(a: np.ndarray, b: np.ndarray) -> float:
    """Goodman–Kruskal λ(a → b) = 1 − g3(a → b)/minority_mass(b); 0 when b is constant."""
    m = minority_mass(b)
    return 1.0 - row_g3(a, b) / m if m > 0 else 0.0


class RoleVerdict(Enum):
    """Gate #2 outcome for one near-copy pair (see :func:`role_verdict`)."""

    ROLE = "role"  # membership-systematic — keep apart, assert as role pair
    VALUE_SYSTEMATIC = "value_systematic"  # role OR concentrated dirt — semantic lane
    ABSTAIN = "abstain"  # too few disagreements to decide (p-floor > α)
    DIRT = "dirt"  # disagreement is noise — merging as alias is safe


@dataclass(frozen=True)
class RoleResult:
    """The gate-#2 evidence for one near-copy pair."""

    verdict: RoleVerdict
    t1_p: float  # best (smallest) membership p across contexts
    t1_context: str | None  # the context column that produced t1_p
    t2_p: float  # value-concentration p (dis vs B)
    k_disagree: int
    alpha: float  # the Bonferroni-corrected threshold both tests were held to


# Below this many disagreement rows, the achievable permutation p-floor is
# α-sensitive (ties dominate: with k=2 the floor sits near 0.02, see the DAT-757
# gate-#2 probe) — any verdict would be a coin flip, so the honest outcome is
# ABSTAIN. The probe's measured power boundary starts at k ≈ 10 (n = 20k).
ROLE_K_FLOOR = 10


def role_verdict(
    dis: np.ndarray,
    contexts: dict[str, np.ndarray],
    b_codes: np.ndarray,
    rng: np.random.Generator,
    *,
    reps: int = PERM_REPS,
    alpha_family: float = 0.05,
    k_floor: int = ROLE_K_FLOOR,
) -> RoleResult:
    """Classify one near-copy pair from its disagreement set (DAT-757 gate #2).

    ``dis`` is ``1{A≠B}`` per row; ``contexts`` are the other candidate columns'
    codes. Bonferroni m = n_contexts + 1 (the T1 family plus T2). Fewer than
    ``k_floor`` disagreements → ABSTAIN (the p-floor is α-sensitive there — any
    decision would be a coin flip). T1 significant → ROLE; else T2 significant →
    VALUE_SYSTEMATIC (escalate, never merge on its own: real dirt is rarely
    marginal-random, so T2 alone cannot separate role from concentrated dirt);
    else if the permutation p-floor cannot reach α → ABSTAIN; else DIRT.
    """
    m = len(contexts) + 1
    alpha = alpha_family / m
    k = int(dis.sum())
    if k < k_floor:
        return RoleResult(
            verdict=RoleVerdict.ABSTAIN,
            t1_p=1.0,
            t1_context=None,
            t2_p=1.0,
            k_disagree=k,
            alpha=alpha,
        )
    t1_ctx: str | None = None
    t1_p = 1.0
    for name, ctx in contexts.items():
        p = perm_pvalue(dis, ctx, rng, reps=reps)
        if p < t1_p:
            t1_ctx, t1_p = name, p
    t2_p = perm_pvalue(dis, b_codes, rng, reps=reps)
    floor = 1.0 / (1.0 + reps)
    if t1_p <= alpha:
        verdict = RoleVerdict.ROLE
    elif t2_p <= alpha:
        verdict = RoleVerdict.VALUE_SYSTEMATIC
    elif floor > alpha:
        verdict = RoleVerdict.ABSTAIN
    else:
        verdict = RoleVerdict.DIRT
    return RoleResult(
        verdict=verdict,
        t1_p=t1_p,
        t1_context=t1_ctx,
        t2_p=t2_p,
        k_disagree=int(dis.sum()),
        alpha=alpha,
    )
