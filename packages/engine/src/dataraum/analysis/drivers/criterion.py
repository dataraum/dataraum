"""The split criterion + null-handling gates (DAT-545, ported from the DAT-544 spike).

Two numpy primitives the tree is built from:

- :func:`build_codes` turns a dimension's raw values into integer group codes,
  applying the two null gates the spike proved load-bearing:
    **(A) dim-present** — rows whose dimension value is NULL get code ``-1`` and are
    dropped from the gain (a column's own missingness is never read as a phantom
    "null slice").
    **(B) missingness-concentration** — a slice value where the MEASURE is
    disproportionately missing (observed-rate < ``missingness_gate`` × the
    dim-present baseline) is dropped too. Min-support alone leaked
    measure-conditional missingness in the spike; this gate (the
    ``slice_conditional_null`` contingency, reimplemented inline — NOT the
    per-column entropy detector) closed it.

- :func:`variance_reduction` is the FLOW gain: the fraction of the measure's
  variance explained by the grouping, over groups that clear ``min_support``.
  ``stock`` measures use the same row-grain reduction (additivity-respecting
  because it never sums); ``ratio`` has its own support-weighted gain (DAT-545 P4).

The magnitude is never compared to a global threshold — the tree RANKS by it and
gates significance with a within-dataset permutation null (see ``tree.py``).
"""

from __future__ import annotations

import numpy as np

# A group must clear this row count to enter the variance computation — both the
# determinant subset and each retained group. The spike calibrated it against
# ~20k-row datasets; the engine passes it through so it can scale with the data.
DEFAULT_MIN_SUPPORT = 200

# A slice value is dropped (B-gate) when its measure-observed rate falls below this
# fraction of the dim-present baseline — i.e. the measure goes disproportionately
# missing inside that slice, which would otherwise manufacture a false driver.
DEFAULT_MISSINGNESS_GATE = 0.5

_DIM_NULL_CODE = -1


def build_codes(
    phys: np.ndarray,
    measure: np.ndarray,
    *,
    handle_nulls: bool,
    missingness_gate: float = DEFAULT_MISSINGNESS_GATE,
) -> tuple[np.ndarray, int]:
    """Encode a dimension's PHYSICAL codes as gated group codes, applying the null gates.

    The dimension arrives pre-factorized to physical integer codes (DAT-580): the
    arrow→polars load assigns each distinct value a code ``0..k-1`` and ``-1`` to NULL,
    so the (A)/(B) gates are vectorized over integers — no per-label Python scan, and no
    string objects resident through the permutation null.

    Args:
        phys: int array of the dimension's physical category codes; ``-1`` = dim-null.
        measure: float array of the measure, row-aligned with ``phys`` (NaN = missing).
        handle_nulls: when True, apply (A) dim-present + (B) missingness gates; when
            False, the ablation baseline — dim-NULL becomes its own category and no
            missingness gate runs (this is what LEAKS, by design).
        missingness_gate: the (B) threshold (fraction of the dim-present baseline).

    Returns:
        ``(codes, n_codes)`` — ``codes[i]`` is the contiguous group of row ``i`` or
        ``_DIM_NULL_CODE`` (-1) if the row is gated out; ``n_codes`` is the number of
        retained groups. Code numbering is in physical order; gains are group-set
        invariant, so the numbering never matters (labels resolve via ``tree._code_labels``).
    """
    present = phys >= 0
    n_phys = int(phys[present].max()) + 1 if present.any() else 0
    measure_observed = ~np.isnan(measure)

    if not handle_nulls:
        # Ablation: dim-null is its own category (code n_phys), no missingness gate.
        has_null = bool((~present).any())
        codes = np.where(present, phys, n_phys)
        return codes, n_phys + (1 if has_null else 0)

    if n_phys == 0:
        # Every row is dim-null (e.g. an entity-constant dim null for all kept entities):
        # all rows gated out, no groups — matches the old pd.unique-of-empty path and keeps
        # the bincount/remap below (which assume ≥1 physical code) from indexing empty.
        return np.full(len(phys), _DIM_NULL_CODE, dtype=int), 0

    # (A) dim-present: only rows with a slice label participate (null → -1, below).
    baseline = measure_observed[present].mean() if present.any() else 0.0
    total = np.bincount(phys[present], minlength=n_phys)
    observed = np.bincount(phys[present & measure_observed], minlength=n_phys)
    with np.errstate(invalid="ignore", divide="ignore"):
        rate = np.where(total > 0, observed / np.maximum(total, 1), 0.0)
    # (B) missingness-concentration: drop a slice where the measure is disproportionately
    # missing — its aggregate would be silently biased. Surviving codes are renumbered
    # contiguously (bincount needs 0..n_codes-1); dropped + null rows go to -1.
    kept = (total > 0) & (rate >= missingness_gate * baseline)
    remap = np.full(n_phys, _DIM_NULL_CODE, dtype=int)
    remap[kept] = np.arange(int(kept.sum()))
    codes = np.where(present, remap[np.where(present, phys, 0)], _DIM_NULL_CODE)
    return codes, int(kept.sum())


def variance_reduction(
    codes: np.ndarray,
    n_codes: int,
    measure: np.ndarray,
    *,
    min_support: int = DEFAULT_MIN_SUPPORT,
) -> float:
    """Fraction of the measure's variance explained by the grouping (flow gain).

    Computed over rows with a retained code (``>= 0``) and an observed measure,
    restricted to groups that clear ``min_support`` (small groups are dropped from
    BOTH the total and within terms, so a long tail of tiny slices can't inflate
    the reduction). Returns ``0.0`` when there is too little support or no variance
    to explain. Range ``[0, 1]``; higher = more explanatory. The value is only ever
    RANKED and permutation-gated — never thresholded absolutely.
    """
    observed = ~np.isnan(measure)
    keep = (codes >= 0) & observed
    if int(keep.sum()) < min_support:
        return 0.0
    c, y = codes[keep], measure[keep]
    counts = np.bincount(c, minlength=n_codes)
    sums = np.bincount(c, weights=y, minlength=n_codes)
    sq_sums = np.bincount(c, weights=y * y, minlength=n_codes)

    big = counts >= min_support
    if int(big.sum()) < 2:
        return 0.0  # need ≥2 supported groups to speak of between-group variance
    n_big, sum_big, sq_big = counts[big], sums[big], sq_sums[big]
    total_n = n_big.sum()
    grand_mean = sum_big.sum() / total_n
    total_var = sq_big.sum() / total_n - grand_mean**2
    if total_var <= 0:
        return 0.0
    within_var = (sq_big.sum() - np.sum(sum_big * sum_big / n_big)) / total_n
    return max(0.0, float((total_var - within_var) / total_var))


def intraclass_correlation(
    entity_codes: np.ndarray,
    n_entities: int,
    measure: np.ndarray,
    *,
    min_entity_rows: int = 2,
) -> float:
    """ICC of the measure within an entity = η² of the measure BY the entity.

    The fraction of the measure's variance that sits BETWEEN entities — exactly
    :func:`variance_reduction` with the entity as the grouping (DAT-544 E2). It
    decides whether the row-wise permutation null is valid: rows are exchangeable
    only when this is ≈0; a high ICC (per-entity-level measure) means the cluster,
    not the row, is the exchangeable unit (DAT-552). Uses a small ``min_entity_rows``
    (not the driver ``min_support``) so ordinary-sized entities still count — an
    entity needs ≥2 rows to carry within-variance.
    """
    return variance_reduction(entity_codes, n_entities, measure, min_support=min_entity_rows)


def weighted_variance_reduction(
    codes: np.ndarray,
    n_codes: int,
    ratio: np.ndarray,
    weight: np.ndarray,
    *,
    min_support: int = DEFAULT_MIN_SUPPORT,
) -> float:
    """Support-weighted variance reduction for a RATIO measure (DAT-545 P4).

    A ratio ``R = Σnum / Σden`` is the weight-``den`` mean of the per-row ratios
    ``r = num/den`` — so the explained fraction is a *weighted* variance reduction of
    ``r`` with weights ``w = den`` (averaging raw per-row ratios would weight a
    ₂-row group like a ₂-million-row one; Simpson's paradox). Rows with a
    non-positive or missing denominator carry no ratio and are dropped. Same shape as
    :func:`variance_reduction` — groups clear ``min_support`` by ROW count, the result
    is ``[0, 1]`` and only ever ranked / permutation-gated.
    """
    valid = (codes >= 0) & ~np.isnan(ratio) & (weight > 0)
    if int(valid.sum()) < min_support:
        return 0.0
    c, r, w = codes[valid], ratio[valid], weight[valid]
    counts = np.bincount(c, minlength=n_codes)
    w_sum = np.bincount(c, weights=w, minlength=n_codes)
    wr = np.bincount(c, weights=w * r, minlength=n_codes)
    wrr = np.bincount(c, weights=w * r * r, minlength=n_codes)

    big = counts >= min_support
    if int(big.sum()) < 2:
        return 0.0
    w_big, wr_big, wrr_big = w_sum[big], wr[big], wrr[big]
    total_w = w_big.sum()
    if total_w <= 0:
        return 0.0
    grand = wr_big.sum() / total_w
    total_var = wrr_big.sum() / total_w - grand**2
    if total_var <= 0:
        return 0.0
    within_var = (wrr_big.sum() - np.sum(wr_big * wr_big / w_big)) / total_w
    return max(0.0, float((total_var - within_var) / total_var))
