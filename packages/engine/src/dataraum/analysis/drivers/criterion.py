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
import pandas as pd

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
    values: np.ndarray,
    measure: np.ndarray,
    *,
    handle_nulls: bool,
    missingness_gate: float = DEFAULT_MISSINGNESS_GATE,
) -> tuple[np.ndarray, int]:
    """Encode a dimension's values as integer group codes, applying the null gates.

    Args:
        values: object array of the dimension's raw values (``None``/NaN = missing).
        measure: float array of the measure, row-aligned with ``values`` (NaN = missing).
        handle_nulls: when True, apply (A) dim-present + (B) missingness gates; when
            False, the ablation baseline — dim-NULL becomes its own ``"__NULL__"``
            category and no missingness gate runs (this is what LEAKS, by design).
        missingness_gate: the (B) threshold (fraction of the dim-present baseline).

    Returns:
        ``(codes, n_codes)`` — ``codes[i]`` is the group of row ``i`` or
        ``_DIM_NULL_CODE`` (-1) if the row is gated out; ``n_codes`` is the number
        of retained groups (the max code + 1).
    """
    dim_null = pd.isna(values)
    measure_observed = ~np.isnan(measure)
    codes = np.full(len(values), _DIM_NULL_CODE, dtype=int)

    if not handle_nulls:
        # Ablation: dim-null is its own category, no missingness gate.
        labelled = np.where(dim_null, "__NULL__", values)
        uniq = pd.unique(labelled)
        for i, label in enumerate(uniq):
            codes[labelled == label] = i
        return codes, len(uniq)

    # (A) dim-present: only rows that carry a slice label participate.
    present = ~dim_null
    baseline = measure_observed[present].mean() if present.any() else 0.0
    next_code = 0
    for label in pd.unique(values[present]):
        in_slice = present & (values == label)
        rate = measure_observed[in_slice].mean() if in_slice.any() else 0.0
        # (B) missingness-concentration: drop a slice where the measure is
        # disproportionately missing — its aggregate would be silently biased.
        if rate < missingness_gate * baseline:
            continue
        codes[in_slice] = next_code
        next_code += 1
    return codes, next_code


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
