"""DAT-545 — ratio (support-weighted) + stock (additivity-respecting) targets.

Ratio is the genuinely new criterion (the spike validated flow only): the group
statistic is Σnum/Σden, support-weighted by the denominator, never the mean of
per-row ratios. Stock reuses the row-grain variance reduction — additivity-respecting
*because* it never sums — so the test confirms reuse + the target-type label.
"""

from __future__ import annotations

import numpy as np

from dataraum.analysis.drivers.criterion import build_codes, weighted_variance_reduction
from dataraum.analysis.drivers.targets import FlowTarget, RatioTarget
from dataraum.analysis.drivers.tree import discover_tree

from .conftest import (
    ALL_DIMS,
    RATIO_DIMS,
    RATIO_NULLS,
    columns,
    factorize_columns,
    make_corpus,
    make_ratio_corpus,
)

ALPHA = 0.05
FDR_BAR = 2 * ALPHA
N_PERM = 200


class TestWeightedCriterion:
    def test_support_weighting_beats_naive_mean(self) -> None:
        # Two groups, same per-row ratio spread, but group B carries 10× the
        # denominator mass. The weighted reduction reflects the pooled ratio, not an
        # unweighted average — a tiny group can't swing it.
        rng = np.random.default_rng(0)
        # group A: 300 rows, ratio ~0.5, small denominators; group B: 3000 rows,
        # ratio ~0.5, large denominators. No between-group ratio difference → ~0 gain.
        num = np.concatenate([rng.normal(0.5, 0.01, 300) * 1.0, rng.normal(0.5, 0.01, 3000) * 10.0])
        den = np.concatenate([np.full(300, 1.0), np.full(3000, 10.0)])
        codes = np.array([0] * 300 + [1] * 3000)
        ratio = num / den
        gain = weighted_variance_reduction(codes, 2, ratio, den, min_support=100)
        assert gain < 0.05  # same ratio in both groups ⇒ no explanatory power

    def test_distinct_group_ratios_give_high_gain(self) -> None:
        rng = np.random.default_rng(0)
        num = np.concatenate([rng.normal(0.2, 0.01, 1000), rng.normal(0.8, 0.01, 1000)])
        den = np.ones(2000)
        codes = np.array([0] * 1000 + [1] * 1000)
        gain = weighted_variance_reduction(codes, 2, num / den, den, min_support=100)
        assert gain > 0.9  # the grouping explains almost all the ratio's variance


class TestRatioTarget:
    def test_recall_and_fdr(self) -> None:
        seeds = 15
        strong = 0
        null_total = 0
        target_type = ""
        for seed in range(seeds):
            rng = np.random.default_rng(seed)
            df = make_ratio_corpus(rng)
            codes_by_dim, labels_by_dim = factorize_columns({d: df[d] for d in RATIO_DIMS})
            target = RatioTarget(
                df["numerator"].to_numpy().astype(float),
                df["denominator"].to_numpy().astype(float),
            )
            rank = discover_tree(
                codes_by_dim,
                labels_by_dim,
                target,
                measure_label="margin",
                dims=RATIO_DIMS,
                rng=rng,
                max_depth=1,
                alpha=ALPHA,
                n_perm=N_PERM,
            )
            target_type = rank.target_type
            sig = {d for d, _ in rank.ranked_dimensions}
            strong += "R_e60" in sig
            null_total += sum(n in sig for n in RATIO_NULLS)
        assert target_type == "ratio"
        assert strong >= int(0.9 * seeds), f"ratio strong-driver recall {strong}/{seeds}"
        assert null_total <= FDR_BAR * len(RATIO_NULLS) * seeds

    def test_invalid_denominator_rows_dropped(self) -> None:
        # den ≤ 0 / NaN rows carry no ratio → excluded from the gain (no divide blow-up).
        num = np.array([1.0, 2.0, 3.0, 4.0])
        den = np.array([2.0, 0.0, np.nan, 8.0])
        target = RatioTarget(num, den)
        assert np.isnan(target.observed[1]) and np.isnan(target.observed[2])
        assert not np.isnan(target.observed[0]) and not np.isnan(target.observed[3])


class TestStockTarget:
    def test_stock_reuses_row_grain_reduction(self) -> None:
        # Stock = additivity-respecting because it never sums: the same row-grain
        # variance reduction as flow, only the target_type label differs.
        rng = np.random.default_rng(0)
        df = make_corpus(rng)
        codes_by_dim, labels_by_dim = factorize_columns({d: df[d] for d in ALL_DIMS})
        target = FlowTarget(df["measure"].to_numpy().astype(float), target_type="stock")
        rank = discover_tree(
            codes_by_dim,
            labels_by_dim,
            target,
            measure_label="balance",
            dims=ALL_DIMS,
            rng=rng,
            max_depth=1,
            alpha=ALPHA,
            n_perm=N_PERM,
        )
        assert rank.target_type == "stock"
        assert "D_e60" in {d for d, _ in rank.ranked_dimensions}

    def test_build_codes_is_target_agnostic(self) -> None:
        # build_codes reads the target's `observed` array (NaN = unobserved) — same
        # for a stock measure as a flow one.
        rng = np.random.default_rng(0)
        df = make_corpus(rng)
        phys, _ = columns(df, "D_e60")
        target = FlowTarget(df["measure"].to_numpy().astype(float), target_type="stock")
        codes, n_codes = build_codes(phys, target.observed, handle_nulls=True)
        assert n_codes >= 2
