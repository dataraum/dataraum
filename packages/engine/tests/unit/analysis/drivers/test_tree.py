"""DAT-545 P2 — the greedy tree + within-dataset permutation null.

Inherits the DAT-544 kill-gate's claims, now over the engine: the STRONG driver
surfaces ~always (≥0.9) while independent nulls stay gated at ≈α (FDR ≤ 2α) even
with a confounded proxy competing; the marginal ±25% driver surfaces at the spike's
documented power floor (majority, ≥0.6 — NOT 90%); recursion doesn't compound FDR at
depth 2; and the tree yields drill paths + deviating slices. Fewer seeds /
permutations than the spike for unit speed; the separation + FDR bars are the same.
"""

from __future__ import annotations

import numpy as np

from dataraum.analysis.drivers.models import DriverRanking
from dataraum.analysis.drivers.targets import FlowTarget
from dataraum.analysis.drivers.tree import discover_tree

from .conftest import ALL_DIMS, INDEPENDENT_NULLS, PROXY, factorize_columns, make_corpus

ALPHA = 0.05
FDR_BAR = 2 * ALPHA
N_PERM = 300


def _run(seed: int, *, max_depth: int) -> DriverRanking:
    rng = np.random.default_rng(seed)
    df = make_corpus(rng)
    codes_by_dim, labels_by_dim = factorize_columns({d: df[d] for d in ALL_DIMS})
    measure = df["measure"].to_numpy().astype(float)
    return discover_tree(
        codes_by_dim,
        labels_by_dim,
        FlowTarget(measure),
        measure_label="measure",
        dims=ALL_DIMS,
        rng=rng,
        max_depth=max_depth,
        alpha=ALPHA,
        n_perm=N_PERM,
    )


class TestSingleNodeSeparation:
    def test_separation_strong_driver_and_fdr(self) -> None:
        # The GREEN kill-claim: the STRONG driver surfaces ~always AND every
        # independent null stays gated at ≈α — separation, no global threshold.
        seeds = 20
        strong = 0
        null_surfaced = dict.fromkeys(INDEPENDENT_NULLS, 0)
        for seed in range(seeds):
            sig = {d for d, _ in _run(seed, max_depth=1).ranked_dimensions}
            strong += "D_e60" in sig
            for n in INDEPENDENT_NULLS:
                null_surfaced[n] += n in sig
        assert strong >= int(0.9 * seeds), f"strong-driver recall {strong}/{seeds}"
        for n, c in null_surfaced.items():
            assert c <= FDR_BAR * seeds, f"null {n} surfaced {c}/{seeds}"

    def test_marginal_driver_power_floor(self) -> None:
        # Power is finite, not infinite (the spike's documented ≈±20–25% floor:
        # ±25%→~30/40). A regression guard, NOT a 90% bar — the ±25% driver
        # surfaces in the MAJORITY of datasets; weaker effects miss safely.
        seeds = 20
        marginal = sum(
            "D_e25" in {d for d, _ in _run(s, max_depth=1).ranked_dimensions} for s in range(seeds)
        )
        assert marginal >= int(0.6 * seeds), f"marginal-driver power {marginal}/{seeds}"

    def test_confounded_proxy_surfaces_without_breaking_fdr(self) -> None:
        seeds = 15
        proxy = 0
        null_total = 0
        for seed in range(seeds):
            sig = {d for d, _ in _run(seed, max_depth=1).ranked_dimensions}
            proxy += PROXY in sig
            null_total += sum(n in sig for n in INDEPENDENT_NULLS)
        # The 80%-copy proxy is a legitimate strong correlate — it should surface.
        assert proxy >= int(0.5 * seeds), f"proxy surfaced only {proxy}/{seeds}"
        # …yet the genuinely-independent nulls stay gated while it competes.
        assert null_total <= FDR_BAR * len(INDEPENDENT_NULLS) * seeds


class TestRecursion:
    def test_depth2_does_not_compound_fdr(self) -> None:
        seeds = 10
        null_children = 0
        total_children = 0
        recursed = 0
        for seed in range(seeds):
            rank = _run(seed, max_depth=2)
            if rank.root is None:
                continue
            for _value, child in rank.root.children:
                total_children += 1
                null_children += child.dimension in INDEPENDENT_NULLS
            recursed += bool(rank.root.children)
        # The tree actually recurses on some datasets (the test isn't vacuous)…
        assert recursed > 0
        # …and a pure independent null surfaces as a depth-2 split at ≤ 2α.
        if total_children:
            assert null_children <= FDR_BAR * total_children


class TestTreeOutputs:
    def test_paths_and_slices(self) -> None:
        rank = _run(0, max_depth=2)
        assert rank.root is not None
        assert rank.n_rows == 20_000
        # The top-ranked dimension is a real driver (or the legit proxy), never a null.
        top = rank.ranked_dimensions[0][0]
        assert top not in INDEPENDENT_NULLS
        # Drill paths are non-empty and start at the root dimension.
        assert rank.driver_paths and all(p[0] == rank.root.dimension for p in rank.driver_paths)
        # Interesting slices carry the root's deviating values, sorted by |effect|.
        assert rank.interesting_slices
        effects = [abs(s.effect) for s in rank.interesting_slices]
        assert effects == sorted(effects, reverse=True)

    def test_no_significant_driver_returns_clean_empty(self) -> None:
        # Pure noise: measure independent of every dim → root None, empty ranking.
        rng = np.random.default_rng(7)
        n = 5_000
        codes_by_dim, labels_by_dim = factorize_columns(
            {f"N{i}": np.array([f"v{v}" for v in rng.integers(0, 5, n)]) for i in range(4)}
        )
        measure = rng.normal(size=n)
        rank = discover_tree(
            codes_by_dim,
            labels_by_dim,
            FlowTarget(measure),
            measure_label="m",
            dims=list(codes_by_dim),
            rng=rng,
            n_perm=N_PERM,
        )
        assert rank.root is None
        assert rank.ranked_dimensions == []
        assert rank.driver_paths == []
