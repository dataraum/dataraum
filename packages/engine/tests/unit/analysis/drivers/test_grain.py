"""DAT-552 P1 — ICC + the entity-grain (cluster-aware) target.

Unit-level: ICC detects within-entity clustering; EntityMeanTarget ranks an
entity-level driver above an entity-level null on entity-collapsed data and permutes
ENTITIES (not rows). The end-to-end ICC-switch lives in test_grain_e2e (P2).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from dataraum.analysis.drivers.criterion import (
    build_codes,
    intraclass_correlation,
    variance_reduction,
    weighted_variance_reduction,
)
from dataraum.analysis.drivers.processor import (
    _home_grain_partition,
    _within_entity_ratio_residual,
    _within_entity_residual,
)
from dataraum.analysis.drivers.targets import EntityMeanTarget

from .conftest import (
    CL_DRIVER,
    CL_ENTITY,
    CL_ENTITY_NULLS,
    CL_RATIO_ROW_DRIVER,
    CL_ROW_DRIVER,
    TE_CUST,
    TE_CUST_DRIVER,
    TE_CUST_NULL,
    TE_DIMS,
    TE_PROD,
    TE_PROD_DRIVER,
    TE_PROD_NULL,
    TE_ROW_NULL,
    make_clustered_corpus,
    make_clustered_ratio_two_driver_corpus,
    make_clustered_two_driver_corpus,
    make_two_entity_corpus,
)


def _codes(series: pd.Series) -> tuple[np.ndarray, int]:
    codes, uniques = pd.factorize(series)
    return codes.astype(int), len(uniques)


class TestHomeGrainPartition:
    """DAT-563: each candidate is assigned to EXACTLY ONE home grain (entity or row)."""

    def test_each_dim_homes_at_one_grain(self) -> None:
        df = make_two_entity_corpus(np.random.default_rng(0))
        home, row = _home_grain_partition(df, [TE_CUST, TE_PROD], TE_DIMS)
        # Customer attrs are constant within customer; product attrs within product.
        assert set(home[TE_CUST]) == {TE_CUST_DRIVER, TE_CUST_NULL}
        assert set(home[TE_PROD]) == {TE_PROD_DRIVER, TE_PROD_NULL}
        assert row == [TE_ROW_NULL]  # varies within both → row-level
        # The partition is exhaustive AND disjoint — every dim lands once, nowhere twice.
        assigned = [d for ds in home.values() for d in ds] + row
        assert sorted(assigned) == sorted(TE_DIMS)

    def test_constant_within_two_homes_at_the_finer_entity(self) -> None:
        # ``region`` is constant within BOTH store and chain (a store sits in one region,
        # a chain spans one region here) → it must home at the FINER (higher-card) entity.
        df = pd.DataFrame(
            {
                "chain": [0, 0, 1, 1, 2, 2],  # 3 chains
                "store": [0, 1, 2, 3, 4, 5],  # 6 stores (finer)
                "region": ["W", "W", "E", "E", "S", "S"],  # constant within store AND chain
            }
        )
        home, row = _home_grain_partition(df, ["chain", "store"], ["region"])
        assert home == {"store": ["region"]}  # finer entity wins the tiebreak
        assert row == []


class TestICC:
    def test_high_on_clustered_measure(self) -> None:
        df = make_clustered_corpus(np.random.default_rng(0))
        ent_codes, n = _codes(df[CL_ENTITY])
        measure = df["measure"].to_numpy(dtype=float)
        icc = intraclass_correlation(ent_codes, n, measure)
        assert icc > 0.3, f"clustered measure should have high ICC, got {icc:.3f}"

    def test_near_zero_on_random_grouping(self) -> None:
        # A grouping unrelated to the measure carries ~no between-group variance.
        df = make_clustered_corpus(np.random.default_rng(0))
        rng = np.random.default_rng(1)
        fake_entity = rng.integers(0, 200, len(df))
        codes, n = _codes(pd.Series(fake_entity))
        icc = intraclass_correlation(codes, n, df["measure"].to_numpy(dtype=float))
        assert icc < 0.05, f"random grouping should have ~0 ICC, got {icc:.3f}"


class TestEntityMeanTarget:
    def _collapse(self, df: pd.DataFrame):
        g = df.groupby(CL_ENTITY, sort=False).agg(
            m=("measure", "mean"),
            w=("measure", "size"),
            drv=(CL_DRIVER, "first"),
            nul=(CL_ENTITY_NULLS[0], "first"),
        )
        means = g["m"].to_numpy(dtype=float)
        sizes = g["w"].to_numpy(dtype=float)
        return g, means, sizes

    def test_driver_outranks_null_at_entity_grain(self) -> None:
        df = make_clustered_corpus(np.random.default_rng(0))
        g, means, sizes = self._collapse(df)
        target = EntityMeanTarget(means, sizes, target_type="flow")
        drv_codes, dn = _codes(g["drv"])
        null_codes, nn = _codes(g["nul"])
        assert target.gain(drv_codes, dn, min_support=2) > target.gain(
            null_codes, nn, min_support=2
        )

    def test_permutes_entities_not_rows(self) -> None:
        df = make_clustered_corpus(np.random.default_rng(0))
        _g, means, sizes = self._collapse(df)
        target = EntityMeanTarget(means, sizes, target_type="stock")
        assert target.observed.size == 200  # one value per entity, not 20k rows
        permuted = target.permuted(np.random.default_rng(2))
        assert permuted.observed.size == 200
        assert permuted.target_type == "stock"
        # The same multiset of entity means, reordered (entity-level shuffle).
        assert sorted(permuted.observed.tolist()) == sorted(target.observed.tolist())

    def test_group_effects_weighted_and_entity_counted(self) -> None:
        df = make_clustered_corpus(np.random.default_rng(0))
        g, means, sizes = self._collapse(df)
        target = EntityMeanTarget(means, sizes, target_type="flow")
        drv_codes, dn = _codes(g["drv"])
        effects = target.group_effects(drv_codes, dn, min_support=2)
        assert effects  # the driver's 4 groups deviate from baseline
        # support is an ENTITY count (≤ 200 entities), never a row count.
        assert all(0 < support <= 200 for _c, _e, support in effects)


class TestWithinEntityDemean:
    """DAT-561 power add-on: the within-entity de-mean recovers a row-level driver that
    the raw (high-ICC) measure dilutes — the residual transform that powers the
    row-level family's null under high ICC."""

    def test_demean_recovers_within_entity_driver_signal(self) -> None:
        df = make_clustered_two_driver_corpus(np.random.default_rng(0))
        measure = df["measure"].to_numpy(dtype=float)
        residual = _within_entity_residual(df, CL_ENTITY, "measure")
        codes, n = build_codes(
            df[CL_ROW_DRIVER].astype(object).to_numpy(), measure, handle_nulls=True
        )
        raw_gain = variance_reduction(codes, n, measure, min_support=2)
        residual_gain = variance_reduction(codes, n, residual, min_support=2)
        # The between-entity variance dilutes the row driver in the raw measure; the
        # de-meaned residual strips it, recovering a multiple-times larger gain.
        assert residual_gain > 3 * raw_gain
        assert residual_gain > 0.3, f"residual gain too weak: {residual_gain:.3f}"

    def test_demean_recovers_within_entity_ratio_signal(self) -> None:
        df = make_clustered_ratio_two_driver_corpus(np.random.default_rng(0))
        num = df["numerator"].to_numpy(dtype=float)
        den = df["denominator"].to_numpy(dtype=float)
        ratio = num / den
        residual, weight = _within_entity_ratio_residual(df, CL_ENTITY, "numerator", "denominator")
        codes, n = build_codes(
            df[CL_RATIO_ROW_DRIVER].astype(object).to_numpy(), ratio, handle_nulls=True
        )
        # Both gains are volume-weighted (weight = denominator); only the de-mean differs.
        raw_gain = weighted_variance_reduction(codes, n, ratio, den, min_support=2)
        residual_gain = weighted_variance_reduction(codes, n, residual, weight, min_support=2)
        assert residual_gain > 3 * raw_gain
        assert residual_gain > 0.3, f"residual ratio gain too weak: {residual_gain:.3f}"

    def test_ratio_residual_handles_null_cluster_key(self) -> None:
        # A NaN cluster key factorizes to code -1; the residual must not crash (bincount
        # rejects negatives) and the row must be excluded (NaN residual, 0 weight).
        df = make_clustered_ratio_two_driver_corpus(np.random.default_rng(0))
        df[CL_ENTITY] = df[CL_ENTITY].astype("float")
        df.loc[0, CL_ENTITY] = np.nan  # one row with no entity
        residual, weight = _within_entity_ratio_residual(df, CL_ENTITY, "numerator", "denominator")
        assert np.isnan(residual[0]) and weight[0] == 0.0
        # the rest are unaffected (still produce finite residuals somewhere)
        assert np.isfinite(residual[1:]).any()
