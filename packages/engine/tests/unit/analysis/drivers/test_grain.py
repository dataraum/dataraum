"""DAT-552 — ICC + the entity-grain (cluster-aware) target.

Unit-level: ICC detects within-entity clustering; EntityMeanTarget ranks an
entity-level driver above an entity-level null on entity-collapsed data and permutes
ENTITIES (not rows). The end-to-end ICC-switch lives in test_grain_e2e.
"""

from __future__ import annotations

import numpy as np
import polars as pl

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
    _physical,
    make_clustered_corpus,
    make_clustered_ratio_two_driver_corpus,
    make_clustered_two_driver_corpus,
    make_two_entity_corpus,
)


def _codes(series: object) -> tuple[np.ndarray, int]:
    codes, labels = _physical(series)
    return codes, len(labels)


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
        df = pl.DataFrame(
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
        measure = df["measure"].to_numpy().astype(float)
        icc = intraclass_correlation(ent_codes, n, measure)
        assert icc > 0.3, f"clustered measure should have high ICC, got {icc:.3f}"

    def test_near_zero_on_random_grouping(self) -> None:
        # A grouping unrelated to the measure carries ~no between-group variance.
        df = make_clustered_corpus(np.random.default_rng(0))
        rng = np.random.default_rng(1)
        fake_entity = rng.integers(0, 200, len(df))
        codes, n = _codes(fake_entity)
        icc = intraclass_correlation(codes, n, df["measure"].to_numpy().astype(float))
        assert icc < 0.05, f"random grouping should have ~0 ICC, got {icc:.3f}"


class TestEntityMeanTarget:
    def _collapse(self, df: pl.DataFrame):
        g = df.group_by(CL_ENTITY, maintain_order=True).agg(
            pl.col("measure").mean().alias("m"),
            pl.col("measure").len().alias("w"),
            pl.col(CL_DRIVER).first().alias("drv"),
            pl.col(CL_ENTITY_NULLS[0]).first().alias("nul"),
        )
        means = g["m"].to_numpy().astype(float)
        sizes = g["w"].to_numpy().astype(float)
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
        measure = df["measure"].to_numpy().astype(float)
        residual = _within_entity_residual(df, CL_ENTITY, "measure")
        phys, _ = _physical(df[CL_ROW_DRIVER])
        codes, n = build_codes(phys, measure, handle_nulls=True)
        raw_gain = variance_reduction(codes, n, measure, min_support=2)
        residual_gain = variance_reduction(codes, n, residual, min_support=2)
        # The between-entity variance dilutes the row driver in the raw measure; the
        # de-meaned residual strips it, recovering a multiple-times larger gain.
        assert residual_gain > 3 * raw_gain
        assert residual_gain > 0.3, f"residual gain too weak: {residual_gain:.3f}"

    def test_demean_recovers_within_entity_ratio_signal(self) -> None:
        df = make_clustered_ratio_two_driver_corpus(np.random.default_rng(0))
        num = df["numerator"].to_numpy().astype(float)
        den = df["denominator"].to_numpy().astype(float)
        ratio = num / den
        residual, weight = _within_entity_ratio_residual(df, CL_ENTITY, "numerator", "denominator")
        phys, _ = _physical(df[CL_RATIO_ROW_DRIVER])
        codes, n = build_codes(phys, ratio, handle_nulls=True)
        # Both gains are volume-weighted (weight = denominator); only the de-mean differs.
        raw_gain = weighted_variance_reduction(codes, n, ratio, den, min_support=2)
        residual_gain = weighted_variance_reduction(codes, n, residual, weight, min_support=2)
        assert residual_gain > 3 * raw_gain
        assert residual_gain > 0.3, f"residual ratio gain too weak: {residual_gain:.3f}"

    def test_ratio_residual_handles_null_cluster_key(self) -> None:
        # A NaN cluster key factorizes to code -1; the residual must not crash (bincount
        # rejects negatives) and the row must be excluded (NaN residual, 0 weight).
        df = make_clustered_ratio_two_driver_corpus(np.random.default_rng(0))
        ent = df[CL_ENTITY].to_list()
        ent[0] = None  # one row with no entity (null cluster key)
        df = df.with_columns(pl.Series(CL_ENTITY, ent))
        residual, weight = _within_entity_ratio_residual(df, CL_ENTITY, "numerator", "denominator")
        assert np.isnan(residual[0]) and weight[0] == 0.0
        # the rest are unaffected (still produce finite residuals somewhere)
        assert np.isfinite(residual[1:]).any()


class TestAliasAndEmptyHeadline:
    """DAT-695: alias dims never home; the headline family must carry content."""

    @staticmethod
    def _frame(rng: np.random.Generator) -> pl.DataFrame:
        # 40 entities × 20 rows; measure clusters strongly by entity (high ICC).
        # ``alias`` is a 1:1 renaming of the entity key; ``home_null`` is a
        # legitimate (non-saturated) entity attribute with NO relation to the
        # measure; ``row_driver`` drives WITHIN-entity variation, so it survives
        # the row family's de-meaning.
        n_ent, per = 40, 20
        entity = [f"e{i}" for i in range(n_ent) for _ in range(per)]
        alias = [f"tenant_{e}" for e in entity]
        home_null = [f"g{i % 3}" for i in range(n_ent) for _ in range(per)]
        row_driver = rng.choice(["hi", "lo"], size=n_ent * per)
        base = np.repeat(rng.normal(0, 50, n_ent), per)
        measure = base + np.where(row_driver == "hi", 5.0, -5.0) + rng.normal(0, 0.5, n_ent * per)
        return pl.DataFrame(
            {
                "entity": entity,
                "alias": alias,
                "home_null": home_null,
                "row_driver": row_driver,
                "measure": measure,
            }
        )

    def test_alias_of_cluster_key_is_dropped_not_homed(self) -> None:
        df = self._frame(np.random.default_rng(7))
        home, row = _home_grain_partition(df, ["entity"], ["alias", "row_driver"])
        assert home == {}  # the alias is neither a home dim...
        assert row == ["row_driver"]  # ...nor row-level — it IS the key, renamed

    def test_headline_skips_empty_family_for_content(self) -> None:
        from dataraum.analysis.drivers.models import Measure
        from dataraum.analysis.drivers.processor import (
            DEFAULT_ICC_THRESHOLD,
            DEFAULT_MIN_ENTITIES,
            _routed_ranking,
        )

        df = self._frame(np.random.default_rng(7))
        rank = _routed_ranking(
            df,
            ["home_null", "row_driver"],
            Measure(target_type="flow", column="measure"),
            ["entity"],
            seed=0,
            max_depth=2,
            alpha=0.05,
            min_support=25,
            missingness_gate=0.5,
            n_perm=200,
            icc_threshold=DEFAULT_ICC_THRESHOLD,
            min_entities=DEFAULT_MIN_ENTITIES,
        )
        # The entity family (home_null over 40 entities, pure noise) ranks
        # nothing; the headline must fall through to the row family that DID
        # find the within-entity driver — never persist ranked: 0 while a
        # non-empty family sits in secondary.
        assert rank.ranked_dimensions, "headline family must carry content"
        assert rank.grain == "row"
        assert "row_driver" in {d for d, _ in rank.ranked_dimensions}

    def test_all_dims_alias_yields_empty_ranking_not_crash(self) -> None:
        """Every candidate an alias → no families at all; the honest empty
        ranking comes back instead of an IndexError (DAT-695 review). DAT-859:
        that empty ranking is now a typed ABSTENTION (insufficient_candidates),
        not a silent measured zero."""
        from dataraum.analysis.drivers.models import AbstainReason, Measure, RankingStatus
        from dataraum.analysis.drivers.processor import (
            DEFAULT_ICC_THRESHOLD,
            DEFAULT_MIN_ENTITIES,
            _routed_ranking,
        )

        df = self._frame(np.random.default_rng(3))
        rank = _routed_ranking(
            df,
            ["alias"],  # the only candidate is a 1:1 renaming of the key
            Measure(target_type="flow", column="measure"),
            ["entity"],
            seed=0,
            max_depth=2,
            alpha=0.05,
            min_support=25,
            missingness_gate=0.5,
            n_perm=50,
            icc_threshold=DEFAULT_ICC_THRESHOLD,
            min_entities=DEFAULT_MIN_ENTITIES,
        )
        assert rank.ranked_dimensions == []
        assert rank.secondary_dimensions == []
        assert rank.n_rows == df.height
        assert rank.status == RankingStatus.ABSTAINED
        assert rank.abstain_reason == AbstainReason.INSUFFICIENT_CANDIDATES


class TestEntityGrainAbstains:
    """DAT-859: ``_entity_grain_ranking``'s honest-empty construction site."""

    def test_every_entity_missing_measure_abstains(self) -> None:
        """Every entity has no usable measure value (all-NaN) → a typed
        ABSTENTION (insufficient_data), not a silent measured zero."""
        from dataraum.analysis.drivers.models import AbstainReason, Measure, RankingStatus
        from dataraum.analysis.drivers.processor import _entity_grain_ranking

        n_ent = 10
        df = pl.DataFrame(
            {
                "entity": [f"e{i}" for i in range(n_ent) for _ in range(5)],
                "dim": [f"g{i % 2}" for i in range(n_ent) for _ in range(5)],
                "measure": [None] * (n_ent * 5),
            }
        )
        rank = _entity_grain_ranking(
            df,
            ["dim"],
            Measure(target_type="flow", column="measure"),
            "entity",
            seed=0,
            alpha=0.05,
            n_perm=50,
            min_entities=2,
        )
        assert rank.status == RankingStatus.ABSTAINED
        assert rank.abstain_reason == AbstainReason.INSUFFICIENT_DATA
        assert rank.grain == "entity"
        assert rank.n_rows == 0
        assert rank.ranked_dimensions == []
        assert rank.root is None

    def test_abstained_family_survives_routed_rankings_secondary_attach(self) -> None:
        """Regression (found in DAT-859 self-review): ``_routed_ranking`` can pick
        an ABSTAINED entity family as ``primary`` via its index-0 fallback (every
        bucket-0/1 family empty) and then ``dataclasses.replace(primary,
        secondary_dimensions=...)`` to attach a DIFFERENT, non-empty family's
        findings — a pre-existing, correct shape (DAT-695) that DAT-859 must not
        break. An early draft of the abstention invariant forbade ANY content
        (including secondary_dimensions) on an abstained ranking, which made this
        exact ``replace()`` raise; the invariant now exempts secondary_dimensions
        (see the DriverRanking docstring) precisely so this keeps working."""
        from dataclasses import replace

        from dataraum.analysis.drivers.models import (
            AbstainReason,
            DriverRanking,
            RankingStatus,
            SecondaryDriver,
        )

        primary = DriverRanking(
            measure="measure",
            target_type="flow",
            n_rows=0,
            grain="entity",
            status=RankingStatus.ABSTAINED,
            abstain_reason=AbstainReason.INSUFFICIENT_DATA,
        )
        secondary = [
            SecondaryDriver(dimension="prod_attr", gain=0.31, grain="entity", entity="prod")
        ]

        out = replace(primary, secondary_dimensions=secondary, entity="cust")

        assert out.status == RankingStatus.ABSTAINED  # still honestly abstained
        assert out.secondary_dimensions == secondary  # the demoted family's finding survives
        assert out.ranked_dimensions == []  # the primary's OWN story stays empty
