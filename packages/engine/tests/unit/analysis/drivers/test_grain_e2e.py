"""DAT-552 — the ICC-switched processor end to end.

Seeds a clustered enriched view (high-ICC measure, repeated entities) + the catalog,
then proves the contrast the spike found: the row-wise null FALSELY surfaces
entity-level nulls (the bug DAT-545 ships), while the cluster-aware entity-grain path
holds FDR — and that a low-ICC cluster key correctly stays row-wise.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import numpy as np
from sqlalchemy.orm import Session

from dataraum.analysis.drivers.models import Measure
from dataraum.analysis.drivers.processor import discover_drivers
from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation, TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.storage import Column, Table

from .conftest import (
    CL_DIMS,
    CL_DRIVER,
    CL_ENTITY,
    CL_ENTITY_NULLS,
    CL_RATIO_DIMS,
    CL_RATIO_ROW_DRIVER,
    CL_ROW_DRIVER,
    CL_ROW_NULL,
    RATIO_TWO_DRIVER_DIMS,
    TE_CUST,
    TE_CUST_DRIVER,
    TE_CUST_NULL,
    TE_DIMS,
    TE_PROD,
    TE_PROD_DRIVER,
    TE_PROD_NULL,
    TE_ROW_NULL,
    TWO_DRIVER_DIMS,
    make_clustered_corpus,
    make_clustered_ratio_corpus,
    make_clustered_ratio_two_driver_corpus,
    make_clustered_two_driver_corpus,
    make_two_entity_corpus,
)

RUN = "session-run-1"
VIEW = "sales_enriched"
ALPHA = 0.05
N_PERM = 200
MEASURE = Measure(target_type="flow", column="measure")
RATIO_MEASURE = Measure(target_type="ratio", numerator="numerator", denominator="denominator")


def _seed_catalog(session: Session, dims: list[str] = CL_DIMS) -> str:
    """Seed the fact + catalog (the candidate dims; the entity key is NOT a slice dim)."""
    fact = Table(
        table_id=str(uuid4()), source_id="s", table_name="sales", layer="typed", duckdb_path="sales"
    )
    session.add(fact)
    for pos, name in enumerate([*dims, CL_ENTITY, "measure"]):
        col = Column(
            column_id=str(uuid4()), table_id=fact.table_id, column_name=name, column_position=pos
        )
        session.add(col)
        if name in dims:  # only real candidate dims are cataloged (not the entity id / measure)
            session.add(
                SliceDefinition(
                    run_id=RUN,
                    table_id=fact.table_id,
                    column_id=col.column_id,
                    column_name=name,
                    slice_priority=1,
                    detection_source="llm",
                )
            )
        if name == "measure":
            # Object-grain role on SemanticAnnotation; catalogue-grain
            # temporal_behavior on ColumnConcept (DAT-637).
            session.add(
                SemanticAnnotation(column_id=col.column_id, run_id=RUN, semantic_role="measure")
            )
            session.add(
                ColumnConcept(column_id=col.column_id, run_id=RUN, temporal_behavior="additive")
            )
    session.add(
        EnrichedView(
            run_id=RUN, fact_table_id=fact.table_id, view_name=VIEW, is_grain_verified=True
        )
    )
    session.flush()
    return fact.table_id


def _seed_ratio_catalog(session: Session, dims: list[str] = CL_RATIO_DIMS) -> str:
    """Seed a fact whose measure is a ratio (numerator/denominator) over a clustered view."""
    fact = Table(
        table_id=str(uuid4()), source_id="s", table_name="sales", layer="typed", duckdb_path="sales"
    )
    session.add(fact)
    for pos, name in enumerate([*dims, CL_ENTITY, "numerator", "denominator"]):
        col = Column(
            column_id=str(uuid4()), table_id=fact.table_id, column_name=name, column_position=pos
        )
        session.add(col)
        if name in dims:
            session.add(
                SliceDefinition(
                    run_id=RUN,
                    table_id=fact.table_id,
                    column_id=col.column_id,
                    column_name=name,
                    slice_priority=1,
                    detection_source="llm",
                )
            )
    session.add(
        EnrichedView(
            run_id=RUN, fact_table_id=fact.table_id, view_name=VIEW, is_grain_verified=True
        )
    )
    session.flush()
    return fact.table_id


def _seed_identities(session: Session, tid: str, cols: list[str]) -> None:
    """Persist the fact's ``identity_columns`` (DAT-565) so the resolver can read them."""
    session.add(
        TableEntity(
            run_id=RUN,
            table_id=tid,
            detected_entity_type="orders",
            identity_columns=[{"column": c, "note": "seed identity"} for c in cols],
        )
    )
    session.flush()


def _write_view(duck: duckdb.DuckDBPyConnection, df) -> None:
    duck.execute(f'DROP TABLE IF EXISTS "{VIEW}"')
    duck.register("cl_df", df)
    duck.execute(f'CREATE TABLE "{VIEW}" AS SELECT * FROM cl_df')
    duck.unregister("cl_df")


def _run(session: Session, duck: duckdb.DuckDBPyConnection, tid: str, seed: int, *, cluster_key):
    _write_view(duck, make_clustered_corpus(np.random.default_rng(100 + seed)))
    return discover_drivers(
        session,
        duckdb_conn=duck,
        fact_table_id=tid,
        run_id=RUN,
        measure=MEASURE,
        cluster_keys=[cluster_key] if cluster_key is not None else None,
        n_perm=N_PERM,
        seed=seed,
    )


class TestClusterAwareSwitch:
    def test_row_wise_null_is_broken_on_clustered_data(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # The bug DAT-545 ships: with no cluster_key, the row-wise null falsely
        # surfaces ENTITY-LEVEL nulls on a clustered measure (the 100%-FDR finding).
        tid = _seed_catalog(real_session)
        seeds = 8
        ent_null_surfaced = 0
        for s in range(seeds):
            rank = _run(real_session, duck, tid, s, cluster_key=None)
            assert rank.grain == "row"
            sig = {d for d, _ in rank.ranked_dimensions}
            ent_null_surfaced += any(n in sig for n in CL_ENTITY_NULLS)
        # Far above 2α — the row-wise null is genuinely broken here (regression guard).
        assert ent_null_surfaced >= seeds // 2, (
            f"expected frequent false positives, got {ent_null_surfaced}/{seeds}"
        )

    def test_entity_grain_controls_fdr_and_finds_driver(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_catalog(real_session)
        seeds = 30  # enough that the 2α FDR bar absorbs normal sampling variance
        driver_found = 0
        ent_null_surfaced = dict.fromkeys(CL_ENTITY_NULLS, 0)
        n_entities = None
        for s in range(seeds):
            rank = _run(real_session, duck, tid, s, cluster_key=CL_ENTITY)
            assert rank.grain == "entity"
            n_entities = rank.n_rows
            sig = {d for d, _ in rank.ranked_dimensions}
            driver_found += CL_DRIVER in sig
            for n in CL_ENTITY_NULLS:
                ent_null_surfaced[n] += n in sig
        # Power scales with ENTITIES, not rows: n_rows reports the 200 entities.
        assert n_entities == 200
        # THE hard claim (the DAT-552 fix): the entity-level nulls stay gated at ≈α.
        for n, c in ent_null_surfaced.items():
            assert c <= 2 * ALPHA * seeds, f"entity null {n} surfaced {c}/{seeds}"
        # The driver surfaces in the MAJORITY — entity-grain power is lower by design
        # (the spike's "power scales with entity count" finding), so NOT a 0.9 bar.
        assert driver_found >= seeds // 2, f"driver recall {driver_found}/{seeds}"

    def test_row_level_dim_routed_to_row_wise_secondary(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # DAT-561: a row-level dim (varies within entity) is NOT in the entity-grain
        # primary — it is routed to the row-wise (secondary) family instead. The
        # entity primary carries only entity-constant dims; the grains never mix.
        tid = _seed_catalog(real_session)
        rank = _run(real_session, duck, tid, 0, cluster_key=CL_ENTITY)
        assert rank.grain == "entity"
        primary = {d for d, _ in rank.ranked_dimensions}
        assert CL_ROW_NULL not in primary
        assert primary <= {CL_DRIVER, *CL_ENTITY_NULLS}  # only entity-constant dims
        # CL_ROW_NULL is random → it won't be a significant secondary either, but every
        # secondary that DID surface is labeled row grain (never entity).
        assert all(s.grain == "row" for s in rank.secondary_dimensions)

    def test_low_icc_cluster_key_stays_row_wise(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Pointing cluster_key at a row-level random column (≈0 ICC) must NOT trigger
        # the entity grain — the row-wise null is valid there.
        tid = _seed_catalog(real_session)
        rank = _run(real_session, duck, tid, 0, cluster_key=CL_ROW_NULL)
        assert rank.grain == "row"

    def test_icc_switch_pinned_to_clustering_strength(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # The switch is driven by ICC, not by the cluster_key's mere presence: the SAME
        # entity key flips entity→row when within-entity noise is inflated enough to
        # drown the between-entity signal below icc_threshold. Pins the 0.10 default.
        tid = _seed_catalog(real_session)

        duck.execute(f'DROP TABLE IF EXISTS "{VIEW}"')
        high = make_clustered_corpus(np.random.default_rng(0))  # default → ICC ≫ 0.10
        duck.register("hi", high)
        duck.execute(f'CREATE TABLE "{VIEW}" AS SELECT * FROM hi')
        duck.unregister("hi")
        hi_rank = discover_drivers(
            real_session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=MEASURE,
            cluster_keys=[CL_ENTITY],
            n_perm=N_PERM,
        )
        assert hi_rank.grain == "entity"

        duck.execute(f'DROP TABLE IF EXISTS "{VIEW}"')
        low = make_clustered_corpus(np.random.default_rng(0), row_sigma=6.0)  # noise drowns signal
        duck.register("lo", low)
        duck.execute(f'CREATE TABLE "{VIEW}" AS SELECT * FROM lo')
        duck.unregister("lo")
        lo_rank = discover_drivers(
            real_session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=MEASURE,
            cluster_keys=[CL_ENTITY],
            n_perm=N_PERM,
        )
        assert lo_rank.grain == "row"


def _run_low_icc(session, duck, tid, seed, *, cluster_key):  # noqa: ANN001, ANN202
    # row_sigma=6 drowns the between-entity signal → ICC ≈ 0.03, the DAT-552 residual
    # regime where the row-wise null FALSELY surfaced a high-K entity-level dim.
    _write_view(duck, make_clustered_corpus(np.random.default_rng(100 + seed), row_sigma=6.0))
    return discover_drivers(
        session,
        duckdb_conn=duck,
        fact_table_id=tid,
        run_id=RUN,
        measure=MEASURE,
        cluster_keys=[cluster_key] if cluster_key is not None else None,
        n_perm=N_PERM,
        seed=seed,
    )


class TestCandidateGrainRouting:
    """DAT-561 — route by candidate constancy, not the measure's global ICC.

    The eval-gate residual: at ICC ≈ 0.03 a high-cardinality entity-LEVEL random dim
    still false-positived under the row-wise null (pseudoreplication — the row-wise null
    is structurally invalid for an entity-constant candidate at ANY ICC). The fix routes
    every entity-constant candidate to the entity grain regardless of ICC.
    """

    def test_entity_constant_dim_never_enters_row_wise_primary(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_catalog(real_session)
        seeds = 30
        in_secondary = 0
        for s in range(seeds):
            rank = _run_low_icc(real_session, duck, tid, s, cluster_key=CL_ENTITY)
            # Low ICC → the row-wise family is primary…
            assert rank.grain == "row"
            primary = {d for d, _ in rank.ranked_dimensions}
            # …and it can ONLY contain row-level dims — every entity-constant candidate
            # was routed to the entity grain (the structural fix; reverting to global-ICC
            # routing puts entity-level dims back into this row-wise primary → fails here).
            assert primary.isdisjoint({CL_DRIVER, *CL_ENTITY_NULLS})
            sec = {d.dimension for d in rank.secondary_dimensions}
            assert all(d.grain == "entity" for d in rank.secondary_dimensions)
            in_secondary += CL_ENTITY_NULLS[1] in sec
        # At the CORRECT (entity) grain the high-K entity-level null is gated at ≈α —
        # the false positive the row-wise null produced is gone.
        assert in_secondary <= 2 * ALPHA * seeds, (
            f"entity-level null surfaced {in_secondary}/{seeds} even at entity grain"
        )

    def test_low_icc_row_level_driver_still_surfaces(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # AC2: routing entity-constant dims out to the entity grain must NOT cost
        # low-ICC row-level recall — a genuine row-level driver still surfaces in the
        # row-wise PRIMARY (raw measure, no de-mean at low ICC). The two-driver corpus
        # at ent_scale=0.08 has ICC ≈ 0.04 and a planted within-entity row driver.
        tid = _seed_catalog(real_session, TWO_DRIVER_DIMS)
        seeds = 10
        found = 0
        for s in range(seeds):
            _write_view(
                duck,
                make_clustered_two_driver_corpus(np.random.default_rng(500 + s), ent_scale=0.08),
            )
            rank = discover_drivers(
                real_session,
                duckdb_conn=duck,
                fact_table_id=tid,
                run_id=RUN,
                measure=MEASURE,
                cluster_keys=[CL_ENTITY],
                n_perm=N_PERM,
                seed=s,
            )
            assert rank.grain == "row"  # low ICC → row-wise family is primary
            found += CL_ROW_DRIVER in {d for d, _ in rank.ranked_dimensions}
        assert found >= seeds // 2, f"low-ICC row-level driver recall {found}/{seeds}"


class TestWithinEntityPower:
    """DAT-561 power add-on (AC4): under high ICC the row-level (secondary) family gates
    on the within-entity de-meaned residual, so a planted within-entity driver surfaces
    and the row-level null stays gated — while the entity-level driver leads the primary
    tree and the two grains never mix.
    """

    def _run(self, session, duck, tid, seed):  # noqa: ANN001, ANN202
        _write_view(duck, make_clustered_two_driver_corpus(np.random.default_rng(400 + seed)))
        return discover_drivers(
            session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=MEASURE,
            cluster_keys=[CL_ENTITY],
            n_perm=N_PERM,
            seed=seed,
        )

    def test_entity_and_within_entity_drivers_cleanly_separated(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_catalog(real_session, TWO_DRIVER_DIMS)
        rank = self._run(real_session, duck, tid, 0)
        assert rank.grain == "entity"  # high ICC → the entity-grain family is primary
        primary = {d for d, _ in rank.ranked_dimensions}
        secondary = {d.dimension for d in rank.secondary_dimensions}
        # The entity-level driver leads the primary; the within-entity driver surfaces in
        # the de-meaned row-wise secondary — and NEITHER bleeds into the other grain.
        assert CL_DRIVER in primary
        assert CL_ROW_DRIVER in secondary
        assert CL_DRIVER not in secondary
        assert CL_ROW_DRIVER not in primary
        assert all(d.grain == "row" for d in rank.secondary_dimensions)

    def test_within_entity_driver_found_and_residual_null_gated(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_catalog(real_session, TWO_DRIVER_DIMS)
        seeds = 30
        driver_found = 0
        row_null_surfaced = 0
        for s in range(seeds):
            rank = self._run(real_session, duck, tid, s)
            secondary = {d.dimension for d in rank.secondary_dimensions}
            driver_found += CL_ROW_DRIVER in secondary
            row_null_surfaced += CL_ROW_NULL in secondary
        # The de-meaned residual gives the within-entity driver real power…
        assert driver_found >= seeds // 2, f"within-entity driver recall {driver_found}/{seeds}"
        # …and the residual null is gated at ≈α (FDR ≤ 2α).
        assert row_null_surfaced <= 2 * ALPHA * seeds, (
            f"row-level null surfaced {row_null_surfaced}/{seeds} on the residual"
        )


class TestClusterAwareRatio:
    """A clustered RATIO measure must get the entity-grain fix too (#321 fold)."""

    def _run_ratio(self, session, duck, tid, seed, *, cluster_key):
        _write_view(duck, make_clustered_ratio_corpus(np.random.default_rng(300 + seed)))
        return discover_drivers(
            session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=RATIO_MEASURE,
            cluster_keys=[cluster_key] if cluster_key is not None else None,
            n_perm=N_PERM,
            seed=seed,
        )

    def test_row_wise_ratio_is_broken_on_clustered_data(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Without cluster_key, a clustered ratio takes the row-wise null → the SAME
        # FDR inflation as a clustered level (the gap #321 closes for ratio too).
        tid = _seed_ratio_catalog(real_session)
        seeds = 8
        ent_null_surfaced = 0
        for s in range(seeds):
            rank = self._run_ratio(real_session, duck, tid, s, cluster_key=None)
            assert rank.grain == "row" and rank.target_type == "ratio"
            sig = {d for d, _ in rank.ranked_dimensions}
            ent_null_surfaced += any(n in sig for n in CL_ENTITY_NULLS)
        assert ent_null_surfaced >= seeds // 2, (
            f"expected false positives, got {ent_null_surfaced}/{seeds}"
        )

    def test_entity_grain_ratio_controls_fdr_and_finds_driver(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_ratio_catalog(real_session)
        seeds = 30
        driver_found = 0
        ent_null_surfaced = dict.fromkeys(CL_ENTITY_NULLS, 0)
        for s in range(seeds):
            rank = self._run_ratio(real_session, duck, tid, s, cluster_key=CL_ENTITY)
            assert rank.grain == "entity" and rank.target_type == "ratio"
            sig = {d for d, _ in rank.ranked_dimensions}
            driver_found += CL_DRIVER in sig
            for n in CL_ENTITY_NULLS:
                ent_null_surfaced[n] += n in sig
        # The entity-grain ratio null (Σnum/Σden per entity) controls FDR…
        for n, c in ent_null_surfaced.items():
            assert c <= 2 * ALPHA * seeds, f"entity null {n} surfaced {c}/{seeds}"
        # …and the entity-level ratio driver surfaces in the majority.
        assert driver_found >= seeds // 2, f"ratio driver recall {driver_found}/{seeds}"


class TestWithinEntityRatioPower:
    """DAT-561 ratio power add-on: under high ICC the row-level (secondary) RATIO family
    gates on the within-entity volume-weighted de-meaned ratio, so a within-entity ratio
    driver surfaces and the row-level null stays gated — while the entity-level ratio
    driver leads the entity-grain primary and the grains never mix.
    """

    def _run(self, session, duck, tid, seed):  # noqa: ANN001, ANN202
        _write_view(duck, make_clustered_ratio_two_driver_corpus(np.random.default_rng(600 + seed)))
        return discover_drivers(
            session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=RATIO_MEASURE,
            cluster_keys=[CL_ENTITY],
            n_perm=N_PERM,
            seed=seed,
        )

    def test_entity_and_within_entity_ratio_drivers_cleanly_separated(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_ratio_catalog(real_session, RATIO_TWO_DRIVER_DIMS)
        rank = self._run(real_session, duck, tid, 0)
        assert rank.grain == "entity" and rank.target_type == "ratio"
        primary = {d for d, _ in rank.ranked_dimensions}
        secondary = {d.dimension for d in rank.secondary_dimensions}
        assert CL_DRIVER in primary
        assert CL_RATIO_ROW_DRIVER in secondary
        assert CL_DRIVER not in secondary
        assert CL_RATIO_ROW_DRIVER not in primary
        assert all(d.grain == "row" for d in rank.secondary_dimensions)

    def test_within_entity_ratio_driver_found_and_residual_null_gated(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_ratio_catalog(real_session, RATIO_TWO_DRIVER_DIMS)
        seeds = 30
        driver_found = 0
        row_null_surfaced = 0
        for s in range(seeds):
            rank = self._run(real_session, duck, tid, s)
            secondary = {d.dimension for d in rank.secondary_dimensions}
            driver_found += CL_RATIO_ROW_DRIVER in secondary
            row_null_surfaced += CL_ROW_NULL in secondary
        assert driver_found >= seeds // 2, (
            f"within-entity ratio driver recall {driver_found}/{seeds}"
        )
        assert row_null_surfaced <= 2 * ALPHA * seeds, (
            f"row-level null surfaced {row_null_surfaced}/{seeds} on the ratio residual"
        )


class TestTwoEntityRouting:
    """DAT-563: N=2 home-grain routing end to end — customer (higher ICC) is primary, the
    product family runs at its own entity grain as a labeled secondary, no grain mixes."""

    def _run(self, session: Session, duck: duckdb.DuckDBPyConnection, tid: str, seed: int):
        _write_view(duck, make_two_entity_corpus(np.random.default_rng(700 + seed)))
        return discover_drivers(
            session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=MEASURE,
            cluster_keys=[TE_CUST, TE_PROD],
            n_perm=N_PERM,
            seed=seed,
        )

    def test_primary_is_highest_icc_entity_secondary_is_the_other(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_catalog(real_session, dims=TE_DIMS)
        rank = self._run(real_session, duck, tid, 0)

        # Customer clusters harder than product → customer is the primary entity grain,
        # and the headline says which (DAT-563 entity label).
        assert rank.grain == "entity"
        assert rank.entity == TE_CUST
        primary = {d for d, _ in rank.ranked_dimensions}
        assert TE_CUST_DRIVER in primary  # the strong customer driver leads the primary

        # The product family ran at ITS OWN entity grain: the product driver surfaces as a
        # secondary labeled (entity, product) — proof the second entity was routed, not
        # collapsed into customer or row.
        prod_secondary = [s for s in rank.secondary_dimensions if s.entity == TE_PROD]
        assert any(s.dimension == TE_PROD_DRIVER for s in prod_secondary)
        assert all(s.grain == "entity" for s in prod_secondary)

        # No grain mixes: product-grain dims never in the customer primary; the row null is
        # never ranked at an entity grain; every dim appears at exactly ONE grain.
        assert TE_PROD_DRIVER not in primary and TE_PROD_NULL not in primary
        assert all(
            s.grain == "row" for s in rank.secondary_dimensions if s.dimension == TE_ROW_NULL
        )
        seen = list(primary) + [s.dimension for s in rank.secondary_dimensions]
        assert len(seen) == len(set(seen)), f"a dim was ranked at two grains: {seen}"

    def test_rerun_determinism(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Identical data + identities + seed → identical routing + ranking (DAT-563 AC6),
        # for N≥2 (the per-entity seed arithmetic + sorted iteration must be stable).
        tid = _seed_catalog(real_session, dims=TE_DIMS)
        a = self._run(real_session, duck, tid, 0)
        b = self._run(real_session, duck, tid, 0)
        assert (a.grain, a.entity) == (b.grain, b.entity)
        assert a.ranked_dimensions == b.ranked_dimensions
        a_sec = [(s.dimension, s.gain, s.grain, s.entity) for s in a.secondary_dimensions]
        b_sec = [(s.dimension, s.gain, s.grain, s.entity) for s in b.secondary_dimensions]
        assert a_sec == b_sec


class TestIdentityResolver:
    """DAT-563: cluster keys RESOLVED from persisted ``identity_columns`` (DAT-565) and
    ICC-verified — ``discover_drivers`` is called with NO ``cluster_keys`` (the real path)."""

    def _resolve(self, session: Session, duck: duckdb.DuckDBPyConnection, tid: str, seed: int = 0):
        return discover_drivers(  # no cluster_keys → resolve from identity_columns
            session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=MEASURE,
            n_perm=N_PERM,
            seed=seed,
        )

    def test_resolves_named_identities_and_routes(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed_catalog(real_session, dims=TE_DIMS)
        _seed_identities(real_session, tid, [TE_CUST, TE_PROD])
        _write_view(duck, make_two_entity_corpus(np.random.default_rng(0)))
        rank = self._resolve(real_session, duck, tid)
        # Both identities resolved + ICC-verified → same routing as the explicit N=2 case.
        assert rank.grain == "entity" and rank.entity == TE_CUST
        assert TE_CUST_DRIVER in {d for d, _ in rank.ranked_dimensions}
        assert any(
            s.entity == TE_PROD and s.dimension == TE_PROD_DRIVER for s in rank.secondary_dimensions
        )

    def test_drops_mis_named_low_icc_identity_no_heuristic(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # The LLM mis-names a row-level RANDOM column as an identity. Cardinality alone
        # can't tell it from a real identity — only ICC can: the measure does not cluster
        # within it, so verification drops it → plain row-wise, no spurious de-mean.
        tid = _seed_catalog(real_session, dims=TE_DIMS)
        _seed_identities(real_session, tid, [TE_ROW_NULL])
        _write_view(duck, make_two_entity_corpus(np.random.default_rng(0)))
        rank = self._resolve(real_session, duck, tid)
        assert rank.grain == "row" and rank.entity is None

    def test_no_identities_falls_back_to_row_wise(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # No TableEntity / no identity_columns named → the N=0 arm: plain row-wise.
        tid = _seed_catalog(real_session, dims=TE_DIMS)
        _write_view(duck, make_two_entity_corpus(np.random.default_rng(0)))
        rank = self._resolve(real_session, duck, tid)
        assert rank.grain == "row"

    def test_flat_denormalized_identity_resolved_not_row_wise(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # A single named identity on a clustered (high-ICC) flat table is resolved +
        # clustered, NOT silently left to the broken row-wise null (the AC).
        tid = _seed_catalog(real_session, dims=CL_DIMS)
        _seed_identities(real_session, tid, [CL_ENTITY])
        _write_view(duck, make_clustered_corpus(np.random.default_rng(0)))
        rank = self._resolve(real_session, duck, tid)
        assert rank.grain == "entity" and rank.entity == CL_ENTITY

    def test_fdr_controlled_per_grain_multi_entity(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # The statistical AC: over many seeds, the null at EACH grain stays gated (≤ 2α)
        # while the strong customer driver recalls — FDR controlled per grain, not pooled
        # across the two entity families + the row family.
        tid = _seed_catalog(real_session, dims=TE_DIMS)
        _seed_identities(real_session, tid, [TE_CUST, TE_PROD])
        seeds = 20
        cust_driver = cust_null = prod_null = row_null = 0
        for s in range(seeds):
            _write_view(duck, make_two_entity_corpus(np.random.default_rng(900 + s)))
            rank = self._resolve(real_session, duck, tid, seed=s)
            primary = {d for d, _ in rank.ranked_dimensions}
            sec = {(x.dimension, x.entity) for x in rank.secondary_dimensions}
            cust_driver += TE_CUST_DRIVER in primary
            cust_null += TE_CUST_NULL in primary  # customer-grain null (the customer primary)
            prod_null += (TE_PROD_NULL, TE_PROD) in sec  # product-grain null
            row_null += any(x.dimension == TE_ROW_NULL for x in rank.secondary_dimensions)
        assert cust_driver >= seeds // 2, f"customer driver recall {cust_driver}/{seeds}"
        assert cust_null <= 2 * ALPHA * seeds, f"customer null surfaced {cust_null}/{seeds}"
        assert prod_null <= 2 * ALPHA * seeds, f"product null surfaced {prod_null}/{seeds}"
        assert row_null <= 2 * ALPHA * seeds, f"row null surfaced {row_null}/{seeds}"
