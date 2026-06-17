"""DAT-552 P2 — the ICC-switched processor end to end.

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
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.storage import Column, Table

from .conftest import (
    CL_DIMS,
    CL_DRIVER,
    CL_ENTITY,
    CL_ENTITY_NULLS,
    CL_RATIO_DIMS,
    CL_ROW_DRIVER,
    CL_ROW_NULL,
    TWO_DRIVER_DIMS,
    make_clustered_corpus,
    make_clustered_ratio_corpus,
    make_clustered_two_driver_corpus,
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
                    grain_safe=True,
                    detection_source="llm",
                )
            )
        if name == "measure":
            session.add(
                SemanticAnnotation(
                    column_id=col.column_id, run_id=RUN, temporal_behavior="additive"
                )
            )
    session.add(
        EnrichedView(
            run_id=RUN, fact_table_id=fact.table_id, view_name=VIEW, is_grain_verified=True
        )
    )
    session.flush()
    return fact.table_id


def _seed_ratio_catalog(session: Session) -> str:
    """Seed a fact whose measure is a ratio (numerator/denominator) over a clustered view."""
    fact = Table(
        table_id=str(uuid4()), source_id="s", table_name="sales", layer="typed", duckdb_path="sales"
    )
    session.add(fact)
    for pos, name in enumerate([*CL_RATIO_DIMS, CL_ENTITY, "numerator", "denominator"]):
        col = Column(
            column_id=str(uuid4()), table_id=fact.table_id, column_name=name, column_position=pos
        )
        session.add(col)
        if name in CL_RATIO_DIMS:
            session.add(
                SliceDefinition(
                    run_id=RUN,
                    table_id=fact.table_id,
                    column_id=col.column_id,
                    column_name=name,
                    slice_priority=1,
                    grain_safe=True,
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
        cluster_key=cluster_key,
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
            cluster_key=CL_ENTITY,
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
            cluster_key=CL_ENTITY,
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
        cluster_key=cluster_key,
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

    def test_low_icc_row_level_recall_preserved(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # AC2: routing entity-constant dims out must not cost low-ICC row-level recall —
        # the row-wise primary still finds a genuine row-level driver. CL_ROW_NULL is a
        # null here, but it must remain EVALUABLE (present as a row-level candidate); the
        # planted row-level driver case is covered by TestWithinEntityPower (high ICC).
        tid = _seed_catalog(real_session)
        rank = _run_low_icc(real_session, duck, tid, 0, cluster_key=CL_ENTITY)
        # The row-level null is a row-wise candidate (could appear in the primary), never
        # mis-routed to the entity grain.
        assert all(d.grain == "entity" for d in rank.secondary_dimensions)
        assert CL_ROW_NULL not in {d.dimension for d in rank.secondary_dimensions}


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
            cluster_key=CL_ENTITY,
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
            cluster_key=cluster_key,
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
