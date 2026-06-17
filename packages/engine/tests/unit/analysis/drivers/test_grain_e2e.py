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
    CL_ROW_NULL,
    make_clustered_corpus,
)

RUN = "session-run-1"
VIEW = "sales_enriched"
ALPHA = 0.05
N_PERM = 200
MEASURE = Measure(target_type="flow", column="measure")


def _seed_catalog(session: Session) -> str:
    """Seed the fact + catalog (the candidate dims; the entity key is NOT a slice dim)."""
    fact = Table(
        table_id=str(uuid4()), source_id="s", table_name="sales", layer="typed", duckdb_path="sales"
    )
    session.add(fact)
    for pos, name in enumerate([*CL_DIMS, CL_ENTITY, "measure"]):
        col = Column(
            column_id=str(uuid4()), table_id=fact.table_id, column_name=name, column_position=pos
        )
        session.add(col)
        if name in CL_DIMS:  # only real candidate dims are cataloged (not the entity id / measure)
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

    def test_row_level_dim_skipped_at_entity_grain(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # The row-level null (varies within entity) can't be collapsed → not evaluated
        # at entity grain (logged + skipped), so it never appears in the ranking.
        tid = _seed_catalog(real_session)
        rank = _run(real_session, duck, tid, 0, cluster_key=CL_ENTITY)
        assert CL_ROW_NULL not in {d for d, _ in rank.ranked_dimensions}

    def test_low_icc_cluster_key_stays_row_wise(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Pointing cluster_key at a row-level random column (≈0 ICC) must NOT trigger
        # the entity grain — the row-wise null is valid there.
        tid = _seed_catalog(real_session)
        rank = _run(real_session, duck, tid, 0, cluster_key=CL_ROW_NULL)
        assert rank.grain == "row"
