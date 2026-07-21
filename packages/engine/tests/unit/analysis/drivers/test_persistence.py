"""DAT-546 — persist driver rankings as a run-versioned begin_session artifact.

Covers the serializer (grain labels preserved, never flattened), the orchestrator
over a seeded session (one grain-labeled row per measure-role column), idempotent
re-run (UPSERT converges), born-loud zero-measures, and session-table scoping. The
statistical behavior of the engine itself is proven in ``test_grain_e2e``.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import numpy as np
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from dataraum.analysis.drivers.db_models import DriverRankingArtifact
from dataraum.analysis.drivers.models import DriverRanking, DriverSlice, SecondaryDriver
from dataraum.analysis.drivers.persistence import (
    _measure_columns,
    persist_driver_rankings,
    ranking_to_row,
)
from dataraum.analysis.semantic.db_models import (
    ColumnConcept,
    SemanticAnnotation,
    TableEntity,
    TableRole,
)
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.storage import Column, Table

from .conftest import (
    CL_DIMS,
    CL_ENTITY,
    TE_CUST,
    TE_CUST_DRIVER,
    TE_DIMS,
    TE_PROD,
    TE_PROD_DRIVER,
    make_clustered_corpus,
    make_two_entity_corpus,
)

RUN = "session-run-1"
VIEW = "sales_enriched"
N_PERM = 200


def _seed(
    session: Session,
    *,
    dims: list[str],
    identities: list[str] | None = None,
    measure_role: str = "measure",
    behavior: str = "additive",
    table_name: str = "sales",
    view_name: str = VIEW,
    table_role: str = TableRole.FACT,
) -> tuple[str, str]:
    """Seed a table + catalog with a ``semantic_role`` column the orchestrator enumerates.

    Returns ``(table_id, measure_column_id)``. ``dims`` become grain-safe slice
    definitions; ``measure`` gets a ``SemanticAnnotation`` with ``measure_role`` (set
    to a non-measure role to exercise the born-loud zero path); identities (if given)
    are persisted as ``TableEntity.identity_columns`` for the resolver to read.

    A ``TableEntity`` row is ALWAYS written (DAT-846: ``_measure_columns`` inner-joins
    it) — ``table_role`` defaults to FACT so existing callers keep exercising the fact
    path; pass ``TableRole.DIMENSION`` to seed the excluded-attribute regression.
    """
    fact = Table(
        table_id=str(uuid4()),
        source_id="s",
        table_name=table_name,
        layer="typed",
        duckdb_path=view_name,
    )
    session.add(fact)
    measure_col_id = ""
    for pos, name in enumerate([*dims, "measure"]):
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
        else:  # the measure column
            measure_col_id = col.column_id
            # Object-grain role on SemanticAnnotation; catalogue-grain
            # temporal_behavior on ColumnConcept (DAT-637).
            session.add(
                SemanticAnnotation(
                    column_id=col.column_id,
                    run_id=RUN,
                    semantic_role=measure_role,
                )
            )
            session.add(
                ColumnConcept(column_id=col.column_id, run_id=RUN, temporal_behavior=behavior)
            )
    session.add(
        EnrichedView(
            run_id=RUN, fact_table_id=fact.table_id, view_name=view_name, is_grain_verified=True
        )
    )
    session.add(
        TableEntity(
            run_id=RUN,
            table_id=fact.table_id,
            detected_entity_type="orders",
            table_role=table_role,
            identity_columns=[{"column": c, "note": "seed"} for c in identities]
            if identities
            else None,
        )
    )
    session.flush()
    return fact.table_id, measure_col_id


def _write_view(duck: duckdb.DuckDBPyConnection, df, view_name: str = VIEW) -> None:
    duck.execute(f'DROP TABLE IF EXISTS "{view_name}"')
    duck.register("seed_df", df)
    duck.execute(f'CREATE TABLE "{view_name}" AS SELECT * FROM seed_df')
    duck.unregister("seed_df")


# --- target-type mapping is pinned to the validated engine's --------------------


def test_temporal_to_target_matches_processor() -> None:
    # persistence inlines the temporal_behavior→target map so the validated processor
    # stays untouched; pin it to processor's so a future vocabulary change can't
    # silently diverge the two (the only thing this duplication risks).
    from dataraum.analysis.drivers import persistence, processor

    assert persistence._TEMPORAL_TO_TARGET == processor._TEMPORAL_TO_TARGET


# --- the serializer: grain labels preserved, never flattened ----------------------


def test_ranking_to_row_preserves_per_item_grain_labels() -> None:
    ranking = DriverRanking(
        measure="revenue",
        target_type="flow",
        n_rows=200,
        grain="entity",
        entity="customer",
        ranked_dimensions=[("region", 0.42), ("channel", 0.19)],
        driver_paths=[["region", "channel"]],
        interesting_slices=[DriverSlice(dimension="region", value="CH", effect=0.5, support=120)],
        secondary_dimensions=[
            SecondaryDriver(dimension="sku", gain=0.22, grain="entity", entity="product"),
            SecondaryDriver(dimension="hour", gain=0.11, grain="row", entity=None),
        ],
    )
    row = ranking_to_row(ranking, run_id=RUN, measure_table_id="t1", measure_column_id="c1")

    assert row["grain"] == "entity"
    assert row["entity"] == "customer"
    assert row["n_rows"] == 200
    assert row["ranked_dimensions"] == [
        {"dimension": "region", "gain": 0.42},
        {"dimension": "channel", "gain": 0.19},
    ]
    assert row["driver_paths"] == [["region", "channel"]]
    assert row["interesting_slices"] == [
        {"dimension": "region", "value": "CH", "effect": 0.5, "support": 120}
    ]
    # The crux: each secondary keeps ITS OWN grain + entity — the product-entity family
    # and the row family are not merged into the primary or into one another.
    assert row["secondary_dimensions"] == [
        {"dimension": "sku", "gain": 0.22, "grain": "entity", "entity": "product"},
        {"dimension": "hour", "gain": 0.11, "grain": "row", "entity": None},
    ]


# --- the orchestrator over a seeded session ---------------------------------------


def test_persist_writes_one_grain_labeled_row_per_measure(
    real_session: Session, duck: duckdb.DuckDBPyConnection
) -> None:
    # Two named identities on a clustered corpus → the engine resolves customer as the
    # primary entity grain and product as a labeled-entity secondary. The persisted row
    # must carry that structure GRANULARLY (DAT-563), not a flattened cross-grain list.
    tid, measure_col_id = _seed(real_session, dims=TE_DIMS, identities=[TE_CUST, TE_PROD])
    _write_view(duck, make_two_entity_corpus(np.random.default_rng(0)))

    n = persist_driver_rankings(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
    assert n == 1

    row = real_session.execute(
        select(DriverRankingArtifact).where(
            DriverRankingArtifact.measure_column_id == measure_col_id,
            DriverRankingArtifact.run_id == RUN,
        )
    ).scalar_one()
    assert row.measure_table_id == tid
    assert row.measure_label == "measure"
    assert row.target_type == "flow"
    assert row.grain == "entity"
    assert row.entity == TE_CUST
    assert row.n_rows > 0
    primary = {d["dimension"] for d in row.ranked_dimensions}
    assert TE_CUST_DRIVER in primary
    # The product family persisted at its OWN entity grain, labeled — never merged.
    prod = [s for s in row.secondary_dimensions if s["entity"] == TE_PROD]
    assert any(s["dimension"] == TE_PROD_DRIVER for s in prod)
    assert all(s["grain"] == "entity" for s in prod)
    assert TE_PROD_DRIVER not in primary


def test_persist_is_idempotent_on_rerun(
    real_session: Session, duck: duckdb.DuckDBPyConnection
) -> None:
    # Same run_id + deterministic engine → a Temporal success-redelivery converges in
    # place: still exactly one row per (measure_column_id, run_id), identical content.
    tid, measure_col_id = _seed(real_session, dims=TE_DIMS, identities=[TE_CUST, TE_PROD])
    _write_view(duck, make_two_entity_corpus(np.random.default_rng(0)))

    persist_driver_rankings(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
    first = real_session.execute(select(DriverRankingArtifact)).scalar_one()
    first_dims = list(first.ranked_dimensions)

    _write_view(duck, make_two_entity_corpus(np.random.default_rng(0)))
    persist_driver_rankings(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)

    rows = real_session.execute(select(DriverRankingArtifact)).scalars().all()
    assert len(rows) == 1  # UPSERT converged, no duplicate
    assert rows[0].ranked_dimensions == first_dims


def test_no_measure_role_persists_nothing(
    real_session: Session, duck: duckdb.DuckDBPyConnection
) -> None:
    # Born-loud zero: a fact whose only annotated column is NOT a measure role yields no
    # rows (and no crash) — nothing to rank, surfaced as an explicit 0.
    tid, _ = _seed(real_session, dims=CL_DIMS, measure_role="attribute")
    _write_view(duck, make_clustered_corpus(np.random.default_rng(0)))

    n = persist_driver_rankings(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
    assert n == 0
    assert real_session.execute(select(DriverRankingArtifact)).first() is None


def test_dimension_table_measure_role_column_persists_nothing(
    real_session: Session, duck: duckdb.DuckDBPyConnection
) -> None:
    """DAT-846: a DIMENSION-role table's measure-labeled column is not a driver measure.

    The per-column LLM judges ``semantic_role`` from the column alone (no fact/dimension
    context), so a numeric dimension attribute — a circuit's latitude — can legitimately
    carry ``'measure'``. Before the table-role filter this hit the born-loud path and
    persisted an empty ``n_rows=0`` ranking row anyway; it must now be excluded upstream,
    never even entering ``discover_drivers``.
    """
    tid, _ = _seed(real_session, dims=CL_DIMS, table_role=TableRole.DIMENSION)
    _write_view(duck, make_clustered_corpus(np.random.default_rng(0)))

    n = persist_driver_rankings(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
    assert n == 0
    assert real_session.execute(select(DriverRankingArtifact)).first() is None


def test_fact_table_measure_role_column_still_ranked(
    real_session: Session, duck: duckdb.DuckDBPyConnection
) -> None:
    """DAT-846 counterpart: a real FACT measure keeps getting its row (born loud)."""
    tid, measure_col_id = _seed(real_session, dims=CL_DIMS, table_role=TableRole.FACT)
    _write_view(duck, make_clustered_corpus(np.random.default_rng(0)))

    n = persist_driver_rankings(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
    assert n == 1
    row = real_session.execute(
        select(DriverRankingArtifact).where(
            DriverRankingArtifact.measure_column_id == measure_col_id
        )
    ).scalar_one()
    assert row.measure_table_id == tid


def test_unclassified_table_persists_nothing(
    real_session: Session, duck: duckdb.DuckDBPyConnection
) -> None:
    """A table with no ``TableEntity`` row for this run is excluded, not defaulted in.

    Mirrors ``enriched_views_phase``'s fact lookup (an INNER join / membership test):
    an absent row means "not known to be a fact," not "assume fact." Directly deletes
    the ``TableEntity`` row ``_seed`` writes to isolate the missing-classification case
    from the DIMENSION case above.
    """
    tid, _ = _seed(real_session, dims=CL_DIMS, table_role=TableRole.FACT)
    real_session.execute(delete(TableEntity).where(TableEntity.table_id == tid))
    real_session.flush()
    _write_view(duck, make_clustered_corpus(np.random.default_rng(0)))

    n = persist_driver_rankings(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
    assert n == 0
    assert real_session.execute(select(DriverRankingArtifact)).first() is None


def test_only_session_tables_are_enumerated(
    real_session: Session, duck: duckdb.DuckDBPyConnection
) -> None:
    # Two facts in the catalog; only one is in session scope → only its measure is ranked.
    in_scope, in_col = _seed(
        real_session, dims=CL_DIMS, identities=[CL_ENTITY], table_name="in_scope", view_name=VIEW
    )
    _seed(
        real_session,
        dims=CL_DIMS,
        identities=[CL_ENTITY],
        table_name="out_of_scope",
        view_name="other_enriched",
    )
    _write_view(duck, make_clustered_corpus(np.random.default_rng(0)))

    n = persist_driver_rankings(real_session, duckdb_conn=duck, table_ids=[in_scope], run_id=RUN)
    assert n == 1
    rows = real_session.execute(select(DriverRankingArtifact)).scalars().all()
    assert len(rows) == 1
    assert rows[0].measure_column_id == in_col


def test_temporal_behavior_pinned_to_run_under_coexisting_concepts(
    real_session: Session,
) -> None:
    """A stale ColumnConcept from another run MUST NOT bleed into this run's pick.

    DAT-637 regression guard: ``temporal_behavior`` is catalogue-grain (ColumnConcept,
    one row per run). A Temporal redelivery after a completed semantic_per_table leaves
    >1 concept row per column; ``_measure_columns`` must pin to ``run_id`` or it picks an
    arbitrary run's behaviour and persists a different ranking than the promoted head.
    """
    tid, measure_col = _seed(real_session, dims=CL_DIMS, behavior="additive")
    # A coexisting concept from a DIFFERENT run, the OPPOSITE behaviour.
    real_session.add(
        ColumnConcept(column_id=measure_col, run_id="stale-run", temporal_behavior="point_in_time")
    )
    real_session.flush()

    measures = _measure_columns(real_session, [tid], run_id=RUN)
    behaviours = {col_id: beh for col_id, _tid, _name, beh in measures}
    assert behaviours[measure_col] == "additive"  # THIS run's, not the stale row's
