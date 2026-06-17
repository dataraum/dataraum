"""DAT-545 P3 — driver discovery over the real catalog + enriched view.

End-to-end: seed a fact's grain-verified enriched view in DuckDB (the spike corpus
+ an exact 1:1 alias column) and the begin_session catalog in SQLite
(SliceDefinition grain_safe + a DimensionHierarchy alias group + SemanticAnnotation
temporal_behavior), then assert discover_drivers ranks the planted drivers, collapses
the alias out of the candidate set, and resolves the target type from the catalog.

The cluster-aware ``cluster_key`` path of the public ``discover_drivers`` API (DAT-552
entity grain + DAT-561 candidate-grain routing / ``secondary_dimensions``) is covered
end-to-end in ``test_grain_e2e.py`` — those tests drive the same public entry point on
clustered corpora.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import numpy as np
from sqlalchemy.orm import Session

from dataraum.analysis.drivers.models import Measure
from dataraum.analysis.drivers.processor import (
    _candidate_dims,
    discover_drivers,
    resolve_target_type,
)
from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.storage import Column, Table

from .conftest import ALL_DIMS, make_corpus

RUN = "session-run-1"
VIEW = "sales_enriched"
ALIAS = "D_e25_alias"  # an exact 1:1 copy of D_e25 → must collapse to canonical D_e25


def _seed(
    session: Session, duck: duckdb.DuckDBPyConnection, *, temporal_behavior: str = "additive"
) -> str:
    """Seed the fact, enriched view (DuckDB), catalog, alias group, and annotation."""
    df = make_corpus(np.random.default_rng(0))
    df[ALIAS] = df["D_e25"]  # 1:1 alias of a driver

    fact = Table(
        table_id=str(uuid4()),
        source_id="src-1",
        table_name="sales",
        layer="typed",
        duckdb_path="sales",
    )
    session.add(fact)
    col_id: dict[str, str] = {}
    for pos, name in enumerate([*ALL_DIMS, ALIAS, "measure"]):
        col = Column(
            column_id=str(uuid4()), table_id=fact.table_id, column_name=name, column_position=pos
        )
        session.add(col)
        col_id[name] = col.column_id

    # Catalog: every dim (incl. the alias) is a grain-safe slice definition.
    for name in [*ALL_DIMS, ALIAS]:
        session.add(
            SliceDefinition(
                run_id=RUN,
                table_id=fact.table_id,
                column_id=col_id[name],
                column_name=name,
                slice_priority=1,
                grain_safe=True,
                detection_source="llm",
            )
        )
    # DAT-537 alias group: {D_e25, D_e25_alias} canonical D_e25.
    session.add(
        DimensionHierarchy(
            run_id=RUN,
            table_id=fact.table_id,
            kind="alias",
            members=[
                {"column_name": "D_e25", "column_id": col_id["D_e25"], "distinct_count": 4},
                {"column_name": ALIAS, "column_id": col_id[ALIAS], "distinct_count": 4},
            ],
            canonical_label="D_e25",
            signature=f"alias:{fact.table_id}:D_e25|{ALIAS}",
            score=0.0,
        )
    )
    session.add(
        SemanticAnnotation(
            column_id=col_id["measure"], run_id=RUN, temporal_behavior=temporal_behavior
        )
    )
    session.add(
        EnrichedView(
            run_id=RUN, fact_table_id=fact.table_id, view_name=VIEW, is_grain_verified=True
        )
    )
    session.flush()

    duck.register("corpus_df", df)
    duck.execute(f'CREATE TABLE "{VIEW}" AS SELECT * FROM corpus_df')
    duck.unregister("corpus_df")
    return fact.table_id


class TestResolveTargetType:
    def test_maps_temporal_behavior(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed(real_session, duck, temporal_behavior="point_in_time")
        col = real_session.query(Column).filter_by(table_id=tid, column_name="measure").one()
        assert resolve_target_type(real_session, column_id=col.column_id, run_id=RUN) == "stock"

    def test_defaults_to_flow_when_unknown(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        _seed(real_session, duck, temporal_behavior="")
        assert resolve_target_type(real_session, column_id="nope", run_id=RUN) == "flow"

    def test_unrecognised_behavior_defaults_to_flow(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # An annotation present but with a value outside {additive, point_in_time}
        # falls back to flow (logged), not an error.
        tid = _seed(real_session, duck, temporal_behavior="periodic")
        col = real_session.query(Column).filter_by(table_id=tid, column_name="measure").one()
        assert resolve_target_type(real_session, column_id=col.column_id, run_id=RUN) == "flow"


class TestDiscoverDrivers:
    def test_alias_collapsed_from_candidates(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed(real_session, duck)
        dims = _candidate_dims(real_session, tid, RUN)
        assert "D_e25" in dims  # canonical kept
        assert ALIAS not in dims  # redundant axis collapsed out (DAT-537)

    def test_non_grain_safe_dims_excluded(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # A dimension flagged grain_safe=False is not a candidate (AC: grain-safety).
        tid = _seed(real_session, duck)
        real_session.execute(
            SliceDefinition.__table__.update()
            .where(SliceDefinition.column_name == "N_lowcard")
            .values(grain_safe=False)
        )
        real_session.flush()
        assert "N_lowcard" not in _candidate_dims(real_session, tid, RUN)

    def test_end_to_end_ranks_drivers(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed(real_session, duck)
        ranking = discover_drivers(
            real_session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=Measure(target_type="flow", column="measure"),
            n_perm=200,
        )
        assert ranking.n_rows == 20_000
        assert ranking.root is not None
        sig = {d for d, _ in ranking.ranked_dimensions}
        assert "D_e60" in sig  # the strong driver surfaces
        assert ALIAS not in sig  # the collapsed alias never even competed
        assert ranking.driver_paths and ranking.driver_paths[0][0] == ranking.root.dimension

    def test_no_enriched_view_returns_empty(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed(real_session, duck)
        real_session.execute(EnrichedView.__table__.update().values(is_grain_verified=False))
        real_session.flush()
        ranking = discover_drivers(
            real_session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=Measure(target_type="flow", column="measure"),
            n_perm=50,
        )
        assert ranking.root is None
        assert ranking.n_rows == 0
