"""DAT-545 P3 — driver discovery over the real catalog + enriched view.

End-to-end: seed a fact's grain-verified enriched view in DuckDB (the spike corpus
+ an exact 1:1 alias column) and the begin_session catalog in SQLite
(SliceDefinition slices + a DimensionHierarchy alias group + SemanticAnnotation
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
from structlog.testing import capture_logs

from dataraum.analysis.drivers.models import Measure
from dataraum.analysis.drivers.processor import (
    _candidate_dims,
    discover_drivers,
    resolve_target_type,
)
from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.semantic.db_models import ColumnConcept
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
    df = df.with_columns(df["D_e25"].alias(ALIAS))  # 1:1 alias of a driver

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

    # Catalog: every dim (incl. the alias) is a slice definition.
    for name in [*ALL_DIMS, ALIAS]:
        session.add(
            SliceDefinition(
                run_id=RUN,
                table_id=fact.table_id,
                column_id=col_id[name],
                column_name=name,
                slice_priority=1,
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
                {
                    "column_name": "D_e25",
                    "column_id": col_id["D_e25"],
                    "distinct_count": 4,
                    "level": 0,
                },
                {"column_name": ALIAS, "column_id": col_id[ALIAS], "distinct_count": 4, "level": 1},
            ],
            canonical_label="D_e25",
            signature=f"alias:{fact.table_id}:D_e25|{ALIAS}",
            g3=0.0,
        )
    )
    session.add(
        ColumnConcept(column_id=col_id["measure"], run_id=RUN, temporal_behavior=temporal_behavior)
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

    def test_needs_confirmation_alias_not_collapsed(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A needs_confirmation alias (the DAT-762 identity judge declined the merge,
        or a role-check near-copy) is an UNCONFIRMED redundancy — collapsing it would
        drop a real axis the flag says we are unsure about, so BOTH members survive."""
        tid = _seed(real_session, duck)
        a, b = [d for d in ALL_DIMS if d != "D_e25"][:2]
        real_session.add(
            DimensionHierarchy(
                run_id=RUN,
                table_id=tid,
                kind="alias",
                members=[
                    {"column_name": a, "column_id": "", "distinct_count": 3, "level": 0},
                    {"column_name": b, "column_id": "", "distinct_count": 3, "level": 1},
                ],
                canonical_label=a,
                signature=f"alias:{tid}:{a}|{b}",
                needs_confirmation=True,
                identity_confidence=0.03,
            )
        )
        real_session.flush()
        dims = _candidate_dims(real_session, tid, RUN)
        assert a in dims and b in dims  # unconfirmed redundancy → both axes kept

    def test_unelected_canonical_keeps_surviving_member(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-806: when the alias canonical is a raw-FK near-key the slicing gate
        excluded (never a SliceDefinition), the surviving ELECTED member is the
        dimension's only representative — it must NOT be discarded against the absent
        canonical, which would orphan the axis and starve the driver-tree
        (``too_few_candidates``)."""
        tid = _seed(real_session, duck)
        member = next(d for d in ALL_DIMS if d != "D_e25")  # an elected slice
        real_session.add(
            DimensionHierarchy(
                run_id=RUN,
                table_id=tid,
                kind="alias",
                members=[
                    {
                        "column_name": "account_id",
                        "column_id": "",
                        "distinct_count": 900,
                        "level": 0,
                    },
                    {"column_name": member, "column_id": "", "distinct_count": 900, "level": 1},
                ],
                canonical_label="account_id",  # a near-key FK — excluded from slices
                signature=f"alias:{tid}:account_id|{member}",
                g3=0.0,
            )
        )
        real_session.flush()
        dims = _candidate_dims(real_session, tid, RUN)
        assert member in dims  # surviving elected member kept, not orphaned
        assert "account_id" not in dims  # the un-elected canonical never was a candidate

    def test_multi_member_class_collapses_to_one_when_canonical_unelected(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-806: a ≥2-elected alias class whose canonical (a near-key FK) is NOT a
        slice collapses to exactly ONE elected representative — not zero (orphan), not
        two (double-count). Exercises the discard branch the single-member case skips."""
        tid = _seed(real_session, duck)
        a, b = [d for d in ALL_DIMS if d != "D_e25"][:2]  # two elected slices
        real_session.add(
            DimensionHierarchy(
                run_id=RUN,
                table_id=tid,
                kind="alias",
                members=[
                    {
                        "column_name": "account_id",
                        "column_id": "",
                        "distinct_count": 900,
                        "level": 0,
                    },
                    {"column_name": a, "column_id": "", "distinct_count": 900, "level": 1},
                    {"column_name": b, "column_id": "", "distinct_count": 900, "level": 2},
                ],
                canonical_label="account_id",  # un-elected near-key FK
                signature=f"alias:{tid}:account_id|{a}|{b}",
                g3=0.0,
            )
        )
        real_session.flush()
        dims = _candidate_dims(real_session, tid, RUN)
        assert len([d for d in (a, b) if d in dims]) == 1  # exactly one survives
        assert "account_id" not in dims

    def test_overlapping_confirmed_groups_collapse_order_independently(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-806 (review): two confirmed alias groups sharing a member (a manual
        teach overlapping an auto group — ``needs_confirmation=False``, no overlap
        guard) form ONE equivalence class → exactly one survivor, independent of the
        ORDER-BY-less query's row order. The per-group collapse yields two survivors."""
        tid = _seed(real_session, duck)
        a, b, c = [d for d in ALL_DIMS if d != "D_e25"][:3]  # three elected slices
        for members, canon, sig in (
            ([a, b], a, f"alias:{tid}:{a}|{b}"),  # G1: a ≡ b
            ([b, c], c, f"alias:{tid}:{b}|{c}"),  # G2: b ≡ c → a~b~c is one class
        ):
            real_session.add(
                DimensionHierarchy(
                    run_id=RUN,
                    table_id=tid,
                    kind="alias",
                    members=[
                        {"column_name": m, "column_id": "", "distinct_count": 3, "level": i}
                        for i, m in enumerate(members)
                    ],
                    canonical_label=canon,
                    signature=sig,
                    g3=0.0,
                )
            )
        real_session.flush()
        dims = _candidate_dims(real_session, tid, RUN)
        assert len([d for d in (a, b, c) if d in dims]) == 1  # one class → one rep

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

    def test_dirty_varchar_measure_does_not_crash(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A measure the typing left VARCHAR (e.g. carrying null sentinels like '~~~~~'
        from a null_tokens injection) must NOT crash driver discovery. The projection
        TRY_CASTs measures, so unparseable values load as NaN (treated as missing by the
        numpy core), not a hard ConversionException → PhaseFailed. Regression: the
        detection-null-v1 begin_session crashed here (driver_rankings, '::DOUBLE')."""
        tid = _seed(real_session, duck)
        # Re-cast the measure to VARCHAR with a deterministic sentinel in ~1/137 of rows,
        # mirroring a null_tokens injection the typing left as a string column.
        duck.execute(
            f'CREATE OR REPLACE TABLE "{VIEW}" AS '
            f"SELECT * REPLACE (CASE WHEN rowid % 137 = 0 THEN '~~~~~' "
            f'ELSE CAST(measure AS VARCHAR) END AS measure) FROM "{VIEW}"'
        )
        ranking = discover_drivers(
            real_session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=Measure(target_type="flow", column="measure"),
            n_perm=50,
        )
        # No raise; the sentinel rows are kept as NaN (n_rows == full frame length).
        assert ranking.n_rows == 20_000

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


class TestRowCountGate:
    """DAT-571: bound the in-memory frame by bottom-k-by-hash sampling over-large views."""

    def test_oversized_view_is_sampled_and_logged(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # The 20k-row corpus with a 5k cap → the materialized frame is capped at 5k rows,
        # and a loud log names the full + sampled counts (the COUNT(*) drives the gate).
        # n_rows == frame length (NaN measure rows included — FlowTarget.observed keeps the
        # raw array), so this pins the bottom-k LIMIT, not a finite-value count.
        tid = _seed(real_session, duck)
        with capture_logs() as logs:
            ranking = discover_drivers(
                real_session,
                duckdb_conn=duck,
                fact_table_id=tid,
                run_id=RUN,
                measure=Measure(target_type="flow", column="measure"),
                n_perm=200,
                max_rows=5_000,
            )
        assert ranking.n_rows == 5_000
        sampled = [e for e in logs if e["event"] == "driver_rankings_view_sampled"]
        assert sampled and sampled[0]["full_n"] == 20_000 and sampled[0]["sample_n"] == 5_000

    def test_normal_view_takes_full_load_path(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # At/below the cap → the validated full-load path, untouched: every row, no log.
        tid = _seed(real_session, duck)
        with capture_logs() as logs:
            ranking = discover_drivers(
                real_session,
                duckdb_conn=duck,
                fact_table_id=tid,
                run_id=RUN,
                measure=Measure(target_type="flow", column="measure"),
                n_perm=50,
                max_rows=1_000_000,
            )
        assert ranking.n_rows == 20_000
        assert not [e for e in logs if e["event"] == "driver_rankings_view_sampled"]

    def test_sampled_ranking_is_deterministic_and_keeps_strong_driver(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Bottom-k-by-hash is a total order → identical ranking across runs (no REPEATABLE
        # or thread dependence). And the sample preserves the ordinal signal: the strong
        # driver still surfaces (recall) while independent nulls stay gated (precision).
        tid = _seed(real_session, duck)
        measure = Measure(target_type="flow", column="measure")
        first = discover_drivers(
            real_session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=measure,
            n_perm=200,
            max_rows=5_000,
        )
        second = discover_drivers(
            real_session,
            duckdb_conn=duck,
            fact_table_id=tid,
            run_id=RUN,
            measure=measure,
            n_perm=200,
            max_rows=5_000,
        )
        assert first.ranked_dimensions == second.ranked_dimensions  # deterministic sample
        sig = {d for d, _ in first.ranked_dimensions}
        assert "D_e60" in sig  # strong driver survives sampling (recall)
        assert "N_lowcard" not in sig and "N_highcard" not in sig  # nulls gated (precision)
