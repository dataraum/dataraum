"""Stack-v4 dimension-identity discovery — DAT-761 (gate stack from DAT-757).

Deterministic FD pass over a fact's grain-verified enriched view. The shared
``seed_sales`` fixture (conftest) encodes a known ``zip → city → state`` chain, two
1:1 aliases, a constant, and a near-key id; the ``TestStackV4`` fixtures encode
the DAT-757 matrix cells the old distinct-ratio g3 could not decide (vacuous
skew, dirty-true edges, role pairs, null-coded columns, measure exclusion).
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.hierarchies.processor import discover_dimension_hierarchies
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.storage import Column

from .conftest import RUN, seed_sales, seed_view


def _rows(session: Session, table_id: str, kind: str) -> list[DimensionHierarchy]:
    return list(
        session.execute(
            select(DimensionHierarchy).where(
                DimensionHierarchy.table_id == table_id,
                DimensionHierarchy.kind == kind,
            )
        ).scalars()
    )


def _members(row: DimensionHierarchy) -> list[str]:
    return [m["column_name"] for m in row.members]


class TestDiscoverDimensionHierarchies:
    def test_drilldown_chain_finest_to_coarsest(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        assert (
            discover_dimension_hierarchies(
                real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN
            )
            > 0
        )
        drills = _rows(real_session, tid, "drilldown")
        assert len(drills) == 1
        row = drills[0]
        # Aliases collapse to canonical (zip < zip_code, state < state_name); the
        # chain is finest → coarsest with the transitive zip → state edge reduced out.
        assert _members(row) == ["zip", "city", "state"]
        assert row.canonical_label == "zip → city → state"
        assert row.score <= 0.01
        assert row.run_id == RUN
        assert row.needs_confirmation is False

    def test_one_to_one_aliases_collapse(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        aliases = {tuple(_members(r)): r for r in _rows(real_session, tid, "alias")}
        assert ("zip", "zip_code") in aliases
        assert ("state", "state_name") in aliases
        # Canonical = lexicographically first member.
        assert aliases[("zip", "zip_code")].canonical_label == "zip"
        assert aliases[("state", "state_name")].canonical_label == "state"

    def test_degenerate_and_near_key_excluded(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        all_members = {
            m
            for r in _rows(real_session, tid, "drilldown") + _rows(real_session, tid, "alias")
            for m in _members(r)
        }
        # 'country' is a constant (dropped both roles); 'order_id' is unique (near-key,
        # never a determinant) — neither appears in any structure.
        assert "country" not in all_members
        assert "order_id" not in all_members

    def test_low_support_flags_needs_confirmation(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Below MIN_SUPPORT_ROWS (100): the chain is found but flagged, not asserted.
        tid = seed_sales(real_session, duck, rows_per_zip=2)  # 12 rows
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        drills = _rows(real_session, tid, "drilldown")
        assert drills and all(r.needs_confirmation for r in drills)

    def test_rerun_is_idempotent(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Success-redelivery (same run_id) converges by upsert on (signature, run_id)."""
        tid = seed_sales(real_session, duck)
        first = discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN
        )
        real_session.commit()
        second = discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN
        )
        real_session.commit()
        assert first == second
        rows = real_session.execute(select(DimensionHierarchy)).scalars().all()
        assert len(rows) == first

    def test_no_enriched_view_no_rows(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Without a grain-verified enriched view there is no queryable substrate.
        tid = seed_sales(real_session, duck)
        real_session.execute(EnrichedView.__table__.update().values(is_grain_verified=False))
        real_session.flush()
        assert (
            discover_dimension_hierarchies(
                real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN
            )
            == 0
        )


class TestStackV4:
    """The DAT-757 cells the distinct-ratio g3 could not decide (DAT-761)."""

    def test_vacuous_skew_killed_true_skew_edge_kept(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """λ ≥ 0.5 kills the vacuous-skew edge; the exact FD onto a skewed flag survives.

        ``vacuous`` is 99.5%-dominant with its minority spread evenly across
        ``det`` groups: row-g3 = 0.005 passes the effect screen vacuously, but
        λ ≈ 0 (no reduction over the majority baseline). ``flag`` is 98%-dominant
        but an exact FD of ``det`` — λ = 1, the edge must survive (the naive
        "skip skewed dependents" rule would have killed it).
        """
        n = 10_000
        det = [f"d{i % 50}" for i in range(n)]
        vacuous = ["1" if (i // 50) % 200 == 0 else "0" for i in range(n)]
        flag = ["1" if i % 50 == 0 else "0" for i in range(n)]
        tid = seed_view(
            real_session, duck, "skewed", {"det": det, "vacuous": vacuous, "flag": flag}
        )
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        chains = [_members(r) for r in _rows(real_session, tid, "drilldown")]
        assert ["det", "flag"] in chains
        assert all("vacuous" not in c for c in chains)

    def test_dirty_true_edge_kept(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A true hierarchy with 0.2% dirty rows stays asserted (row-g3 tolerance)."""
        n = 5_000
        city = [f"c{i % 40}" for i in range(n)]
        state = [f"s{((i % 40) // 5 + 1) % 8}" if i % 500 == 0 else f"s{(i % 40) // 5}" for i in range(n)]
        tid = seed_view(real_session, duck, "dirty_geo", {"city": city, "state": state})
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        drills = _rows(real_session, tid, "drilldown")
        assert [_members(r) for r in drills] == [["city", "state"]]
        assert 0.0 < drills[0].score <= 0.01

    def test_role_pair_kept_apart(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A role-playing near-copy (bill-to ≠ sold-to on dropship rows) is persisted
        as ``kind='role'`` — never merged as an alias, never stacked as an edge.

        Pairwise, ``soldto``/``billto`` agree on 99.25% of rows and pass the alias
        screen; the disagreement set is fully driven by ``channel`` (T1), which is
        exactly the SAP SALT BILLTO↔PAYER over-merge the round caught and reversed.
        """
        n_cust, rows_per = 800, 10
        soldto, billto, channel = [], [], []
        for r in range(n_cust * rows_per):
            c = r % n_cust
            drop = c < 6
            soldto.append(f"c{c}")
            billto.append("hub" if drop else f"c{c}")
            channel.append("dropship" if drop else "standard")
        tid = seed_view(
            real_session, duck, "orders",
            {"soldto": soldto, "billto": billto, "channel": channel},
        )
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        roles = _rows(real_session, tid, "role")
        assert [_members(r) for r in roles] == [["billto", "soldto"]]
        assert roles[0].needs_confirmation is False
        # Not merged, and no drilldown stacks the two role siblings as levels.
        for r in _rows(real_session, tid, "alias") + _rows(real_session, tid, "drilldown"):
            assert not {"soldto", "billto"} <= set(_members(r))

    def test_partial_null_edge_rescued_by_pairwise_deletion(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The null-policy lane, edge arm: 10% join-miss NULLs in the dependent
        used to cost the whole edge (row-g3 = 0.10 under null-as-category);
        pairwise deletion recovers the exact FD over the complete rows."""
        n = 5_000
        city = [f"c{i % 40}" for i in range(n)]
        state = [None if i % 10 == 0 else f"s{(i % 40) // 5}" for i in range(n)]
        tid = seed_view(real_session, duck, "null_geo", {"city": city, "state": state})
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        drills = _rows(real_session, tid, "drilldown")
        assert [_members(r) for r in drills] == [["city", "state"]]

    def test_null_coded_columns_alias(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The null-policy lane: ``{1, NULL}`` columns are axes, not constants.

        SQL ``COUNT(DISTINCT)`` sees 1 distinct value and the old pass dropped
        them silently (the rel-hm FN/Active lesson); null-as-category keeps them
        eligible and the exact copy merges as an alias.
        """
        n = 3_000
        fn = ["1" if i % 7 < 2 else None for i in range(n)]
        city = [f"c{i % 30}" for i in range(n)]
        tid = seed_view(
            real_session, duck, "customers", {"fn": fn, "active": list(fn), "city": city}
        )
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        aliases = [_members(r) for r in _rows(real_session, tid, "alias")]
        assert ["active", "fn"] in aliases

    def test_measure_columns_excluded(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The additivity lane: a ``semantic_role='measure'`` column never enters
        FD discovery, even when it would form a perfect structure."""
        n = 600
        zips = [f"z{i % 6}" for i in range(n)]
        amount = [str((i % 6) * 100) for i in range(n)]
        city = [f"c{i % 3}" for i in range(n)]
        tid = seed_view(
            real_session, duck, "billing", {"zip": zips, "amount": amount, "city": city}
        )
        amount_id = real_session.execute(
            select(Column.column_id).where(Column.table_id == tid, Column.column_name == "amount")
        ).scalar_one()
        real_session.add(
            SemanticAnnotation(
                annotation_id=str(uuid4()), column_id=amount_id, run_id=RUN,
                semantic_role="measure",
            )
        )
        real_session.flush()
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        all_members = {
            m
            for kind in ("drilldown", "alias", "role")
            for r in _rows(real_session, tid, kind)
            for m in _members(r)
        }
        assert "amount" not in all_members
        assert "zip" in all_members  # the zip → city structure itself is found

    def test_unregistered_column_participates(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A view column with no catalog ``Column`` row still participates —
        provenance is metadata (``column_id=''``), not a gate (the widened
        candidate universe is the view, not the slice catalog)."""
        n = 500
        state = [f"s{i % 8}" for i in range(n)]
        tid = seed_view(
            real_session, duck, "wide", {"state": state, "mystery": list(state)},
            register={"state"},
        )
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)
        aliases = _rows(real_session, tid, "alias")
        assert [_members(r) for r in aliases] == [["mystery", "state"]]
        by_name = {m["column_name"]: m["column_id"] for m in aliases[0].members}
        assert by_name["mystery"] == "" and by_name["state"] != ""


class TestDimensionHierarchiesPhaseSkip:
    """The phase's should_skip preconditions (deterministic re-run discipline)."""

    def _ctx(self, session: Session, duck: duckdb.DuckDBPyConnection, table_ids: list[str]):
        from dataraum.pipeline.base import PhaseContext

        return PhaseContext(
            session=session,
            duckdb_conn=duck,
            table_ids=table_ids,
            run_id=RUN,
            config={},
        )

    def test_skips_when_no_enriched_view(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        from dataraum.pipeline.phases.dimension_hierarchies_phase import DimensionHierarchiesPhase

        # A table id with no grain-verified enriched view → no substrate.
        reason = DimensionHierarchiesPhase().should_skip(self._ctx(real_session, duck, ["nope"]))
        assert reason is not None and "enriched views" in reason

    def test_does_not_skip_with_catalog(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        from dataraum.pipeline.phases.dimension_hierarchies_phase import DimensionHierarchiesPhase

        tid = seed_sales(real_session, duck)
        assert DimensionHierarchiesPhase().should_skip(self._ctx(real_session, duck, [tid])) is None
