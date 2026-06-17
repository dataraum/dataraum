"""g3 dimension-hierarchy / alias discovery — DAT-537.

Deterministic FD pass over a fact's grain-verified enriched view. The shared
``seed_sales`` fixture (conftest) encodes a known ``zip → city → state`` chain, two
1:1 aliases, a constant, and a near-key id, so the verdicts (chain, alias collapse,
guards) are checkable.
"""

from __future__ import annotations

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.hierarchies.processor import discover_dimension_hierarchies
from dataraum.analysis.views.db_models import EnrichedView

from .conftest import RUN, seed_sales


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
