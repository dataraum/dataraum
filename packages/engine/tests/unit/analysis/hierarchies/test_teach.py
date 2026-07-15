"""Hierarchy/alias teach materialization — DAT-537.

The teach mirrors the relationship-overlay pattern minus keeper-lift-up + witness
(g3 is deterministic). A ``ConfigOverlay(type='hierarchy')`` reject suppresses a
g3 structure; add asserts a ``manual`` drilldown; alias asserts a ``manual`` alias.
These run inside ``discover_dimension_hierarchies`` against the shared fixture.
"""

from __future__ import annotations

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.hierarchies.processor import discover_dimension_hierarchies
from dataraum.storage import ConfigOverlay

from .conftest import RUN, seed_sales


def _teach(session: Session, action: str, table_id: str, members: list[str]) -> None:
    session.add(
        ConfigOverlay(
            type="hierarchy",
            payload={"action": action, "table_id": table_id, "members": members},
        )
    )
    session.flush()


def _discover(session: Session, duck: duckdb.DuckDBPyConnection, tid: str) -> int:
    return discover_dimension_hierarchies(session, duckdb_conn=duck, table_ids=[tid], run_id=RUN)


def _by_members(session: Session, kind: str) -> dict[tuple[str, ...], DimensionHierarchy]:
    rows = session.execute(
        select(DimensionHierarchy).where(DimensionHierarchy.kind == kind)
    ).scalars()
    return {tuple(m["column_name"] for m in r.members): r for r in rows}


class TestHierarchyTeach:
    def test_reject_suppresses_g3_drilldown(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        # The g3 pass finds the zip/city/state chain; rejecting it (by member set,
        # any order) drops the structure this run. Stored coarse → fine (DAT-779).
        _teach(real_session, "reject", tid, ["state", "zip", "city"])
        _discover(real_session, duck, tid)
        assert ("state", "city", "zip") not in _by_members(real_session, "drilldown")

    def test_add_materializes_manual_drilldown(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        # Assert a chain the g3 pass would not surface on its own. The teach INPUT is
        # finest → coarsest (``city, state``); STORAGE is coarse → fine with explicit
        # levels (DAT-779), so it lands as ``state → city``.
        _teach(real_session, "add", tid, ["city", "state"])
        _discover(real_session, duck, tid)
        row = _by_members(real_session, "drilldown")[("state", "city")]
        assert row.detection_source == "manual"
        assert row.needs_confirmation is False
        assert row.canonical_label == "state → city"
        assert [m["level"] for m in row.members] == [0, 1]
        assert row.g3 == 0.0  # a manual add asserts an exact FD
        # The catalog resolves the member column ids (provenance), not left blank.
        assert all(m["column_id"] for m in row.members)

    def test_alias_materializes_manual_alias(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = seed_sales(real_session, duck)
        _teach(real_session, "alias", tid, ["city", "state_name"])
        _discover(real_session, duck, tid)
        row = _by_members(real_session, "alias")[("city", "state_name")]
        assert row.detection_source == "manual"
        assert row.canonical_label == "city"

    def test_superseded_teach_is_ignored(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        from datetime import UTC, datetime

        tid = seed_sales(real_session, duck)
        overlay = ConfigOverlay(
            type="hierarchy",
            payload={"action": "reject", "table_id": tid, "members": ["zip", "city", "state"]},
            superseded_at=datetime.now(UTC),
        )
        real_session.add(overlay)
        real_session.flush()
        _discover(real_session, duck, tid)
        # The reject was undone (superseded) → the g3 chain survives (coarse → fine).
        assert ("state", "city", "zip") in _by_members(real_session, "drilldown")
