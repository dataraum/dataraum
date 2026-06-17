"""g3 dimension-hierarchy / alias discovery — DAT-537.

Deterministic FD pass over a fact's grain-verified enriched view. The fixture
seeds metadata in in-memory SQLite (FKs off, the resolve-test pattern) and the
queryable enriched view as an in-memory DuckDB table whose rows encode a known
``zip → city → state`` chain, two 1:1 aliases, a degenerate constant, and a
near-key id — so the verdicts (chain, alias collapse, guards) are checkable.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.hierarchies.processor import discover_dimension_hierarchies
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.storage import Column, Table, init_database

_RUN = "session-run-1"
_VIEW = "sales_enriched"

# zip → (city, state): multiple zips per city, multiple cities per state — a real
# FD chain (not a 1:1 bijection, which would read as an alias instead).
_ZIP_MAP = {
    "07001": ("newark", "nj"),
    "07002": ("newark", "nj"),
    "07003": ("jersey", "nj"),
    "10001": ("nyc", "ny"),
    "10002": ("nyc", "ny"),
    "10003": ("albany", "ny"),
}
_STATE_NAME = {"nj": "New Jersey", "ny": "New York"}
# The catalog's grain-safe slice dimensions on the enriched view.
_DIMS = ["zip", "zip_code", "city", "state", "state_name", "country", "order_id"]


@pytest.fixture
def real_session() -> Iterator[Session]:
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _record):  # noqa: ANN001, ANN202
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    try:
        with factory() as s:
            yield s
    finally:
        engine.dispose()


@pytest.fixture
def duck() -> Iterator[duckdb.DuckDBPyConnection]:
    conn = duckdb.connect(":memory:")
    try:
        yield conn
    finally:
        conn.close()


def _seed(session: Session, duck: duckdb.DuckDBPyConnection, *, rows_per_zip: int = 20) -> str:
    """Seed the fact, its grain-verified enriched view, the catalog, and DuckDB rows.

    ``zip_code`` is a 1:1 copy of ``zip`` and ``state_name`` of ``state`` (alias
    groups); ``country`` is constant (degenerate); ``order_id`` is unique (near-key).
    Returns the fact ``table_id``.
    """
    table = Table(
        table_id=str(uuid4()),
        source_id="src-1",
        table_name="sales",
        layer="typed",
        duckdb_path="sales",
    )
    session.add(table)
    col_ids: dict[str, str] = {}
    for pos, name in enumerate(_DIMS):
        column = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name=name,
            column_position=pos,
            resolved_type="VARCHAR",
        )
        session.add(column)
        col_ids[name] = column.column_id
        session.add(
            SliceDefinition(
                run_id=_RUN,
                table_id=table.table_id,
                column_id=column.column_id,
                column_name=name,
                slice_priority=1,
                slice_type="categorical",
                grain_safe=True,
                detection_source="llm",
            )
        )
    session.add(
        EnrichedView(
            run_id=_RUN,
            fact_table_id=table.table_id,
            view_name=_VIEW,
            is_grain_verified=True,
        )
    )
    session.flush()

    duck.execute(
        f"CREATE TABLE {_VIEW} ("
        "zip VARCHAR, zip_code VARCHAR, city VARCHAR, state VARCHAR, "
        "state_name VARCHAR, country VARCHAR, order_id BIGINT)"
    )
    values: list[str] = []
    oid = 0
    for _ in range(rows_per_zip):
        for zip_code, (city, state) in _ZIP_MAP.items():
            oid += 1
            values.append(
                f"('{zip_code}', '{zip_code}', '{city}', '{state}', "
                f"'{_STATE_NAME[state]}', 'us', {oid})"
            )
    duck.execute(f"INSERT INTO {_VIEW} VALUES {', '.join(values)}")  # noqa: S608 — test data
    return table.table_id


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
        tid = _seed(real_session, duck)
        assert (
            discover_dimension_hierarchies(
                real_session, duckdb_conn=duck, table_ids=[tid], run_id=_RUN
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
        assert row.run_id == _RUN
        assert row.needs_confirmation is False

    def test_one_to_one_aliases_collapse(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed(real_session, duck)
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=_RUN)
        aliases = {tuple(_members(r)): r for r in _rows(real_session, tid, "alias")}
        assert ("zip", "zip_code") in aliases
        assert ("state", "state_name") in aliases
        # Canonical = lexicographically first member.
        assert aliases[("zip", "zip_code")].canonical_label == "zip"
        assert aliases[("state", "state_name")].canonical_label == "state"

    def test_degenerate_and_near_key_excluded(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        tid = _seed(real_session, duck)
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=_RUN)
        all_members = {
            m
            for r in _rows(real_session, tid, "drilldown") + _rows(real_session, tid, "alias")
            for m in _members(r)
        }
        # 'country' is a constant (degenerate, dropped both roles); 'order_id' is
        # unique (near-key, never a determinant) — neither appears in any structure.
        assert "country" not in all_members
        assert "order_id" not in all_members

    def test_low_support_flags_needs_confirmation(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Below MIN_SUPPORT_ROWS (100): the chain is found but flagged, not asserted.
        tid = _seed(real_session, duck, rows_per_zip=2)  # 12 rows
        discover_dimension_hierarchies(real_session, duckdb_conn=duck, table_ids=[tid], run_id=_RUN)
        drills = _rows(real_session, tid, "drilldown")
        assert drills and all(r.needs_confirmation for r in drills)

    def test_rerun_is_idempotent(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Success-redelivery (same run_id) converges by upsert on (signature, run_id)."""
        tid = _seed(real_session, duck)
        first = discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=_RUN
        )
        real_session.commit()
        second = discover_dimension_hierarchies(
            real_session, duckdb_conn=duck, table_ids=[tid], run_id=_RUN
        )
        real_session.commit()
        assert first == second
        rows = real_session.execute(select(DimensionHierarchy)).scalars().all()
        assert len(rows) == first

    def test_no_enriched_view_no_rows(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # Without a grain-verified enriched view there is no queryable substrate.
        tid = _seed(real_session, duck)
        real_session.execute(EnrichedView.__table__.update().values(is_grain_verified=False))
        real_session.flush()
        assert (
            discover_dimension_hierarchies(
                real_session, duckdb_conn=duck, table_ids=[tid], run_id=_RUN
            )
            == 0
        )
