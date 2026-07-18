"""Shared fixtures for the hierarchy-discovery tests (DAT-537).

Metadata lives in in-memory SQLite (FKs off, the resolve-test pattern); the
queryable enriched view is an in-memory DuckDB table seeded by ``seed_sales`` so a
known ``zip → city → state`` chain, two 1:1 aliases, a constant, and a near-key id
are present for both the g3 and the teach tests.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.hierarchies.judge import AliasIdentityVerdict
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table, init_database

RUN = "session-run-1"


class StubIdentityJudge:
    """A ``DimensionIdentityJudge`` stand-in for the discovery tests.

    Discovery calls only ``alias_identity``; this returns one verdict per
    candidate at the configured ``confidence`` (default: 0.95 — a clear alias, so
    an existing alias test that expects a relabeling bijection to merge still
    passes). The verdict is confidence-only (DAT-762): a low ``confidence`` (e.g.
    0.03) exercises the coincidental-bijection surface path; ``fail=True`` returns
    a failed Result (the judge-unavailable posture — the pair must surface, never
    merge). ``calls`` records each candidate batch.
    """

    def __init__(self, *, confidence: float = 0.95, fail: bool = False) -> None:
        self._conf = confidence
        self._fail = fail
        self.calls: list[list[dict]] = []

    def alias_identity(self, *, candidates: list[dict]) -> Result[list[AliasIdentityVerdict]]:
        self.calls.append(candidates)
        if self._fail:
            return Result.fail("stub judge unavailable")
        return Result.ok(
            [
                AliasIdentityVerdict(pair_ref=c["ref"], confidence=self._conf, reason="stub")
                for c in candidates
            ]
        )


def approving_judge() -> StubIdentityJudge:
    """Fresh stub that approves every relabeling bijection (default discovery judge)."""
    return StubIdentityJudge()


VIEW = "sales_enriched"

# zip → (city, state): multiple zips per city, multiple cities per state — a real
# FD chain (not a 1:1 bijection, which would read as an alias instead).
ZIP_MAP = {
    "07001": ("newark", "nj"),
    "07002": ("newark", "nj"),
    "07003": ("jersey", "nj"),
    "10001": ("nyc", "ny"),
    "10002": ("nyc", "ny"),
    "10003": ("albany", "ny"),
}
STATE_NAME = {"nj": "New Jersey", "ny": "New York"}
DIMS = ["zip", "zip_code", "city", "state", "state_name", "country", "order_id"]


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


def seed_view(
    session: Session,
    duck: duckdb.DuckDBPyConnection,
    view_name: str,
    columns: dict[str, list],
    *,
    register: set[str] | None = None,
) -> str:
    """Seed a fact + grain-verified enriched view from raw column lists.

    ``columns`` maps view column name → values (None = SQL NULL). ``register``
    names the columns that get catalog ``Column`` rows (default: all) — a column
    left out exercises the unresolved-provenance path (``column_id=""``).
    Returns the fact ``table_id``.
    """
    table = Table(
        table_id=str(uuid4()),
        source_id="src-1",
        table_name=view_name,
        layer="typed",
        duckdb_path=view_name,
    )
    session.add(table)
    for pos, name in enumerate(columns):
        if register is not None and name not in register:
            continue
        session.add(
            Column(
                column_id=str(uuid4()),
                table_id=table.table_id,
                column_name=name,
                column_position=pos,
                resolved_type="VARCHAR",
            )
        )
    session.add(
        EnrichedView(
            run_id=RUN, fact_table_id=table.table_id, view_name=view_name, is_grain_verified=True
        )
    )
    session.flush()

    names = list(columns)
    duck.execute(f'CREATE TABLE "{view_name}" (' + ", ".join(f'"{n}" VARCHAR' for n in names) + ")")
    rows = list(zip(*columns.values(), strict=True))
    duck.executemany(
        f'INSERT INTO "{view_name}" VALUES ({", ".join("?" for _ in names)})',  # noqa: S608
        [[None if v is None else str(v) for v in row] for row in rows],
    )
    return table.table_id


def seed_sales(session: Session, duck: duckdb.DuckDBPyConnection, *, rows_per_zip: int = 20) -> str:
    """Seed the fact, its grain-verified enriched view, the catalog, and DuckDB rows.

    ``zip_code`` is a 1:1 copy of ``zip`` and ``state_name`` of ``state`` (alias
    groups); ``country`` is constant; ``order_id`` is unique (near-key). Returns the
    fact ``table_id``.
    """
    table = Table(
        table_id=str(uuid4()),
        source_id="src-1",
        table_name="sales",
        layer="typed",
        duckdb_path="sales",
    )
    session.add(table)
    for pos, name in enumerate(DIMS):
        column = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name=name,
            column_position=pos,
            resolved_type="VARCHAR",
        )
        session.add(column)
        session.add(
            SliceDefinition(
                run_id=RUN,
                table_id=table.table_id,
                column_id=column.column_id,
                column_name=name,
                slice_priority=1,
                slice_type="categorical",
                detection_source="llm",
            )
        )
    session.add(
        EnrichedView(
            run_id=RUN, fact_table_id=table.table_id, view_name=VIEW, is_grain_verified=True
        )
    )
    session.flush()

    duck.execute(
        f"CREATE TABLE {VIEW} ("
        "zip VARCHAR, zip_code VARCHAR, city VARCHAR, state VARCHAR, "
        "state_name VARCHAR, country VARCHAR, order_id BIGINT)"
    )
    values: list[str] = []
    oid = 0
    for _ in range(rows_per_zip):
        for zip_code, (city, state) in ZIP_MAP.items():
            oid += 1
            values.append(
                f"('{zip_code}', '{zip_code}', '{city}', '{state}', "
                f"'{STATE_NAME[state]}', 'us', {oid})"
            )
    duck.execute(f"INSERT INTO {VIEW} VALUES {', '.join(values)}")  # noqa: S608 — test data
    return table.table_id
