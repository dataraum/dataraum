"""``_store_candidates`` is idempotent under at-least-once redelivery (DAT-502).

A Temporal activity can commit then crash before acking, re-running with the
SAME ``run_id``. The candidate writer is a form-(a) upsert on
``uq_relationship_columns_method`` — no run-scoped clear: a redelivered batch
converges on the same rows (in-batch dedup + ON CONFLICT update), and a prior
run's candidates stay untouched.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.detector import _store_candidates
from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate
from dataraum.storage import Column, Table, init_database


@pytest.fixture
def session_factory():
    """In-memory SQLite engine with all tables; FKs off so parent rows are optional."""
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
        yield factory
    finally:
        engine.dispose()


def _seed(factory: Any) -> None:
    with factory() as session:
        session.add_all(
            [
                Table(
                    table_id="t-orders",
                    source_id="src-1",
                    table_name="orders",
                    layer="typed",
                    duckdb_path="orders",
                ),
                Table(
                    table_id="t-customers",
                    source_id="src-1",
                    table_name="customers",
                    layer="typed",
                    duckdb_path="customers",
                ),
                Column(
                    column_id="col-o-cust",
                    table_id="t-orders",
                    column_name="customer_id",
                    column_position=0,
                ),
                Column(
                    column_id="col-c-id",
                    table_id="t-customers",
                    column_name="id",
                    column_position=0,
                ),
            ]
        )
        session.commit()


def _candidates(confidence: float = 0.8) -> list[RelationshipCandidate]:
    return [
        RelationshipCandidate(
            table1="orders",
            table2="customers",
            join_candidates=[
                JoinCandidate(
                    column1="customer_id",
                    column2="id",
                    join_confidence=confidence,
                    cardinality="many-to-one",
                )
            ],
        )
    ]


_TABLE_IDS = ["t-orders", "t-customers"]


def test_redelivery_same_run_converges(session_factory: Any) -> None:
    """Re-running the committed writer body with the SAME run_id updates in place."""
    _seed(session_factory)

    with session_factory() as session:
        _store_candidates(session, _TABLE_IDS, _candidates(0.8), run_id="run-A")
        session.commit()

    # The at-least-once redelivery: same run_id, freshly recomputed batch.
    with session_factory() as session:
        _store_candidates(session, _TABLE_IDS, _candidates(0.9), run_id="run-A")
        session.commit()

    with session_factory() as session:
        rows = list(session.execute(select(Relationship)).scalars())
    assert len(rows) == 1  # converged, no duplicate
    assert rows[0].run_id == "run-A"
    assert rows[0].confidence == 0.9  # the redelivered batch's value won


def test_prior_run_untouched_and_coexists(session_factory: Any) -> None:
    """A new run's candidates land alongside a prior run's, never clearing them."""
    _seed(session_factory)

    with session_factory() as session:
        _store_candidates(session, _TABLE_IDS, _candidates(0.8), run_id="run-A")
        session.commit()
    with session_factory() as session:
        _store_candidates(session, _TABLE_IDS, _candidates(0.7), run_id="run-B")
        session.commit()

    with session_factory() as session:
        rows = list(session.execute(select(Relationship)).scalars())
        total = session.scalar(select(func.count()).select_from(Relationship))
    assert total == 2
    by_run = {r.run_id: r for r in rows}
    assert by_run["run-A"].confidence == 0.8  # prior run untouched
    assert by_run["run-B"].confidence == 0.7


def test_in_batch_duplicate_key_dedups(session_factory: Any) -> None:
    """The same column pair surfacing twice in one batch writes one row (last wins)."""
    _seed(session_factory)

    batch = _candidates(0.5) + _candidates(0.6)
    with session_factory() as session:
        _store_candidates(session, _TABLE_IDS, batch, run_id="run-A")
        session.commit()

    with session_factory() as session:
        rows = list(session.execute(select(Relationship)).scalars())
    assert len(rows) == 1
    assert rows[0].confidence == 0.6
