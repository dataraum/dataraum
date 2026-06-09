"""``persist_column_annotations`` is idempotent under at-least-once retries (DAT-413).

A Temporal activity can commit then crash before acking, re-running with the
SAME ``run_id``. The per-column semantic writer now upserts on
``(column_id, run_id)`` instead of a plain ``session.add`` — so a re-run does not
duplicate the annotation row (which would make the head-resolved
``load_semantic`` loader's ``scalar_one_or_none()`` raise), and a second
``run_id`` for the same column coexists.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    ColumnSemanticOutput,
    TableColumnAnnotation,
)
from dataraum.analysis.semantic.processor import persist_column_annotations
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


def _seed_table_and_column(factory: Any) -> None:
    with factory() as session:
        session.add(Table(table_id="tbl-1", source_id="src-1", table_name="orders", layer="typed"))
        session.add(
            Column(
                column_id="col-1",
                table_id="tbl-1",
                column_name="amount",
                column_position=0,
            )
        )
        session.commit()


def _output(role: str) -> ColumnAnnotationOutput:
    return ColumnAnnotationOutput(
        tables=[
            TableColumnAnnotation(
                table_name="orders",
                columns=[
                    ColumnSemanticOutput(
                        column_name="amount",
                        semantic_role=role,  # type: ignore[arg-type]
                        entity_type="transaction_amount",
                        business_term="Transaction Amount",
                        description="The amount of the order.",
                        confidence=0.9,
                        temporal_behavior_claim="flow",
                        temporal_behavior_claim_confidence=0.9,
                    )
                ],
            )
        ]
    )


def test_reinsert_same_run_does_not_duplicate(session_factory: Any) -> None:
    """Re-running the writer with the SAME run_id updates in place (the retry)."""
    _seed_table_and_column(session_factory)

    with session_factory() as session:
        n = persist_column_annotations(
            session,
            _output("measure"),
            ["tbl-1"],
            annotated_by="model-x",
            session_id="sess-1",
            run_id="run-A",
        )
        session.commit()
    assert n == 1

    # The at-least-once retry: same run_id, refreshed role.
    with session_factory() as session:
        persist_column_annotations(
            session,
            _output("dimension"),
            ["tbl-1"],
            annotated_by="model-x",
            session_id="sess-1",
            run_id="run-A",
        )
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id == "col-1")
            ).scalars()
        )
    assert len(rows) == 1  # no duplicate, no raise
    assert rows[0].semantic_role == "dimension"  # retry value won


def test_second_run_id_coexists(session_factory: Any) -> None:
    """A second run's annotation for the same column lands alongside the first."""
    _seed_table_and_column(session_factory)

    with session_factory() as session:
        persist_column_annotations(
            session,
            _output("measure"),
            ["tbl-1"],
            annotated_by="model-x",
            session_id="sess-1",
            run_id="run-A",
        )
        persist_column_annotations(
            session,
            _output("dimension"),
            ["tbl-1"],
            annotated_by="model-x",
            session_id="sess-1",
            run_id="run-B",
        )
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id == "col-1")
            ).scalars()
        )
        total = session.scalar(select(func.count()).select_from(SemanticAnnotation))
    assert total == 2
    assert {r.run_id for r in rows} == {"run-A", "run-B"}
