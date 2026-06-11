"""Resolved layer for temporal_behavior — ADR-0009 / DAT-445.

``resolve_temporal_behavior`` collapses each column's stock/flow adjudication onto
its ``SemanticAnnotation`` row: ``temporal_behavior`` becomes the pooled-resolved
value (the ontology prior reconciled with the LLM claim) and
``temporal_behavior_contested`` records the disagreement. Total ignorance (no
witness) leaves the ontology backfill untouched. In-memory SQLite, FKs off so we
skip parent rows — same pattern as the loader tests.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.resolve import resolve_temporal_behavior
from dataraum.storage import init_database

_RUN = "run-1"


@pytest.fixture
def real_session() -> Iterator[Session]:
    """In-memory SQLite session with all tables; FKs off so we skip parent rows."""
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


def _seed_annotation(
    session: Session,
    column_id: str,
    run_id: str = _RUN,
    *,
    ontology_behaviour: str = "point_in_time",
) -> None:
    """The annotation semantic_per_column wrote, carrying the ONTOLOGY prior."""
    session.add(
        SemanticAnnotation(
            annotation_id=str(uuid4()),
            session_id="sess-1",
            column_id=column_id,
            run_id=run_id,
            semantic_role="measure",
            temporal_behavior=ontology_behaviour,
        )
    )
    session.flush()


def _seed_object(
    session: Session,
    column_id: str,
    *,
    resolved: str | None,
    contested: bool,
    run_id: str = _RUN,
) -> None:
    """A temporal_behavior EntropyObject row, carrying the resolved verdict."""
    session.add(
        EntropyObjectRecord(
            object_id=str(uuid4()),
            session_id="sess-1",
            layer="semantic",
            dimension="temporal",
            sub_dimension="temporal_behavior",
            target=f"column:t.{column_id}",
            column_id=column_id,
            run_id=run_id,
            score=0.5,
            detector_id="temporal_behavior",
            evidence=[{"resolved": resolved, "contested": contested}],
        )
    )
    session.flush()


def _read(session: Session, column_id: str, run_id: str = _RUN) -> SemanticAnnotation:
    return session.execute(
        select(SemanticAnnotation).where(
            SemanticAnnotation.column_id == column_id,
            SemanticAnnotation.run_id == run_id,
        )
    ).scalar_one()


def test_conflict_overwrites_behaviour_and_flags_contested(real_session: Session) -> None:
    """concept said stock, the pool resolved to flow under contest → write both."""
    _seed_annotation(real_session, "col-a", ontology_behaviour="point_in_time")
    _seed_object(real_session, "col-a", resolved="additive", contested=True)

    updated = resolve_temporal_behavior(real_session, _RUN)

    assert updated == 1
    row = _read(real_session, "col-a")
    assert row.temporal_behavior == "additive"  # the adjudicated value replaced the prior
    assert row.temporal_behavior_contested is True


def test_uncontested_agreement_writes_behaviour_quietly(real_session: Session) -> None:
    _seed_annotation(real_session, "col-b", ontology_behaviour="point_in_time")
    _seed_object(real_session, "col-b", resolved="point_in_time", contested=False)

    assert resolve_temporal_behavior(real_session, _RUN) == 1
    row = _read(real_session, "col-b")
    assert row.temporal_behavior == "point_in_time"
    assert row.temporal_behavior_contested is False


def test_total_ignorance_leaves_ontology_backfill_untouched(real_session: Session) -> None:
    """resolved=None (no witness opined) → the ontology prior is preserved, no write."""
    _seed_annotation(real_session, "col-c", ontology_behaviour="point_in_time")
    _seed_object(real_session, "col-c", resolved=None, contested=False)

    assert resolve_temporal_behavior(real_session, _RUN) == 0
    row = _read(real_session, "col-c")
    assert row.temporal_behavior == "point_in_time"  # unchanged
    assert row.temporal_behavior_contested is None  # never written


def test_only_this_runs_objects_resolve(real_session: Session) -> None:
    """Run-versioned (DAT-413): an object from another run does not touch this row."""
    _seed_annotation(real_session, "col-d", run_id=_RUN, ontology_behaviour="point_in_time")
    _seed_object(real_session, "col-d", resolved="additive", contested=True, run_id="run-other")

    assert resolve_temporal_behavior(real_session, _RUN) == 0
    row = _read(real_session, "col-d")
    assert row.temporal_behavior == "point_in_time"


def test_no_records_returns_zero(real_session: Session) -> None:
    assert resolve_temporal_behavior(real_session, _RUN) == 0
