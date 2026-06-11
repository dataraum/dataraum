"""Wiring tests for the relationship_discovery detector shell (ADR-0009).

Drives the REAL relationship-scoped post-step on SQLite (engine → snapshot →
loader → shell → measurement) — the same harness that caught the
relationship_entropy silent no-fire. Pins that the shell emits one witnessed
EntropyObject per defined focal pair, reading every witness class the pair's
rows carry, and stays honestly silent when the catalog has no defined row.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.engine import run_detector_post_step
from dataraum.investigation.db_models import InvestigationSession
from dataraum.storage import Column, Source, Table
from dataraum.storage.base import init_database

_CANDIDATE_EVIDENCE = {
    "join_confidence": 0.59,
    "statistical_confidence": 1.0,
    "algorithm": "exact",
    "left_referential_integrity": 80.0,
    "orphan_count": 200,
}


@pytest.fixture
def session() -> Session:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    init_database(engine)
    db = Session(engine)
    db.add_all(
        [
            Source(source_id="src-1", name="finance", source_type="csv"),
            InvestigationSession(session_id="sess-1", intent="calibration"),
            Table(table_id="t-pay", source_id="src-1", table_name="payments", layer="typed"),
            Table(table_id="t-inv", source_id="src-1", table_name="invoices", layer="typed"),
            Column(column_id="c-fk", table_id="t-pay", column_name="invoice_id", column_position=0),
            Column(column_id="c-pk", table_id="t-inv", column_name="id", column_position=0),
        ]
    )
    db.commit()
    yield db
    db.close()


def _add_relationship(
    db: Session, *, method: str, evidence: dict[str, Any] | None, confidence: float
) -> None:
    db.add(
        Relationship(
            session_id="sess-1",
            run_id="run-1",
            from_table_id="t-pay",
            from_column_id="c-fk",
            to_table_id="t-inv",
            to_column_id="c-pk",
            relationship_type="foreign_key",
            cardinality=None,
            confidence=confidence,
            detection_method=method,
            evidence=evidence,
            is_confirmed=False,
        )
    )
    db.commit()


def _run(db: Session) -> list[EntropyObjectRecord]:
    run_detector_post_step(
        db,
        "relationship_discovery",
        None,
        session_id="sess-1",
        table_ids=["t-pay", "t-inv"],
        run_id="run-1",
    )
    db.commit()
    return list(
        db.execute(
            select(EntropyObjectRecord).where(
                EntropyObjectRecord.detector_id == "relationship_discovery"
            )
        ).scalars()
    )


class TestRelationshipDiscoveryWiring:
    def test_confirmed_pair_pools_overlap_and_llm(self, session: Session) -> None:
        """candidate (overlap stats) + llm → one witnessed object on the pair."""
        _add_relationship(
            session, method="candidate", evidence=dict(_CANDIDATE_EVIDENCE), confidence=0.59
        )
        _add_relationship(session, method="llm", evidence={}, confidence=0.9)

        records = _run(session)

        assert len(records) == 1
        record = records[0]
        assert record.target == "relationship:c-fk::c-pk"
        assert record.score > 0.0  # the witnesses disagree about genuineness
        evidence = record.evidence[0]
        assert evidence["methods_present"] == ["candidate", "llm"]
        assert evidence["llm_confirmed_this_run"] is True
        assert evidence["value_overlap"]["join_confidence"] == 0.59
        assert set(evidence["posterior"]) == {"genuine", "spurious"}
        # C/U → teach routing (DAT-447): an adjudicated pair always hands the
        # user an executable relationship-overlay action on the column pair.
        teach = evidence["teach_suggestion"]
        assert teach["type"] == "relationship"
        assert teach["action"] in ("confirm", "reject")
        assert (teach["from_column_id"], teach["to_column_id"]) == ("c-fk", "c-pk")

    def test_keeper_pair_without_llm_row_marks_unconfirmed(self, session: Session) -> None:
        _add_relationship(
            session, method="candidate", evidence=dict(_CANDIDATE_EVIDENCE), confidence=0.59
        )
        _add_relationship(session, method="keeper", evidence={"action": "keep"}, confidence=1.0)

        records = _run(session)

        assert len(records) == 1
        assert records[0].evidence[0]["llm_confirmed_this_run"] is False

    def test_candidate_only_pair_is_not_focal(self, session: Session) -> None:
        """Bare candidates never become focal pairs (the defined-catalog contract)."""
        _add_relationship(
            session, method="candidate", evidence=dict(_CANDIDATE_EVIDENCE), confidence=0.59
        )

        assert _run(session) == []

    def test_detector_properties(self) -> None:
        from dataraum.entropy.detectors.structural.relationship_discovery import (
            RelationshipDiscoveryDetector,
        )

        detector = RelationshipDiscoveryDetector()
        assert detector.detector_id == "relationship_discovery"
        assert detector.scope == "relationship"
        # The gate key and the stored key are the SAME (the DAT-405 lesson).
        assert detector.required_analyses == ["relationships"]
