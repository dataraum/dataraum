"""Teach-shadowing regression for the relationship pair loader (DAT-405 class).

``load_relationship_for_pair`` picks the highest-precedence row for a directional
column pair (manual > keeper > llm > candidate). Teach-materialized rows
(``materialize_relationship_overlays``) carry overlay provenance only —
``{"source": "config_overlay", ...}`` — never the measured RI evidence the
``relationship_entropy`` detector scores. Without merging, the moment a user
teaches a relationship (add → manual, silent-accept → keeper) the representative
row shadows the measured candidate/llm rows, ``detect()`` finds no RI metric and
returns ``[]`` — the orphan-rate measurement silently disappears exactly when the
relationship is confirmed (recall = 0 on the taught pair, no error anywhere).

These tests drive the REAL wiring end to end on SQLite (engine post-step →
snapshot → loader → detector), reproducing the silent no-fire and pinning the
fix: the representative keeps its identity (method, confidence, confirmation),
and the measured RI evidence keys are backfilled from the best row that has them.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.detectors.loaders import load_relationship_for_pair
from dataraum.entropy.engine import run_detector_post_step
from dataraum.investigation.db_models import InvestigationSession
from dataraum.storage import Column, Source, Table
from dataraum.storage.base import init_database

_RI_EVIDENCE = {
    "left_referential_integrity": 80.0,
    "right_referential_integrity": 95.0,
    "orphan_count": 200,
    "left_total_count": 1000,
    "cardinality_verified": True,
}

_OVERLAY_EVIDENCE = {"source": "config_overlay", "action": "add"}


@pytest.fixture
def session() -> Session:
    """An in-memory substrate with one typed FK pair (payments → invoices)."""
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
    db: Session,
    *,
    method: str,
    evidence: dict[str, Any],
    confidence: float,
    is_confirmed: bool = False,
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
            cardinality="many-to-one" if method == "candidate" else None,
            confidence=confidence,
            detection_method=method,
            evidence=evidence,
            is_confirmed=is_confirmed,
        )
    )
    db.commit()


def _run_relationship_entropy(db: Session) -> list[EntropyObjectRecord]:
    run_detector_post_step(
        db,
        "relationship_entropy",
        None,
        session_id="sess-1",
        table_ids=["t-pay", "t-inv"],
        run_id="run-1",
    )
    db.commit()
    return list(
        db.execute(
            select(EntropyObjectRecord).where(
                EntropyObjectRecord.detector_id == "relationship_entropy"
            )
        ).scalars()
    )


class TestTeachShadowedPairStillScores:
    """A teach must not silently delete the orphan-rate measurement."""

    def test_manual_row_shadowing_measured_candidate_still_fires(self, session: Session) -> None:
        """manual (overlay evidence only) + candidate (measured RI) → detector fires."""
        _add_relationship(session, method="candidate", evidence=dict(_RI_EVIDENCE), confidence=0.59)
        _add_relationship(
            session,
            method="manual",
            evidence=dict(_OVERLAY_EVIDENCE),
            confidence=1.0,
            is_confirmed=True,
        )

        records = _run_relationship_entropy(session)

        assert len(records) == 1, (
            "relationship_entropy emitted nothing for a taught pair whose candidate row "
            "carries measured RI evidence — the teach silently killed the measurement"
        )
        assert records[0].score == pytest.approx(0.2)  # 1 - 80/100

    def test_keeper_row_shadowing_measured_llm_still_fires(self, session: Session) -> None:
        """keeper (silent accept) + llm (measured RI) → detector fires."""
        _add_relationship(session, method="llm", evidence=dict(_RI_EVIDENCE), confidence=0.9)
        _add_relationship(
            session,
            method="keeper",
            evidence={"source": "config_overlay", "action": "keep"},
            confidence=1.0,
            is_confirmed=True,
        )

        records = _run_relationship_entropy(session)

        assert len(records) == 1
        assert records[0].score == pytest.approx(0.2)

    def test_pair_with_no_measured_evidence_anywhere_stays_silent(self, session: Session) -> None:
        """Only an overlay row, no measured row → honest ignorance, no fabricated score."""
        _add_relationship(
            session,
            method="manual",
            evidence=dict(_OVERLAY_EVIDENCE),
            confidence=1.0,
            is_confirmed=True,
        )

        records = _run_relationship_entropy(session)

        assert records == []


class TestLoaderRepresentativeMerge:
    """The loader keeps the representative's identity, merging only measured keys."""

    def test_representative_identity_is_manual_with_backfilled_ri(self, session: Session) -> None:
        _add_relationship(session, method="candidate", evidence=dict(_RI_EVIDENCE), confidence=0.59)
        _add_relationship(
            session,
            method="manual",
            evidence=dict(_OVERLAY_EVIDENCE),
            confidence=1.0,
            is_confirmed=True,
        )

        rel = load_relationship_for_pair(
            session, "c-fk", "c-pk", session_id="sess-1", run_id="run-1"
        )

        assert rel is not None
        assert rel["detection_method"] == "manual"
        assert rel["confidence"] == 1.0
        evidence = rel["evidence"]
        # Overlay provenance preserved, measured RI backfilled from the candidate.
        assert evidence["source"] == "config_overlay"
        assert evidence["left_referential_integrity"] == 80.0
        assert evidence["orphan_count"] == 200
        assert evidence["ri_evidence_source"] == "candidate"

    def test_representative_own_evidence_wins_over_backfill(self, session: Session) -> None:
        """A measured key already on the representative is never overwritten."""
        _add_relationship(session, method="candidate", evidence=dict(_RI_EVIDENCE), confidence=0.59)
        llm_evidence = dict(_RI_EVIDENCE, left_referential_integrity=85.0)
        _add_relationship(session, method="llm", evidence=llm_evidence, confidence=0.9)

        rel = load_relationship_for_pair(
            session, "c-fk", "c-pk", session_id="sess-1", run_id="run-1"
        )

        assert rel is not None
        assert rel["detection_method"] == "llm"
        assert rel["evidence"]["left_referential_integrity"] == 85.0
        assert "ri_evidence_source" not in rel["evidence"]


class TestRowsForPairDirectionAgnostic:
    """``load_relationship_rows_for_pair`` finds rows in EITHER direction.

    Wave-2 cal finding: candidate rows persisted parent→child were invisible to
    the child→parent defined-pair lookup, so the value_overlap witness collapsed
    to uniform and was dropped before persisting — silent on every such pair.
    """

    def test_reversed_candidate_row_is_found(self, session: Session) -> None:
        from dataraum.entropy.detectors.loaders import load_relationship_rows_for_pair

        # Candidate persisted parent→child (reversed); llm in the exact direction.
        session.add(
            Relationship(
                session_id="sess-1",
                run_id="run-1",
                from_table_id="t-inv",
                from_column_id="c-pk",
                to_table_id="t-pay",
                to_column_id="c-fk",
                relationship_type="foreign_key",
                confidence=0.93,
                detection_method="candidate",
                evidence={"jaccard_similarity": 0.62},
            )
        )
        session.commit()
        _add_relationship(session, method="llm", evidence={}, confidence=0.9)

        rows = load_relationship_rows_for_pair(
            session, "c-fk", "c-pk", session_id="sess-1", run_id="run-1"
        )
        assert set(rows) == {"candidate", "llm"}
        assert rows["candidate"]["confidence"] == 0.93

    def test_exact_direction_wins_over_reversed_for_same_method(self, session: Session) -> None:
        from dataraum.entropy.detectors.loaders import load_relationship_rows_for_pair

        _add_relationship(session, method="candidate", evidence={}, confidence=0.59)
        session.add(
            Relationship(
                session_id="sess-1",
                run_id="run-1",
                from_table_id="t-inv",
                from_column_id="c-pk",
                to_table_id="t-pay",
                to_column_id="c-fk",
                relationship_type="foreign_key",
                confidence=0.93,
                detection_method="candidate",
                evidence={},
            )
        )
        session.commit()

        rows = load_relationship_rows_for_pair(
            session, "c-fk", "c-pk", session_id="sess-1", run_id="run-1"
        )
        assert rows["candidate"]["confidence"] == 0.59  # the requested direction's row
