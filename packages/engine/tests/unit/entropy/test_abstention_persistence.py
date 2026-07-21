"""Abstention persistence (DAT-853): first-class rows, end to end.

The primitive's contract at the persistence layer:
- the harness's silent paths (can_run False / detect exception) leave a
  queryable ``entropy_objects`` row via ``run_detector_post_step``;
- the status/score/reason pairing is DB-enforced (CHECK), not convention;
- the readiness rollup's third outcome (coverage) round-trips through
  ``entropy_readiness``.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dataraum.entropy.core.storage import EntropyRepository
from dataraum.entropy.db_models import EntropyObjectRecord, EntropyReadinessRecord
from dataraum.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    EntropyDetector,
)
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.engine import run_detector_post_step
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.readiness import persist_readiness
from dataraum.storage import Source
from dataraum.storage.models import Column, Table
from tests.conftest import baseline_run_id


class NeverRunnableDetector(EntropyDetector):
    """Column-scoped detector whose required analysis never loads → can_run False."""

    detector_id = "never_runnable"
    layer = Layer.STRUCTURAL
    dimension = Dimension.TYPES
    sub_dimension = SubDimension.TYPE_FIDELITY
    required_analyses = [AnalysisKey.TYPING]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:  # pragma: no cover
        return []


def _seed_table(session: Session) -> tuple[str, str]:
    session.add(Source(source_id="src_abst", name="src_abst", source_type="csv"))
    session.add(
        Table(table_id="tbl_abst", source_id="src_abst", table_name="orders", layer="typed")
    )
    session.add(
        Column(column_id="col_abst", table_id="tbl_abst", column_name="amount", column_position=0)
    )
    session.flush()
    return "tbl_abst", "col_abst"


class TestPostStepPersistsAbstentions:
    def test_can_run_false_is_a_row_not_a_skip(self, session: Session) -> None:
        """The DAT-405 shape, fixed structurally: a skipped detector leaves a trace."""
        table_id, column_id = _seed_table(session)
        registry = DetectorRegistry()
        registry.register(NeverRunnableDetector())

        # BOTH references: engine.py imports from base at call time; snapshot.py
        # bound its own name at module import.
        with (
            patch("dataraum.entropy.detectors.base.get_default_registry", return_value=registry),
            patch("dataraum.entropy.snapshot.get_default_registry", return_value=registry),
        ):
            count = run_detector_post_step(
                session,
                "never_runnable",
                table_ids=[table_id],
                run_id=baseline_run_id(),
            )
        session.flush()

        assert count == 1
        record = session.execute(select(EntropyObjectRecord)).scalar_one()
        assert record.status == "abstained"
        assert record.abstain_reason == "missing_inputs"
        assert record.score is None
        assert record.detector_id == "never_runnable"
        assert record.table_id == table_id
        assert record.column_id == column_id
        assert record.run_id == baseline_run_id()

    def test_repository_roundtrip_preserves_status(self, session: Session) -> None:
        table_id, _ = _seed_table(session)
        registry = DetectorRegistry()
        registry.register(NeverRunnableDetector())
        # BOTH references: engine.py imports from base at call time; snapshot.py
        # bound its own name at module import.
        with (
            patch("dataraum.entropy.detectors.base.get_default_registry", return_value=registry),
            patch("dataraum.entropy.snapshot.get_default_registry", return_value=registry),
        ):
            run_detector_post_step(
                session, "never_runnable", table_ids=[table_id], run_id=baseline_run_id()
            )
        session.flush()

        objs = EntropyRepository(session).load_for_tables([table_id])
        assert len(objs) == 1
        assert objs[0].status == "abstained"
        assert objs[0].abstain_reason == "missing_inputs"
        assert objs[0].score is None


class TestCheckConstraints:
    """The pairing is DB-enforced — magic values cannot reach the table."""

    def _base_row(self, **overrides: object) -> EntropyObjectRecord:
        fields: dict[str, object] = {
            "layer": "value",
            "dimension": "nulls",
            "sub_dimension": "null_ratio",
            "target": "column:orders.amount",
            "run_id": baseline_run_id(),
            "detector_id": "null_ratio",
            "score": 0.5,
        }
        fields.update(overrides)
        return EntropyObjectRecord(**fields)

    def test_measured_without_score_rejected(self, session: Session) -> None:
        session.add(self._base_row(score=None))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_abstained_with_score_rejected(self, session: Session) -> None:
        session.add(self._base_row(status="abstained", abstain_reason="not_applicable", score=0.5))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_unknown_status_rejected(self, session: Session) -> None:
        session.add(self._base_row(status="skipped"))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_unknown_reason_rejected(self, session: Session) -> None:
        session.add(self._base_row(status="abstained", score=None, abstain_reason="because"))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_valid_abstention_accepted(self, session: Session) -> None:
        session.add(self._base_row(status="abstained", score=None, abstain_reason="not_applicable"))
        session.flush()  # no raise

    def test_unknown_coverage_rejected(self, session: Session) -> None:
        session.add(
            EntropyReadinessRecord(
                target="column:orders.amount",
                run_id=baseline_run_id(),
                band="ready",
                worst_intent_risk=0.0,
                coverage="unknown",
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()


class TestReadinessCoveragePersistence:
    def test_unmeasured_target_persists_a_row(self, session: Session) -> None:
        """An all-abstained loss-path target writes coverage='unmeasured' — not silence."""
        table_id, column_id = _seed_table(session)
        session.add(
            EntropyObjectRecord(
                layer="value",
                dimension="nulls",
                sub_dimension="null_ratio",
                target="column:orders.amount",
                table_id=table_id,
                column_id=column_id,
                run_id=baseline_run_id(),
                detector_id="null_ratio",
                score=None,
                status="abstained",
                abstain_reason="missing_inputs",
            )
        )
        session.flush()

        rows = persist_readiness(session, [table_id], run_id=baseline_run_id())
        session.flush()

        assert rows == 1
        rec = session.execute(select(EntropyReadinessRecord)).scalar_one()
        assert rec.coverage == "unmeasured"
        assert rec.band == "ready"  # frozen vocabulary — vacuous, coverage says so
        assert rec.worst_intent_risk == 0.0
        assert rec.abstentions == [
            {
                "detector": "null_ratio",
                "reason": "missing_inputs",
                "intents": ["aggregation_intent", "query_intent", "reporting_intent"],
            }
        ]

    def test_measured_target_persists_coverage_measured(self, session: Session) -> None:
        table_id, column_id = _seed_table(session)
        session.add(
            EntropyObjectRecord(
                layer="value",
                dimension="nulls",
                sub_dimension="null_ratio",
                target="column:orders.amount",
                table_id=table_id,
                column_id=column_id,
                run_id=baseline_run_id(),
                detector_id="null_ratio",
                score=0.9,
            )
        )
        session.flush()

        persist_readiness(session, [table_id], run_id=baseline_run_id())
        session.flush()

        rec = session.execute(select(EntropyReadinessRecord)).scalar_one()
        assert rec.coverage == "measured"
        assert rec.abstentions is None
        assert rec.band == "blocked"  # 0.9 × 0.7 aggregation = 0.63
