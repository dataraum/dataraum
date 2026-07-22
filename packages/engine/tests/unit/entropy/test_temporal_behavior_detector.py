"""TemporalBehaviorDetector emission + the cross-surface stock/flow invariant (DAT-847).

Two concerns, one home:

1. **Detector emission.** A resolved stock/flow verdict emits a MEASURED object
   carrying the posterior + the pooled ignorance (loud, even at the 0.62-0.75
   lone-name-reader regime the defect named). A MEASURE the pool could not determine
   (total ignorance — no opinionated witness) emits a wave-2 ``abstained`` object
   (``insufficient_data``) rather than a silent skip, so the undetermined column is
   visible in the coverage/abstention trace. A column that is not a measure (no
   stock/flow claim) stays silent — no abstention flood over identifiers/dimensions.

2. **Cross-surface consistency.** For one column in one run the three surfaces cannot
   silently disagree: the resolved label (``ColumnConcept.temporal_behavior``) MUST
   follow the current run's pooled result (the ``temporal_behavior`` EntropyObject's
   ``resolved`` evidence — the readiness evidence's source), NEVER a stale prior and
   NEVER the raw pre-adjudication claim (``SemanticAnnotation.temporal_behavior_claim``
   is INPUT, not truth). Pinned end to end: detector → object → ``resolve`` → concept.

In-memory SQLite, FKs off so we skip parent rows — same pattern as the resolve tests.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.computational.temporal_behavior import TemporalBehaviorDetector
from dataraum.entropy.models import (
    ABSTAIN_INSUFFICIENT_DATA,
    STATUS_ABSTAINED,
    STATUS_MEASURED,
    EntropyObject,
)
from dataraum.entropy.resolve import resolve_temporal_behavior
from dataraum.storage import init_database

_RUN = "run-1"


# --- detector emission (pure, no DB) -----------------------------------------


def _context(**semantic: Any) -> DetectorContext:
    """A column detect context whose semantic analysis is ``semantic`` (or absent)."""
    ctx = DetectorContext(table_name="t", column_name="c", column_id="col-1", run_id=_RUN)
    if semantic:
        ctx.analysis_results["semantic"] = semantic
    return ctx


def _detect(**semantic: Any) -> list[EntropyObject]:
    return TemporalBehaviorDetector().detect(_context(**semantic))


def test_lone_llm_claim_emits_measured_verdict_with_loud_ignorance() -> None:
    """A lone name-read (no data-grounded witness) still resolves a label — the wave's
    stance is keep-the-verdict, make-ignorance-loud (not withhold). The pooled ignorance
    lands in the 0.62-0.75 regime the defect named and rides the evidence, so the
    readiness/loss path sees it; the label is NOT silently confident."""
    objs = _detect(
        semantic_role="measure",
        temporal_behavior_claim="stock",
        temporal_behavior_claim_confidence=0.9,
    )
    assert len(objs) == 1
    obj = objs[0]
    assert obj.status == STATUS_MEASURED
    assert obj.evidence[0]["resolved"] == "point_in_time"
    assert 0.6 < obj.evidence[0]["ignorance"] < 0.8  # loud, not masqueraded as confident


def test_structural_overrules_name_read_resolves_from_data() -> None:
    """The LLM name-reads 'stock'; the data reconciles per_period → flow. The measured
    verdict follows the DATA (additive), never the raw claim."""
    ctx = _context(
        semantic_role="measure",
        temporal_behavior_claim="stock",
        temporal_behavior_claim_confidence=0.9,
    )
    ctx.analysis_results["structural"] = {"pattern": "per_period", "match_rate": 0.9}
    objs = TemporalBehaviorDetector().detect(ctx)
    assert len(objs) == 1
    assert objs[0].status == STATUS_MEASURED
    assert objs[0].evidence[0]["resolved"] == "additive"


def test_unsure_measure_abstains_insufficient_data() -> None:
    """A MEASURE the LLM was unsure about, with no data grounding → total ignorance.
    Emit a wave-2 abstention (insufficient_data), not a silent skip, so the undetermined
    measure is visible in the coverage/abstention trace (DAT-847)."""
    objs = _detect(semantic_role="measure", temporal_behavior_claim="unsure")
    assert len(objs) == 1
    obj = objs[0]
    assert obj.status == STATUS_ABSTAINED
    assert obj.abstain_reason == ABSTAIN_INSUFFICIENT_DATA
    assert obj.score is None
    assert obj.evidence[0]["ignorance"] == pytest.approx(1.0)
    assert "resolved" not in obj.evidence[0]  # nothing resolved → resolve writes NULL


def test_non_measure_stays_silent_despite_mandatory_unsure_claim() -> None:
    """Every column carries a REQUIRED ``temporal_behavior_claim`` — non-measures get
    ``unsure`` (models.py). So claim presence cannot discriminate: the abstention is gated
    on ``semantic_role == 'measure'``. A key / dimension column with its mandatory
    ``unsure`` claim emits NO object, so identifiers and dimensions never wallpaper the
    coverage/abstention trace with insufficient_data (DAT-847 review fix)."""
    assert _detect(semantic_role="key", temporal_behavior_claim="unsure") == []
    assert _detect(semantic_role="dimension", temporal_behavior_claim="unsure") == []
    assert _detect(semantic_role="timestamp", temporal_behavior_claim="unsure") == []


def test_no_semantic_annotation_returns_empty() -> None:
    assert _detect() == []


# --- cross-surface consistency (detector → object → resolve → concept) --------


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


def _record_from_object(obj: EntropyObject, column_id: str) -> EntropyObjectRecord:
    """Persist the detector's object as the run's ``temporal_behavior`` record — the
    same conversion the engine does, minimal shape."""
    return EntropyObjectRecord(
        object_id=obj.object_id,
        layer=obj.layer,
        dimension=obj.dimension,
        sub_dimension=obj.sub_dimension,
        target=obj.target,
        column_id=column_id,
        run_id=_RUN,
        score=obj.score,
        status=obj.status,
        abstain_reason=obj.abstain_reason,
        detector_id=obj.detector_id,
        evidence=obj.evidence,
    )


def _seed_concept(session: Session, column_id: str, *, prior: str | None) -> None:
    session.add(ColumnConcept(column_id=column_id, run_id=_RUN, temporal_behavior=prior))
    session.flush()


def _seed_raw_claim(session: Session, column_id: str, *, claim: str) -> None:
    session.add(SemanticAnnotation(column_id=column_id, run_id=_RUN, temporal_behavior_claim=claim))
    session.flush()


def _concept(session: Session, column_id: str) -> ColumnConcept:
    return session.execute(
        select(ColumnConcept).where(
            ColumnConcept.column_id == column_id, ColumnConcept.run_id == _RUN
        )
    ).scalar_one()


def _raw_claim(session: Session, column_id: str) -> str | None:
    return session.execute(
        select(SemanticAnnotation.temporal_behavior_claim).where(
            SemanticAnnotation.column_id == column_id, SemanticAnnotation.run_id == _RUN
        )
    ).scalar_one()


def test_resolved_label_follows_the_pool_not_the_raw_claim(real_session: Session) -> None:
    """The invariant, overrule case: the LLM claim is 'stock', the data reconciles to
    flow, a stale 'point_in_time' sits on the concept. After resolve the concept must
    equal the CURRENT run's pooled ``resolved`` ('additive') — never the stale prior,
    never the raw claim. The raw claim is left untouched as INPUT."""
    ctx = DetectorContext(table_name="t", column_name="c", column_id="col-x", run_id=_RUN)
    ctx.analysis_results["semantic"] = {
        "semantic_role": "measure",
        "temporal_behavior_claim": "stock",
        "temporal_behavior_claim_confidence": 0.9,
    }
    ctx.analysis_results["structural"] = {"pattern": "per_period", "match_rate": 0.9}
    [obj] = TemporalBehaviorDetector().detect(ctx)
    pooled_resolved = obj.evidence[0]["resolved"]
    assert pooled_resolved == "additive"  # the data decided, against the name

    _seed_concept(real_session, "col-x", prior="point_in_time")  # a stale confident label
    _seed_raw_claim(real_session, "col-x", claim="stock")
    real_session.add(_record_from_object(obj, "col-x"))
    real_session.flush()

    resolve_temporal_behavior(real_session, _RUN)

    concept = _concept(real_session, "col-x")
    assert concept.temporal_behavior == pooled_resolved  # surface 1 == surface 2 (the pool)
    assert concept.temporal_behavior != "point_in_time"  # not the stale prior
    assert concept.temporal_behavior != "stock"  # not the raw claim
    assert _raw_claim(real_session, "col-x") == "stock"  # raw claim untouched — it is INPUT


def test_abstained_measure_clears_the_label_raw_claim_survives(real_session: Session) -> None:
    """The invariant, abstention case: an undetermined measure (LLM unsure, no data) →
    abstained object → the resolved label is CLEARED to NULL (no stale survives), while
    the raw claim survives as input and the readiness surface sees the abstention."""
    ctx = DetectorContext(table_name="t", column_name="c", column_id="col-y", run_id=_RUN)
    ctx.analysis_results["semantic"] = {
        "semantic_role": "measure",
        "temporal_behavior_claim": "unsure",
    }
    [obj] = TemporalBehaviorDetector().detect(ctx)
    assert obj.status == STATUS_ABSTAINED  # surface 2 (readiness) sees the abstention

    _seed_concept(real_session, "col-y", prior="additive")  # a stale confident label
    _seed_raw_claim(real_session, "col-y", claim="unsure")
    real_session.add(_record_from_object(obj, "col-y"))
    real_session.flush()

    resolve_temporal_behavior(real_session, _RUN)

    assert _concept(real_session, "col-y").temporal_behavior is None  # cleared, falls loud
    assert _raw_claim(real_session, "col-y") == "unsure"  # raw claim survives as INPUT
