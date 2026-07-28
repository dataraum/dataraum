"""Stored-sign detector + resolved layer — DAT-875.

Two surfaces, tested end to end because the contract between them is the whole
point: the detector writes ``evidence[0]['resolved']`` and ``resolve_stored_sign``
reads exactly that key onto ``ColumnConcept.stored_sign``. A rename on either side
must fail here, not in a live run whose SQL author silently loses the fact.

Emission is pure (no DB); the round-trip uses in-memory SQLite with FKs off so we
skip parent rows — the pattern the temporal_behavior tests established.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.semantic.db_models import ColumnConcept
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.computational.stored_sign import StoredSignDetector
from dataraum.entropy.models import (
    ABSTAIN_INSUFFICIENT_DATA,
    STATUS_ABSTAINED,
    STATUS_MEASURED,
    EntropyObject,
)
from dataraum.entropy.resolve import resolve_stored_sign
from dataraum.storage import init_database

_RUN = "run-1"

# The corpus signature this fact exists to fix: a balance stored raw as debit −
# credit reconciles every account family under ONE ledger direction.
_UNIFORM = {"n_entities": 8, "fired_primary": 8, "fired_mirror": 0, "fired_both": 0}
# A balance already normalized per family splits into two disjoint sets.
_SPLIT = {"n_entities": 8, "fired_primary": 4, "fired_mirror": 4, "fired_both": 0}


# --- detector emission (pure, no DB) -----------------------------------------


def _context(partition: dict[str, Any] | None = None, **semantic: Any) -> DetectorContext:
    ctx = DetectorContext(
        table_name="balance_sheet", column_name="ending_balance", column_id="col-1", run_id=_RUN
    )
    if semantic:
        ctx.analysis_results["semantic"] = semantic
    if partition:
        ctx.analysis_results["partition"] = partition
    return ctx


def _detect(partition: dict[str, Any] | None = None, **semantic: Any) -> list[EntropyObject]:
    return StoredSignDetector().detect(_context(partition, **semantic))


def test_uniform_partition_resolves_ledger_signed() -> None:
    """The DAT-875 corpus case: one ledger direction fits every account family, so
    the stored balance was never normalized — credit-normal accounts read negative."""
    objs = _detect(_UNIFORM, semantic_role="measure", stored_sign_claim="unsure")
    assert len(objs) == 1
    assert objs[0].status == STATUS_MEASURED
    assert objs[0].evidence[0]["resolved"] == "ledger_signed"


def test_split_partition_resolves_natural_balance() -> None:
    objs = _detect(_SPLIT, semantic_role="measure", stored_sign_claim="unsure")
    assert len(objs) == 1
    assert objs[0].evidence[0]["resolved"] == "natural_balance"


def test_partition_overrules_a_disagreeing_name_read() -> None:
    """The agent reads the name as already-natural; the data says one uniform family.
    The measured verdict follows the DATA — the claim's evidence cannot contain the
    answer, since its samples are per column and never row-aligned."""
    objs = _detect(
        _UNIFORM,
        semantic_role="measure",
        stored_sign_claim="natural_balance",
        stored_sign_claim_confidence=0.9,
    )
    assert objs[0].evidence[0]["resolved"] == "ledger_signed"
    assert objs[0].evidence[0]["contested"] is True


def test_undetermined_measure_abstains_insufficient_data() -> None:
    """A catalogued MEASURE the agent was unsure about, with no lineage this run →
    total ignorance. A wave-2 abstention, not a silent skip, so the gap is visible in
    the coverage trace and resolve writes NULL rather than leaving a stale label."""
    objs = _detect(semantic_role="measure", stored_sign_claim="unsure")
    assert len(objs) == 1
    obj = objs[0]
    assert obj.status == STATUS_ABSTAINED
    assert obj.abstain_reason == ABSTAIN_INSUFFICIENT_DATA
    assert obj.evidence[0]["ignorance"] == pytest.approx(1.0)
    assert "resolved" not in obj.evidence[0]


def test_non_measure_stays_silent() -> None:
    """Every catalogued column carries a mandatory claim, so claim presence cannot
    discriminate — the role does. A dimension is not a storage-convention question."""
    assert _detect(semantic_role="dimension", stored_sign_claim="unsure") == []


def test_no_catalogue_grain_stays_silent() -> None:
    """add_source: no ColumnConcept under the run, so no claim slot at all. That is
    the wrong grain for this question, not an undetermined column — no abstention."""
    assert _detect(semantic_role="measure") == []


def test_no_semantic_row_emits_nothing() -> None:
    assert StoredSignDetector().detect(_context()) == []


def test_witness_provenance_records_both_reads() -> None:
    objs = _detect(
        _UNIFORM,
        semantic_role="measure",
        stored_sign_claim="natural_balance",
        stored_sign_claim_confidence=0.9,
    )
    assert {w.witness_id for w in objs[0].witnesses} == {"llm_claim", "sign_partition"}


def test_no_teach_suggestion_the_convention_is_data_determined() -> None:
    """No teach on either path: the partition witness already wins, so there is no
    format for a human to teach here (the DAT-657 stance, and the reason the
    teach-vocabulary guard must never start seeing this module)."""
    measured = _detect(_UNIFORM, semantic_role="measure", stored_sign_claim="unsure")
    abstained = _detect(semantic_role="measure", stored_sign_claim="unsure")
    for objs in (measured, abstained):
        assert "teach_suggestion" not in objs[0].evidence[0]


# --- detector -> EntropyObject -> resolve -> ColumnConcept -------------------


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


def _persist(session: Session, obj: EntropyObject, column_id: str) -> None:
    session.add(
        EntropyObjectRecord(
            run_id=_RUN,
            detector_id="stored_sign",
            column_id=column_id,
            layer=str(obj.layer),
            dimension=str(obj.dimension),
            sub_dimension=str(obj.sub_dimension),
            target=obj.target,
            score=obj.score,
            evidence=obj.evidence,
            status=obj.status,
            abstain_reason=obj.abstain_reason,
        )
    )
    session.flush()


def _concept(session: Session, column_id: str) -> ColumnConcept:
    return session.execute(
        select(ColumnConcept).where(ColumnConcept.column_id == column_id)
    ).scalar_one()


def test_resolve_writes_the_measured_convention(real_session: Session) -> None:
    real_session.add(ColumnConcept(column_id="col-1", run_id=_RUN, meaning="m"))
    real_session.flush()
    objs = _detect(_UNIFORM, semantic_role="measure", stored_sign_claim="unsure")
    _persist(real_session, objs[0], "col-1")

    assert resolve_stored_sign(real_session, _RUN) == 1
    assert _concept(real_session, "col-1").stored_sign == "ledger_signed"


def test_resolve_is_idempotent(real_session: Session) -> None:
    real_session.add(ColumnConcept(column_id="col-1", run_id=_RUN, meaning="m"))
    real_session.flush()
    _persist(
        real_session,
        _detect(_SPLIT, semantic_role="measure", stored_sign_claim="unsure")[0],
        "col-1",
    )

    resolve_stored_sign(real_session, _RUN)
    resolve_stored_sign(real_session, _RUN)
    assert _concept(real_session, "col-1").stored_sign == "natural_balance"


def test_abstention_clears_a_stale_label_to_null(real_session: Session) -> None:
    """Fail closed: a run that lost its witness must not leave a confident sign
    standing. A stale label here is a silently wrong sign in generated SQL — exactly
    the failure this fact exists to prevent — so NULL is written THROUGH."""
    real_session.add(
        ColumnConcept(column_id="col-1", run_id=_RUN, meaning="m", stored_sign="ledger_signed")
    )
    real_session.flush()
    objs = _detect(semantic_role="measure", stored_sign_claim="unsure")
    _persist(real_session, objs[0], "col-1")

    assert resolve_stored_sign(real_session, _RUN) == 1
    assert _concept(real_session, "col-1").stored_sign is None


def test_resolve_is_scoped_to_its_run(real_session: Session) -> None:
    real_session.add(ColumnConcept(column_id="col-1", run_id="other-run", meaning="m"))
    real_session.flush()
    _persist(
        real_session,
        _detect(_UNIFORM, semantic_role="measure", stored_sign_claim="unsure")[0],
        "col-1",
    )

    assert resolve_stored_sign(real_session, _RUN) == 0
    assert _concept(real_session, "col-1").stored_sign is None


def test_no_objects_is_a_no_op(real_session: Session) -> None:
    assert resolve_stored_sign(real_session, _RUN) == 0
