"""The null-semantics adjudication detector (ADR-0009, DAT-457).

Drives detect() with injected analysis_results (no DB/DuckDB) and asserts the
per-column object + worst-token-conflict score + loud per-token witness evidence.
"""

from __future__ import annotations

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.value.null_token_adjudication import NullSemanticsDetector

_TYPING = {
    "resolved_type": "DECIMAL",
    "parse_success_rate": 0.9,
    "failed_examples": ["N/A", "PENDING_REVIEW"],
}
_VOCAB = ["", "NULL", "N/A"]


def _context(quarantine: dict) -> DetectorContext:
    return DetectorContext(
        table_name="orders",
        column_name="amount",
        analysis_results={
            "typing": _TYPING,
            "quarantine_tokens": quarantine,
            "null_vocab": _VOCAB,
        },
    )


def test_emits_one_per_column_object() -> None:
    ctx = _context(
        {
            "rejected_tokens": [
                {"token": "N/A", "count": 60},
                {"token": "PENDING_REVIEW", "count": 40},
            ],
            "total_rejected": 100,
        }
    )
    objects = NullSemanticsDetector().detect(ctx)
    assert len(objects) == 1
    obj = objects[0]
    assert obj.target == "column:orders.amount"
    assert obj.sub_dimension == "null_semantics"
    assert {e["token"] for e in obj.evidence} == {"N/A", "PENDING_REVIEW"}


def test_score_is_worst_token_conflict_driven_by_novel_sentinel() -> None:
    # PENDING_REVIEW (novel sentinel) disagrees with the vocabulary → its conflict
    # dominates the column score.
    ctx = _context(
        {"rejected_tokens": [{"token": "PENDING_REVIEW", "count": 95}], "total_rejected": 100}
    )
    obj = NullSemanticsDetector().detect(ctx)[0]
    pending = next(e for e in obj.evidence if e["token"] == "PENDING_REVIEW")
    assert obj.score == pending["conflict"]
    assert obj.score > 0.2


def test_evidence_is_the_per_token_summary() -> None:
    ctx = _context({"rejected_tokens": [{"token": "N/A", "count": 95}], "total_rejected": 100})
    (entry,) = NullSemanticsDetector().detect(ctx)[0].evidence
    assert set(entry["posterior"]) == {"is-null", "is-value"}
    assert {"token", "claim_field", "conflict", "ignorance"} <= set(entry)
    assert "witnesses" not in entry  # witnesses live on obj.witnesses → claim_witnesses


def test_witnesses_carried_for_persistence() -> None:
    ctx = _context({"rejected_tokens": [{"token": "N/A", "count": 95}], "total_rejected": 100})
    obj = NullSemanticsDetector().detect(ctx)[0]
    assert {w.witness_id for w in obj.witnesses} == {
        "quarantine_clustering",
        "type_claim",
        "null_vocabulary",
    }
    assert all(w.claim_field == "null_token:N/A" for w in obj.witnesses)
    assert all(set(w.distribution) == {"is-null", "is-value"} for w in obj.witnesses)


def test_no_rejected_tokens_emits_nothing() -> None:
    ctx = _context({"rejected_tokens": [], "total_rejected": 0})
    assert NullSemanticsDetector().detect(ctx) == []
