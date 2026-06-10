"""The derived-value adjudication detector shell (ADR-0009, 2nd witness).

Drives detect() with injected analysis_results (no DB/DuckDB) and asserts the
score contract (honest discovered mismatch rate; an LLM hypothesis alone never
moves the scalar), the loss-readable per-slot evidence, and the witness
provenance carried for persistence.
"""

from __future__ import annotations

from typing import Any

import pytest

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.computational.derived_values import DerivedValueDetector

_DISCOVERED = {
    "derived_columns": [
        {
            "derived_column_name": "total",
            "match_rate": 0.99,
            "formula": "subtotal * tax_rate",
            "derivation_type": "product",
            "source_column_names": ["subtotal", "tax_rate"],
        }
    ]
}


def _context(
    correlation: dict[str, Any] | None = None,
    semantic: dict[str, Any] | None = None,
    grading: dict[str, Any] | None = None,
    reliabilities: dict[str, float] | None = None,
) -> DetectorContext:
    results: dict[str, Any] = {}
    if correlation is not None:
        results["correlation"] = correlation
    if semantic is not None:
        results["semantic"] = semantic
    if grading is not None:
        results["hypothesis_grading"] = grading
    if reliabilities is not None:
        results["reliabilities"] = reliabilities
    return DetectorContext(table_name="orders", column_name="total", analysis_results=results)


def test_nothing_in_play_emits_nothing() -> None:
    assert DerivedValueDetector().detect(_context()) == []
    assert DerivedValueDetector().detect(_context(correlation={"derived_columns": []})) == []


def test_score_stays_the_honest_discovered_mismatch_rate() -> None:
    # The eval's ordering contract: score = 1 − match_rate of the discovered
    # formula, regardless of the witness pool around it.
    ctx = _context(
        correlation=_DISCOVERED,
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.1, "matches": 10, "total": 100},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    assert obj.score == pytest.approx(0.01)  # 1 − 0.99, no boost
    assert obj.target == "column:orders.total"
    assert obj.sub_dimension == "formula_match"


def test_hypothesis_alone_never_drives_the_scalar() -> None:
    # No discovered formula: the LLM's broken hypothesis surfaces as grounded
    # conflict in evidence (loss path), NOT as a unilateral mismatch score.
    ctx = _context(
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.1, "matches": 10, "total": 100},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    assert obj.score == 0.0
    (entry,) = obj.evidence
    assert entry["hypothesized"] and not entry["discovered"]
    assert entry["formula_conflict"] > 0.3


def test_divergent_hypothesis_evidence_carries_loss_readable_conflict() -> None:
    ctx = _context(
        correlation=_DISCOVERED,
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.1, "matches": 10, "total": 100},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    assert len(obj.evidence) == 2  # discovered slot + hypothesis slot
    hyp = next(e for e in obj.evidence if e["hypothesized"])
    disc = next(e for e in obj.evidence if e["discovered"])
    assert {"claim_field", "formula_conflict", "formula_ignorance", "posterior"} <= set(hyp)
    assert hyp["formula_conflict"] > disc["formula_conflict"]
    assert "witnesses" not in hyp  # witnesses live on obj.witnesses → claim_witnesses


def test_matching_hypothesis_is_one_quiet_corroborated_slot() -> None:
    ctx = _context(
        correlation=_DISCOVERED,
        semantic={
            "derived_formula_hypothesis": "tax_rate * subtotal",  # commutative = same claim
            "derived_formula_confidence": 0.9,
        },
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    (entry,) = obj.evidence
    assert entry["discovered"] and entry["hypothesized"]
    assert entry["formula_conflict"] < 0.1
    assert {w.witness_id for w in obj.witnesses} == {"formula_discovery", "llm_hypothesis"}


def test_legacy_display_keys_survive_on_discovered_slots() -> None:
    (obj,) = DerivedValueDetector().detect(_context(correlation=_DISCOVERED))
    (entry,) = obj.evidence
    assert entry["status"] == "exact"
    assert entry["formula"] == "subtotal * tax_rate"
    assert entry["source_columns"] == ["subtotal", "tax_rate"]
    assert entry["derivation_type"] == "product"


def test_witnesses_carried_for_persistence() -> None:
    ctx = _context(
        correlation=_DISCOVERED,
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.1, "matches": 10, "total": 100},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    claim_fields = {w.claim_field for w in obj.witnesses}
    assert claim_fields == {e["claim_field"] for e in obj.evidence}
    assert all(set(w.distribution) == {"holds", "fails"} for w in obj.witnesses)


def test_threaded_reliabilities_reach_the_witnesses() -> None:
    ctx = _context(
        correlation=_DISCOVERED,
        semantic={
            "derived_formula_hypothesis": "tax_rate * subtotal",
            "derived_formula_confidence": 0.8,
        },
        reliabilities={"formula_discovery": 0.51, "llm_hypothesis": 0.42},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    by_id = {w.witness_id: w.reliability for w in obj.witnesses}
    assert by_id == {"formula_discovery": 0.51, "llm_hypothesis": 0.42}
