"""The derived-value adjudication detector shell (ADR-0009, 2nd witness).

Drives detect() with injected analysis_results (no DB/DuckDB) and asserts the
score contract — score = max(best graded mismatch, hygiene-passing name-vs-data
identity conflict); an ungraded or hygiene-failing LLM hypothesis never moves
it — the loss-readable per-slot evidence, and the witness provenance carried
for persistence.
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
    declaration: dict[str, Any] | None = None,
    reliabilities: dict[str, float] | None = None,
) -> DetectorContext:
    results: dict[str, Any] = {}
    if correlation is not None:
        results["correlation"] = correlation
    if semantic is not None:
        results["semantic"] = semantic
    if grading is not None:
        results["hypothesis_grading"] = grading
    if declaration is not None:
        results["declaration"] = declaration
    if reliabilities is not None:
        results["reliabilities"] = reliabilities
    return DetectorContext(table_name="orders", column_name="total", analysis_results=results)


def test_nothing_in_play_emits_nothing() -> None:
    assert DerivedValueDetector().detect(_context()) == []
    assert DerivedValueDetector().detect(_context(correlation={"derived_columns": []})) == []


def test_corroborated_discovery_scores_the_honest_mismatch_rate() -> None:
    # Name and data agree (the hypothesis canonicalizes to the discovered
    # formula): the identity-conflict leg is quiet and the score is the plain
    # 1 − match_rate of the discovered formula. No boost.
    ctx = _context(
        correlation=_DISCOVERED,
        semantic={
            "derived_formula_hypothesis": "tax_rate * subtotal",  # commutative = same claim
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.99, "matches": 99, "total": 100},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    (entry,) = obj.evidence
    # max(1 − 0.99, residual pooled conflict of two near-agreeing witnesses) —
    # either way a corroborated formula stays far below any band.
    assert obj.score == pytest.approx(max(0.01, entry["formula_conflict"]))
    assert obj.score < 0.05
    assert obj.target == "column:orders.total"
    assert obj.sub_dimension == "formula_match"


def test_wholesale_divergence_scores_the_identity_conflict() -> None:
    # The wave-2 cal-corpus false negative: the data follows the DISCOVERED
    # formula perfectly (mismatch leg 0.0) while the NAME advertises a
    # different identity whose grading fails — the pooled conflict on that
    # hygiene-passing claim is the entropy and must reach the score (and so
    # the loss rollup). detection-derived-cal-v1: 3/3 wholesale columns were
    # silently ready under the scalar-only contract.
    ctx = _context(
        correlation={
            "derived_columns": [
                {
                    "derived_column_name": "total",
                    "match_rate": 1.0,
                    "formula": "subtotal * tax_rate",
                    "derivation_type": "product",
                    "source_column_names": ["subtotal", "tax_rate"],
                }
            ]
        },
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.0, "matches": 0, "total": 100},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    hyp = next(e for e in obj.evidence if e["hypothesized"])
    assert obj.score == pytest.approx(hyp["formula_conflict"])
    assert obj.score > 0.3  # bands — visible to the loss rollup, not evidence-only


def test_graded_hypothesis_drives_the_scalar() -> None:
    # No discovered formula, but the hypothesis was GRADED over the rows: the
    # match rate is DATA (the LLM only chose which identity to test — the
    # validation-SQL division of labor), so the measured violation rate IS the
    # mismatch score. Batch-1 recall miss: the injection pushed the discovered
    # formula below the persistence cut and the measured 13% violation scored
    # 0.0 — invisible in both the scalar and the pooled conflict.
    ctx = _context(
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.1, "matches": 10, "total": 100},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    assert obj.score == pytest.approx(0.9)
    (entry,) = obj.evidence
    assert entry["hypothesized"] and not entry["discovered"]
    assert entry["formula_conflict"] > 0.3


def test_low_confidence_or_thin_grading_gates_the_scalar() -> None:
    # Statistical hygiene (review wave-1): a guessy hypothesis or a thin sample
    # still pools, but cannot drive obj.score.
    for semantic, grading in (
        (  # confidence below the floor
            {"derived_formula_hypothesis": "subtotal + tax", "derived_formula_confidence": 0.2},
            {"match_rate": 0.1, "matches": 10, "total": 100},
        ),
        (  # gradable sample below the floor
            {"derived_formula_hypothesis": "subtotal + tax", "derived_formula_confidence": 0.9},
            {"match_rate": 0.0, "matches": 0, "total": 3},
        ),
    ):
        ctx = _context(semantic=semantic, grading=grading)
        (obj,) = DerivedValueDetector().detect(ctx)
        assert obj.score == 0.0


def test_ungraded_hypothesis_never_drives_the_scalar() -> None:
    # No grading (source columns didn't resolve — the hallucination guard
    # abstained): an LLM guess alone never moves the mismatch score.
    ctx = _context(
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
    )
    objects = DerivedValueDetector().detect(ctx)
    for obj in objects:
        assert obj.score == 0.0


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


def test_emitted_evidence_carries_the_validation_teach_suggestion() -> None:
    # DAT-447 Option B routing: every emitted evidence entry carries the
    # validation teach suggestion — always-emit (no thresholds), naming the
    # column and the check intent, picking NO truth (the user declares).
    (obj,) = DerivedValueDetector().detect(_context(correlation=_DISCOVERED))
    for entry in obj.evidence:
        assert entry["teach_suggestion"] == {
            "type": "validation",
            "check": "expected_formula",
            "table": "orders",
            "column": "total",
        }


def test_declared_formula_matching_data_collapses_conflict_on_its_claim() -> None:
    # The closure shape (DAT-447): pre-teach the name advertises subtotal+tax
    # but the rows violate it (graded 0.1) — the hypothesis claim is contested.
    # The user declares the formula the rows actually follow; the loader graded
    # it 0.99. On the DECLARED claim the human and the data agree → conflict
    # collapses there and the posterior resolves holds. The name-vs-data
    # dispute stays honest on the hypothesis claim (no override of the LLM).
    ctx = _context(
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.1, "matches": 10, "total": 100},
        declaration={"formula": "subtotal * tax_rate", "match_rate": 0.99},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    declared = next(e for e in obj.evidence if e["declared"])
    contested = next(e for e in obj.evidence if e["hypothesized"])
    assert declared["formula_conflict"] < 0.1
    assert declared["posterior"]["holds"] > 0.5
    assert contested["formula_conflict"] > 0.3
    witness_ids = {w.witness_id for w in obj.witnesses if w.claim_field == declared["claim_field"]}
    assert "human_declaration" in witness_ids


def test_declared_formula_violated_by_data_keeps_conflict_high() -> None:
    # The human is a witness, never an oracle: the rows follow the discovered
    # product formula perfectly while the user declares subtotal+tax, which the
    # loader graded broken (0.1). On the declared claim human-holds pools
    # against data-fails → conflict stays high AND reaches the score (honest
    # conflict, no silent trust in the declaration).
    ctx = _context(
        correlation=_DISCOVERED,
        declaration={"formula": "subtotal + tax", "match_rate": 0.1},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    declared = next(e for e in obj.evidence if e["declared"])
    assert declared["formula_conflict"] > 0.3  # flagging is weight-robust: contested, loud
    assert obj.score >= declared["formula_conflict"]  # the conflict reaches the score


def test_matching_declaration_closes_the_column_score() -> None:
    # The teach CLOSES (eval contract: "stable — a teach closes it"). Full
    # wholesale shape: rows follow the discovered product formula perfectly,
    # the name advertises subtotal+tax (graded 0.0 — identity conflict ~0.8
    # drove the score pre-teach). The user declares the formula the rows
    # follow: the declared claim becomes the column's identity risk, witnesses
    # AGREE there, and the column score drops below every band. The
    # name-vs-data dispute survives in evidence as a naming finding — visible,
    # not banding. Aggregation semantics, not an override.
    ctx = _context(
        correlation=_DISCOVERED,
        semantic={
            "derived_formula_hypothesis": "subtotal + tax",
            "derived_formula_confidence": 0.9,
        },
        grading={"match_rate": 0.0, "matches": 0, "total": 100},
        declaration={"formula": "tax_rate * subtotal", "match_rate": 0.99},
    )
    (obj,) = DerivedValueDetector().detect(ctx)
    contested = next(e for e in obj.evidence if e["hypothesized"])
    assert contested["formula_conflict"] > 0.3  # the naming dispute stays loud in evidence
    assert obj.score < 0.15  # ...but the settled identity no longer bands the column


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
