"""The unit-consistency adjudication detector (ADR-0009, DAT-428).

Drives detect() with injected analysis_results (no DB/DuckDB) and asserts the
per-column object: pooled-conflict score + the per-witness breakdown on obj.witnesses.
"""

from __future__ import annotations

import random

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.semantic.unit_consistency import UnitConsistencyDetector

_rng = random.Random(11)
_SINGLE_SCALE = [abs(_rng.gauss(500, 60)) for _ in range(200)]
_SCALE_MIXED = [abs(_rng.gauss(500, 60)) for _ in range(120)] + [
    abs(_rng.gauss(500_000, 60_000)) for _ in range(80)
]


def _context(values: list[float], unit_confidence: float | None) -> DetectorContext:
    return DetectorContext(
        table_name="invoices",
        column_name="amount",
        analysis_results={"values": values, "unit_confidence": unit_confidence},
    )


def test_emits_one_per_column_object_with_conflict_score() -> None:
    obj = UnitConsistencyDetector().detect(_context(_SCALE_MIXED, 0.9))[0]
    assert obj.target == "column:invoices.amount"
    assert obj.sub_dimension == "unit_consistency"
    assert obj.score == obj.evidence[0]["conflict"]


def test_scale_mix_under_a_declared_unit_scores_above_clean() -> None:
    clean = UnitConsistencyDetector().detect(_context(_SINGLE_SCALE, 0.9))[0]
    mixed = UnitConsistencyDetector().detect(_context(_SCALE_MIXED, 0.9))[0]
    assert mixed.score > clean.score


def test_witnesses_carried_for_persistence() -> None:
    obj = UnitConsistencyDetector().detect(_context(_SCALE_MIXED, 0.8))[0]
    assert {w.witness_id for w in obj.witnesses} == {"magnitude_modality", "declared_unit"}
    assert all(w.claim_field == "unit" for w in obj.witnesses)
    assert all(set(w.distribution) == {"consistent", "mixed"} for w in obj.witnesses)


def test_no_values_emits_nothing() -> None:
    assert UnitConsistencyDetector().detect(_context([], 0.9)) == []
