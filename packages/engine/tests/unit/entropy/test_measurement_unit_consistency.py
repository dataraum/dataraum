"""Unit-consistency adjudication (ADR-0009, DAT-428) — the second pooled measurement.

Proves the witness/pooling template generalizes beyond null_semantics: a different
claim space {consistent, mixed} and different witnesses (log-magnitude bimodality +
declared unit) run on the SAME pooling engine. The scale-mix case (kEUR among EUR)
must raise conflict C; a clean single-scale column must stay quiet.
"""

from __future__ import annotations

import random

from dataraum.entropy.measurements.unit_consistency import (
    bimodality_coefficient,
    declared_unit_distribution,
    magnitude_modality_distribution,
    measure_unit_consistency,
)

_rng = random.Random(7)
_SINGLE_SCALE = [abs(_rng.gauss(500, 60)) for _ in range(200)]  # one decade, unimodal
_SCALE_MIXED = [abs(_rng.gauss(500, 60)) for _ in range(120)] + [
    abs(_rng.gauss(500_000, 60_000)) for _ in range(80)  # kEUR scale mixed into EUR
]


def test_bimodality_coefficient_separates_unimodal_from_bimodal() -> None:
    unimodal = [_rng.gauss(0, 1) for _ in range(400)]
    bimodal = [_rng.gauss(-4, 0.5) for _ in range(200)] + [_rng.gauss(4, 0.5) for _ in range(200)]
    assert bimodality_coefficient(unimodal) < 0.55  # below the uniform reference
    assert bimodality_coefficient(bimodal) > 0.55  # clearly bimodal
    assert bimodality_coefficient([1.0, 1.0, 1.0]) == 0.0  # degenerate / no variance


def test_magnitude_modality_reads_scale_mix_as_mixed() -> None:
    clean = magnitude_modality_distribution(_SINGLE_SCALE)
    mixed = magnitude_modality_distribution(_SCALE_MIXED)
    assert clean["mixed"] < clean["consistent"]  # single scale leans consistent
    assert mixed["mixed"] > mixed["consistent"]  # two scales lean mixed


def test_magnitude_modality_abstains_below_min_sample() -> None:
    assert magnitude_modality_distribution([100.0, 200.0, 300.0]) == {
        "consistent": 0.5,
        "mixed": 0.5,
    }


def test_declared_unit_claims_consistency_or_abstains() -> None:
    assert declared_unit_distribution(0.9)["consistent"] > 0.5  # confident unit → consistent
    assert declared_unit_distribution(None) == {"consistent": 0.5, "mixed": 0.5}  # no unit → abstain
    assert declared_unit_distribution(0.0) == {"consistent": 0.5, "mixed": 0.5}


def test_scale_mix_under_a_declared_unit_raises_conflict() -> None:
    # The adjudication: magnitude reads MIXED while the declared unit insists SINGLE
    # → the witnesses disagree → conflict C is high. A clean column → both agree → low C.
    clean = measure_unit_consistency(_SINGLE_SCALE, unit_confidence=0.9)
    contested = measure_unit_consistency(_SCALE_MIXED, unit_confidence=0.9)
    assert contested.result.conflict > clean.result.conflict
    assert contested.result.conflict > 0.2  # a real disagreement, not noise


def test_witnesses_are_carried_for_persistence() -> None:
    adj = measure_unit_consistency(_SCALE_MIXED, unit_confidence=0.8)
    assert {w.witness_id for w in adj.witnesses} == {"magnitude_modality", "declared_unit"}
    assert all(len(w.distribution) == 2 for w in adj.witnesses)
