"""Loss layer — risk(intent) = w_conflict·C + w_ignorance·U (ADR-0009, DAT-457).

Severity lives in the loss table, not the score: the SAME conflict is weighted
differently per intent, and a resolved column (C→0, e.g. after a teach) collapses
to low risk so the band actually moves.
"""

from __future__ import annotations

import pytest

from dataraum.entropy.loss import LossConfig, compute_loss_risk, loss_risk_for_object
from dataraum.entropy.models import EntropyObject

_CONFIG = LossConfig(
    measurements={
        "null_semantics": {
            "query_intent": {"conflict": 0.4, "ignorance": 0.2},
            "aggregation_intent": {"conflict": 0.9, "ignorance": 0.5},
        }
    }
)


def _obj(conflict: float, ignorance: float, detector_id: str = "null_semantics") -> EntropyObject:
    return EntropyObject(
        detector_id=detector_id,
        score=conflict,
        evidence=[{"token": "x", "ignorance": ignorance, "posterior": {}}],
    )


def test_risk_is_weighted_conflict_plus_ignorance() -> None:
    risk = loss_risk_for_object(_obj(0.5, 0.4), _CONFIG)
    assert risk["query_intent"] == pytest.approx(0.4 * 0.5 + 0.2 * 0.4)
    assert risk["aggregation_intent"] == pytest.approx(0.9 * 0.5 + 0.5 * 0.4)


def test_aggregation_punishes_conflict_harder_than_query() -> None:
    # The point of the loss TABLE: the same C is catastrophic for aggregation,
    # mild for query — severity is per-intent, not baked into one score.
    risk = loss_risk_for_object(_obj(0.5, 0.0), _CONFIG)
    assert risk["aggregation_intent"] > risk["query_intent"]


def test_resolved_column_collapses_risk() -> None:
    # Teach-closure: C 0.25 → ~0.02 drops the band.
    contested = loss_risk_for_object(_obj(0.25, 0.4), _CONFIG)
    resolved = loss_risk_for_object(_obj(0.02, 0.4), _CONFIG)
    assert resolved["aggregation_intent"] < contested["aggregation_intent"]


def test_risk_clamped_to_one() -> None:
    assert all(r <= 1.0 for r in loss_risk_for_object(_obj(1.0, 1.0), _CONFIG).values())


def test_non_loss_detector_yields_nothing() -> None:
    assert loss_risk_for_object(_obj(0.9, 0.5, detector_id="type_fidelity"), _CONFIG) == {}


def test_compute_loss_risk_takes_worst_across_objects() -> None:
    merged = compute_loss_risk([_obj(0.2, 0.1), _obj(0.6, 0.3)], _CONFIG)
    assert merged["aggregation_intent"] == pytest.approx(0.9 * 0.6 + 0.5 * 0.3)


def test_empty_objects_no_risk() -> None:
    assert compute_loss_risk([], _CONFIG) == {}
