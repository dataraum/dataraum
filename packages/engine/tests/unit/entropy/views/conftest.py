"""Pytest fixtures for entropy views tests.

EntropyObject builder for readiness tests. The readiness rollup is loss-only
(entropy/loss.yaml) — no network fixtures (DAT-442).
"""

from dataraum.entropy.models import EntropyObject


def make_entropy_object(
    *,
    layer: str = "value",
    dimension: str = "nulls",
    sub_dimension: str = "null_ratio",
    target: str = "column:test_table.col1",
    score: float = 0.5,
    evidence: list | None = None,
    detector_id: str = "null_ratio",
) -> EntropyObject:
    """Create an EntropyObject for testing.

    Defaults to ``null_ratio`` — a loss measurement carrying all three intents — so
    a bare object drives the rollup. Pass ``detector_id`` not in ``loss.yaml`` (e.g.
    ``"benford"``) to exercise the direct-signal path.
    """
    return EntropyObject(
        layer=layer,
        dimension=dimension,
        sub_dimension=sub_dimension,
        target=target,
        score=score,
        evidence=evidence or [{"metric": "test", "value": score}],
        detector_id=detector_id,
    )


def make_abstention(
    *,
    target: str = "column:test_table.col1",
    detector_id: str = "null_ratio",
    reason: str = "not_applicable",
    sub_dimension: str = "null_ratio",
) -> EntropyObject:
    """An abstained EntropyObject (DAT-853): score None, first-class row."""
    return EntropyObject(
        layer="value",
        dimension="nulls",
        sub_dimension=sub_dimension,
        target=target,
        score=None,
        status="abstained",
        abstain_reason=reason,
        detector_id=detector_id,
    )
