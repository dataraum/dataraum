"""Tests for business_cycle_health entropy detector."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.semantic.business_cycle_health import (
    BusinessCycleHealthDetector,
)


@pytest.fixture
def detector() -> BusinessCycleHealthDetector:
    return BusinessCycleHealthDetector()


def _make_context(
    cycles: list | None = None,
    table_id: str = "t1",
    table_name: str = "orders",
) -> DetectorContext:
    ctx = DetectorContext(
        table_id=table_id,
        table_name=table_name,
    )
    if cycles is not None:
        ctx.analysis_results["business_cycles"] = cycles
    return ctx


def _make_cycle(
    *,
    cycle_name: str = "order_to_cash",
    cycle_type: str = "revenue",
    canonical_type: str | None = "order_to_cash",
    confidence: float = 0.9,
    completion_rate: float = 0.85,
    total_records: int = 1000,
    completed_cycles: int = 850,
    tables_involved: list[str] | None = None,
) -> MagicMock:
    c = MagicMock()
    c.cycle_name = cycle_name
    c.cycle_type = cycle_type
    c.canonical_type = canonical_type
    c.confidence = confidence
    c.completion_rate = completion_rate
    c.total_records = total_records
    c.completed_cycles = completed_cycles
    c.tables_involved = tables_involved or ["orders", "invoices"]
    return c


class TestDetectNoCycles:
    def test_no_cycles_returns_zero(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context(cycles=[])
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 0.0
        assert objects[0].evidence[0]["reason"] == "no_cycles_involving_table"

    def test_no_data_loaded(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context()
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 0.0


class TestDetectHealthy:
    def test_high_completion_high_confidence(self, detector: BusinessCycleHealthDetector):
        """90% confidence, 85% completion → score = max(0.15, 0.10) = 0.15."""
        ctx = _make_context(cycles=[_make_cycle(confidence=0.9, completion_rate=0.85)])
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.15)

    def test_perfect_cycle(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context(cycles=[_make_cycle(confidence=1.0, completion_rate=1.0)])
        objects = detector.detect(ctx)
        assert objects[0].score == 0.0


class TestDetectUnhealthy:
    def test_low_completion(self, detector: BusinessCycleHealthDetector):
        """30% completion → score = max(0.7, 0.1) = 0.7."""
        ctx = _make_context(cycles=[_make_cycle(completion_rate=0.3, confidence=0.9)])
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.7)

    def test_low_confidence(self, detector: BusinessCycleHealthDetector):
        """40% confidence → score = max(0.15, 0.6) = 0.6."""
        ctx = _make_context(cycles=[_make_cycle(confidence=0.4, completion_rate=0.85)])
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.6)

    def test_null_completion_rate(self, detector: BusinessCycleHealthDetector):
        """None completion_rate treated as 0.0 → score = 1.0."""
        ctx = _make_context(cycles=[_make_cycle(completion_rate=None, confidence=0.9)])
        # MagicMock returns the default we set, but let's override
        cycle = _make_cycle()
        cycle.completion_rate = None
        cycle.confidence = 0.9
        ctx = _make_context(cycles=[cycle])
        objects = detector.detect(ctx)
        assert objects[0].score == 1.0


class TestMaxAggregation:
    def test_worst_cycle_drives_score(self, detector: BusinessCycleHealthDetector):
        """Two cycles — worst one wins."""
        ctx = _make_context(
            cycles=[
                _make_cycle(cycle_name="healthy", confidence=0.95, completion_rate=0.9),
                _make_cycle(cycle_name="unhealthy", confidence=0.3, completion_rate=0.2),
            ]
        )
        objects = detector.detect(ctx)
        # max(1-0.2, 1-0.3) = max(0.8, 0.7) = 0.8
        assert objects[0].score == pytest.approx(0.8)


class TestEvidence:
    def test_evidence_per_cycle(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context(
            cycles=[
                _make_cycle(cycle_name="c1"),
                _make_cycle(cycle_name="c2"),
            ]
        )
        objects = detector.detect(ctx)
        evidence = objects[0].evidence
        assert len(evidence) == 2
        assert evidence[0]["cycle_name"] == "c1"
        assert evidence[1]["cycle_name"] == "c2"
        assert "confidence" in evidence[0]
        assert "completion_rate" in evidence[0]


class TestResolutionOptions:
    def test_resolution_on_low_health(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context(cycles=[_make_cycle(completion_rate=0.2)])
        objects = detector.detect(ctx)
        assert len(objects[0].resolution_options) == 1
        assert objects[0].resolution_options[0].action == "investigate_cycle_health"

    def test_no_resolution_when_healthy(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context(cycles=[_make_cycle(confidence=0.9, completion_rate=0.9)])
        objects = detector.detect(ctx)
        assert len(objects[0].resolution_options) == 0


class TestDetectorProperties:
    def test_detector_id(self, detector: BusinessCycleHealthDetector):
        assert detector.detector_id == "business_cycle_health"

    def test_scope(self, detector: BusinessCycleHealthDetector):
        assert detector.scope == "table"

    def test_layer(self, detector: BusinessCycleHealthDetector):
        assert str(detector.layer) == "semantic"

    def test_required_analyses(self, detector: BusinessCycleHealthDetector):
        assert str(detector.required_analyses[0]) == "business_cycles"
