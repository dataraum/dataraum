"""Tests for cross_table_consistency entropy detector."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.computational.cross_table_consistency import (
    CrossTableConsistencyDetector,
    _score_validation_result,
)


@pytest.fixture
def detector() -> CrossTableConsistencyDetector:
    return CrossTableConsistencyDetector()


def _make_context(
    validations: list | None = None,
    table_id: str = "t1",
    table_name: str = "orders",
) -> DetectorContext:
    ctx = DetectorContext(
        table_id=table_id,
        table_name=table_name,
    )
    if validations is not None:
        ctx.analysis_results["validation"] = validations
    return ctx


def _make_result(
    *,
    passed: bool = False,
    status: str = "failed",
    severity: str = "critical",
    details: dict | None = None,
    validation_id: str = "v1",
    message: str | None = None,
) -> MagicMock:
    r = MagicMock()
    r.passed = passed
    r.status = status
    r.severity = severity
    r.details = details or {}
    r.validation_id = validation_id
    r.message = message
    return r


class TestScoreValidationResult:
    def test_passed_returns_zero(self):
        r = _make_result(passed=True)
        assert _score_validation_result(r) == 0.0

    def test_error_returns_moderate(self):
        r = _make_result(status="error")
        assert _score_validation_result(r) == 0.5

    def test_comparison_failure_returns_one(self):
        r = _make_result(details={"check_type": "comparison"})
        assert _score_validation_result(r) == 1.0

    def test_balance_small_difference(self):
        """$50k difference on $50M total → small raw ratio, sqrt-boosted."""
        r = _make_result(
            details={
                "check_type": "balance",
                "difference": 50_000,
                "magnitude": 50_000_000,
            }
        )
        score = _score_validation_result(r)
        assert 0.02 < score < 0.05  # sqrt(0.001) ≈ 0.032

    def test_balance_zero_magnitude(self):
        r = _make_result(details={"check_type": "balance", "difference": 100, "magnitude": 0})
        assert _score_validation_result(r) == 1.0

    def test_aggregate_orphan_rate(self):
        """5% orphans → sqrt(0.05) ≈ 0.224."""
        r = _make_result(details={"check_type": "aggregate", "orphan_rate": 0.05})
        score = _score_validation_result(r)
        assert 0.20 < score < 0.25

    def test_constraint_violations(self):
        """10 violations in 1000 rows → sqrt(0.01) ≈ 0.1."""
        r = _make_result(
            details={"check_type": "constraint", "violation_count": 10, "total_rows": 1000}
        )
        score = _score_validation_result(r)
        assert 0.09 < score < 0.11

    def test_unknown_check_type_uses_severity(self):
        r = _make_result(severity="medium", details={"check_type": "exotic"})
        assert _score_validation_result(r) == 0.4


class TestDetectNoResults:
    def test_no_validations_returns_zero(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context(validations=[])
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 0.0

    def test_no_data_loaded(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context()
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 0.0


class TestDetectFailures:
    def test_single_critical_failure(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context(validations=[_make_result(details={"check_type": "comparison"})])
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 1.0

    def test_max_aggregation(self, detector: CrossTableConsistencyDetector):
        """Worst failure drives the score."""
        ctx = _make_context(
            validations=[
                _make_result(passed=True, status="passed", validation_id="v1"),
                _make_result(
                    details={"check_type": "aggregate", "orphan_rate": 0.05},
                    validation_id="v2",
                ),
            ]
        )
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.2236, abs=1e-3)

    def test_evidence_per_check(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context(
            validations=[
                _make_result(validation_id="v1", passed=True, status="passed"),
                _make_result(validation_id="v2", severity="high"),
            ]
        )
        objects = detector.detect(ctx)
        evidence = objects[0].evidence
        assert len(evidence) == 2
        assert evidence[0]["validation_id"] == "v1"
        assert evidence[1]["validation_id"] == "v2"


class TestResolutionOptions:
    def test_resolution_on_failure(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context(validations=[_make_result(details={"check_type": "comparison"})])
        objects = detector.detect(ctx)
        assert len(objects[0].resolution_options) == 1
        assert objects[0].resolution_options[0].action == "investigate_reconciliation"

    def test_no_resolution_when_all_pass(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context(validations=[_make_result(passed=True, status="passed")])
        objects = detector.detect(ctx)
        assert len(objects[0].resolution_options) == 0


class TestDetectorProperties:
    def test_detector_id(self, detector: CrossTableConsistencyDetector):
        assert detector.detector_id == "cross_table_consistency"

    def test_scope(self, detector: CrossTableConsistencyDetector):
        assert detector.scope == "table"

    def test_layer(self, detector: CrossTableConsistencyDetector):
        assert str(detector.layer) == "computational"

    def test_required_analyses(self, detector: CrossTableConsistencyDetector):
        assert str(detector.required_analyses[0]) == "validation"
