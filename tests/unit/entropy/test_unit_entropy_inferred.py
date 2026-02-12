"""Tests for UnitEntropyDetector cross-column unit inference."""

import pytest

from dataraum.entropy.detectors import DetectorContext
from dataraum.entropy.detectors.semantic.unit_entropy import UnitEntropyDetector


class TestUnitEntropyInference:
    """Tests for unit_source_column inference in UnitEntropyDetector."""

    @pytest.fixture
    def detector(self) -> UnitEntropyDetector:
        return UnitEntropyDetector()

    def test_inferred_unit_from_dimension(self, detector: UnitEntropyDetector):
        """When unit_source_column is set, score should be 0.2 (inferred) not 0.8 (missing)."""
        context = DetectorContext(
            table_name="journal_entries",
            column_name="amount",
            analysis_results={
                "typing": {
                    "detected_unit": None,
                    "unit_confidence": 0.0,
                },
                "semantic": {
                    "semantic_role": "measure",
                    "unit_source_column": "currency_code",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.2, abs=0.01)
        assert results[0].evidence[0]["unit_status"] == "inferred_from_dimension"
        assert results[0].evidence[0]["unit_source_column"] == "currency_code"

    def test_missing_unit_no_inference(self, detector: UnitEntropyDetector):
        """Without unit_source_column, missing unit should still score 0.8."""
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "typing": {
                    "detected_unit": None,
                    "unit_confidence": 0.0,
                },
                "semantic": {
                    "semantic_role": "measure",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.8, abs=0.01)
        assert results[0].evidence[0]["unit_status"] == "missing"

    def test_declared_unit_takes_precedence(self, detector: UnitEntropyDetector):
        """Declared unit should score lower than inferred."""
        context = DetectorContext(
            table_name="orders",
            column_name="total_usd",
            analysis_results={
                "typing": {
                    "detected_unit": "USD",
                    "unit_confidence": 0.9,
                },
                "semantic": {
                    "semantic_role": "measure",
                    "unit_source_column": "currency_code",  # Also has inference
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Declared unit takes precedence over inferred
        assert results[0].score == pytest.approx(0.1, abs=0.01)
        assert results[0].evidence[0]["unit_status"] == "declared"

    def test_non_measure_skipped(self, detector: UnitEntropyDetector):
        """Non-measure columns should be skipped entirely."""
        context = DetectorContext(
            table_name="customers",
            column_name="name",
            analysis_results={
                "typing": {},
                "semantic": {
                    "semantic_role": "attribute",
                },
            },
        )

        results = detector.detect(context)
        assert len(results) == 0

    def test_inferred_unit_no_resolution_needed(self, detector: UnitEntropyDetector):
        """Inferred unit (score 0.2) should not suggest resolution (threshold 0.3)."""
        context = DetectorContext(
            table_name="journal_entries",
            column_name="amount",
            analysis_results={
                "typing": {
                    "detected_unit": None,
                    "unit_confidence": 0.0,
                },
                "semantic": {
                    "semantic_role": "measure",
                    "unit_source_column": "currency_code",
                },
            },
        )

        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].resolution_options == []
