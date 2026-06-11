"""Tests for computational layer entropy detectors."""

import pytest

from dataraum.entropy.detectors import (
    DerivedValueDetector,
    DetectorContext,
)


class TestDerivedValueDetector:
    """Tests for DerivedValueDetector."""

    @pytest.fixture
    def detector(self) -> DerivedValueDetector:
        """Create detector instance."""
        return DerivedValueDetector()

    def test_no_formula_detected(self, detector: DerivedValueDetector):
        """No formula detected → nothing to measure: ignorance, not a fabricated 1.0.

        The old no_formula→score=1.0 branch was theater (DAT-442 two-table): a column
        with no detected derived formula is not a 100%-broken derivation.
        """
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "correlation": {
                    "derived_columns": [],
                }
            },
        )

        assert detector.detect(context) == []

    def test_exact_formula_match(self, detector: DerivedValueDetector):
        """Test low entropy for exact formula match."""
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "correlation": {
                    "derived_columns": [
                        {
                            "derived_column_name": "total",
                            "match_rate": 1.0,
                            "formula": "quantity * unit_price",
                            "derivation_type": "product",
                            "source_column_names": ["quantity", "unit_price"],
                        }
                    ],
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["status"] == "exact"
        assert results[0].evidence[0]["formula"] == "quantity * unit_price"

    def test_near_exact_formula_match(self, detector: DerivedValueDetector):
        """Test low entropy for near-exact formula match."""
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "correlation": {
                    "derived_columns": [
                        {
                            "derived_column_name": "total",
                            "match_rate": 0.97,
                            "formula": "quantity * unit_price",
                        }
                    ],
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Honest mismatch rate=0.03 (no boost, DAT-442)
        assert results[0].score == pytest.approx(0.03, abs=0.01)
        assert results[0].evidence[0]["status"] == "near_exact"

    def test_approximate_formula_match(self, detector: DerivedValueDetector):
        """Test moderate entropy for approximate formula match."""
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "correlation": {
                    "derived_columns": [
                        {
                            "derived_column_name": "total",
                            "match_rate": 0.85,
                            "formula": "subtotal + tax",
                        }
                    ],
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Honest mismatch rate=0.15 (no boost, DAT-442)
        assert results[0].score == pytest.approx(0.15, abs=0.01)
        assert results[0].evidence[0]["status"] == "approximate"

    def test_poor_formula_match(self, detector: DerivedValueDetector):
        """Test high entropy for poor formula match."""
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "correlation": {
                    "derived_columns": [
                        {
                            "derived_column_name": "total",
                            "match_rate": 0.6,
                            "formula": "a + b",
                        }
                    ],
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Honest mismatch rate=0.40 (no boost, DAT-442)
        assert results[0].score == pytest.approx(0.40, abs=0.01)
        assert results[0].evidence[0]["status"] == "poor"

    def test_column_not_in_derived_list(self, detector: DerivedValueDetector):
        """Column not in the derived list → no formula → nothing to measure (empty)."""
        context = DetectorContext(
            table_name="orders",
            column_name="other_col",
            analysis_results={
                "correlation": {
                    "derived_columns": [
                        {
                            "derived_column_name": "total",
                            "match_rate": 1.0,
                            "formula": "a + b",
                        }
                    ],
                }
            },
        )

        # Column not in derived list = no formula → ignorance, not a fabricated 1.0.
        assert detector.detect(context) == []

    def test_evidence_includes_source_columns(self, detector: DerivedValueDetector):
        """Test evidence includes source columns."""
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "correlation": {
                    "derived_columns": [
                        {
                            "derived_column_name": "total",
                            "match_rate": 0.9,
                            "formula": "qty * price",
                            "source_column_names": ["qty", "price"],
                        }
                    ],
                }
            },
        )

        results = detector.detect(context)

        evidence = results[0].evidence[0]
        assert evidence["source_columns"] == ["qty", "price"]

    def test_detector_properties(self, detector: DerivedValueDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "derived_value"
        assert detector.layer == "computational"
        assert detector.dimension == "derived_values"
        # No required_analyses (second-witness landing): either witness path may
        # be absent — load_data self-loads correlation AND the semantic
        # hypothesis; detect() measures whatever is present.
        assert detector.required_analyses == []
