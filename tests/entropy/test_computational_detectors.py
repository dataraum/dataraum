"""Tests for computational layer entropy detectors."""

import pytest

from dataraum_context.entropy.detectors import (
    DerivedValueDetector,
    DetectorContext,
)


class TestDerivedValueDetector:
    """Tests for DerivedValueDetector."""

    @pytest.fixture
    def detector(self) -> DerivedValueDetector:
        """Create detector instance."""
        return DerivedValueDetector()

    @pytest.mark.asyncio
    async def test_no_formula_detected(self, detector: DerivedValueDetector):
        """Test max entropy when no formula is detected."""
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "correlation": {
                    "derived_columns": [],
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert results[0].evidence[0]["status"] == "no_formula"
        # Should suggest declaring formula
        actions = [opt.action for opt in results[0].resolution_options]
        assert "declare_formula" in actions

    @pytest.mark.asyncio
    async def test_exact_formula_match(self, detector: DerivedValueDetector):
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

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["status"] == "exact"
        assert results[0].evidence[0]["formula"] == "quantity * unit_price"

    @pytest.mark.asyncio
    async def test_near_exact_formula_match(self, detector: DerivedValueDetector):
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

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.03, abs=0.01)
        assert results[0].evidence[0]["status"] == "near_exact"

    @pytest.mark.asyncio
    async def test_approximate_formula_match(self, detector: DerivedValueDetector):
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

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.15, abs=0.01)
        assert results[0].evidence[0]["status"] == "approximate"
        # Should suggest verification
        actions = [opt.action for opt in results[0].resolution_options]
        assert "verify_formula" in actions

    @pytest.mark.asyncio
    async def test_poor_formula_match(self, detector: DerivedValueDetector):
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

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.4, abs=0.01)
        assert results[0].evidence[0]["status"] == "poor"
        # Should suggest investigation
        actions = [opt.action for opt in results[0].resolution_options]
        assert "verify_formula" in actions
        assert "investigate_mismatches" in actions

    @pytest.mark.asyncio
    async def test_column_not_in_derived_list(self, detector: DerivedValueDetector):
        """Test entropy when column is not in derived columns list."""
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

        results = await detector.detect(context)

        # Column not in derived list = no formula detected
        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert results[0].evidence[0]["status"] == "no_formula"

    @pytest.mark.asyncio
    async def test_evidence_includes_source_columns(self, detector: DerivedValueDetector):
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

        results = await detector.detect(context)

        evidence = results[0].evidence[0]
        assert evidence["source_columns"] == ["qty", "price"]

    @pytest.mark.asyncio
    async def test_cascade_dimensions_on_declare(self, detector: DerivedValueDetector):
        """Test declare_formula resolution cascades to semantic."""
        context = DetectorContext(
            table_name="orders",
            column_name="unknown",
            analysis_results={
                "correlation": {
                    "derived_columns": [],
                }
            },
        )

        results = await detector.detect(context)

        declare_opt = next(
            (opt for opt in results[0].resolution_options if opt.action == "declare_formula"),
            None,
        )
        assert declare_opt is not None
        assert "semantic.business_meaning" in declare_opt.cascade_dimensions

    def test_detector_properties(self, detector: DerivedValueDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "derived_value"
        assert detector.layer == "computational"
        assert detector.dimension == "derived_values"
        assert detector.required_analyses == ["correlation"]
