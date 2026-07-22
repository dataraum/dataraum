"""Tests for value layer entropy detectors."""

import pytest

from dataraum.entropy.detectors import (
    BenfordDetector,
    DetectorContext,
    NullRatioDetector,
)


class TestNullRatioDetector:
    """Tests for NullRatioDetector."""

    @pytest.fixture
    def detector(self) -> NullRatioDetector:
        """Create detector instance."""
        return NullRatioDetector()

    def test_no_nulls(self, detector: NullRatioDetector):
        """Test entropy is 0 for no nulls."""
        context = DetectorContext(
            table_name="orders",
            column_name="id",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.0,
                    "null_count": 0,
                    "total_count": 1000,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "none"

    def test_low_nulls(self, detector: NullRatioDetector):
        """Test low entropy for minimal nulls."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.02,
                    "null_count": 20,
                    "total_count": 1000,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.02, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "minimal"

    def test_high_nulls(self, detector: NullRatioDetector):
        """Test high entropy for significant nulls."""
        context = DetectorContext(
            table_name="orders",
            column_name="discount",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.5,
                    "null_count": 500,
                    "total_count": 1000,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.5, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "critical"

    def test_max_entropy_at_full_nulls(self, detector: NullRatioDetector):
        """Test entropy is 1.0 for fully null column."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "null_ratio": 1.0,
                }
            },
        )

        results = detector.detect(context)

        assert results[0].score == pytest.approx(1.0, abs=0.01)

    def test_detector_properties(self, detector: NullRatioDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "null_ratio"
        assert detector.layer == "value"
        assert detector.dimension == "nulls"
        assert detector.required_analyses == ["statistics"]


class TestBenfordDetector:
    """Tests for BenfordDetector — KL-surprise scoring (DAT-442).

    Score = ``surprise_score(observed leading digits, Benford)``: ~0 when the
    distribution follows Benford, high when it departs. Sample-size invariant, so
    no chi-square / Cramér's-V machinery (clean data no longer floors at 0.7).
    """

    # Production digit_distribution shape: a dict keyed by string digit "1".."9".
    _BENFORD_DIST = {
        "1": 0.301,
        "2": 0.176,
        "3": 0.125,
        "4": 0.097,
        "5": 0.079,
        "6": 0.067,
        "7": 0.058,
        "8": 0.051,
        "9": 0.046,
    }
    # Mass piled where Benford expects little (digit 5, digit 9) → high surprise.
    _SKEWED_DIST = {
        "1": 0.05,
        "2": 0.05,
        "3": 0.05,
        "4": 0.05,
        "5": 0.45,
        "6": 0.05,
        "7": 0.05,
        "8": 0.05,
        "9": 0.20,
    }

    @pytest.fixture
    def detector(self) -> BenfordDetector:
        """Create detector instance."""
        return BenfordDetector()

    def _context(
        self,
        digit_distribution: dict[str, float] | None,
        *,
        total_count: int = 1000,
        role: str = "measure",
    ) -> DetectorContext:
        """Build a detector context with the given observed digit distribution."""
        analysis: dict[str, object] = {"status": "compliant", "chi_square": 5.0, "p_value": 0.8}
        if digit_distribution is not None:
            analysis["digit_distribution"] = digit_distribution
        return DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "total_count": total_count,
                    "quality": {"benford_analysis": analysis},
                },
                "semantic": {"semantic_role": role},
            },
        )

    def test_skip_non_measure_column(self, detector: BenfordDetector):
        """Benford only applies to measure columns."""
        ctx = self._context(self._BENFORD_DIST, role="key")
        assert detector.detect(ctx) == []

    def test_skip_no_benford_data(self, detector: BenfordDetector):
        """Skip if no Benford analysis available."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {"quality": {}},
                "semantic": {"semantic_role": "measure"},
            },
        )
        assert detector.detect(context) == []

    def test_skip_without_digit_distribution(self, detector: BenfordDetector):
        """The observed digit distribution is required for a surprise score.

        The old boolean/p-value scoring path is gone: a benford_analysis without
        ``digit_distribution`` carries no observed distribution to score against.
        """
        assert detector.detect(self._context(None)) == []

    def test_skip_small_sample(self, detector: BenfordDetector):
        """Below min_sample_size the leading-digit frequencies are too noisy."""
        assert detector.detect(self._context(self._BENFORD_DIST, total_count=50)) == []

    def test_compliant_scores_near_zero(self, detector: BenfordDetector):
        """A Benford-following distribution is unsurprising → score ~0.

        This is the precision fix: clean financial data scores ~0, not the 0.7
        the chi-square boost curve produced at large n.
        """
        results = detector.detect(self._context(self._BENFORD_DIST))
        assert len(results) == 1
        assert results[0].score < 0.02
        assert results[0].evidence[0]["kl_bits"] < 0.05
        assert results[0].evidence[0]["status"] == "compliant"

    def test_skewed_distribution_is_high_surprise(self, detector: BenfordDetector):
        """A distribution that departs Benford hard → high surprise."""
        results = detector.detect(self._context(self._SKEWED_DIST))
        assert len(results) == 1
        assert results[0].score > 0.4

    def test_surprise_orders_skewed_above_compliant(self, detector: BenfordDetector):
        """The score discriminates: skewed > compliant (real recall signal)."""
        compliant = detector.detect(self._context(self._BENFORD_DIST))[0].score
        skewed = detector.detect(self._context(self._SKEWED_DIST))[0].score
        assert skewed > compliant

    def test_detector_properties(self, detector: BenfordDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "benford"
        assert detector.layer == "value"
        assert detector.dimension == "distribution"
        assert detector.required_analyses == ["statistics", "semantic"]


class TestBenfordAbstention:
    """DAT-843/853: a bounded-magnitude column abstains — never a KL verdict."""

    @pytest.fixture
    def detector(self) -> BenfordDetector:
        return BenfordDetector()

    def _context_not_applicable(self) -> DetectorContext:
        return DetectorContext(
            table_name="orders",
            column_name="rating",
            analysis_results={
                "statistics": {
                    "total_count": 1000,
                    "quality": {
                        "benford_analysis": {
                            "status": "not_applicable",
                            "magnitude_span_decades": 0.95,
                            "interpretation": "Benford's Law not applicable: values span 0.95 decades (< 1 order of magnitude)",
                        }
                    },
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

    def test_not_applicable_abstains(self, detector: BenfordDetector):
        results = detector.detect(self._context_not_applicable())
        assert len(results) == 1
        obj = results[0]
        assert obj.status == "abstained"
        assert obj.abstain_reason == "not_applicable"
        assert obj.score is None
        assert obj.evidence[0]["magnitude_span_decades"] == 0.95
