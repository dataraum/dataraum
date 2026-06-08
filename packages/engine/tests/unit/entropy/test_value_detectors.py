"""Tests for value layer entropy detectors."""

import pytest

from dataraum.entropy.detectors import (
    BenfordDetector,
    DetectorContext,
    NullRatioDetector,
    OutlierRateDetector,
    TemporalDriftDetector,
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


class TestOutlierRateDetector:
    """Tests for OutlierRateDetector."""

    @pytest.fixture
    def detector(self) -> OutlierRateDetector:
        """Create detector instance."""
        return OutlierRateDetector()

    def test_no_outliers(self, detector: OutlierRateDetector):
        """Test entropy is 0 for no outliers."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.0,
                        "iqr_outlier_count": 0,
                        "iqr_lower_fence": 10.0,
                        "iqr_upper_fence": 100.0,
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "none"

    def test_few_outliers(self, detector: OutlierRateDetector):
        """Test low entropy for few outliers (piecewise scoring)."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.005,
                        "iqr_outlier_count": 5,
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # 0.5% is halfway through the 0-1% (minimal) band → score ~0.075
        assert results[0].score == pytest.approx(0.075, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "minimal"

    def test_significant_outliers(self, detector: OutlierRateDetector):
        """Test moderate entropy for significant outliers (piecewise scoring)."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.08,
                        "iqr_outlier_count": 80,
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # 8% is 60% through the 5-10% (significant) band → score ~0.55
        assert results[0].score == pytest.approx(0.55, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "significant"

    def test_high_outliers(self, detector: OutlierRateDetector):
        """Test high entropy for 20%+ outliers (piecewise scoring reaches 1.0)."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.20,
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "critical"

    def test_piecewise_scoring_curve(self, detector: OutlierRateDetector):
        """Test piecewise scoring at key breakpoints."""
        test_cases = [
            (0.0, 0.0),  # 0% → 0.0
            (0.01, 0.15),  # 1% → 0.15
            (0.05, 0.40),  # 5% → 0.40
            (0.10, 0.65),  # 10% → 0.65
            (0.20, 1.0),  # 20% → 1.0
            (0.50, 1.0),  # 50% → capped at 1.0
        ]
        for ratio, expected_score in test_cases:
            context = DetectorContext(
                table_name="test",
                column_name="col",
                analysis_results={
                    "statistics": {
                        "outlier_detection": {"iqr_outlier_ratio": ratio},
                    },
                    "semantic": {"semantic_role": "measure"},
                },
            )
            results = detector.detect(context)
            assert results[0].score == pytest.approx(expected_score, abs=0.01), (
                f"ratio={ratio}: expected {expected_score}, got {results[0].score}"
            )

    def test_direct_stats_format(self, detector: OutlierRateDetector):
        """Test detector works with direct stats format (piecewise scoring)."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "iqr_outlier_ratio": 0.03,
                    "iqr_outlier_count": 30,
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # 3% is halfway through the 1-5% (moderate) band → score ~0.275
        assert results[0].score == pytest.approx(0.275, abs=0.01)

    def test_excluded_column_returns_empty(self, detector: OutlierRateDetector):
        """Excluded columns (no outlier_detection key) return [] not a false 0-score."""
        context = DetectorContext(
            table_name="fx_rates",
            column_name="rate",
            analysis_results={
                "statistics": {
                    "quality": {
                        "benford_compliant": True,
                        "benford_analysis": {"is_compliant": True},
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert results == []

    def test_skip_key_column(self, detector: OutlierRateDetector):
        """Test outlier detection is skipped for key columns."""
        context = DetectorContext(
            table_name="orders",
            column_name="order_id",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.05,
                        "iqr_outlier_count": 50,
                    }
                },
                "semantic": {
                    "semantic_role": "key",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 0

    def test_skip_foreign_key_column(self, detector: OutlierRateDetector):
        """Test outlier detection is skipped for foreign key columns."""
        context = DetectorContext(
            table_name="order_items",
            column_name="order_id",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.08,
                    }
                },
                "semantic": {
                    "semantic_role": "foreign_key",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 0

    def test_runs_for_measure_column(self, detector: OutlierRateDetector):
        """Test outlier detection runs normally for measure columns."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.05,
                    }
                },
                "semantic": {
                    "semantic_role": "measure",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score > 0

    def test_cv_attenuation_proportional(self, detector: OutlierRateDetector):
        """High-CV columns get proportionally dampened scores using robust_cv."""
        context = DetectorContext(
            table_name="orders",
            column_name="fx_rate",
            analysis_results={
                "statistics": {
                    "outlier_detection": {"iqr_outlier_ratio": 0.10},
                    "profile_data": {"numeric_stats": {"robust_cv": 6.5}},
                },
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # Raw score at 10% = 0.65. robust_cv=6.5, threshold=2.0 → dampen = 2.0/6.5 ≈ 0.308
        # Attenuated = 0.65 * 0.308 = 0.200
        assert results[0].score == pytest.approx(0.200, abs=0.01)
        assert results[0].evidence[0]["cv_attenuated"] is True
        assert results[0].evidence[0]["robust_cv"] == 6.5

    def test_cv_attenuation_preserves_ordering(self, detector: OutlierRateDetector):
        """Two columns with same robust_cv: higher outlier ratio → higher attenuated score."""
        scores = []
        for ratio in [0.10, 0.15]:
            context = DetectorContext(
                table_name="orders",
                column_name="amount",
                analysis_results={
                    "statistics": {
                        "outlier_detection": {"iqr_outlier_ratio": ratio},
                        "profile_data": {"numeric_stats": {"robust_cv": 4.0}},
                    },
                    "semantic": {"semantic_role": "measure"},
                },
            )
            results = detector.detect(context)
            scores.append(results[0].score)
        assert scores[1] > scores[0], f"Higher ratio should give higher score: {scores}"

    def test_no_cv_attenuation_below_threshold(self, detector: OutlierRateDetector):
        """Scores are not attenuated when robust_cv is below threshold."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {"iqr_outlier_ratio": 0.10},
                    "profile_data": {"numeric_stats": {"robust_cv": 1.5}},
                },
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # No attenuation: raw score at 10% = 0.65
        assert results[0].score == pytest.approx(0.65, abs=0.01)
        assert "cv_attenuated" not in results[0].evidence[0]

    def test_no_attenuation_without_robust_cv(self, detector: OutlierRateDetector):
        """No attenuation when only classical cv is available (no silent fallback)."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {"iqr_outlier_ratio": 0.10},
                    "profile_data": {"numeric_stats": {"cv": 4.0}},
                },
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # No attenuation: raw score at 10% = 0.65 (classical cv is ignored)
        assert results[0].score == pytest.approx(0.65, abs=0.01)
        assert "cv_attenuated" not in results[0].evidence[0]

    def test_detector_properties(self, detector: OutlierRateDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "outlier_rate"
        assert detector.layer == "value"
        assert detector.dimension == "outliers"
        assert detector.required_analyses == ["statistics", "semantic"]


class _MockDriftSummary:
    """Lightweight mock for ColumnDriftSummary (avoids DB session)."""

    def __init__(
        self,
        column_name: str,
        max_js_divergence: float,
        mean_js_divergence: float,
        periods_analyzed: int,
        periods_with_drift: int,
        drift_evidence_json: dict | None = None,
    ):
        self.column_name = column_name
        self.max_js_divergence = max_js_divergence
        self.mean_js_divergence = mean_js_divergence
        self.periods_analyzed = periods_analyzed
        self.periods_with_drift = periods_with_drift
        self.drift_evidence_json = drift_evidence_json


class TestTemporalDriftDetector:
    """Tests for TemporalDriftDetector."""

    @pytest.fixture
    def detector(self) -> TemporalDriftDetector:
        return TemporalDriftDetector()

    def test_no_drift_summaries(self, detector: TemporalDriftDetector):
        """Returns empty when no drift summaries available."""
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": []},
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_no_matching_column(self, detector: TemporalDriftDetector):
        """Returns empty when column not in drift summaries."""
        summary = _MockDriftSummary("other_col", 0.5, 0.3, 5, 2)
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": [summary]},
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_zero_drift(self, detector: TemporalDriftDetector):
        """Score is 0 when JS divergence is 0."""
        summary = _MockDriftSummary("amount", 0.0, 0.0, 5, 0)
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)

    def test_mild_drift(self, detector: TemporalDriftDetector):
        """Score ~0.3 for mean JS 0.1 (scoring uses mean, not max)."""
        summary = _MockDriftSummary("amount", 0.2, 0.1, 5, 1)
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.3, abs=0.01)

    def test_moderate_drift(self, detector: TemporalDriftDetector):
        """Score ~0.7 for mean JS 0.3."""
        summary = _MockDriftSummary("amount", 0.5, 0.3, 5, 2)
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7, abs=0.01)

    def test_severe_drift(self, detector: TemporalDriftDetector):
        """Score 1.0 for mean JS 0.5+."""
        summary = _MockDriftSummary("amount", 0.7, 0.6, 5, 4)
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)

    def test_evidence_includes_drift_details(self, detector: TemporalDriftDetector):
        """Evidence includes drift summary info."""
        summary = _MockDriftSummary(
            "amount",
            0.4,
            0.2,
            5,
            3,
            drift_evidence_json={
                "worst_period": "2024-Q3",
                "worst_js": 0.4,
                "top_shifts": [
                    {
                        "category": "Active",
                        "baseline_pct": 45,
                        "period_pct": 12,
                        "period": "2024-Q3",
                    }
                ],
            },
        )
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        ev = results[0].evidence[0]
        assert ev["max_js_divergence"] == 0.4
        assert ev["worst_period"] == "2024-Q3"
        assert len(ev["top_shifts"]) == 1

    def test_skip_key_column(self, detector: TemporalDriftDetector):
        """Drift detection is skipped for key columns."""
        summary = _MockDriftSummary("order_id", 0.693, 0.5, 5, 5)
        context = DetectorContext(
            table_name="orders",
            column_name="order_id",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "key"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_skip_point_in_time_measure(self, detector: TemporalDriftDetector):
        """Periodic snapshot measures (period balances) drift by data-model
        design — skipped (DAT-405; clean trial_balance balances scored 0.45+)."""
        summary = _MockDriftSummary("debit_balance", 0.6, 0.5, 12, 8)
        context = DetectorContext(
            table_name="trial_balance",
            column_name="debit_balance",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {
                    "semantic_role": "measure",
                    "temporal_behavior": "point_in_time",
                },
            },
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_additive_measure_keeps_drift_detection(self, detector: TemporalDriftDetector):
        """Additive measures (transaction amounts) are still scored."""
        summary = _MockDriftSummary("amount", 0.5, 0.3, 5, 2)
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {
                    "semantic_role": "measure",
                    "temporal_behavior": "additive",
                },
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7, abs=0.01)

    def test_skip_foreign_key_column(self, detector: TemporalDriftDetector):
        """Drift detection is skipped for foreign key columns."""
        summary = _MockDriftSummary("vendor_id", 0.693, 0.5, 5, 5)
        context = DetectorContext(
            table_name="invoices",
            column_name="vendor_id",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "foreign_key"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_skip_identifier_column(self, detector: TemporalDriftDetector):
        """Drift detection is skipped for identifier columns."""
        summary = _MockDriftSummary("entry_id", 0.693, 0.5, 5, 5)
        context = DetectorContext(
            table_name="journal_entries",
            column_name="entry_id",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "identifier"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_runs_for_measure_column(self, detector: TemporalDriftDetector):
        """Drift detection runs normally for measure columns."""
        summary = _MockDriftSummary("amount", 0.3, 0.15, 5, 2)
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "drift_summaries": [summary],
                "semantic": {"semantic_role": "measure"},
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score > 0

    def test_detector_properties(self, detector: TemporalDriftDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "temporal_drift"
        assert detector.layer == "value"
        assert detector.dimension == "temporal"
        assert detector.required_analyses == ["drift_summaries", "semantic"]


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
        analysis: dict[str, object] = {"is_compliant": True, "chi_square": 5.0, "p_value": 0.8}
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
        assert results[0].evidence[0]["is_compliant"] is True

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
