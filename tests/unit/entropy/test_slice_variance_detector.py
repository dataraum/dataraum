"""Tests for SliceVarianceDetector."""

import pytest

from dataraum.entropy.detectors import DetectorContext, SliceVarianceDetector


class TestSliceVarianceDetector:
    """Tests for SliceVarianceDetector."""

    @pytest.fixture
    def detector(self) -> SliceVarianceDetector:
        return SliceVarianceDetector()

    def test_stable_column_score_zero(self, detector: SliceVarianceDetector):
        """Stable column with identical stats across slices → score 0."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.05,
                        "distinct_count": 100,
                        "outlier_ratio": 0.01,
                        "benford_p_value": 0.5,
                        "row_count": 1000,
                        "slice_table_name": "slice_orders_region_us",
                    },
                    {
                        "null_ratio": 0.05,
                        "distinct_count": 100,
                        "outlier_ratio": 0.01,
                        "benford_p_value": 0.5,
                        "row_count": 1000,
                        "slice_table_name": "slice_orders_region_eu",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.001)
        assert results[0].evidence[0]["exceeded_thresholds"] == []

    def test_varying_null_ratios(self, detector: SliceVarianceDetector):
        """Varying null ratios across slices → score reflects null_spread."""
        context = DetectorContext(
            table_name="orders",
            column_name="discount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 50,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_orders_region_us",
                    },
                    {
                        "null_ratio": 0.20,
                        "distinct_count": 50,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_orders_region_eu",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # null_spread = 0.20, threshold = 0.10
        # norm = min(0.20 / (2 * 0.10), 1.0) = 1.0
        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert "null_spread" in results[0].evidence[0]["exceeded_thresholds"]

    def test_varying_outlier_ratios(self, detector: SliceVarianceDetector):
        """Varying outlier ratios → score reflects outlier_spread."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_a",
                    },
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 100,
                        "outlier_ratio": 0.08,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_b",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # outlier_spread = 0.08, threshold = 0.25
        # norm = min(0.08 / (2 * 0.25), 1.0) = 0.16
        assert results[0].score == pytest.approx(0.16, abs=0.01)
        assert "outlier_spread" not in results[0].evidence[0]["exceeded_thresholds"]

    def test_fewer_than_two_slices_returns_empty(self, detector: SliceVarianceDetector):
        """With < 2 slices, no variance measurable → empty result."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.5,
                        "distinct_count": 10,
                        "outlier_ratio": 0.1,
                        "benford_p_value": None,
                        "row_count": 100,
                        "slice_table_name": "slice_a",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_no_slice_profiles_returns_empty(self, detector: SliceVarianceDetector):
        """No slice profiles → empty result."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={},
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_score_at_threshold_is_half(self, detector: SliceVarianceDetector):
        """At exactly the threshold, normalized score = 0.5."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_a",
                    },
                    {
                        "null_ratio": 0.10,  # spread = 0.10 = threshold
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_b",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # null_spread = 0.10, norm = 0.10 / (2 * 0.10) = 0.5
        assert results[0].score == pytest.approx(0.5, abs=0.01)

    def test_score_at_double_threshold_is_one(self, detector: SliceVarianceDetector):
        """At 2x threshold, normalized score = 1.0."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_a",
                    },
                    {
                        "null_ratio": 0.20,  # spread = 0.20 = 2x threshold
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_b",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)

    def test_benford_spread(self, detector: SliceVarianceDetector):
        """Benford p-value variance contributes to score."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": 0.8,
                        "row_count": 1000,
                        "slice_table_name": "slice_a",
                    },
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": 0.001,
                        "row_count": 1000,
                        "slice_table_name": "slice_b",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # benford_spread = 0.799, threshold = 0.30
        # norm = min(0.799 / (2 * 0.30), 1.0) = 1.0
        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert "benford_spread" in results[0].evidence[0]["exceeded_thresholds"]

    def test_distinct_ratio_variance(self, detector: SliceVarianceDetector):
        """Distinct count ratio variance contributes to score."""
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 5,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_a",
                    },
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 15,  # ratio = 3.0
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_b",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # distinct_ratio = 3.0, threshold = 5.0
        # spread = 3.0 - 1.0 = 2.0, threshold_spread = 4.0
        # norm = min(2.0 / (2 * 4.0), 1.0) = 0.25
        assert results[0].score == pytest.approx(0.25, abs=0.01)
        assert "distinct_ratio" not in results[0].evidence[0]["exceeded_thresholds"]

    def test_resolution_options_for_nonzero_score(self, detector: SliceVarianceDetector):
        """Non-zero score produces accept_finding resolution option."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_a",
                    },
                    {
                        "null_ratio": 0.15,
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_b",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        actions = [opt.action for opt in results[0].resolution_options]
        assert "accept_finding" in actions

    def test_detector_properties(self, detector: SliceVarianceDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "slice_variance"
        assert detector.layer == "value"
        assert detector.dimension == "variance"
        assert detector.sub_dimension == "slice_stability"
        assert detector.scope == "column"
        assert detector.required_analyses == []

    def test_multiple_spreads_max_wins(self, detector: SliceVarianceDetector):
        """Score is the max of all normalized spreads."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 100,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_a",
                    },
                    {
                        "null_ratio": 0.05,  # null_norm = 0.05/0.20 = 0.25
                        "distinct_count": 100,
                        "outlier_ratio": 0.06,  # outlier_norm = 0.06/0.10 = 0.6
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_b",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # null_norm = 0.05/(2*0.10) = 0.25
        # outlier_norm = 0.06/(2*0.25) = 0.12
        # max(0.25, 0.12) = 0.25
        assert results[0].score == pytest.approx(0.25, abs=0.01)

    def test_zero_distinct_count_reports_none(self, detector: SliceVarianceDetector):
        """When min distinct_count is 0, ratio is None (undefined), not misleading 0.0."""
        context = DetectorContext(
            table_name="orders",
            column_name="optional_field",
            analysis_results={
                "slice_profiles": [
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 0,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_a",
                    },
                    {
                        "null_ratio": 0.0,
                        "distinct_count": 50,
                        "outlier_ratio": 0.0,
                        "benford_p_value": None,
                        "row_count": 1000,
                        "slice_table_name": "slice_b",
                    },
                ]
            },
        )
        results = detector.detect(context)
        assert len(results) == 1
        # distinct_ratio is undefined → None in evidence, does not contribute to score
        assert results[0].evidence[0]["distinct_ratio"] is None
        assert results[0].score == pytest.approx(0.0, abs=0.001)
