"""Unit tests for period-level temporal slice analysis functions."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from dataraum.analysis.temporal_slicing.analyzer import (
    _analyze_completeness,
    _compute_period_metrics,
    _detect_volume_anomalies,
    analyze_period_metrics,
    persist_period_results,
)
from dataraum.analysis.temporal_slicing.db_models import TemporalSliceAnalysis
from dataraum.analysis.temporal_slicing.models import (
    CompletenessResult,
    PeriodAnalysisResult,
    PeriodMetrics,
    TemporalSliceConfig,
    VolumeAnomalyResult,
)

# ---------------------------------------------------------------------------
# _compute_period_metrics tests
# ---------------------------------------------------------------------------


class TestComputePeriodMetrics:
    """Tests for _compute_period_metrics."""

    def _make_conn(
        self, period_data: list[tuple[int, int]], last_day_counts: list[int] | None = None
    ):
        """Create a mock DuckDB connection.

        Args:
            period_data: List of (row_count, observed_days) per period.
            last_day_counts: Optional list of last-day row counts per period.
        """
        conn = MagicMock()
        call_index = {"i": 0}

        if last_day_counts is None:
            # For simple tests, assume last day has average daily volume
            last_day_counts = []
            for row_count, observed_days in period_data:
                if observed_days > 0:
                    last_day_counts.append(row_count // observed_days)
                else:
                    last_day_counts.append(0)

        def execute_side_effect(sql, params=None):
            idx = call_index["i"]
            mock_result = MagicMock()

            if "COUNT(DISTINCT" in sql:
                # Period metrics query
                period_idx = idx // 2  # Two queries per period (count + last day)
                if period_idx < len(period_data):
                    mock_result.fetchone.return_value = period_data[period_idx]
                else:
                    mock_result.fetchone.return_value = (0, 0)
            elif "MAX(CAST" in sql:
                # Last day query
                period_idx = (idx - 1) // 2
                if period_idx < len(last_day_counts):
                    mock_result.fetchone.return_value = (last_day_counts[period_idx],)
                else:
                    mock_result.fetchone.return_value = (0,)
            else:
                mock_result.fetchone.return_value = (0, 0)

            call_index["i"] += 1
            return mock_result

        conn.execute = execute_side_effect
        return conn

    def test_basic_metrics(self):
        """Compute metrics for a single full-coverage period."""
        conn = self._make_conn([(100, 31)], [3])
        periods = [(date(2024, 1, 1), date(2024, 2, 1), "2024-01")]

        result = _compute_period_metrics("slice_t", "ts", conn, periods)

        assert len(result) == 1
        m = result[0]
        assert m.period_label == "2024-01"
        assert m.row_count == 100
        assert m.expected_days == 31
        assert m.observed_days == 31
        assert m.coverage_ratio == 1.0

    def test_partial_coverage(self):
        """Coverage ratio is correct for partial period."""
        conn = self._make_conn([(50, 15)], [3])
        periods = [(date(2024, 1, 1), date(2024, 2, 1), "2024-01")]

        result = _compute_period_metrics("slice_t", "ts", conn, periods)

        assert len(result) == 1
        assert result[0].coverage_ratio == pytest.approx(15 / 31, abs=0.01)

    def test_empty_period(self):
        """Zero rows and zero observed days for empty period."""
        conn = self._make_conn([(0, 0)])
        periods = [(date(2024, 1, 1), date(2024, 2, 1), "2024-01")]

        result = _compute_period_metrics("slice_t", "ts", conn, periods)

        assert len(result) == 1
        assert result[0].row_count == 0
        assert result[0].coverage_ratio == 0.0
        assert result[0].last_day_ratio == 0.0

    def test_rolling_statistics_computed(self):
        """Rolling avg/std/z-score computed using trailing window."""
        # 4 periods: 3 baseline + 1 that gets a z-score
        # Trailing window=3, so period[3] uses [100,100,100] as baseline
        conn = self._make_conn([(100, 30), (100, 28), (100, 31), (500, 30)])
        periods = [
            (date(2024, 1, 1), date(2024, 2, 1), "2024-01"),
            (date(2024, 2, 1), date(2024, 3, 1), "2024-02"),
            (date(2024, 3, 1), date(2024, 4, 1), "2024-03"),
            (date(2024, 4, 1), date(2024, 5, 1), "2024-04"),
        ]

        result = _compute_period_metrics("slice_t", "ts", conn, periods)

        assert len(result) == 4
        # First 3 periods: not enough baseline history, z_score=0
        assert result[0].z_score == 0.0
        assert result[1].z_score == 0.0
        assert result[2].z_score == 0.0
        # Period 4: trailing window=[100,100,100], current=500
        # avg=100, std=0 → z_score=0 (all same, no variance in baseline)
        # Actually std=0 so z=0. Use varied baseline instead.

    def test_trailing_window_z_score(self):
        """Z-score uses trailing window (excludes current value)."""
        # Baseline periods: 100, 110, 90 → avg=100, std≈8.16
        # Current period: 200 → z = (200-100)/8.16 ≈ 12.2
        conn = self._make_conn([(100, 30), (110, 28), (90, 31), (200, 30)])
        periods = [
            (date(2024, 1, 1), date(2024, 2, 1), "2024-01"),
            (date(2024, 2, 1), date(2024, 3, 1), "2024-02"),
            (date(2024, 3, 1), date(2024, 4, 1), "2024-03"),
            (date(2024, 4, 1), date(2024, 5, 1), "2024-04"),
        ]

        result = _compute_period_metrics("slice_t", "ts", conn, periods)

        # Period 4 should have a high z-score since 200 is far from avg~100
        assert result[3].z_score is not None
        assert result[3].z_score > 2.5  # Well above anomaly threshold

    def test_period_over_period_change(self):
        """Period-over-period change computed correctly."""
        conn = self._make_conn([(100, 30), (150, 28)])
        periods = [
            (date(2024, 1, 1), date(2024, 2, 1), "2024-01"),
            (date(2024, 2, 1), date(2024, 3, 1), "2024-02"),
        ]

        result = _compute_period_metrics("slice_t", "ts", conn, periods)

        assert result[0].period_over_period_change is None  # First period has no previous
        assert result[1].period_over_period_change == pytest.approx(0.5, abs=0.01)


# ---------------------------------------------------------------------------
# _analyze_completeness tests
# ---------------------------------------------------------------------------


class TestAnalyzeCompleteness:
    """Tests for _analyze_completeness."""

    @pytest.fixture
    def config(self) -> TemporalSliceConfig:
        return TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 4, 1),
            completeness_threshold=0.9,
            last_day_ratio_threshold=0.3,
        )

    def test_full_coverage_is_complete(self, config: TemporalSliceConfig):
        """Period with 100% coverage is complete."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=100,
                expected_days=31,
                observed_days=31,
                coverage_ratio=1.0,
                last_day_ratio=1.0,
            )
        ]

        results = _analyze_completeness(metrics, config)

        assert len(results) == 1
        assert results[0].is_complete is True
        assert results[0].has_early_cutoff is False
        assert results[0].days_missing_at_end == 0

    def test_below_threshold_is_incomplete(self, config: TemporalSliceConfig):
        """Period below completeness_threshold is incomplete."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=50,
                expected_days=31,
                observed_days=20,
                coverage_ratio=20 / 31,  # ~0.645
                last_day_ratio=1.0,
            )
        ]

        results = _analyze_completeness(metrics, config)

        assert results[0].is_complete is False
        assert results[0].days_missing_at_end == 11

    def test_early_cutoff_detected(self, config: TemporalSliceConfig):
        """Early cutoff: missing days + low last_day_ratio."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=50,
                expected_days=31,
                observed_days=20,
                coverage_ratio=20 / 31,
                last_day_ratio=0.1,  # Very low last day volume
            )
        ]

        results = _analyze_completeness(metrics, config)

        assert results[0].has_early_cutoff is True

    def test_no_early_cutoff_if_last_day_normal(self, config: TemporalSliceConfig):
        """No early cutoff when last day has normal volume despite missing days."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=50,
                expected_days=31,
                observed_days=25,
                coverage_ratio=25 / 31,
                last_day_ratio=0.8,  # Normal last day volume
            )
        ]

        results = _analyze_completeness(metrics, config)

        assert results[0].has_early_cutoff is False

    def test_at_threshold_is_complete(self, config: TemporalSliceConfig):
        """Period at exactly the threshold is complete."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=90,
                expected_days=31,
                observed_days=28,
                coverage_ratio=0.9,  # Exactly at threshold
                last_day_ratio=1.0,
            )
        ]

        results = _analyze_completeness(metrics, config)

        assert results[0].is_complete is True


# ---------------------------------------------------------------------------
# _detect_volume_anomalies tests
# ---------------------------------------------------------------------------


class TestDetectVolumeAnomalies:
    """Tests for _detect_volume_anomalies."""

    @pytest.fixture
    def config(self) -> TemporalSliceConfig:
        return TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 4, 1),
            volume_zscore_threshold=2.5,
        )

    def test_normal_volume_no_anomaly(self, config: TemporalSliceConfig):
        """Normal z-score produces no anomaly."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=100,
                expected_days=31,
                observed_days=31,
                coverage_ratio=1.0,
                last_day_ratio=1.0,
                z_score=0.5,
            )
        ]

        results = _detect_volume_anomalies(metrics, config)

        assert len(results) == 1
        assert results[0].is_anomaly is False
        assert results[0].anomaly_type is None

    def test_spike_detected(self, config: TemporalSliceConfig):
        """High positive z-score is a spike."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=500,
                expected_days=31,
                observed_days=31,
                coverage_ratio=1.0,
                last_day_ratio=1.0,
                z_score=3.5,
            )
        ]

        results = _detect_volume_anomalies(metrics, config)

        assert results[0].is_anomaly is True
        assert results[0].anomaly_type == "spike"

    def test_drop_detected(self, config: TemporalSliceConfig):
        """Large negative z-score is a drop."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=10,
                expected_days=31,
                observed_days=31,
                coverage_ratio=1.0,
                last_day_ratio=1.0,
                z_score=-3.0,
            )
        ]

        results = _detect_volume_anomalies(metrics, config)

        assert results[0].is_anomaly is True
        assert results[0].anomaly_type == "drop"

    def test_gap_detected(self, config: TemporalSliceConfig):
        """Zero rows is always a gap, regardless of z-score."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=0,
                expected_days=31,
                observed_days=0,
                coverage_ratio=0.0,
                last_day_ratio=0.0,
                z_score=0.0,  # Even zero z-score with 0 rows is a gap
            )
        ]

        results = _detect_volume_anomalies(metrics, config)

        assert results[0].is_anomaly is True
        assert results[0].anomaly_type == "gap"

    def test_at_threshold_not_anomaly(self, config: TemporalSliceConfig):
        """Z-score exactly at threshold is not anomaly (must exceed)."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=100,
                expected_days=31,
                observed_days=31,
                coverage_ratio=1.0,
                last_day_ratio=1.0,
                z_score=2.5,  # Exactly at threshold
            )
        ]

        results = _detect_volume_anomalies(metrics, config)

        assert results[0].is_anomaly is False

    def test_period_over_period_change_preserved(self, config: TemporalSliceConfig):
        """Period-over-period change is passed through."""
        metrics = [
            PeriodMetrics(
                period_label="2024-01",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 2, 1),
                row_count=100,
                expected_days=31,
                observed_days=31,
                coverage_ratio=1.0,
                last_day_ratio=1.0,
                z_score=1.0,
                period_over_period_change=0.25,
            )
        ]

        results = _detect_volume_anomalies(metrics, config)

        assert results[0].period_over_period_change == pytest.approx(0.25)


# ---------------------------------------------------------------------------
# analyze_period_metrics integration test (with mock DuckDB)
# ---------------------------------------------------------------------------


class TestAnalyzePeriodMetrics:
    """Tests for the top-level analyze_period_metrics orchestrator."""

    def test_empty_range_returns_empty(self):
        """Empty date range returns empty result."""
        conn = MagicMock()
        config = TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 1, 1),  # Same date
        )

        result = analyze_period_metrics("slice_t", "ts", conn, config)

        assert result.success
        assert result.value is not None
        assert result.value.total_periods == 0

    def test_returns_combined_results(self):
        """Orchestrator returns metrics, completeness, and anomaly results."""
        conn = MagicMock()
        # Mock two periods: one full, one partial
        call_index = {"i": 0}

        def execute_side_effect(sql, params=None):
            idx = call_index["i"]
            mock_result = MagicMock()
            if "COUNT(DISTINCT" in sql:
                # Period 1: full, Period 2: partial
                if idx == 0:
                    mock_result.fetchone.return_value = (100, 31)
                elif idx == 2:
                    mock_result.fetchone.return_value = (10, 5)
                else:
                    mock_result.fetchone.return_value = (0, 0)
            elif "MAX(CAST" in sql:
                if idx == 1:
                    mock_result.fetchone.return_value = (3,)
                elif idx == 3:
                    mock_result.fetchone.return_value = (1,)
                else:
                    mock_result.fetchone.return_value = (0,)
            else:
                mock_result.fetchone.return_value = (0, 0)
            call_index["i"] += 1
            return mock_result

        conn.execute = execute_side_effect

        config = TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 3, 1),
        )

        result = analyze_period_metrics("slice_t", "ts", conn, config)

        assert result.success
        val = result.value
        assert val is not None
        assert val.total_periods == 2
        assert len(val.period_metrics) == 2
        assert len(val.completeness_results) == 2
        assert len(val.volume_anomalies) == 2


# ---------------------------------------------------------------------------
# persist_period_results test
# ---------------------------------------------------------------------------


class TestPersistPeriodResults:
    """Tests for persist_period_results."""

    @pytest.fixture(autouse=True)
    def _ensure_models(self):
        """Import all models to ensure SQLAlchemy mapper initialization succeeds."""
        # Must import ALL model modules to satisfy cross-model relationships
        import dataraum.analysis.correlation.db_models  # noqa: F401
        import dataraum.analysis.cycles.db_models  # noqa: F401
        import dataraum.analysis.relationships.db_models  # noqa: F401
        import dataraum.analysis.semantic.db_models  # noqa: F401
        import dataraum.analysis.slicing.db_models  # noqa: F401
        import dataraum.analysis.statistics.db_models  # noqa: F401
        import dataraum.analysis.temporal.db_models  # noqa: F401
        import dataraum.analysis.temporal_slicing.db_models  # noqa: F401
        import dataraum.analysis.typing.db_models  # noqa: F401
        import dataraum.analysis.validation.db_models  # noqa: F401
        import dataraum.pipeline.db_models  # noqa: F401
        import dataraum.query.db_models  # noqa: F401
        import dataraum.storage.models  # noqa: F401

    def test_persists_correct_count(self):
        """Creates one DB record per period."""
        session = MagicMock()
        result = PeriodAnalysisResult(
            slice_table_name="slice_status_active",
            time_column="ts",
            total_periods=2,
            incomplete_periods=1,
            anomaly_count=0,
            period_metrics=[
                PeriodMetrics(
                    period_label="2024-01",
                    period_start=date(2024, 1, 1),
                    period_end=date(2024, 2, 1),
                    row_count=100,
                    expected_days=31,
                    observed_days=31,
                    coverage_ratio=1.0,
                    last_day_ratio=1.0,
                    z_score=0.0,
                ),
                PeriodMetrics(
                    period_label="2024-02",
                    period_start=date(2024, 2, 1),
                    period_end=date(2024, 3, 1),
                    row_count=50,
                    expected_days=29,
                    observed_days=15,
                    coverage_ratio=15 / 29,
                    last_day_ratio=0.1,
                    z_score=-2.0,
                ),
            ],
            completeness_results=[
                CompletenessResult(
                    period_label="2024-01",
                    is_complete=True,
                    coverage_ratio=1.0,
                    has_early_cutoff=False,
                    days_missing_at_end=0,
                ),
                CompletenessResult(
                    period_label="2024-02",
                    is_complete=False,
                    coverage_ratio=15 / 29,
                    has_early_cutoff=True,
                    days_missing_at_end=14,
                ),
            ],
            volume_anomalies=[
                VolumeAnomalyResult(
                    period_label="2024-01",
                    is_anomaly=False,
                    anomaly_type=None,
                    z_score=0.0,
                ),
                VolumeAnomalyResult(
                    period_label="2024-02",
                    is_anomaly=False,
                    anomaly_type=None,
                    z_score=-2.0,
                ),
            ],
        )

        persist_result = persist_period_results(result, session)

        assert persist_result.success
        assert persist_result.value == 2
        assert session.add.call_count == 2

        # Verify the records passed to session.add are TemporalSliceAnalysis instances
        for call in session.add.call_args_list:
            record = call[0][0]
            assert isinstance(record, TemporalSliceAnalysis)

    def test_issues_json_populated_for_incomplete(self):
        """Issues JSON is populated for incomplete period with early cutoff."""
        session = MagicMock()
        result = PeriodAnalysisResult(
            slice_table_name="slice_t",
            time_column="ts",
            total_periods=1,
            incomplete_periods=1,
            anomaly_count=1,
            period_metrics=[
                PeriodMetrics(
                    period_label="2024-01",
                    period_start=date(2024, 1, 1),
                    period_end=date(2024, 2, 1),
                    row_count=10,
                    expected_days=31,
                    observed_days=10,
                    coverage_ratio=10 / 31,
                    last_day_ratio=0.1,
                    z_score=-3.0,
                ),
            ],
            completeness_results=[
                CompletenessResult(
                    period_label="2024-01",
                    is_complete=False,
                    coverage_ratio=10 / 31,
                    has_early_cutoff=True,
                    days_missing_at_end=21,
                ),
            ],
            volume_anomalies=[
                VolumeAnomalyResult(
                    period_label="2024-01",
                    is_anomaly=True,
                    anomaly_type="drop",
                    z_score=-3.0,
                ),
            ],
        )

        persist_result = persist_period_results(result, session)

        assert persist_result.success
        record = session.add.call_args_list[0][0][0]
        assert record.issues_json is not None
        issue_types = [i["type"] for i in record.issues_json]
        assert "incomplete" in issue_types
        assert "early_cutoff" in issue_types
        assert "volume_drop" in issue_types
