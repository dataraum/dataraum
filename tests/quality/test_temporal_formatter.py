"""Tests for temporal quality formatter."""

from datetime import datetime

from dataraum_context.core.formatting.base import ThresholdConfig
from dataraum_context.core.formatting.config import (
    FormatterConfig,
    MetricGroupConfig,
)
from dataraum_context.quality.formatting.temporal import (
    format_freshness_group,
    format_patterns_group,
    format_stability_group,
    format_temporal_completeness_group,
    format_temporal_quality,
)


class TestFreshnessGroup:
    """Tests for freshness metric group formatting."""

    def test_format_fresh_data(self):
        """Test formatting with fresh data."""
        result = format_freshness_group(
            staleness_days=0.5,
            last_update=datetime(2024, 1, 15, 10, 30),
        )

        assert result.group_name == "freshness"
        assert result.overall_severity == "none"
        assert "staleness_days" in result.metrics
        assert "within the last day" in result.metrics["staleness_days"].interpretation

    def test_format_stale_data(self):
        """Test formatting with stale data."""
        result = format_freshness_group(
            staleness_days=45,
        )

        assert result.overall_severity in ["moderate", "high", "severe"]
        assert "45 days stale" in result.metrics["staleness_days"].interpretation

    def test_format_with_update_frequency(self):
        """Test formatting includes update frequency info."""
        result = format_freshness_group(
            staleness_days=1,
            update_frequency_score=0.95,
            median_interval_seconds=3600,  # 1 hour
        )

        assert "update_frequency_score" in result.metrics
        assert "highly regular" in result.metrics["update_frequency_score"].interpretation
        assert result.metrics["update_frequency_score"].details["median_interval"] == "1.0 hours"

    def test_format_irregular_updates(self):
        """Test formatting with irregular updates."""
        result = format_freshness_group(
            staleness_days=2,
            update_frequency_score=0.3,
            median_interval_seconds=86400 * 3,  # 3 days
        )

        assert "irregular" in result.metrics["update_frequency_score"].interpretation
        assert result.metrics["update_frequency_score"].details["median_interval"] == "3.0 days"

    def test_data_freshness_alternative(self):
        """Test data_freshness_days used when staleness_days absent."""
        result = format_freshness_group(
            data_freshness_days=5.5,
            is_stale=False,
        )

        assert "data_freshness_days" in result.metrics
        assert "5.5 days old" in result.metrics["data_freshness_days"].interpretation


class TestTemporalCompletenessGroup:
    """Tests for temporal completeness group formatting."""

    def test_format_complete_coverage(self):
        """Test formatting with complete temporal coverage."""
        result = format_temporal_completeness_group(
            completeness_ratio=0.99,
            expected_periods=365,
            actual_periods=362,
            gap_count=0,
        )

        assert result.group_name == "temporal_completeness"
        assert result.overall_severity == "none"
        assert "99.0%" in result.metrics["completeness_ratio"].interpretation

    def test_format_incomplete_coverage(self):
        """Test formatting with incomplete temporal coverage."""
        result = format_temporal_completeness_group(
            completeness_ratio=0.75,
            expected_periods=100,
            actual_periods=75,
            gap_count=5,
        )

        assert result.overall_severity in ["moderate", "high"]
        assert "25 missing" in result.metrics["completeness_ratio"].interpretation

    def test_format_with_gaps(self):
        """Test formatting includes gap information."""
        gaps = [
            {
                "gap_start": "2024-01-15",
                "gap_end": "2024-01-20",
                "gap_length_days": 5,
                "severity": "moderate",
            },
            {
                "gap_start": "2024-03-01",
                "gap_end": "2024-03-03",
                "gap_length_days": 2,
                "severity": "minor",
            },
        ]

        result = format_temporal_completeness_group(
            completeness_ratio=0.95,
            gap_count=2,
            largest_gap_days=5,
            gaps=gaps,
        )

        assert result.samples is not None
        assert len(result.samples) == 2

    def test_format_large_gap(self):
        """Test formatting with large gap."""
        result = format_temporal_completeness_group(
            completeness_ratio=0.8,
            gap_count=1,
            largest_gap_days=45,
        )

        assert "largest_gap_days" in result.metrics
        assert "45 days" in result.metrics["largest_gap_days"].interpretation
        assert "1.5 months" in result.metrics["largest_gap_days"].interpretation

    def test_format_no_gaps(self):
        """Test formatting with no gaps."""
        result = format_temporal_completeness_group(
            completeness_ratio=1.0,
            gap_count=0,
        )

        assert "No temporal gaps" in result.metrics["gap_count"].interpretation


class TestPatternsGroup:
    """Tests for temporal patterns group formatting."""

    def test_format_strong_seasonality(self):
        """Test formatting with strong seasonality."""
        result = format_patterns_group(
            has_seasonality=True,
            seasonality_strength=0.85,
            seasonality_period="monthly",
            seasonality_peaks={"month": 12},
        )

        assert result.group_name == "patterns"
        assert "seasonality" in result.metrics
        assert "Strong" in result.metrics["seasonality"].interpretation
        assert "monthly" in result.metrics["seasonality"].interpretation

    def test_format_no_seasonality(self):
        """Test formatting with no seasonality."""
        result = format_patterns_group(
            has_seasonality=False,
        )

        assert "No significant seasonality" in result.metrics["seasonality"].interpretation

    def test_format_trend_increasing(self):
        """Test formatting with increasing trend."""
        result = format_patterns_group(
            has_trend=True,
            trend_strength=0.7,
            trend_direction="increasing",
            trend_slope=0.0025,
        )

        assert "trend" in result.metrics
        assert "Moderate" in result.metrics["trend"].interpretation
        assert "increasing" in result.metrics["trend"].interpretation
        assert "+0.0025" in result.metrics["trend"].interpretation

    def test_format_trend_decreasing(self):
        """Test formatting with decreasing trend."""
        result = format_patterns_group(
            has_trend=True,
            trend_strength=0.9,
            trend_direction="decreasing",
            trend_slope=-0.015,
        )

        assert "Strong" in result.metrics["trend"].interpretation
        assert "decreasing" in result.metrics["trend"].interpretation

    def test_format_change_points(self):
        """Test formatting with change points."""
        change_points = [
            {"detected_at": "2024-03-15", "change_type": "level_shift", "magnitude": 15.5},
            {"detected_at": "2024-06-01", "change_type": "trend_break", "magnitude": 8.2},
        ]

        result = format_patterns_group(
            has_seasonality=False,
            change_point_count=2,
            change_points=change_points,
        )

        assert "change_points" in result.metrics
        assert "2 change point" in result.metrics["change_points"].interpretation
        assert result.metrics["change_points"].severity == "low"

    def test_format_many_change_points(self):
        """Test formatting with many change points triggers higher severity."""
        result = format_patterns_group(
            change_point_count=8,
        )

        assert result.metrics["change_points"].severity == "high"
        assert "frequent structural changes" in result.metrics["change_points"].interpretation

    def test_format_weekly_peaks(self):
        """Test formatting with weekly seasonality peaks."""
        result = format_patterns_group(
            has_seasonality=True,
            seasonality_strength=0.6,
            seasonality_period="weekly",
            seasonality_peaks={"day_of_week": 4},  # Friday
        )

        assert result.metrics["seasonality"].details["peaks"] == "peak on Fri"


class TestStabilityGroup:
    """Tests for distribution stability group formatting."""

    def test_format_stable_distribution(self):
        """Test formatting with stable distribution."""
        result = format_stability_group(
            stability_score=0.95,
            shift_count=0,
        )

        assert result.group_name == "stability"
        assert result.overall_severity == "none"
        assert "highly stable" in result.metrics["stability_score"].interpretation

    def test_format_unstable_distribution(self):
        """Test formatting with unstable distribution."""
        result = format_stability_group(
            stability_score=0.35,
            shift_count=8,
        )

        assert result.overall_severity in ["high", "severe"]
        assert "instability" in result.metrics["stability_score"].interpretation

    def test_format_with_shifts(self):
        """Test formatting includes shift details."""
        shifts = [
            {
                "period1_start": "2024-01-01",
                "period2_end": "2024-03-31",
                "shift_direction": "increasing",
                "shift_magnitude": 0.25,
            },
        ]

        result = format_stability_group(
            stability_score=0.7,
            shift_count=1,
            shifts=shifts,
        )

        assert result.metrics["shift_count"].details is not None
        assert "shifts" in result.metrics["shift_count"].details

    def test_format_moderate_shifts(self):
        """Test formatting with moderate number of shifts."""
        result = format_stability_group(
            stability_score=0.65,
            shift_count=4,
            max_ks_statistic=0.15,
        )

        assert result.metrics["shift_count"].severity == "moderate"
        assert result.metrics["stability_score"].details["max_ks_statistic"] == 0.15


class TestTemporalQualityMain:
    """Tests for main format_temporal_quality function."""

    def test_combines_all_groups(self):
        """Test main function includes all groups."""
        result = format_temporal_quality(
            staleness_days=2,
            completeness_ratio=0.95,
            has_seasonality=True,
            seasonality_strength=0.7,
            stability_score=0.85,
        )

        assert "temporal_quality" in result
        tq = result["temporal_quality"]

        assert "overall_severity" in tq
        assert "groups" in tq
        assert "freshness" in tq["groups"]
        assert "completeness" in tq["groups"]
        assert "patterns" in tq["groups"]
        assert "stability" in tq["groups"]

    def test_overall_severity_is_worst(self):
        """Test overall severity is worst of all groups."""
        result = format_temporal_quality(
            staleness_days=1,  # none
            completeness_ratio=0.99,  # none
            gap_count=0,  # none
            stability_score=0.3,  # severe
        )

        tq = result["temporal_quality"]
        assert tq["overall_severity"] in ["high", "severe"]

    def test_column_name_passed_through(self):
        """Test column name is included in output."""
        result = format_temporal_quality(
            staleness_days=1,
            column_name="created_at",
        )

        assert result["temporal_quality"]["column_name"] == "created_at"

    def test_custom_config(self):
        """Test custom configuration is respected."""
        # Create strict config for staleness
        strict_config = FormatterConfig(
            groups={
                "temporal": MetricGroupConfig(
                    thresholds={
                        "staleness_days": ThresholdConfig(
                            thresholds={"none": 0.5},  # Very strict - must be < 0.5 days
                            default_severity="critical",
                        ),
                    }
                ),
            }
        )

        result = format_temporal_quality(
            staleness_days=1,  # Would be "none" with default, but "critical" with strict
            config=strict_config,
        )

        freshness = result["temporal_quality"]["groups"]["freshness"]
        assert freshness["metrics"]["staleness_days"]["severity"] == "critical"

    def test_handles_missing_data(self):
        """Test handles missing optional fields gracefully."""
        result = format_temporal_quality(
            staleness_days=1,
            # All other fields missing
        )

        assert "temporal_quality" in result
        assert result["temporal_quality"]["overall_severity"] is not None

    def test_output_structure(self):
        """Test output has expected structure."""
        result = format_temporal_quality(
            staleness_days=5,
            completeness_ratio=0.9,
            expected_periods=100,
            actual_periods=90,
            gap_count=3,
            largest_gap_days=7,
            has_seasonality=True,
            seasonality_strength=0.8,
            seasonality_period="weekly",
            stability_score=0.75,
            shift_count=2,
        )

        tq = result["temporal_quality"]

        # Check freshness group structure
        freshness = tq["groups"]["freshness"]
        assert "severity" in freshness
        assert "interpretation" in freshness
        assert "metrics" in freshness
        assert "staleness_days" in freshness["metrics"]
        assert "value" in freshness["metrics"]["staleness_days"]
        assert "severity" in freshness["metrics"]["staleness_days"]
        assert "interpretation" in freshness["metrics"]["staleness_days"]

        # Check completeness has expected metrics
        completeness = tq["groups"]["completeness"]
        assert "completeness_ratio" in completeness["metrics"]
        assert "gap_count" in completeness["metrics"]

        # Check patterns has seasonality
        patterns = tq["groups"]["patterns"]
        assert "seasonality" in patterns["metrics"]

    def test_empty_groups_still_present(self):
        """Test that groups appear even with no data."""
        result = format_temporal_quality()

        tq = result["temporal_quality"]
        assert "freshness" in tq["groups"]
        assert "completeness" in tq["groups"]
        assert "patterns" in tq["groups"]
        assert "stability" in tq["groups"]
