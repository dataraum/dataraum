"""Tests for statistical quality formatter."""

from dataraum_context.quality.formatting.base import ThresholdConfig
from dataraum_context.quality.formatting.config import FormatterConfig, MetricGroupConfig
from dataraum_context.quality.formatting.statistical import (
    format_benford_group,
    format_completeness_group,
    format_outliers_group,
    format_statistical_quality,
)


class TestCompletenessGroup:
    """Tests for completeness metric group formatting."""

    def test_format_low_null_ratio(self):
        """Test formatting with acceptable null ratio."""
        result = format_completeness_group(
            null_ratio=0.005,
            null_count=50,
            total_count=10000,
        )

        assert result.group_name == "completeness"
        assert result.overall_severity == "none"
        assert "null_ratio" in result.metrics
        assert result.metrics["null_ratio"].severity == "none"
        assert "0.5%" in result.metrics["null_ratio"].interpretation

    def test_format_high_null_ratio(self):
        """Test formatting with high null ratio."""
        result = format_completeness_group(
            null_ratio=0.35,
            null_count=3500,
            total_count=10000,
        )

        assert result.overall_severity == "high"
        assert result.metrics["null_ratio"].severity == "high"
        assert "35.0%" in result.metrics["null_ratio"].interpretation

    def test_format_with_cardinality(self):
        """Test formatting includes cardinality info."""
        result = format_completeness_group(
            null_ratio=0.01,
            cardinality_ratio=0.995,
            distinct_count=9950,
        )

        assert "cardinality_ratio" in result.metrics
        assert "unique" in result.metrics["cardinality_ratio"].interpretation.lower()

    def test_column_pattern_override(self):
        """Test ID columns have stricter thresholds."""
        # With default config, ID columns should be critical for any nulls
        result = format_completeness_group(
            null_ratio=0.001,  # Would be "none" for regular columns
            column_name="user_id",
        )

        # ID columns have stricter thresholds
        assert result.metrics["null_ratio"].severity == "critical"

    def test_low_cardinality_interpretation(self):
        """Test low cardinality is flagged as potential categorical."""
        result = format_completeness_group(
            null_ratio=0.0,
            cardinality_ratio=0.005,
        )

        assert "categorical" in result.metrics["cardinality_ratio"].interpretation.lower()


class TestOutliersGroup:
    """Tests for outlier detection group formatting."""

    def test_format_no_outliers(self):
        """Test formatting with no outliers."""
        result = format_outliers_group(
            iqr_outlier_ratio=0.005,
            iqr_outlier_count=50,
        )

        assert result.group_name == "outliers"
        assert result.overall_severity == "none"
        assert result.metrics["iqr_outlier_ratio"].severity == "none"

    def test_format_high_outliers(self):
        """Test formatting with high outlier ratio."""
        result = format_outliers_group(
            iqr_outlier_ratio=0.15,
            iqr_outlier_count=1500,
            iqr_lower_fence=-100.0,
            iqr_upper_fence=500.0,
        )

        assert result.overall_severity == "high"
        assert "15.0%" in result.metrics["iqr_outlier_ratio"].interpretation
        assert result.metrics["iqr_outlier_ratio"].details["lower_fence"] == -100.0

    def test_format_with_isolation_forest(self):
        """Test formatting includes Isolation Forest results."""
        result = format_outliers_group(
            iqr_outlier_ratio=0.05,
            isolation_forest_ratio=0.08,
            isolation_forest_count=800,
        )

        assert "isolation_forest_ratio" in result.metrics
        assert "Isolation Forest" in result.metrics["isolation_forest_ratio"].interpretation

    def test_format_with_samples(self):
        """Test outlier samples are included."""
        samples = [
            {"value": -15234, "method": "iqr"},
            {"value": 892000, "method": "iqr"},
            {"value": 1500000, "method": "isolation_forest"},
        ]

        result = format_outliers_group(
            iqr_outlier_ratio=0.08,
            outlier_samples=samples,
        )

        assert result.samples is not None
        assert len(result.samples) == 3
        assert -15234 in result.samples

    def test_samples_limited_to_five(self):
        """Test samples are limited to 5."""
        samples = [{"value": i} for i in range(10)]

        result = format_outliers_group(
            iqr_outlier_ratio=0.1,
            outlier_samples=samples,
        )

        assert len(result.samples) == 5

    def test_worst_severity_wins(self):
        """Test overall severity is worst of both methods."""
        result = format_outliers_group(
            iqr_outlier_ratio=0.005,  # none
            isolation_forest_ratio=0.15,  # high
        )

        assert result.overall_severity == "high"


class TestBenfordGroup:
    """Tests for Benford's Law analysis group formatting."""

    def test_format_benford_compliant(self):
        """Test formatting with Benford-compliant data."""
        result = format_benford_group(
            is_compliant=True,
            p_value=0.25,
            chi_square=8.5,
        )

        assert result.group_name == "benford"
        assert result.overall_severity == "none"
        assert "Conforms" in result.interpretation

    def test_format_benford_violation(self):
        """Test formatting with Benford violation."""
        result = format_benford_group(
            is_compliant=False,
            p_value=0.001,
            chi_square=45.2,
        )

        assert result.overall_severity in ["high", "severe", "critical"]
        assert "Deviates" in result.metrics["p_value"].interpretation

    def test_format_no_benford_data(self):
        """Test formatting when Benford analysis not available."""
        result = format_benford_group()

        assert result.overall_severity == "none"
        assert "not applicable" in result.interpretation.lower()
        assert len(result.metrics) == 0

    def test_format_with_digit_distribution(self):
        """Test digit distribution is included."""
        distribution = {
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

        result = format_benford_group(
            is_compliant=True,
            p_value=0.5,
            digit_distribution=distribution,
        )

        assert "digit_distribution" in result.metrics
        assert "deviation" in result.metrics["digit_distribution"].interpretation.lower()


class TestStatisticalQualityMain:
    """Tests for main format_statistical_quality function."""

    def test_combines_all_groups(self):
        """Test main function includes all groups."""
        result = format_statistical_quality(
            null_ratio=0.02,
            iqr_outlier_ratio=0.05,
            benford_compliant=True,
            benford_p_value=0.3,
        )

        assert "statistical_quality" in result
        sq = result["statistical_quality"]

        assert "overall_severity" in sq
        assert "groups" in sq
        assert "completeness" in sq["groups"]
        assert "outliers" in sq["groups"]
        assert "benford" in sq["groups"]

    def test_overall_severity_is_worst(self):
        """Test overall severity is worst of all groups."""
        result = format_statistical_quality(
            null_ratio=0.005,  # none
            iqr_outlier_ratio=0.005,  # none
            benford_compliant=False,
            benford_p_value=0.0001,  # severe
        )

        sq = result["statistical_quality"]
        assert sq["overall_severity"] in ["high", "severe", "critical"]

    def test_column_name_passed_through(self):
        """Test column name is included in output."""
        result = format_statistical_quality(
            null_ratio=0.01,
            column_name="revenue",
        )

        assert result["statistical_quality"]["column_name"] == "revenue"

    def test_custom_config(self):
        """Test custom configuration is respected."""
        # Create strict config
        strict_config = FormatterConfig(
            groups={
                "completeness": MetricGroupConfig(
                    thresholds={
                        "null_ratio": ThresholdConfig(
                            thresholds={"none": 0.001},  # Very strict
                            default_severity="critical",
                        ),
                    }
                ),
            }
        )

        result = format_statistical_quality(
            null_ratio=0.005,  # Would be "none" with default, but "critical" with strict
            config=strict_config,
        )

        completeness = result["statistical_quality"]["groups"]["completeness"]
        assert completeness["metrics"]["null_ratio"]["severity"] == "critical"

    def test_handles_missing_data(self):
        """Test handles missing optional fields gracefully."""
        result = format_statistical_quality(
            null_ratio=0.01,
            # All other fields missing
        )

        assert "statistical_quality" in result
        assert result["statistical_quality"]["overall_severity"] is not None

    def test_output_structure(self):
        """Test output has expected structure."""
        result = format_statistical_quality(
            null_ratio=0.15,
            null_count=1500,
            total_count=10000,
            iqr_outlier_ratio=0.08,
            outlier_samples=[{"value": 999}],
        )

        sq = result["statistical_quality"]

        # Check completeness group structure
        completeness = sq["groups"]["completeness"]
        assert "severity" in completeness
        assert "interpretation" in completeness
        assert "metrics" in completeness
        assert "null_ratio" in completeness["metrics"]
        assert "value" in completeness["metrics"]["null_ratio"]
        assert "severity" in completeness["metrics"]["null_ratio"]
        assert "interpretation" in completeness["metrics"]["null_ratio"]

        # Check outliers group has samples
        outliers = sq["groups"]["outliers"]
        assert "samples" in outliers
