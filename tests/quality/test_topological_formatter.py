"""Tests for topological quality formatter."""

from dataraum_context.core.formatting.base import ThresholdConfig
from dataraum_context.core.formatting.config import (
    FormatterConfig,
    MetricGroupConfig,
)
from dataraum_context.quality.formatting.topological import (
    format_complexity_group,
    format_cycles_group,
    format_structure_group,
    format_topological_quality,
    format_topological_stability_group,
)


class TestStructureGroup:
    """Tests for structure metric group formatting."""

    def test_format_connected_structure(self):
        """Test formatting with fully connected structure."""
        result = format_structure_group(
            betti_0=1,
            betti_1=0,
            is_connected=True,
            has_cycles=False,
        )

        assert result.group_name == "structure"
        assert result.overall_severity == "none"
        assert "Fully connected" in result.metrics["betti_0"].interpretation

    def test_format_fragmented_structure(self):
        """Test formatting with fragmented structure."""
        result = format_structure_group(
            betti_0=5,
            is_connected=False,
            orphaned_components=3,
        )

        assert result.overall_severity in ["moderate", "high"]
        assert "fragmented" in result.metrics["betti_0"].interpretation.lower()

    def test_format_with_cycles(self):
        """Test formatting with cycles."""
        result = format_structure_group(
            betti_0=1,
            betti_1=3,
            has_cycles=True,
        )

        assert "betti_1" in result.metrics
        assert "3 topological cycles" in result.metrics["betti_1"].interpretation

    def test_format_no_cycles(self):
        """Test formatting without cycles."""
        result = format_structure_group(
            betti_0=1,
            betti_1=0,
            has_cycles=False,
        )

        assert "tree-like" in result.metrics["betti_1"].interpretation

    def test_format_with_voids(self):
        """Test formatting with higher-dimensional voids."""
        result = format_structure_group(
            betti_0=1,
            betti_1=0,
            betti_2=2,
        )

        assert "betti_2" in result.metrics
        assert "unusual" in result.metrics["betti_2"].interpretation.lower()

    def test_format_orphaned_components(self):
        """Test formatting with orphaned components."""
        result = format_structure_group(
            betti_0=4,
            orphaned_components=3,
        )

        assert "orphaned_components" in result.metrics
        assert "3 orphaned" in result.metrics["orphaned_components"].interpretation


class TestCyclesGroup:
    """Tests for cycles metric group formatting."""

    def test_format_no_cycles(self):
        """Test formatting with no cycles."""
        result = format_cycles_group(
            cycle_count=0,
        )

        assert result.group_name == "cycles"
        assert result.overall_severity == "none"
        assert "No persistent cycles" in result.metrics["cycle_count"].interpretation

    def test_format_with_cycles(self):
        """Test formatting with persistent cycles."""
        cycles = [
            {
                "cycle_type": "money_flow",
                "involved_columns": ["amount", "balance"],
                "persistence": 0.8,
            },
            {
                "cycle_type": "order_fulfillment",
                "involved_columns": ["order_id", "status"],
                "persistence": 0.6,
            },
        ]

        result = format_cycles_group(
            cycle_count=2,
            cycles=cycles,
        )

        assert "2 persistent cycle" in result.metrics["cycle_count"].interpretation
        assert result.metrics["cycle_count"].details is not None

    def test_format_anomalous_cycles(self):
        """Test formatting with anomalous cycles."""
        anomalous = [
            {
                "cycle_type": "unexpected",
                "anomaly_reason": "Circular reference detected",
                "involved_columns": ["a", "b"],
            },
        ]

        result = format_cycles_group(
            cycle_count=3,
            anomalous_cycle_count=1,
            anomalous_cycles=anomalous,
        )

        assert "anomalous_cycle_count" in result.metrics
        assert "requires investigation" in result.metrics["anomalous_cycle_count"].interpretation

    def test_format_many_cycles(self):
        """Test formatting with many cycles."""
        result = format_cycles_group(
            cycle_count=8,
        )

        assert "complex flow patterns" in result.metrics["cycle_count"].interpretation


class TestComplexityGroup:
    """Tests for complexity metric group formatting."""

    def test_format_low_complexity(self):
        """Test formatting with low complexity."""
        result = format_complexity_group(
            structural_complexity=2,
        )

        assert result.group_name == "complexity"
        assert result.overall_severity == "none"
        assert "Low structural complexity" in result.metrics["structural_complexity"].interpretation

    def test_format_high_complexity(self):
        """Test formatting with high complexity."""
        result = format_complexity_group(
            structural_complexity=12,
            complexity_within_bounds=False,
        )

        assert result.overall_severity in ["moderate", "high"]
        assert "Very high" in result.metrics["structural_complexity"].interpretation

    def test_format_with_entropy(self):
        """Test formatting with persistence entropy."""
        result = format_complexity_group(
            structural_complexity=5,
            persistent_entropy=1.8,
        )

        assert "persistent_entropy" in result.metrics
        assert "rich topology" in result.metrics["persistent_entropy"].interpretation

    def test_format_complexity_zscore_normal(self):
        """Test formatting with normal z-score."""
        result = format_complexity_group(
            structural_complexity=4,
            complexity_z_score=0.5,
        )

        assert "complexity_z_score" in result.metrics
        assert result.metrics["complexity_z_score"].severity == "none"
        assert "normal range" in result.metrics["complexity_z_score"].interpretation

    def test_format_complexity_zscore_high(self):
        """Test formatting with high z-score."""
        result = format_complexity_group(
            structural_complexity=10,
            complexity_z_score=3.5,
        )

        assert result.metrics["complexity_z_score"].severity == "high"
        assert "significantly higher" in result.metrics["complexity_z_score"].interpretation

    def test_format_complexity_trend(self):
        """Test complexity trend is included in details."""
        result = format_complexity_group(
            structural_complexity=5,
            complexity_trend="increasing",
        )

        assert result.metrics["structural_complexity"].details["trend"] == "increasing"


class TestTopologicalStabilityGroup:
    """Tests for topological stability group formatting."""

    def test_format_stable_topology(self):
        """Test formatting with stable topology."""
        result = format_topological_stability_group(
            bottleneck_distance=0.02,
            is_stable=True,
            stability_level="stable",
        )

        assert result.group_name == "topological_stability"
        assert result.overall_severity == "none"
        assert "Very stable" in result.metrics["bottleneck_distance"].interpretation

    def test_format_unstable_topology(self):
        """Test formatting with unstable topology."""
        result = format_topological_stability_group(
            bottleneck_distance=0.35,
            is_stable=False,
            stability_level="unstable",
        )

        assert result.overall_severity in ["moderate", "high", "severe"]
        assert "Significant" in result.metrics["bottleneck_distance"].interpretation

    def test_format_component_changes(self):
        """Test formatting with component changes."""
        result = format_topological_stability_group(
            bottleneck_distance=0.15,
            components_added=2,
            components_removed=1,
        )

        assert "component_changes" in result.metrics
        assert "+2 added" in result.metrics["component_changes"].interpretation
        assert "-1 removed" in result.metrics["component_changes"].interpretation

    def test_format_cycle_changes(self):
        """Test formatting with cycle changes."""
        result = format_topological_stability_group(
            bottleneck_distance=0.1,
            cycles_added=3,
            cycles_removed=0,
        )

        assert "cycle_changes" in result.metrics
        assert "+3 added" in result.metrics["cycle_changes"].interpretation

    def test_format_many_changes(self):
        """Test formatting with many changes triggers higher severity."""
        result = format_topological_stability_group(
            components_added=5,
            components_removed=3,
            cycles_added=4,
        )

        assert result.overall_severity in ["moderate", "high"]


class TestTopologicalQualityMain:
    """Tests for main format_topological_quality function."""

    def test_combines_all_groups(self):
        """Test main function includes all groups."""
        result = format_topological_quality(
            betti_0=1,
            betti_1=2,
            cycle_count=2,
            structural_complexity=3,
            bottleneck_distance=0.05,
        )

        assert "topological_quality" in result
        tq = result["topological_quality"]

        assert "overall_severity" in tq
        assert "groups" in tq
        assert "structure" in tq["groups"]
        assert "cycles" in tq["groups"]
        assert "complexity" in tq["groups"]
        assert "stability" in tq["groups"]

    def test_overall_severity_is_worst(self):
        """Test overall severity is worst of all groups."""
        result = format_topological_quality(
            betti_0=1,  # none
            betti_1=0,  # none
            cycle_count=0,  # none
            structural_complexity=15,  # high (very complex)
        )

        tq = result["topological_quality"]
        assert tq["overall_severity"] in ["moderate", "high", "severe"]

    def test_table_name_passed_through(self):
        """Test table name is included in output."""
        result = format_topological_quality(
            betti_0=1,
            table_name="transactions",
        )

        assert result["topological_quality"]["table_name"] == "transactions"

    def test_custom_config(self):
        """Test custom configuration is respected."""
        strict_config = FormatterConfig(
            groups={
                "topology": MetricGroupConfig(
                    thresholds={
                        "betti_0": ThresholdConfig(
                            thresholds={"none": 1},  # Only 1 component allowed
                            default_severity="critical",
                        ),
                    }
                ),
            }
        )

        result = format_topological_quality(
            betti_0=2,  # Would be "none" with default, but "critical" with strict
            config=strict_config,
        )

        structure = result["topological_quality"]["groups"]["structure"]
        assert structure["metrics"]["betti_0"]["severity"] == "critical"

    def test_handles_missing_data(self):
        """Test handles missing optional fields gracefully."""
        result = format_topological_quality(
            betti_0=1,
            # All other fields missing
        )

        assert "topological_quality" in result
        assert result["topological_quality"]["overall_severity"] is not None

    def test_includes_anomalies(self):
        """Test anomalies are included when present."""
        anomalies = [
            {
                "anomaly_type": "unexpected_cycle",
                "severity": "high",
                "description": "Circular dependency",
            },
        ]

        result = format_topological_quality(
            betti_0=1,
            has_anomalies=True,
            anomalies=anomalies,
        )

        assert "anomalies" in result["topological_quality"]
        assert len(result["topological_quality"]["anomalies"]) == 1

    def test_includes_warnings(self):
        """Test warnings are included when present."""
        warnings = ["High cycle count may indicate data issues", "Review orphaned components"]

        result = format_topological_quality(
            betti_0=1,
            quality_warnings=warnings,
        )

        assert "warnings" in result["topological_quality"]
        assert len(result["topological_quality"]["warnings"]) == 2

    def test_output_structure(self):
        """Test output has expected structure."""
        result = format_topological_quality(
            betti_0=2,
            betti_1=3,
            is_connected=False,
            has_cycles=True,
            cycle_count=3,
            anomalous_cycle_count=1,
            structural_complexity=5,
            persistent_entropy=1.2,
            bottleneck_distance=0.1,
        )

        tq = result["topological_quality"]

        # Check structure group structure
        structure = tq["groups"]["structure"]
        assert "severity" in structure
        assert "interpretation" in structure
        assert "metrics" in structure
        assert "betti_0" in structure["metrics"]
        assert "value" in structure["metrics"]["betti_0"]
        assert "severity" in structure["metrics"]["betti_0"]
        assert "interpretation" in structure["metrics"]["betti_0"]

        # Check cycles has expected metrics
        cycles = tq["groups"]["cycles"]
        assert "cycle_count" in cycles["metrics"]

    def test_empty_groups_still_present(self):
        """Test that groups appear even with no data."""
        result = format_topological_quality()

        tq = result["topological_quality"]
        assert "structure" in tq["groups"]
        assert "cycles" in tq["groups"]
        assert "complexity" in tq["groups"]
        assert "stability" in tq["groups"]
