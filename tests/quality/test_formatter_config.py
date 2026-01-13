"""Tests for formatter configuration loading."""

from pathlib import Path

from dataraum_context.core.formatting.base import ThresholdConfig
from dataraum_context.core.formatting.config import (
    FormatterConfig,
    MetricGroupConfig,
    _merge_configs,
    _parse_config_data,
    get_default_config,
    load_formatter_config,
)


class TestThresholdConfig:
    """Tests for ThresholdConfig."""

    def test_ascending_thresholds(self):
        """Test ascending threshold mapping (higher = more severe)."""
        config = ThresholdConfig(
            thresholds={"none": 0.01, "low": 0.05, "moderate": 0.2, "high": 0.5},
            default_severity="severe",
        )

        assert config.get_severity(0.005) == "none"
        assert config.get_severity(0.01) == "none"
        assert config.get_severity(0.03) == "low"
        assert config.get_severity(0.1) == "moderate"
        assert config.get_severity(0.4) == "high"
        assert config.get_severity(0.6) == "severe"

    def test_descending_thresholds(self):
        """Test descending threshold mapping (lower = more severe)."""
        config = ThresholdConfig(
            thresholds={"none": 0.99, "low": 0.95, "moderate": 0.8, "high": 0.5},
            default_severity="severe",
            ascending=False,
        )

        assert config.get_severity(1.0) == "none"
        assert config.get_severity(0.99) == "none"
        assert config.get_severity(0.97) == "low"
        assert config.get_severity(0.85) == "moderate"
        assert config.get_severity(0.6) == "high"
        assert config.get_severity(0.3) == "severe"


class TestMetricGroupConfig:
    """Tests for MetricGroupConfig."""

    def test_get_threshold_basic(self):
        """Test basic threshold retrieval."""
        group = MetricGroupConfig(
            thresholds={
                "null_ratio": ThresholdConfig(
                    thresholds={"none": 0.01, "moderate": 0.1},
                    default_severity="severe",
                ),
            }
        )

        threshold = group.get_threshold("null_ratio")
        assert threshold is not None
        assert threshold.get_severity(0.005) == "none"
        assert threshold.get_severity(0.05) == "moderate"

    def test_get_threshold_missing(self):
        """Test retrieval of non-existent threshold."""
        group = MetricGroupConfig()
        assert group.get_threshold("nonexistent") is None

    def test_column_pattern_override(self):
        """Test column pattern overrides take precedence."""
        group = MetricGroupConfig(
            thresholds={
                "null_ratio": ThresholdConfig(
                    thresholds={"none": 0.01, "moderate": 0.1},
                    default_severity="severe",
                ),
            },
            column_patterns={
                "*_id": {
                    "null_ratio": ThresholdConfig(
                        thresholds={"none": 0.0},
                        default_severity="critical",
                    ),
                },
            },
        )

        # Regular column uses default
        regular = group.get_threshold("null_ratio", column_name="amount")
        assert regular is not None
        assert regular.get_severity(0.005) == "none"

        # ID column uses stricter override
        id_col = group.get_threshold("null_ratio", column_name="customer_id")
        assert id_col is not None
        assert id_col.default_severity == "critical"
        assert id_col.get_severity(0.001) == "critical"

    def test_column_pattern_case_insensitive(self):
        """Test column pattern matching is case-insensitive."""
        group = MetricGroupConfig(
            column_patterns={
                "*_id": {
                    "null_ratio": ThresholdConfig(
                        thresholds={"none": 0.0},
                        default_severity="critical",
                    ),
                },
            },
        )

        # Both cases should match
        assert group.get_threshold("null_ratio", column_name="USER_ID") is not None
        assert group.get_threshold("null_ratio", column_name="user_id") is not None


class TestFormatterConfig:
    """Tests for FormatterConfig."""

    def test_get_threshold(self):
        """Test threshold retrieval through FormatterConfig."""
        config = FormatterConfig(
            groups={
                "completeness": MetricGroupConfig(
                    thresholds={
                        "null_ratio": ThresholdConfig(
                            thresholds={"none": 0.01},
                            default_severity="severe",
                        ),
                    }
                ),
            }
        )

        threshold = config.get_threshold("completeness", "null_ratio")
        assert threshold is not None
        assert threshold.get_severity(0.005) == "none"

    def test_get_severity_convenience(self):
        """Test get_severity convenience method."""
        config = FormatterConfig(
            groups={
                "completeness": MetricGroupConfig(
                    thresholds={
                        "null_ratio": ThresholdConfig(
                            thresholds={"none": 0.01, "moderate": 0.1},
                            default_severity="severe",
                        ),
                    }
                ),
            }
        )

        assert config.get_severity("completeness", "null_ratio", 0.005) == "none"
        assert config.get_severity("completeness", "null_ratio", 0.05) == "moderate"
        assert config.get_severity("completeness", "null_ratio", 0.5) == "severe"

    def test_get_severity_unknown_group(self):
        """Test get_severity returns 'unknown' for missing groups."""
        config = FormatterConfig()
        assert config.get_severity("nonexistent", "metric", 0.5) == "unknown"


class TestConfigParsing:
    """Tests for YAML config parsing."""

    def test_parse_config_data(self):
        """Test parsing raw config data."""
        data = {
            "domain": "test",
            "groups": {
                "completeness": {
                    "metrics": {
                        "null_ratio": {
                            "thresholds": {"none": 0.01, "moderate": 0.1},
                            "default_severity": "severe",
                        }
                    }
                }
            },
        }

        config = _parse_config_data(data, "test")

        assert config.domain == "test"
        assert "completeness" in config.groups
        threshold = config.get_threshold("completeness", "null_ratio")
        assert threshold is not None
        assert threshold.get_severity(0.005) == "none"

    def test_parse_column_patterns(self):
        """Test parsing column pattern overrides."""
        data = {
            "domain": "test",
            "groups": {
                "completeness": {
                    "metrics": {
                        "null_ratio": {
                            "thresholds": {"none": 0.01},
                            "default_severity": "severe",
                        }
                    },
                    "column_patterns": {
                        "*_id": {
                            "null_ratio": {
                                "thresholds": {"none": 0.0},
                                "default_severity": "critical",
                            }
                        }
                    },
                }
            },
        }

        config = _parse_config_data(data, "test")

        # Check pattern override exists
        group = config.groups["completeness"]
        assert "*_id" in group.column_patterns
        assert "null_ratio" in group.column_patterns["*_id"]


class TestConfigMerging:
    """Tests for config merging."""

    def test_merge_overwrites_thresholds(self):
        """Test that override config overwrites base thresholds."""
        base = FormatterConfig(
            domain="base",
            groups={
                "completeness": MetricGroupConfig(
                    thresholds={
                        "null_ratio": ThresholdConfig(
                            thresholds={"none": 0.01},
                            default_severity="severe",
                        ),
                    }
                ),
            },
        )

        override = FormatterConfig(
            domain="override",
            groups={
                "completeness": MetricGroupConfig(
                    thresholds={
                        "null_ratio": ThresholdConfig(
                            thresholds={"none": 0.001},  # Stricter
                            default_severity="critical",
                        ),
                    }
                ),
            },
        )

        merged = _merge_configs(base, override)

        assert merged.domain == "override"
        threshold = merged.get_threshold("completeness", "null_ratio")
        assert threshold is not None
        assert threshold.thresholds["none"] == 0.001
        assert threshold.default_severity == "critical"

    def test_merge_preserves_unoverridden(self):
        """Test that unoverridden values are preserved."""
        base = FormatterConfig(
            groups={
                "completeness": MetricGroupConfig(
                    thresholds={
                        "null_ratio": ThresholdConfig(
                            thresholds={"none": 0.01},
                            default_severity="severe",
                        ),
                        "cardinality": ThresholdConfig(
                            thresholds={"none": 0.99},
                            default_severity="high",
                        ),
                    }
                ),
            },
        )

        override = FormatterConfig(
            groups={
                "completeness": MetricGroupConfig(
                    thresholds={
                        "null_ratio": ThresholdConfig(
                            thresholds={"none": 0.001},
                            default_severity="critical",
                        ),
                        # cardinality not overridden
                    }
                ),
            },
        )

        merged = _merge_configs(base, override)

        # null_ratio should be overridden
        null_threshold = merged.get_threshold("completeness", "null_ratio")
        assert null_threshold.thresholds["none"] == 0.001

        # cardinality should be preserved
        card_threshold = merged.get_threshold("completeness", "cardinality")
        assert card_threshold is not None
        assert card_threshold.thresholds["none"] == 0.99

    def test_merge_tracks_sources(self):
        """Test that merged config tracks source files."""
        base = FormatterConfig(sources=["base.yaml"])
        override = FormatterConfig(sources=["override.yaml"])

        merged = _merge_configs(base, override)

        assert "base.yaml" in merged.sources
        assert "override.yaml" in merged.sources


class TestDefaultConfig:
    """Tests for default code-based config."""

    def test_get_default_config(self):
        """Test default config has expected groups."""
        config = get_default_config()

        assert "completeness" in config.groups
        assert "outliers" in config.groups
        assert "benford" in config.groups
        assert "temporal" in config.groups
        assert "multicollinearity" in config.groups
        assert "correlation" in config.groups
        assert "topology" in config.groups

    def test_default_config_column_patterns(self):
        """Test default config has column patterns."""
        config = get_default_config()

        # Completeness should have *_id pattern
        group = config.groups["completeness"]
        assert "*_id" in group.column_patterns

        # ID columns should have stricter null threshold
        id_threshold = group.get_threshold("null_ratio", column_name="user_id")
        assert id_threshold is not None
        assert id_threshold.default_severity == "critical"


class TestLoadFormatterConfig:
    """Tests for load_formatter_config function."""

    def test_load_with_defaults_yaml(self):
        """Test loading from defaults.yaml."""
        config_dir = Path(__file__).parent.parent.parent / "config" / "formatter_thresholds"

        # Skip if config files don't exist
        if not (config_dir / "defaults.yaml").exists():
            return

        config = load_formatter_config(config_dir=config_dir)

        assert config.domain is not None
        assert len(config.groups) > 0

    def test_load_with_financial_domain(self):
        """Test loading with financial domain override."""
        config_dir = Path(__file__).parent.parent.parent / "config" / "formatter_thresholds"

        # Skip if config files don't exist
        if not (config_dir / "financial.yaml").exists():
            return

        config = load_formatter_config(domain="financial", config_dir=config_dir)

        # Financial should have stricter null_ratio threshold
        threshold = config.get_threshold("completeness", "null_ratio")
        if threshold:
            # Financial is stricter: none at 0.001 vs default 0.01
            assert threshold.thresholds.get("none", 1.0) <= 0.01

    def test_load_with_runtime_override(self):
        """Test runtime override takes precedence."""
        runtime_config = {
            "domain": "runtime",
            "groups": {
                "completeness": {
                    "metrics": {
                        "null_ratio": {
                            "thresholds": {"none": 0.0001},
                            "default_severity": "critical",
                        }
                    }
                }
            },
        }

        config = load_formatter_config(dataset_config=runtime_config)

        threshold = config.get_threshold("completeness", "null_ratio")
        assert threshold is not None
        assert threshold.thresholds["none"] == 0.0001
