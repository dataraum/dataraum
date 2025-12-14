"""Configuration loading for quality formatters.

Provides YAML-based threshold configuration with resolution hierarchy:
    defaults (code) → domain (YAML) → dataset (YAML) → runtime (API)

Usage:
    from dataraum_context.quality.formatting.config import load_formatter_config

    # Load with domain override
    config = load_formatter_config(domain="financial")

    # Get threshold for a metric group
    null_threshold = config.get_threshold("completeness", "null_ratio")
    severity = null_threshold.get_severity(0.15)
"""

import fnmatch
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dataraum_context.quality.formatting.base import ThresholdConfig

logger = logging.getLogger(__name__)

# Default config directory
DEFAULT_CONFIG_DIR = (
    Path(__file__).parent.parent.parent.parent.parent / "config" / "formatter_thresholds"
)


@dataclass
class MetricGroupConfig:
    """Configuration for a group of related metrics.

    A metric group contains thresholds for multiple related metrics
    and optionally column-pattern-specific overrides.
    """

    # Thresholds for metrics in this group
    thresholds: dict[str, ThresholdConfig] = field(default_factory=dict)

    # Column pattern overrides: pattern -> metric -> ThresholdConfig
    # e.g., {"*_id": {"null_ratio": ThresholdConfig(...)}}
    column_patterns: dict[str, dict[str, ThresholdConfig]] = field(default_factory=dict)

    def get_threshold(
        self,
        metric: str,
        column_name: str | None = None,
    ) -> ThresholdConfig | None:
        """Get threshold config for a metric, considering column patterns.

        Args:
            metric: Metric name (e.g., "null_ratio")
            column_name: Optional column name for pattern matching

        Returns:
            ThresholdConfig or None if not found
        """
        # Check column patterns first (most specific)
        if column_name:
            for pattern, overrides in self.column_patterns.items():
                if fnmatch.fnmatch(column_name.lower(), pattern.lower()):
                    if metric in overrides:
                        return overrides[metric]

        # Fall back to default threshold for this metric
        return self.thresholds.get(metric)


@dataclass
class FormatterConfig:
    """Complete formatter configuration with all metric groups.

    Metric groups correspond to interpretation units:
    - completeness: null_ratio, null_count
    - outliers: iqr_outlier_ratio, isolation_forest_ratio
    - benford: benford_compliant, chi_square, p_value
    - temporal: staleness_days, gap_count, completeness_ratio
    - etc.
    """

    # Metric group configurations
    groups: dict[str, MetricGroupConfig] = field(default_factory=dict)

    # Domain this config applies to (e.g., "financial", "marketing")
    domain: str | None = None

    # Source files that contributed to this config
    sources: list[str] = field(default_factory=list)

    def get_threshold(
        self,
        group: str,
        metric: str,
        column_name: str | None = None,
    ) -> ThresholdConfig | None:
        """Get threshold config for a metric in a group.

        Args:
            group: Metric group name (e.g., "completeness")
            metric: Metric name (e.g., "null_ratio")
            column_name: Optional column name for pattern matching

        Returns:
            ThresholdConfig or None if not found
        """
        group_config = self.groups.get(group)
        if group_config:
            return group_config.get_threshold(metric, column_name)
        return None

    def get_severity(
        self,
        group: str,
        metric: str,
        value: float,
        column_name: str | None = None,
    ) -> str:
        """Convenience method to get severity for a metric value.

        Args:
            group: Metric group name
            metric: Metric name
            value: Metric value
            column_name: Optional column name for pattern matching

        Returns:
            Severity level string, or "unknown" if threshold not found
        """
        threshold = self.get_threshold(group, metric, column_name)
        if threshold:
            return threshold.get_severity(value)
        return "unknown"


def _parse_threshold_config(data: dict[str, Any]) -> ThresholdConfig:
    """Parse a threshold config from YAML data.

    Expected format:
        thresholds:
          none: 0.01
          low: 0.05
          moderate: 0.2
          high: 0.5
        default_severity: severe
        ascending: true  # optional, default true
    """
    thresholds = data.get("thresholds", {})
    default_severity = data.get("default_severity", "severe")
    ascending = data.get("ascending", True)

    return ThresholdConfig(
        thresholds=thresholds,
        default_severity=default_severity,
        ascending=ascending,
    )


def _parse_metric_group(data: dict[str, Any]) -> MetricGroupConfig:
    """Parse a metric group configuration from YAML data.

    Expected format:
        metrics:
          null_ratio:
            thresholds: {none: 0.01, ...}
          cardinality_ratio:
            thresholds: {none: 0.99, ...}
            ascending: false
        column_patterns:
          "*_id":
            null_ratio:
              thresholds: {none: 0.0}
              default_severity: critical
    """
    group = MetricGroupConfig()

    # Parse metric thresholds
    metrics_data = data.get("metrics", {})
    for metric_name, metric_config in metrics_data.items():
        if isinstance(metric_config, dict):
            group.thresholds[metric_name] = _parse_threshold_config(metric_config)

    # Parse column pattern overrides
    patterns_data = data.get("column_patterns", {})
    for pattern, pattern_metrics in patterns_data.items():
        group.column_patterns[pattern] = {}
        for metric_name, metric_config in pattern_metrics.items():
            if isinstance(metric_config, dict):
                group.column_patterns[pattern][metric_name] = _parse_threshold_config(metric_config)

    return group


def _load_yaml_config(path: Path) -> dict[str, Any]:
    """Load a YAML configuration file."""
    if not path.exists():
        logger.debug(f"Config file not found: {path}")
        return {}

    with open(path) as f:
        data = yaml.safe_load(f)

    return data or {}


def _merge_configs(base: FormatterConfig, override: FormatterConfig) -> FormatterConfig:
    """Merge two configs, with override taking precedence.

    Override values replace base values at the threshold level.
    """
    merged = FormatterConfig(
        domain=override.domain or base.domain,
        sources=base.sources + override.sources,
    )

    # Start with base groups
    for group_name, group_config in base.groups.items():
        merged.groups[group_name] = MetricGroupConfig(
            thresholds=dict(group_config.thresholds),
            column_patterns={
                pattern: dict(metrics) for pattern, metrics in group_config.column_patterns.items()
            },
        )

    # Apply overrides
    for group_name, group_config in override.groups.items():
        if group_name not in merged.groups:
            merged.groups[group_name] = MetricGroupConfig()

        # Override thresholds
        for metric, threshold in group_config.thresholds.items():
            merged.groups[group_name].thresholds[metric] = threshold

        # Override column patterns
        for pattern, metrics in group_config.column_patterns.items():
            if pattern not in merged.groups[group_name].column_patterns:
                merged.groups[group_name].column_patterns[pattern] = {}
            for metric, threshold in metrics.items():
                merged.groups[group_name].column_patterns[pattern][metric] = threshold

    return merged


def load_formatter_config(
    domain: str | None = None,
    dataset_config: dict[str, Any] | None = None,
    config_dir: Path | None = None,
) -> FormatterConfig:
    """Load formatter configuration with resolution hierarchy.

    Resolution order (later overrides earlier):
    1. defaults.yaml (base thresholds)
    2. {domain}.yaml (domain-specific, e.g., financial.yaml)
    3. dataset_config (runtime overrides)

    Args:
        domain: Domain name for domain-specific thresholds
        dataset_config: Runtime override configuration
        config_dir: Directory containing YAML config files

    Returns:
        Merged FormatterConfig
    """
    config_dir = config_dir or DEFAULT_CONFIG_DIR

    # Start with empty config
    config = FormatterConfig(sources=[])

    # Load defaults
    defaults_path = config_dir / "defaults.yaml"
    defaults_data = _load_yaml_config(defaults_path)
    if defaults_data:
        config = _parse_config_data(defaults_data, "defaults")
        config.sources.append(str(defaults_path))

    # Load domain-specific config
    if domain:
        domain_path = config_dir / f"{domain}.yaml"
        domain_data = _load_yaml_config(domain_path)
        if domain_data:
            domain_config = _parse_config_data(domain_data, domain)
            config = _merge_configs(config, domain_config)
            config.sources.append(str(domain_path))

    # Apply runtime overrides
    if dataset_config:
        runtime_config = _parse_config_data(dataset_config, "runtime")
        config = _merge_configs(config, runtime_config)
        config.sources.append("runtime")

    return config


def _parse_config_data(data: dict[str, Any], source: str) -> FormatterConfig:
    """Parse raw config data into FormatterConfig."""
    config = FormatterConfig(
        domain=data.get("domain", source),
        sources=[source],
    )

    # Parse each group
    groups_data = data.get("groups", {})
    for group_name, group_data in groups_data.items():
        if isinstance(group_data, dict):
            config.groups[group_name] = _parse_metric_group(group_data)

    return config


# === Default Thresholds (Code-based fallback) ===


def get_default_config() -> FormatterConfig:
    """Get default formatter config from code.

    This provides fallback thresholds when YAML files are not available.
    """
    config = FormatterConfig(domain="defaults", sources=["code"])

    # Completeness group
    config.groups["completeness"] = MetricGroupConfig(
        thresholds={
            "null_ratio": ThresholdConfig(
                thresholds={"none": 0.01, "low": 0.05, "moderate": 0.2, "high": 0.5},
                default_severity="severe",
            ),
            "completeness_ratio": ThresholdConfig(
                thresholds={"none": 0.99, "low": 0.95, "moderate": 0.8, "high": 0.5},
                default_severity="severe",
                ascending=False,
            ),
        },
        column_patterns={
            "*_id": {
                "null_ratio": ThresholdConfig(
                    thresholds={"none": 0.0},
                    default_severity="critical",
                ),
            },
            "*_optional": {
                "null_ratio": ThresholdConfig(
                    thresholds={"none": 0.5, "low": 0.7, "moderate": 0.9},
                    default_severity="high",
                ),
            },
        },
    )

    # Outliers group
    config.groups["outliers"] = MetricGroupConfig(
        thresholds={
            "iqr_outlier_ratio": ThresholdConfig(
                thresholds={"none": 0.01, "low": 0.05, "moderate": 0.1, "high": 0.2},
                default_severity="severe",
            ),
            "isolation_forest_ratio": ThresholdConfig(
                thresholds={"none": 0.01, "low": 0.05, "moderate": 0.1, "high": 0.2},
                default_severity="severe",
            ),
        },
    )

    # Benford group
    config.groups["benford"] = MetricGroupConfig(
        thresholds={
            "p_value": ThresholdConfig(
                thresholds={"none": 0.1, "low": 0.05, "moderate": 0.01, "high": 0.001},
                default_severity="severe",
                ascending=False,  # Lower p-value = more severe
            ),
        },
    )

    # Temporal group
    config.groups["temporal"] = MetricGroupConfig(
        thresholds={
            "staleness_days": ThresholdConfig(
                thresholds={"none": 7, "low": 30, "moderate": 90, "high": 180},
                default_severity="severe",
            ),
            "gap_count": ThresholdConfig(
                thresholds={"none": 0, "low": 2, "moderate": 5, "high": 10},
                default_severity="severe",
            ),
            "temporal_completeness": ThresholdConfig(
                thresholds={"none": 0.99, "low": 0.95, "moderate": 0.8, "high": 0.5},
                default_severity="severe",
                ascending=False,
            ),
        },
    )

    # Multicollinearity group
    config.groups["multicollinearity"] = MetricGroupConfig(
        thresholds={
            "vif": ThresholdConfig(
                thresholds={"none": 1.0, "low": 2.5, "moderate": 5.0, "high": 10.0},
                default_severity="severe",
            ),
            "condition_index": ThresholdConfig(
                thresholds={"none": 10.0, "moderate": 30.0},
                default_severity="severe",
            ),
        },
    )

    # Correlation group
    config.groups["correlation"] = MetricGroupConfig(
        thresholds={
            "correlation_coefficient": ThresholdConfig(
                thresholds={"none": 0.3, "low": 0.5, "moderate": 0.7, "high": 0.9},
                default_severity="severe",
            ),
        },
    )

    # Domain group
    config.groups["domain"] = MetricGroupConfig(
        thresholds={
            "compliance_score": ThresholdConfig(
                thresholds={"none": 0.99, "low": 0.95, "moderate": 0.8, "high": 0.5},
                default_severity="severe",
                ascending=False,
            ),
            "violation_count": ThresholdConfig(
                thresholds={"none": 0, "low": 2, "moderate": 5, "high": 10},
                default_severity="severe",
            ),
            "balance_difference": ThresholdConfig(
                thresholds={"none": 0.01, "low": 1.0, "moderate": 100.0, "high": 1000.0},
                default_severity="critical",
            ),
            "sign_compliance": ThresholdConfig(
                thresholds={"none": 0.999, "low": 0.99, "moderate": 0.95, "high": 0.9},
                default_severity="critical",
                ascending=False,
            ),
        },
    )

    # Topology group
    config.groups["topology"] = MetricGroupConfig(
        thresholds={
            "betti_0": ThresholdConfig(
                thresholds={"none": 1, "low": 2, "moderate": 4, "high": 8},
                default_severity="severe",
            ),
            "betti_1": ThresholdConfig(
                thresholds={"none": 0, "low": 2, "moderate": 5, "high": 10},
                default_severity="severe",
            ),
            "orphaned_components": ThresholdConfig(
                thresholds={"none": 0, "low": 1, "moderate": 3, "high": 5},
                default_severity="severe",
            ),
            "cycle_count": ThresholdConfig(
                thresholds={"none": 0, "low": 3, "moderate": 6, "high": 10},
                default_severity="severe",
            ),
            "anomalous_cycle_count": ThresholdConfig(
                thresholds={"none": 0, "low": 1, "moderate": 2, "high": 3},
                default_severity="severe",
            ),
            "structural_complexity": ThresholdConfig(
                thresholds={"none": 3, "low": 6, "moderate": 10, "high": 15},
                default_severity="severe",
            ),
            "bottleneck_distance": ThresholdConfig(
                thresholds={"none": 0.05, "low": 0.1, "moderate": 0.2, "high": 0.3},
                default_severity="severe",
            ),
        },
    )

    return config
