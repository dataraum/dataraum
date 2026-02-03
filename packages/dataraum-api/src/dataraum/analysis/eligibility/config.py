"""Column eligibility configuration.

Loads and validates the column eligibility configuration from YAML.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class EligibilityThresholds:
    """Thresholds for eligibility evaluation."""

    max_null_ratio: float = 1.0  # Column is INELIGIBLE if null_ratio >= this
    eliminate_single_value: bool = True  # Eliminate columns with single value
    warn_null_ratio: float = 0.5  # Column gets WARN status if null_ratio > this


@dataclass
class EligibilityRule:
    """A single eligibility rule."""

    id: str
    condition: str  # Python expression to evaluate
    status: str  # ELIGIBLE, WARN, or INELIGIBLE
    reason: str  # Human-readable reason template


@dataclass
class EligibilityConfig:
    """Column eligibility configuration."""

    version: str = "1.0"
    thresholds: EligibilityThresholds = field(default_factory=EligibilityThresholds)
    rules: list[EligibilityRule] = field(default_factory=list)
    default_status: str = "ELIGIBLE"

    # Patterns for likely key columns (fail pipeline if ineligible)
    key_patterns: list[str] = field(default_factory=lambda: ["_id$", "^id$", "_key$"])

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EligibilityConfig:
        """Create config from dictionary."""
        thresholds = EligibilityThresholds(
            max_null_ratio=data.get("thresholds", {}).get("max_null_ratio", 1.0),
            eliminate_single_value=data.get("thresholds", {}).get("eliminate_single_value", True),
            warn_null_ratio=data.get("thresholds", {}).get("warn_null_ratio", 0.5),
        )

        rules = []
        for rule_data in data.get("rules", []):
            rules.append(
                EligibilityRule(
                    id=rule_data["id"],
                    condition=rule_data["condition"],
                    status=rule_data["status"],
                    reason=rule_data["reason"],
                )
            )

        return cls(
            version=data.get("version", "1.0"),
            thresholds=thresholds,
            rules=rules,
            default_status=data.get("default_status", "ELIGIBLE"),
            key_patterns=data.get("key_patterns", ["_id$", "^id$", "_key$"]),
        )


_cached_config: EligibilityConfig | None = None


def load_eligibility_config(config_path: Path | None = None) -> EligibilityConfig:
    """Load eligibility configuration from YAML.

    Uses default config if no path provided or file doesn't exist.

    Args:
        config_path: Optional path to config file

    Returns:
        EligibilityConfig instance
    """
    global _cached_config

    if _cached_config is not None and config_path is None:
        return _cached_config

    if config_path is None:
        # Look for config in standard locations
        possible_paths = [
            Path("config/column_eligibility.yaml"),
            Path(__file__).parent.parent.parent.parent / "config" / "column_eligibility.yaml",
        ]
        for path in possible_paths:
            if path.exists():
                config_path = path
                break

    if config_path is not None and config_path.exists():
        with open(config_path) as f:
            data = yaml.safe_load(f)
            config = EligibilityConfig.from_dict(data)
    else:
        # Use defaults
        config = _get_default_config()

    if config_path is None:
        _cached_config = config

    return config


def _get_default_config() -> EligibilityConfig:
    """Get default eligibility configuration."""
    return EligibilityConfig(
        version="1.0",
        thresholds=EligibilityThresholds(
            max_null_ratio=1.0,
            eliminate_single_value=True,
            warn_null_ratio=0.5,
        ),
        rules=[
            EligibilityRule(
                id="all_null",
                condition="null_ratio >= max_null_ratio",
                status="INELIGIBLE",
                reason="Column has {null_ratio:.0%} null values - no usable data",
            ),
            EligibilityRule(
                id="single_value",
                condition="distinct_count == 1 and eliminate_single_value",
                status="INELIGIBLE",
                reason="Column has single value - no variance for analysis",
            ),
            EligibilityRule(
                id="high_null",
                condition="null_ratio > warn_null_ratio",
                status="WARN",
                reason="High null ratio ({null_ratio:.0%}) may affect analysis",
            ),
            EligibilityRule(
                id="near_constant",
                condition="cardinality_ratio < 0.01 and distinct_count <= 3",
                status="WARN",
                reason="Near-constant column with only {distinct_count} distinct values",
            ),
        ],
        default_status="ELIGIBLE",
        key_patterns=["_id$", "^id$", "_key$"],
    )


def clear_config_cache() -> None:
    """Clear the cached configuration."""
    global _cached_config
    _cached_config = None
