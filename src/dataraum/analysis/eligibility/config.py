"""Column eligibility configuration.

Loads and validates the column eligibility configuration from YAML.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class EligibilityThresholds:
    """Thresholds for eligibility evaluation."""

    max_null_ratio: float
    eliminate_single_value: bool
    warn_null_ratio: float


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

    version: str
    thresholds: EligibilityThresholds
    rules: list[EligibilityRule]
    default_status: str
    key_patterns: list[str] = field(default_factory=lambda: ["_id$", "^id$", "_key$"])

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EligibilityConfig:
        """Create config from dictionary.

        Requires all fields to be present in the config — no silent defaults.
        """
        thresholds_data = data["thresholds"]
        thresholds = EligibilityThresholds(
            max_null_ratio=thresholds_data["max_null_ratio"],
            eliminate_single_value=thresholds_data["eliminate_single_value"],
            warn_null_ratio=thresholds_data["warn_null_ratio"],
        )

        rules = [
            EligibilityRule(
                id=rule_data["id"],
                condition=rule_data["condition"],
                status=rule_data["status"],
                reason=rule_data["reason"],
            )
            for rule_data in data["rules"]
        ]

        return cls(
            version=data["version"],
            thresholds=thresholds,
            rules=rules,
            default_status=data["default_status"],
            key_patterns=data.get("key_patterns", ["_id$", "^id$", "_key$"]),
        )


def load_eligibility_config() -> EligibilityConfig:
    """Load eligibility configuration from YAML.

    Returns:
        EligibilityConfig instance
    """
    from dataraum.core.config import load_yaml_config

    data = load_yaml_config("system/column_eligibility.yaml")
    return EligibilityConfig.from_dict(data)
