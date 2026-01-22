"""Pattern detection for type inference.

This module provides value-based pattern matching for type inference.
Patterns are defined in config/patterns/default.yaml.

IMPORTANT: Type inference is based ONLY on value patterns, NOT column names.
Column names are semantically meaningful but fragile for type inference
(e.g., "balance" could be numeric or text depending on context).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import yaml

from dataraum.core.config import get_settings
from dataraum.core.models.base import DataType


@dataclass
class Pattern:
    """A single pattern definition for value matching.

    Patterns match against actual cell values (not column names).
    """

    name: str
    pattern: str
    inferred_type: DataType
    semantic_type: str | None = None
    detected_unit: str | None = None
    case_sensitive: bool = True
    pii: bool = False
    ambiguous: bool = False
    locale: str | None = None
    examples: list[str] | None = None
    standardization_expr: str | None = None  # DuckDB SQL to normalize before cast

    # Compiled regex (set in __post_init__)
    _regex: re.Pattern[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Compile regex pattern."""
        flags = 0 if self.case_sensitive else re.IGNORECASE
        self._regex = re.compile(self.pattern, flags)

    def matches(self, value: str) -> bool:
        """Check if value matches this pattern.

        Args:
            value: String value to check

        Returns:
            True if pattern matches
        """
        if not value:
            return False
        return self._regex.match(value) is not None


class PatternConfig:
    """Pattern detection configuration.

    Loads patterns from YAML configuration and provides matching functionality.
    Only supports VALUE patterns - column name patterns are intentionally excluded.
    """

    def __init__(self, config_dict: dict[str, object]):
        self._config = config_dict
        self._patterns: list[Pattern] = []
        self._load_patterns()

    def _load_patterns(self) -> None:
        """Load all value patterns from configuration."""
        # Load value patterns from all categories
        for category in [
            "date_patterns",
            "identifier_patterns",
            "numeric_patterns",
            "currency_patterns",
            "boolean_patterns",
        ]:
            patterns_list = cast(list[dict[str, Any]], self._config.get(category, []))
            for pattern_dict in patterns_list:
                try:
                    # Convert inferred_type string to DataType enum
                    inferred_type_str = pattern_dict.get("inferred_type", "VARCHAR")
                    inferred_type = DataType[inferred_type_str]

                    pattern = Pattern(
                        name=pattern_dict["name"],
                        pattern=pattern_dict["pattern"],
                        inferred_type=inferred_type,
                        semantic_type=pattern_dict.get("semantic_type"),
                        detected_unit=pattern_dict.get("detected_unit"),
                        case_sensitive=pattern_dict.get("case_sensitive", True),
                        pii=pattern_dict.get("pii", False),
                        ambiguous=pattern_dict.get("ambiguous", False),
                        locale=pattern_dict.get("locale"),
                        examples=pattern_dict.get("examples"),
                        standardization_expr=pattern_dict.get("standardization_expr"),
                    )
                    self._patterns.append(pattern)
                except KeyError:
                    # Skip invalid patterns
                    continue

    def get_patterns(self) -> list[Pattern]:
        """Get all value patterns.

        Returns:
            List of Pattern objects
        """
        return self._patterns

    def match_value(self, value: str) -> list[Pattern]:
        """Find all patterns that match a value.

        Args:
            value: String value to match

        Returns:
            List of matching Pattern objects
        """
        matches = []
        for pattern in self._patterns:
            if pattern.matches(value):
                matches.append(pattern)
        return matches


def load_pattern_config(config_path: Path | None = None) -> PatternConfig:
    """Load pattern configuration from YAML.

    Args:
        config_path: Optional path to config file. If None, uses default from settings.

    Returns:
        PatternConfig instance
    """
    if config_path is None:
        settings = get_settings()
        config_path = settings.config_path / "patterns" / "default.yaml"

    with open(config_path) as f:
        config_dict = yaml.safe_load(f)

    return PatternConfig(config_dict)
