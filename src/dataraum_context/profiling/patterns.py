"""Pattern detection configuration loader."""

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from dataraum_context.core.config import get_settings
from dataraum_context.core.models.base import DataType


@dataclass
class Pattern:
    """A single pattern definition."""

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

    def __post_init__(self):
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


@dataclass
class ColumnNamePattern:
    """Pattern for column name matching."""

    pattern: str
    likely_role: str | None = None
    likely_type: DataType | None = None
    semantic_type: str | None = None

    def __post_init__(self):
        """Compile regex pattern."""
        self._regex = re.compile(self.pattern, re.IGNORECASE)

    def matches(self, column_name: str) -> bool:
        """Check if column name matches this pattern.

        Args:
            column_name: Column name to check

        Returns:
            True if pattern matches
        """
        return self._regex.match(column_name) is not None


class PatternConfig:
    """Pattern detection configuration."""

    def __init__(self, config_dict: dict):
        self._config = config_dict
        self._patterns: list[Pattern] = []
        self._column_name_patterns: list[ColumnNamePattern] = []
        self._load_patterns()

    def _load_patterns(self):
        """Load all patterns from configuration."""
        # Load value patterns
        for category in [
            "date_patterns",
            "identifier_patterns",
            "numeric_patterns",
            "currency_patterns",
            "boolean_patterns",
        ]:
            for pattern_dict in self._config.get(category, []):
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
                    )
                    self._patterns.append(pattern)
                except KeyError:
                    # Skip invalid patterns
                    continue

        # Load column name patterns
        for pattern_dict in self._config.get("column_name_patterns", []):
            try:
                likely_type = None
                if "likely_type" in pattern_dict:
                    likely_type = DataType[pattern_dict["likely_type"]]

                pattern = ColumnNamePattern(
                    pattern=pattern_dict["pattern"],
                    likely_role=pattern_dict.get("likely_role"),
                    likely_type=likely_type,
                    semantic_type=pattern_dict.get("semantic_type"),
                )
                self._column_name_patterns.append(pattern)
            except KeyError:
                continue

    def get_value_patterns(self) -> list[Pattern]:
        """Get all value patterns.

        Returns:
            List of Pattern objects
        """
        return self._patterns

    def get_column_name_patterns(self) -> list[ColumnNamePattern]:
        """Get all column name patterns.

        Returns:
            List of ColumnNamePattern objects
        """
        return self._column_name_patterns

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

    def match_column_name(self, column_name: str) -> list[ColumnNamePattern]:
        """Find all patterns that match a column name.

        Args:
            column_name: Column name to match

        Returns:
            List of matching ColumnNamePattern objects
        """
        matches = []
        for pattern in self._column_name_patterns:
            if pattern.matches(column_name):
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
