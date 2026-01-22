"""Null value configuration loader."""

from pathlib import Path
from typing import Any

import yaml

from dataraum.core.config import get_settings


class NullValueConfig:
    """Null value configuration for staging."""

    def __init__(self, config_dict: dict[str, Any]):
        self._config = config_dict

    def get_null_strings(self, include_placeholders: bool = True) -> list[str]:
        """Get list of strings to treat as NULL.

        Args:
            include_placeholders: Whether to include placeholder nulls (-, --, etc.)

        Returns:
            List of null string representations
        """
        null_strings = []

        # Standard nulls
        for item in self._config.get("standard_nulls", []):
            null_strings.append(item["value"])

        # Spreadsheet nulls
        for item in self._config.get("spreadsheet_nulls", []):
            null_strings.append(item["value"])

        # Placeholder nulls (optional)
        if include_placeholders:
            for item in self._config.get("placeholder_nulls", []):
                # Skip single-char placeholders that need context
                context = item.get("context")
                if context != "single_char_only" or len(item["value"]) > 1:
                    null_strings.append(item["value"])

        # Missing indicators
        for item in self._config.get("missing_indicators", []):
            null_strings.append(item["value"])

        # Remove empty string (handled separately in DuckDB)
        # null_strings = [s for s in null_strings if s != ""]

        return null_strings

    def should_trim_whitespace(self) -> bool:
        """Check if whitespace should be trimmed before null checking."""
        result = self._config.get("whitespace_rules", {}).get("trim_before_check", True)
        return bool(result)

    def treat_whitespace_as_null(self) -> bool:
        """Check if whitespace-only strings should be treated as NULL."""
        result = self._config.get("whitespace_rules", {}).get("treat_whitespace_only_as_null", True)
        return bool(result)


def load_null_value_config(config_path: Path | None = None) -> NullValueConfig:
    """Load null value configuration from YAML.

    Args:
        config_path: Optional path to config file. If None, uses default from settings.

    Returns:
        NullValueConfig instance
    """
    if config_path is None:
        settings = get_settings()
        config_path = settings.config_path / "null_values.yaml"

    with open(config_path) as f:
        config_dict = yaml.safe_load(f)

    return NullValueConfig(config_dict)
