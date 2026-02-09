"""Null value configuration loader."""

from typing import Any

from dataraum.core.config import load_yaml_config


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

        return null_strings


def load_null_value_config() -> NullValueConfig:
    """Load null value configuration from YAML.

    Returns:
        NullValueConfig instance
    """
    config_dict = load_yaml_config("system/null_values.yaml")
    return NullValueConfig(config_dict)
