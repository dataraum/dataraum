"""Unit tests for null value configuration."""

from dataraum.sources.csv.null_values import NullValueConfig, load_null_value_config


class TestNullValueConfig:
    """Tests for NullValueConfig."""

    def test_loads_from_yaml(self):
        config = load_null_value_config()
        null_strings = config.get_null_strings()
        assert isinstance(null_strings, list)
        assert len(null_strings) > 0

    def test_includes_standard_nulls(self):
        config = load_null_value_config()
        null_strings = config.get_null_strings()
        assert "NULL" in null_strings
        assert "None" in null_strings
        assert "N/A" in null_strings

    def test_includes_spreadsheet_nulls(self):
        config = load_null_value_config()
        null_strings = config.get_null_strings()
        assert "#N/A" in null_strings

    def test_includes_missing_indicators(self):
        config = load_null_value_config()
        null_strings = config.get_null_strings()
        assert "MISSING" in null_strings
        assert "UNKNOWN" in null_strings

    def test_placeholders_included_by_default(self):
        config = load_null_value_config()
        null_strings = config.get_null_strings(include_placeholders=True)
        assert "--" in null_strings

    def test_placeholders_excluded_when_requested(self):
        config = load_null_value_config()
        with_placeholders = config.get_null_strings(include_placeholders=True)
        without_placeholders = config.get_null_strings(include_placeholders=False)
        assert len(with_placeholders) > len(without_placeholders)

    def test_single_char_placeholders_filtered(self):
        """Single-char placeholders with context=single_char_only should be excluded."""
        config = NullValueConfig(
            {
                "placeholder_nulls": [
                    {"value": "-", "context": "single_char_only"},
                    {"value": "--"},
                ],
            }
        )
        null_strings = config.get_null_strings(include_placeholders=True)
        assert "-" not in null_strings
        assert "--" in null_strings

    def test_empty_config(self):
        config = NullValueConfig({})
        null_strings = config.get_null_strings()
        assert null_strings == []
