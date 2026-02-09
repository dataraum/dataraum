"""Tests for pattern detection.

Tests the value-based pattern matching used for type inference.
Column name patterns are intentionally NOT supported.
"""

import pytest

from dataraum.analysis.typing.patterns import Pattern, PatternConfig, load_pattern_config
from dataraum.core.models.base import DataType


class TestPattern:
    """Tests for the Pattern class."""

    def test_pattern_matches_date_iso(self):
        """Test ISO date pattern matching."""
        pattern = Pattern(
            name="iso_date",
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            inferred_type=DataType.DATE,
        )
        assert pattern.matches("2024-01-15")
        assert pattern.matches("2023-12-31")
        assert not pattern.matches("01-15-2024")
        assert not pattern.matches("not a date")

    def test_pattern_matches_integer(self):
        """Test integer pattern matching."""
        pattern = Pattern(
            name="integer",
            pattern=r"^-?\d+$",
            inferred_type=DataType.INTEGER,
        )
        assert pattern.matches("123")
        assert pattern.matches("-456")
        assert pattern.matches("0")
        assert not pattern.matches("12.34")
        assert not pattern.matches("abc")

    def test_pattern_case_insensitive(self):
        """Test case-insensitive pattern matching."""
        pattern = Pattern(
            name="boolean",
            pattern=r"^(true|false|yes|no)$",
            inferred_type=DataType.BOOLEAN,
            case_sensitive=False,
        )
        assert pattern.matches("true")
        assert pattern.matches("TRUE")
        assert pattern.matches("True")
        assert pattern.matches("yes")
        assert pattern.matches("YES")
        assert not pattern.matches("maybe")

    def test_pattern_empty_value(self):
        """Test that empty values don't match."""
        pattern = Pattern(
            name="any",
            pattern=r".*",
            inferred_type=DataType.VARCHAR,
        )
        assert not pattern.matches("")
        assert not pattern.matches(None)  # type: ignore[arg-type]

    def test_pattern_with_unit(self):
        """Test pattern with detected unit."""
        pattern = Pattern(
            name="usd_currency",
            pattern=r"^\$[\d,]+(\.\d{2})?$",
            inferred_type=DataType.DECIMAL,
            detected_unit="USD",
        )
        assert pattern.matches("$1,234.56")
        assert pattern.matches("$100")
        assert pattern.detected_unit == "USD"


class TestPatternConfig:
    """Tests for the PatternConfig class."""

    def test_load_config_from_dict(self):
        """Test loading patterns from a dictionary."""
        config_dict = {
            "numeric_patterns": [
                {
                    "name": "integer",
                    "pattern": r"^-?\d+$",
                    "inferred_type": "INTEGER",
                },
                {
                    "name": "decimal",
                    "pattern": r"^-?\d+\.\d+$",
                    "inferred_type": "DOUBLE",
                },
            ]
        }
        config = PatternConfig(config_dict)
        patterns = config.get_patterns()

        assert len(patterns) == 2
        assert patterns[0].name == "integer"
        assert patterns[0].inferred_type == DataType.INTEGER
        assert patterns[1].name == "decimal"
        assert patterns[1].inferred_type == DataType.DOUBLE

    def test_match_value_returns_all_matches(self):
        """Test that match_value returns all matching patterns."""
        config_dict = {
            "numeric_patterns": [
                {
                    "name": "integer",
                    "pattern": r"^-?\d+$",
                    "inferred_type": "INTEGER",
                },
                {
                    "name": "positive_integer",
                    "pattern": r"^\d+$",
                    "inferred_type": "INTEGER",
                },
            ]
        }
        config = PatternConfig(config_dict)

        # "123" should match both patterns
        matches = config.match_value("123")
        assert len(matches) == 2

        # "-123" should only match the general integer pattern
        matches = config.match_value("-123")
        assert len(matches) == 1
        assert matches[0].name == "integer"

    def test_no_column_name_patterns(self):
        """Test that PatternConfig does not support column name patterns.

        Column name pattern matching was intentionally removed as it's
        fragile and semantically meaningful names should be handled
        by semantic analysis, not type inference.
        """
        config_dict = {
            "column_name_patterns": [
                {
                    "pattern": ".*_id$",
                    "likely_type": "INTEGER",
                }
            ]
        }
        config = PatternConfig(config_dict)

        # PatternConfig should not have column name matching
        assert not hasattr(config, "match_column_name")
        assert not hasattr(config, "get_column_name_patterns")


class TestLoadPatternConfig:
    """Tests for the load_pattern_config function."""

    def test_load_default_config(self):
        """Test loading the default pattern configuration."""
        try:
            config = load_pattern_config()
            patterns = config.get_patterns()

            # Should have loaded some patterns
            assert len(patterns) > 0

            # Check that common patterns exist
            pattern_names = {p.name for p in patterns}
            # At minimum, we should have some date and numeric patterns
            assert any("date" in name.lower() for name in pattern_names)
        except FileNotFoundError:
            pytest.skip("Default pattern config not found")
