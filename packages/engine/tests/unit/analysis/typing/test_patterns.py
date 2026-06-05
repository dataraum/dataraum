"""Tests for pattern detection.

Tests the value-based pattern matching used for type inference.
Column name patterns are intentionally NOT supported.
"""

from dataraum.analysis.typing.patterns import (
    Pattern,
    PatternConfig,
    normalize_standardization_expr,
)
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

    def test_pattern_matches_date_dd_mon_yy(self):
        """Test DD-Mon-YY date pattern matching."""
        pattern = Pattern(
            name="dd_mon_yy",
            pattern=r"^\d{1,2}-[A-Za-z]{3}-\d{2}$",
            inferred_type=DataType.DATE,
        )
        assert pattern.matches("01-Apr-25")
        assert pattern.matches("28-Feb-25")
        assert pattern.matches("1-Mar-25")
        assert not pattern.matches("01-Apr-2025")
        assert not pattern.matches("2024-01-15")
        assert not pattern.matches("01/15/24")

    def test_pattern_matches_date_dd_mon_yyyy(self):
        """Test DD-Mon-YYYY date pattern matching."""
        pattern = Pattern(
            name="dd_mon_yyyy",
            pattern=r"^\d{1,2}-[A-Za-z]{3}-\d{4}$",
            inferred_type=DataType.DATE,
        )
        assert pattern.matches("01-Apr-2025")
        assert pattern.matches("15-Jan-2024")
        assert not pattern.matches("01-Apr-25")
        assert not pattern.matches("2024-01-15")

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


class TestNormalizeStandardizationExpr:
    """TRY_-normalization of standardization expressions.

    DuckDB's STRPTIME/CAST throw on non-conforming input, and TRY_CAST does
    NOT catch inner-function errors — one malformed value would score a
    pattern 0.0 in inference or fail the typed-table CREATE outright.
    """

    def test_strptime_rewritten(self):
        assert (
            normalize_standardization_expr("STRPTIME(\"{col}\", '%d.%m.%Y')")
            == "TRY_STRPTIME(\"{col}\", '%d.%m.%Y')"
        )

    def test_inner_cast_rewritten(self):
        expr = "CAST(REPLACE(\"{col}\", '%', '') AS DOUBLE) / 100"
        assert (
            normalize_standardization_expr(expr)
            == "TRY_CAST(REPLACE(\"{col}\", '%', '') AS DOUBLE) / 100"
        )

    def test_idempotent_on_try_variants(self):
        expr = 'COALESCE(TRY_STRPTIME("{col}", \'%d.%m.%Y\'), TRY_CAST("{col}" AS DATE))'
        assert normalize_standardization_expr(expr) == expr

    def test_other_functions_untouched(self):
        expr = 'MAKE_DATE(TRY_CAST(LEFT("{col}", 4) AS INT), 1, 1)'
        assert normalize_standardization_expr(expr) == expr

    def test_pattern_normalizes_on_construction(self):
        pattern = Pattern(
            name="eu_date",
            pattern=r"^\d{1,2}\.\d{1,2}\.\d{2,4}$",
            inferred_type=DataType.DATE,
            standardization_expr="STRPTIME(\"{col}\", '%d.%m.%Y')",
        )
        assert pattern.standardization_expr == "TRY_STRPTIME(\"{col}\", '%d.%m.%Y')"

    def test_taught_override_pattern_normalized(self):
        """Overlay-taught patterns get the same normalization as builtins."""
        config = PatternConfig(
            {
                "overrides": {
                    "patterns": {
                        "de_date_ddmmyyyy": {
                            "pattern": r"^\d{1,2}\.\d{1,2}\.\d{4}$",
                            "standardization_expr": "STRPTIME(\"{col}\", '%d.%m.%Y')",
                        }
                    }
                }
            }
        )
        (p,) = config.get_patterns()
        assert p.standardization_expr == "TRY_STRPTIME(\"{col}\", '%d.%m.%Y')"
        # inferred_type defaults to DATE for override patterns with an expr
        assert p.inferred_type == DataType.DATE


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

    def test_config_loads_dd_mon_patterns(self):
        """Test that DD-Mon-YY/YYYY patterns load from config and match values."""
        from dataraum.analysis.typing.patterns import load_pattern_config

        config = load_pattern_config()
        pattern_names = {p.name for p in config.get_patterns()}
        assert "dd_mon_yy" in pattern_names
        assert "dd_mon_yyyy" in pattern_names

        # Verify matching against actual financial date formats
        matches = config.match_value("01-Apr-25")
        match_names = {m.name for m in matches}
        assert "dd_mon_yy" in match_names

        matches = config.match_value("15-Jan-2024")
        match_names = {m.name for m in matches}
        assert "dd_mon_yyyy" in match_names

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
