"""Tests for shared TUI formatting utilities."""

from dataraum.cli.tui.formatting import (
    format_evidence_field,
    format_priority_color,
    format_score_color,
)


class TestFormatEvidenceField:
    """Tests for format_evidence_field."""

    def test_float_ratio_shows_percentage(self):
        result = format_evidence_field("null_rate", 0.15)
        assert "15.0%" in result
        assert "Null Rate:" in result

    def test_float_confidence_shows_percentage(self):
        result = format_evidence_field("type_confidence", 0.95)
        assert "95.0%" in result

    def test_float_generic_shows_three_decimals(self):
        result = format_evidence_field("score", 0.123456)
        assert "0.123" in result

    def test_bool_true_shows_yes(self):
        result = format_evidence_field("is_valid", True)
        assert "yes" in result

    def test_bool_false_shows_no(self):
        result = format_evidence_field("is_valid", False)
        assert "no" in result

    def test_int_shows_with_commas(self):
        result = format_evidence_field("total_count", 1234567)
        assert "1,234,567" in result

    def test_empty_list_shows_none(self):
        result = format_evidence_field("items", [])
        assert "(none)" in result

    def test_list_truncated_at_five(self):
        result = format_evidence_field("values", list(range(10)))
        assert "+5" in result

    def test_list_short_no_truncation(self):
        result = format_evidence_field("values", [1, 2, 3])
        assert "1, 2, 3" in result
        assert "+" not in result

    def test_dict_shows_key_value_pairs(self):
        result = format_evidence_field("metadata", {"a": 1, "b": 2})
        assert "a=1" in result

    def test_string_value(self):
        result = format_evidence_field("detector", "type_detector")
        assert "type_detector" in result

    def test_long_string_truncated(self):
        long_val = "x" * 100
        result = format_evidence_field("text", long_val)
        assert "..." in result
        assert len(result) < 120

    def test_key_formatting_replaces_underscores(self):
        result = format_evidence_field("my_field_name", "val")
        assert "My Field Name:" in result

    def test_value_with_brackets_escaped(self):
        """Values containing [ ] chars must produce valid Textual markup."""
        from textual.content import Content

        result = format_evidence_field("info", "has [dim] brackets [/dim]")
        Content.from_markup(result)  # Should not raise

    def test_uppercase_bracket_escaped(self):
        """Uppercase-starting brackets like [Account] must be escaped for Textual."""
        from textual.content import Content

        result = format_evidence_field(
            "description",
            "1 distinct values in [Account type=Expenses] vs 282 in [Income]",
        )
        Content.from_markup(result)  # Should not raise

    def test_truncated_bracket_escaped(self):
        """A bracket split by truncation must still be escaped."""
        from textual.content import Content

        # This is >80 chars with a [ near position 70 that gets truncated mid-tag
        val = "Account name has different value sets by slice: 1 distinct values in [Account type=Expenses] vs 282"
        result = format_evidence_field("description", val)
        Content.from_markup(result)  # Should not raise

    def test_dict_with_list_values_escaped(self):
        """Dict values containing lists (with brackets) must be escaped."""
        from textual.content import Content

        result = format_evidence_field("data", {"key": ["a", "b"]})
        Content.from_markup(result)  # Should not raise

    def test_list_with_nested_list_escaped(self):
        """Nested list values must be escaped."""
        from textual.content import Content

        result = format_evidence_field("data", [[1, 2], [3, 4]])
        Content.from_markup(result)  # Should not raise

    def test_all_types_produce_valid_markup(self):
        """Every value type must produce valid Textual markup."""
        from textual.content import Content

        cases = [
            ("f", 0.5),
            ("rate", 0.1),
            ("b", True),
            ("i", 42),
            ("l", [1, "two", {"three": 3}]),
            ("d", {"k": [1, 2], "m": {"n": "o"}}),
            ("s", "hello [world]"),
            ("s_upper", "test [Account] bracket"),
            ("n", None),
            ("e", []),
        ]
        for key, value in cases:
            result = format_evidence_field(key, value)
            Content.from_markup(result)  # Should not raise


class TestFormatScoreColor:
    """Tests for format_score_color."""

    def test_high_score_red(self):
        assert format_score_color(0.5) == "red"

    def test_medium_score_yellow(self):
        assert format_score_color(0.2) == "yellow"

    def test_low_score_green(self):
        assert format_score_color(0.1) == "green"

    def test_boundary_high(self):
        assert format_score_color(0.31) == "red"

    def test_boundary_medium(self):
        assert format_score_color(0.16) == "yellow"

    def test_zero_green(self):
        assert format_score_color(0.0) == "green"


class TestFormatPriorityColor:
    """Tests for format_priority_color."""

    def test_high_red(self):
        assert format_priority_color("high") == "red"

    def test_medium_yellow(self):
        assert format_priority_color("medium") == "yellow"

    def test_low_green(self):
        assert format_priority_color("low") == "green"

    def test_case_insensitive(self):
        assert format_priority_color("HIGH") == "red"

    def test_unknown_white(self):
        assert format_priority_color("unknown") == "white"
