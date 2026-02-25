"""Tests for SQL Knowledge Base utility functions."""

from dataraum.query.snippet_utils import (
    determine_usage_type,
    normalize_expression,
    normalize_sql,
    sql_similarity,
)


class TestNormalizeSql:
    """Tests for SQL normalization."""

    def test_lowercase(self):
        assert normalize_sql("SELECT * FROM Foo") == "select * from foo"

    def test_collapse_whitespace(self):
        assert normalize_sql("SELECT  *\n  FROM   foo") == "select * from foo"

    def test_strip_leading_trailing(self):
        assert normalize_sql("  SELECT 1  ") == "select 1"

    def test_empty_string(self):
        assert normalize_sql("") == ""


class TestSqlSimilarity:
    """Tests for SQL similarity comparison."""

    def test_identical(self):
        sql = "SELECT SUM(amount) FROM orders"
        assert sql_similarity(sql, sql) == 1.0

    def test_identical_after_normalization(self):
        a = "SELECT  SUM(amount)\n  FROM orders"
        b = "select sum(amount) from orders"
        assert sql_similarity(a, b) == 1.0

    def test_completely_different(self):
        a = "SELECT 1"
        b = "DELETE FROM orders WHERE id = 999"
        score = sql_similarity(a, b)
        assert score < 0.5

    def test_minor_difference(self):
        a = "SELECT SUM(amount) AS value FROM orders WHERE type = 'sale'"
        b = "SELECT SUM(amount) AS value FROM orders WHERE type = 'purchase'"
        score = sql_similarity(a, b)
        assert score > 0.8  # Very similar
        assert score < 1.0  # But not identical

    def test_empty_strings(self):
        assert sql_similarity("", "") == 1.0
        assert sql_similarity("SELECT 1", "") == 0.0
        assert sql_similarity("", "SELECT 1") == 0.0

    def test_whitespace_only_difference(self):
        a = "SELECT SUM(x) FROM t"
        b = "SELECT  SUM(x)  FROM  t"
        assert sql_similarity(a, b) == 1.0


class TestNormalizeExpression:
    """Tests for expression normalization."""

    def test_simple_multiplication(self):
        """Commutative: a * b → sorted."""
        normalized, fields, bindings = normalize_expression("revenue * days_in_period")
        assert fields == ["days_in_period", "revenue"]
        # After sorting: days_in_period * revenue → {A} * {B}
        assert "{A}" in normalized
        assert "{B}" in normalized
        assert "*" in normalized

    def test_simple_addition(self):
        """Commutative: b + a → sorted."""
        normalized, fields, bindings = normalize_expression("z_field + a_field")
        assert fields == ["a_field", "z_field"]
        # Should be sorted: a_field + z_field → {A} + {B}

    def test_division_preserved(self):
        """Non-commutative: a / b stays as a / b."""
        normalized, fields, bindings = normalize_expression(
            "accounts_receivable / revenue"
        )
        assert fields == ["accounts_receivable", "revenue"]
        # accounts_receivable → {A}, revenue → {B}
        # Division is not commutative, so order preserved: {A} / {B}
        assert normalized.index("{A}") < normalized.index("{B}")

    def test_subtraction_preserved(self):
        """Non-commutative: a - b stays as a - b."""
        normalized, fields, _ = normalize_expression("total - discount")
        assert fields == ["discount", "total"]
        # total → {B} (sorted second), discount → {A}
        # But subtraction preserves order, so {B} - {A}

    def test_complex_expression_with_parens(self):
        """DSO formula: (accounts_receivable / revenue) * days_in_period."""
        normalized, fields, bindings = normalize_expression(
            "(accounts_receivable / revenue) * days_in_period"
        )
        assert fields == ["accounts_receivable", "days_in_period", "revenue"]
        assert len(bindings) == 3

    def test_single_field(self):
        """Single field: no operators."""
        normalized, fields, bindings = normalize_expression("revenue")
        assert fields == ["revenue"]
        assert normalized == "{A}"
        assert bindings["{A}"] == "revenue"

    def test_duplicate_field(self):
        """Same field used twice: only appears once in sorted list."""
        normalized, fields, bindings = normalize_expression("revenue + revenue")
        assert fields == ["revenue"]
        assert len(bindings) == 1

    def test_bindings_are_correct(self):
        """Verify bindings map placeholders back to field names."""
        _, fields, bindings = normalize_expression(
            "(accounts_receivable / revenue) * days_in_period"
        )
        for placeholder, field_name in bindings.items():
            assert field_name in fields
            assert placeholder.startswith("{")
            assert placeholder.endswith("}")


class TestDetermineUsageType:
    """Tests for usage type determination."""

    def test_no_snippet_provided(self):
        assert determine_usage_type("SELECT 1", None) == "newly_generated"

    def test_exact_reuse(self):
        sql = "SELECT SUM(amount) FROM orders"
        assert determine_usage_type(sql, sql) == "exact_reuse"

    def test_exact_reuse_with_whitespace_diff(self):
        a = "SELECT  SUM(amount)\n  FROM orders"
        b = "select sum(amount) from orders"
        assert determine_usage_type(a, b) == "exact_reuse"

    def test_adapted(self):
        """Similar SQL but with minor changes."""
        original = "SELECT SUM(amount) AS value FROM typed_orders WHERE type IN ('sale', 'revenue')"
        adapted = "SELECT SUM(amount) AS total_value FROM typed_orders WHERE category IN ('sale', 'revenue')"
        result = determine_usage_type(adapted, original)
        assert result == "adapted"

    def test_provided_not_used(self):
        """Completely different SQL."""
        original = "SELECT SUM(amount) FROM orders"
        generated = "SELECT COUNT(*) FROM customers GROUP BY region"
        assert determine_usage_type(generated, original) == "provided_not_used"
