"""Unit tests for deterministic formula composition (DAT-636).

Covers every finance formula shape from the smoke corpus, the NULLIF division guard, and the
born-loud failures (unknown operand, unsupported construct). The composed SQL is
also executed against in-memory DuckDB CTEs to prove it is valid and evaluates to
the arithmetic result — no LLM, no smoke.
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.graphs.formula_composer import compose_constant_sql, compose_formula_sql

# Every distinct formula expression in packages/dataraum-config/verticals/finance,
# paired with its declared dependency step ids.
_FINANCE_FORMULAS: list[tuple[str, set[str]]] = [
    ("revenue - cost_of_goods_sold", {"revenue", "cost_of_goods_sold"}),
    (
        "revenue - cost_of_goods_sold - operating_expense",
        {"revenue", "cost_of_goods_sold", "operating_expense"},
    ),
    ("gross_profit - operating_expense", {"gross_profit", "operating_expense"}),
    ("operating_income - interest - tax", {"operating_income", "interest", "tax"}),
    ("operating_income + depreciation", {"operating_income", "depreciation"}),
    ("dso + dio - dpo", {"dso", "dio", "dpo"}),
    ("current_assets / current_liabilities", {"current_assets", "current_liabilities"}),
    ("net_income / revenue * 100", {"net_income", "revenue"}),
    ("ebitda / revenue * 100", {"ebitda", "revenue"}),
    ("operating_income / revenue * 100", {"operating_income", "revenue"}),
    ("(revenue - cost_of_goods_sold) / revenue * 100", {"revenue", "cost_of_goods_sold"}),
    (
        "(accounts_receivable / revenue) * days_in_period",
        {"accounts_receivable", "revenue", "days_in_period"},
    ),
    (
        "(inventory / cost_of_goods_sold) * days_in_period",
        {"inventory", "cost_of_goods_sold", "days_in_period"},
    ),
    (
        "(accounts_payable / cost_of_goods_sold) * days_in_period",
        {"accounts_payable", "cost_of_goods_sold", "days_in_period"},
    ),
]


class TestComposeConstantSql:
    def test_integer_constant_stays_integer(self) -> None:
        # days_in_period=30 → matches the snippet the LLM path produced.
        assert compose_constant_sql(30) == "SELECT 30 AS value"

    def test_integer_valued_float_stays_integer(self) -> None:
        assert compose_constant_sql(365.0) == "SELECT 365 AS value"

    def test_fractional_constant_is_float(self) -> None:
        assert compose_constant_sql(1.5) == "SELECT 1.5 AS value"

    def test_non_numeric_fails_loud(self) -> None:
        with pytest.raises(ValueError, match="not numeric"):
            compose_constant_sql("not-a-number")


class TestComposeFormulaSql:
    @pytest.mark.parametrize(("expression", "deps"), _FINANCE_FORMULAS)
    def test_every_finance_formula_composes(self, expression: str, deps: set[str]) -> None:
        sql = compose_formula_sql(expression, deps)
        assert sql.startswith("SELECT ") and sql.endswith(" AS value")
        # Every operand is referenced via its step CTE; no bare table/identifier leaks.
        for dep in deps:
            assert f"(SELECT value FROM {dep})" in sql

    def test_subtraction_references_each_dep_cte(self) -> None:
        sql = compose_formula_sql("revenue - cost_of_goods_sold", {"revenue", "cost_of_goods_sold"})
        assert sql == (
            "SELECT ((SELECT value FROM revenue) - (SELECT value FROM cost_of_goods_sold)) AS value"
        )

    def test_division_guards_denominator_with_nullif(self) -> None:
        sql = compose_formula_sql(
            "current_assets / current_liabilities", {"current_assets", "current_liabilities"}
        )
        assert "NULLIF((SELECT value FROM current_liabilities), 0)" in sql
        # The numerator is NOT wrapped — only the denominator.
        assert "NULLIF((SELECT value FROM current_assets)" not in sql

    def test_ratio_times_constant_literal_passes_through(self) -> None:
        sql = compose_formula_sql("net_income / revenue * 100", {"net_income", "revenue"})
        assert "100" in sql
        assert "NULLIF((SELECT value FROM revenue), 0)" in sql

    def test_unknown_operand_fails_loud(self) -> None:
        """An operand not in the declared deps must raise, never fabricate a CTE."""
        with pytest.raises(ValueError, match="not a declared dependency"):
            compose_formula_sql("revenue - cogs", {"revenue", "cost_of_goods_sold"})

    def test_unsupported_construct_fails_loud(self) -> None:
        with pytest.raises(ValueError, match="unsupported"):
            compose_formula_sql("max(revenue, 0)", {"revenue"})

    def test_unparseable_expression_fails_loud(self) -> None:
        with pytest.raises(ValueError, match="unparseable"):
            compose_formula_sql("revenue -", {"revenue"})


class TestComposedSqlExecutes:
    """The composed SQL must be valid DuckDB and evaluate to the arithmetic result."""

    @staticmethod
    def _run(expression: str, dep_values: dict[str, float]) -> object:
        final_sql = compose_formula_sql(expression, set(dep_values))
        ctes = ", ".join(f"{step} AS (SELECT {v} AS value)" for step, v in dep_values.items())
        conn = duckdb.connect(":memory:")
        try:
            return conn.execute(f"WITH {ctes} {final_sql}").fetchone()[0]
        finally:
            conn.close()

    def test_gross_profit_subtracts(self) -> None:
        assert (
            self._run(
                "revenue - cost_of_goods_sold", {"revenue": 1000.0, "cost_of_goods_sold": 600.0}
            )
            == 400.0
        )

    def test_dso_ratio_times_period(self) -> None:
        # (200 / 1000) * 30 = 6
        assert (
            self._run(
                "(accounts_receivable / revenue) * days_in_period",
                {"accounts_receivable": 200.0, "revenue": 1000.0, "days_in_period": 30.0},
            )
            == 6.0
        )

    def test_margin_percentage(self) -> None:
        # (1000 - 600) / 1000 * 100 = 40
        assert (
            self._run(
                "(revenue - cost_of_goods_sold) / revenue * 100",
                {"revenue": 1000.0, "cost_of_goods_sold": 600.0},
            )
            == 40.0
        )

    def test_zero_denominator_yields_null_not_error(self) -> None:
        # revenue = 0 → NULLIF makes the division NULL, propagating (not a crash).
        assert self._run("net_income / revenue * 100", {"net_income": 50.0, "revenue": 0.0}) is None

    def test_multi_term_subtraction(self) -> None:
        # 1000 - 600 - 200 = 200
        assert (
            self._run(
                "revenue - cost_of_goods_sold - operating_expense",
                {"revenue": 1000.0, "cost_of_goods_sold": 600.0, "operating_expense": 200.0},
            )
            == 200.0
        )
