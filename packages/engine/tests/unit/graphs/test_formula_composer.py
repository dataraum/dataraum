"""Unit tests for deterministic formula composition (DAT-636).

Covers every finance formula shape from the smoke corpus, the NULLIF division guard, and the
born-loud failures (unknown operand, unsupported construct). The composed SQL is
also executed against in-memory DuckDB CTEs to prove it is valid and evaluates to
the arithmetic result — no LLM, no smoke.
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.graphs.formula_composer import (
    compose_constant_sql,
    compose_extract_sql,
    compose_formula_sql,
    extract_parts_dict,
    same_name_keys,
)

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


class TestComposeExtractSql:
    """DAT-671 parts-at-source: the extract render is the ONE engine-side place
    clause parts become a string; the parts themselves persist as the artifact."""

    def test_simple_aggregate(self) -> None:
        assert compose_extract_sql(
            "SUM(credit) - SUM(debit)",
            "enriched_journal_lines",
            ["account_id__account_type = 'revenue'"],
        ) == (
            "SELECT SUM(credit) - SUM(debit) AS value\n"
            "FROM enriched_journal_lines\n"
            "WHERE account_id__account_type = 'revenue'"
        )

    def test_multiple_predicates_and_compose_parenthesized(self) -> None:
        sql = compose_extract_sql("SUM(x)", "t", ["a = 1 OR a = 2", "b = 3"])
        assert sql.endswith("WHERE (a = 1 OR a = 2) AND (b = 3)")

    def test_no_predicates(self) -> None:
        assert compose_extract_sql("SUM(x)", "t", []) == "SELECT SUM(x) AS value\nFROM t"

    def test_fall_loud_shape(self) -> None:
        # Ungroundable concept: NULL expression, no relation, no predicates.
        assert compose_extract_sql("NULL", None, []) == "SELECT NULL AS value"

    def test_blank_predicates_dropped(self) -> None:
        assert compose_extract_sql("SUM(x)", "t", ["", "  ", "a = 1"]).endswith("WHERE a = 1")

    def test_renders_and_executes(self) -> None:
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t (a VARCHAR, x DOUBLE)")
        conn.execute("INSERT INTO t VALUES ('k', 2.0), ('k', 3.0), ('other', 9.0)")
        sql = compose_extract_sql("SUM(x)", "t", ["a = 'k'"])
        row = conn.execute(sql).fetchone()
        assert row is not None and row[0] == 5.0


class TestCrossFactKeyAliasing:
    """DAT-809: two facts spell ONE conformed dimension with their own columns.

    Each side must group by its OWN column and project it under the shared axis
    identity — that alias is what `compose_formula_sql` merges the carriers on.
    """

    def test_projects_the_local_column_under_the_axis_alias(self) -> None:
        assert compose_extract_sql("SUM(balance)", "ap_balances", [], [("acct", "account")]) == (
            'SELECT "acct" AS "account", SUM(balance) AS value\nFROM ap_balances\nGROUP BY "acct"'
        )

    def test_groups_on_the_source_column_not_the_alias(self) -> None:
        """Aliasing a projection must never change which rows collapse together.

        If GROUP BY followed the alias, an axis alias colliding with a DIFFERENT
        column on this relation would silently regroup the extract — the parts
        would stop being a partition of the rows the scalar aggregates.
        """
        sql = compose_extract_sql("SUM(x)", "t", [], [("acct", "account_id")])
        assert sql.endswith('GROUP BY "acct"')

    def test_same_name_keys_is_the_single_relation_degenerate_case(self) -> None:
        assert compose_extract_sql(
            "SUM(x)", "t", [], same_name_keys("a", "b")
        ) == compose_extract_sql("SUM(x)", "t", [], [("a", "a"), ("b", "b")])

    def test_a_blank_alias_is_dropped_like_a_blank_column(self) -> None:
        assert compose_extract_sql("SUM(x)", "t", [], [("a", "  ")]) == (
            "SELECT SUM(x) AS value\nFROM t"
        )

    def test_quotes_both_sides_of_the_pair(self) -> None:
        sql = compose_extract_sql("SUM(x)", "t", [], [('we"ird', 'ax"is')])
        assert '"we""ird" AS "ax""is"' in sql
        assert sql.endswith('GROUP BY "we""ird"')

    def test_differently_spelled_carriers_merge_on_the_shared_alias(self) -> None:
        """The end-to-end crossing, executed: a name INTERSECTION finds nothing here.

        `gl_entries.account_id` and `ap_balances.acct` are one conformed dimension.
        The stock carrier is pinned to its reporting instant (W3's period binding
        rides in the persisted `where` parts) rather than re-aggregated to LAST —
        pinning is what makes the per-account balance well-defined.
        """
        conn = duckdb.connect(":memory:")
        conn.execute(
            "CREATE TABLE gl_entries AS SELECT * FROM (VALUES"
            " ('4000', 100.0), ('4000', 50.0), ('5000', 200.0), ('5000', 25.0),"
            " ('6000', 70.0)) t(account_id, amount)"
        )
        conn.execute(
            "CREATE TABLE ap_balances AS SELECT * FROM (VALUES"
            " ('4000', 1000.0, DATE '2024-03-31'), ('4000', 900.0, DATE '2024-02-29'),"
            " ('5000', 400.0, DATE '2024-03-31'), ('7000', 10.0, DATE '2024-03-31'))"
            " t(acct, balance, as_of)"
        )
        flow = compose_extract_sql("SUM(amount)", "gl_entries", [], [("account_id", "account")])
        stock = compose_extract_sql(
            "SUM(balance)", "ap_balances", ["\"as_of\" = DATE '2024-03-31'"], [("acct", "account")]
        )
        merged = compose_formula_sql(
            "ap / gl", {"ap", "gl"}, group_by=["account"], grouped_steps=frozenset({"ap", "gl"})
        )
        rows = conn.execute(
            f"WITH gl AS ({flow}), ap AS ({stock}) SELECT * FROM ({merged}) ORDER BY 1"
        ).fetchall()

        # 6000 is flow-only and 7000 stock-only: the FULL OUTER keeps both, and the
        # missing operand yields NULL — "not computable for this account", never 0.
        assert rows == [
            ("4000", 1000.0 / 150.0),
            ("5000", 400.0 / 225.0),
            ("6000", None),
            ("7000", None),
        ]


class TestExtractPartsDict:
    def test_general_clause_shape(self) -> None:
        """No binding ⇒ no key — a flow must never grow a half-filled period record."""
        assert extract_parts_dict("SUM(x)", "t", ["a = 1", " b = 2 "]) == {
            "select": [{"expr": "SUM(x)", "alias": "value"}],
            "from": ["t"],
            "where": ["a = 1", "b = 2"],
        }

    def test_period_binding_rides_the_parts(self) -> None:
        """DAT-887's observable: a resolved instant is PERSISTED with the clause parts.

        This is the wire the read surface un-nests into ``current_groundings
        .resolved_period``; without it the binding is applied but ungradeable.
        """
        record = {"as_of": "2025-12-01 00:00:00", "axis": "period"}
        assert extract_parts_dict("SUM(x)", "t", [], record)["period_binding"] == record

    def test_fall_loud_has_empty_from(self) -> None:
        assert extract_parts_dict("NULL", None, []) == {
            "select": [{"expr": "NULL", "alias": "value"}],
            "from": [],
            "where": [],
        }
