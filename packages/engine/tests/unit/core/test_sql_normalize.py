"""Unit tests for SQL canonicalization used by view-recipe version gating (DAT-415).

Backed by DuckDB's ``json_serialize_sql`` (DAT-654): equality is structural
(parse-tree modulo ``query_location``), so whitespace/case noise collapses while
identifiers and clause order do not.
"""

from __future__ import annotations

from dataraum.core.sql_normalize import canonical_sql, sql_equivalent


class TestSqlEquivalent:
    def test_whitespace_and_case_noise_is_equal(self) -> None:
        a = 'CREATE OR REPLACE VIEW "enriched_orders" AS SELECT * FROM "typed"."orders"'
        b = 'create or replace view "enriched_orders" as\n    select   *\n    from "typed"."orders"'
        assert sql_equivalent(a, b)

    def test_different_joined_table_is_not_equal(self) -> None:
        a = 'SELECT f.* FROM "orders" AS f LEFT JOIN "customers" AS c ON f."cid" = c."id"'
        b = 'SELECT f.* FROM "orders" AS f LEFT JOIN "suppliers" AS c ON f."cid" = c."id"'
        assert not sql_equivalent(a, b)

    def test_added_join_is_not_equal(self) -> None:
        a = 'SELECT f.* FROM "orders" AS f'
        b = 'SELECT f.* FROM "orders" AS f LEFT JOIN "customers" AS c ON f."cid" = c."id"'
        assert not sql_equivalent(a, b)

    def test_three_part_fqn_view_ddl_round_trips(self) -> None:
        # The real recipe DDL is a three-part ``catalog.schema."quoted"`` FQN — pin
        # that json_serialize_sql canonicalizes it stably (case/whitespace noise →
        # equal, a real join change → not equal), so the gate never spuriously
        # re-versions.
        a = (
            'CREATE OR REPLACE VIEW lake.typed."enriched_csv__orders" AS '
            'SELECT f.* FROM lake.typed."csv__orders" AS f '
            'LEFT JOIN lake.typed."csv__customers" AS c ON f."cid" = c."id"'
        )
        b = (
            'create or replace view lake.typed."enriched_csv__orders" as\n'
            '  select f.*\n  from lake.typed."csv__orders" as f\n'
            '  left join lake.typed."csv__customers" as c on f."cid" = c."id"'
        )
        assert sql_equivalent(a, b)
        # A different joined dimension is a genuine change → a new version.
        c = a.replace("csv__customers", "csv__suppliers")
        assert not sql_equivalent(a, c)

    def test_create_view_wrapper_is_stripped_to_the_inner_select(self) -> None:
        # json_serialize_sql serializes SELECT only; the machine-generated
        # ``CREATE … VIEW … AS`` envelope is stripped, so a view DDL and its inner
        # SELECT share one canonical key — the wrapper carries no identity.
        inner = 'SELECT f.* FROM lake.typed."orders" AS f'
        view = f'CREATE OR REPLACE VIEW lake.typed."enriched_orders" AS {inner}'
        assert canonical_sql(view) == canonical_sql(inner)

    def test_reordered_select_list_is_not_equal(self) -> None:
        # SELECT-list order is part of the view's identity (output column order);
        # canonicalization preserves it — a re-ordered projection is a real change.
        a = 'SELECT f."a", f."b" FROM "orders" AS f'
        b = 'SELECT f."b", f."a" FROM "orders" AS f'
        assert not sql_equivalent(a, b)

    def test_arbitrary_input_does_not_raise_and_is_reflexive(self) -> None:
        # Unparseable input must fall back to byte-comparison, not raise.
        garbage = "}{ not valid sql at all ;;;"
        assert canonical_sql(garbage) == garbage.strip()
        assert sql_equivalent(garbage, garbage)
        assert not sql_equivalent(garbage, garbage + " extra")
