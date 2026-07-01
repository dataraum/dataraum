"""Tests for enriched view SQL builder."""

import duckdb

from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql


class TestBuildEnrichedViewSql:
    """Tests for build_enriched_view_sql.

    The builder takes caller-composed fully-qualified DuckDB names (DAT-415):
    the view target and every source are FQNs, so the view is collision-free
    across sources. These tests pass the FQNs the phase composes
    (``lake.typed."enriched_<source>__<table>"``) and assert on the emitted SQL.
    """

    def test_no_joins(self):
        """View with no dimension joins is just the fact table."""
        sql, dim_cols = build_enriched_view_sql(
            view_fqn='lake.typed."enriched_csv__orders"',
            fact_fqn='lake.typed."csv__orders"',
            dimension_joins=[],
        )

        assert 'CREATE OR REPLACE VIEW lake.typed."enriched_csv__orders"' in sql
        assert 'lake.typed."csv__orders"' in sql
        assert dim_cols == []

    def test_single_dimension_join(self):
        """View with one dimension join."""
        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path='lake.typed."csv__customers"',
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name", "country"],
                relationship_id="rel-1",
            )
        ]

        sql, dim_cols = build_enriched_view_sql(
            view_fqn='lake.typed."enriched_csv__orders"',
            fact_fqn='lake.typed."csv__orders"',
            dimension_joins=joins,
        )

        assert "f.*" in sql
        assert 'AS "customer_id__name"' in sql
        assert 'AS "customer_id__country"' in sql
        assert 'LEFT JOIN lake.typed."csv__customers"' in sql
        assert 'ON f."customer_id"' in sql
        assert dim_cols == ["customer_id__name", "customer_id__country"]

    def test_multiple_dimension_joins(self):
        """View with multiple dimension joins."""
        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path='lake.typed."csv__customers"',
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name"],
                relationship_id="rel-1",
            ),
            DimensionJoin(
                dim_table_name="products",
                dim_duckdb_path='lake.typed."csv__products"',
                fact_fk_column="product_id",
                dim_pk_column="id",
                include_columns=["product_name", "category"],
                relationship_id="rel-2",
            ),
        ]

        sql, dim_cols = build_enriched_view_sql(
            view_fqn='lake.typed."enriched_csv__order_lines"',
            fact_fqn='lake.typed."csv__order_lines"',
            dimension_joins=joins,
        )

        assert sql.count("LEFT JOIN") == 2
        assert "customer_id__name" in dim_cols
        assert "product_id__product_name" in dim_cols
        assert "product_id__category" in dim_cols
        assert len(dim_cols) == 3

    def test_colliding_column_names_are_made_unique(self):
        """Two joins sharing a fact-column prefix + a same-named dim column must not
        collide — the builder disambiguates by construction (uq_table_column safety)."""
        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path='lake.typed."csv__customers"',
                fact_fk_column="org",  # same prefix on purpose
                dim_pk_column="org",
                include_columns=["city"],
            ),
            DimensionJoin(
                dim_table_name="vendors",
                dim_duckdb_path='lake.typed."csv__vendors"',
                fact_fk_column="org",  # same prefix on purpose
                dim_pk_column="org",
                include_columns=["city"],
            ),
        ]
        _sql, dim_cols = build_enriched_view_sql(
            view_fqn='lake.typed."enriched_csv__txn"',
            fact_fqn='lake.typed."csv__txn"',
            dimension_joins=joins,
        )
        assert len(dim_cols) == len(set(dim_cols))  # no duplicates
        assert dim_cols == ["org__city", "org__city_2"]

    def test_same_dim_table_joined_twice_produces_unique_column_names(self):
        """Same dimension table joined via two different FK columns gets distinct column names."""
        joins = [
            DimensionJoin(
                dim_table_name="sachkontenstamm",
                dim_duckdb_path='lake.typed."csv__sachkontenstamm"',
                fact_fk_column="kontonummer_des_gegenkontos",
                dim_pk_column="kontonummer_des_kontos",
                include_columns=["beschriftung", "zusfkt"],
                relationship_id="rel-1",
            ),
            DimensionJoin(
                dim_table_name="sachkontenstamm",
                dim_duckdb_path='lake.typed."csv__sachkontenstamm"',
                fact_fk_column="kontonummer_des_kontos",
                dim_pk_column="kontonummer_des_kontos",
                include_columns=["beschriftung", "zusfkt"],
                relationship_id="rel-2",
            ),
        ]

        sql, dim_cols = build_enriched_view_sql(
            view_fqn='lake.typed."enriched_csv__kontobuchungen"',
            fact_fqn='lake.typed."csv__kontobuchungen"',
            dimension_joins=joins,
        )

        # All four columns must be distinct — no duplicates
        assert len(dim_cols) == len(set(dim_cols)), f"Duplicate column names: {dim_cols}"
        assert "kontonummer_des_gegenkontos__beschriftung" in dim_cols
        assert "kontonummer_des_gegenkontos__zusfkt" in dim_cols
        assert "kontonummer_des_kontos__beschriftung" in dim_cols
        assert "kontonummer_des_kontos__zusfkt" in dim_cols
        # SQL aliases for the two joins must also be distinct
        assert sql.count("LEFT JOIN") == 2

    def test_reserved_word_table_alias_builds_executable_sql(self):
        """A dim table whose initials form a SQL reserved word (``accounts_source``
        → ``as``) must still build a view DuckDB can *execute* — the alias is
        quoted, so the join parses instead of failing at ``as``."""
        joins = [
            DimensionJoin(
                dim_table_name="accounts_source",  # initials -> "as", a keyword
                dim_duckdb_path="dim",
                fact_fk_column="account_id",
                dim_pk_column="id",
                include_columns=["label"],
            )
        ]
        sql, _ = build_enriched_view_sql(view_fqn="v", fact_fqn="fact", dimension_joins=joins)
        assert 'AS "as"' in sql  # the reserved-word alias is quoted

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE fact (account_id INTEGER)")
        con.execute("CREATE TABLE dim (id INTEGER, label VARCHAR)")
        con.execute(sql)  # must not raise a parser error on the bare `as`
