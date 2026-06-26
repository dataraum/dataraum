"""Tests for enriched view SQL builder."""

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

    def test_composite_key_join_ands_every_pair(self):
        """A composite key (DAT-277) ANDs all component pairs in the ON clause."""
        joins = [
            DimensionJoin(
                dim_table_name="coa",
                dim_duckdb_path='lake.typed."csv__coa"',
                fact_fk_column="account",
                dim_pk_column="account_name",
                include_columns=["account_type"],
                relationship_id="rel-1",
                key_pairs=[("account", "account_name"), ("business_id", "business_id")],
            )
        ]

        sql, dim_cols = build_enriched_view_sql(
            view_fqn='lake.typed."enriched_csv__txn"',
            fact_fqn='lake.typed."csv__txn"',
            dimension_joins=joins,
        )

        assert 'f."account" = ' in sql
        assert ' AND ' in sql
        assert 'f."business_id" = ' in sql
        # the column prefix still uses the anchor fact column
        assert dim_cols == ["account__account_type"]

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
