"""Tests for enriched view SQL builder."""

from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql


class TestBuildEnrichedViewSql:
    """Tests for build_enriched_view_sql."""

    def test_no_joins(self):
        """View with no dimension joins is just the fact table."""
        view_name, sql, dim_cols = build_enriched_view_sql(
            fact_table_name="orders",
            fact_duckdb_path="typed_orders",
            dimension_joins=[],
        )

        assert view_name == "enriched_orders"
        assert "typed_orders" in sql
        assert dim_cols == []
        assert "CREATE OR REPLACE VIEW" in sql

    def test_single_dimension_join(self):
        """View with one dimension join."""
        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path="typed_customers",
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name", "country"],
                relationship_id="rel-1",
            )
        ]

        view_name, sql, dim_cols = build_enriched_view_sql(
            fact_table_name="orders",
            fact_duckdb_path="typed_orders",
            dimension_joins=joins,
        )

        assert view_name == "enriched_orders"
        assert "f.*" in sql
        assert 'AS "customer_id__name"' in sql
        assert 'AS "customer_id__country"' in sql
        assert "LEFT JOIN" in sql
        assert 'ON f."customer_id"' in sql
        assert dim_cols == ["customer_id__name", "customer_id__country"]

    def test_multiple_dimension_joins(self):
        """View with multiple dimension joins."""
        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path="typed_customers",
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name"],
                relationship_id="rel-1",
            ),
            DimensionJoin(
                dim_table_name="products",
                dim_duckdb_path="typed_products",
                fact_fk_column="product_id",
                dim_pk_column="id",
                include_columns=["product_name", "category"],
                relationship_id="rel-2",
            ),
        ]

        view_name, sql, dim_cols = build_enriched_view_sql(
            fact_table_name="order_lines",
            fact_duckdb_path="typed_order_lines",
            dimension_joins=joins,
        )

        assert view_name == "enriched_order_lines"
        assert sql.count("LEFT JOIN") == 2
        assert "customer_id__name" in dim_cols
        assert "product_id__product_name" in dim_cols
        assert "product_id__category" in dim_cols
        assert len(dim_cols) == 3

    def test_same_dim_table_joined_twice_produces_unique_column_names(self):
        """Same dimension table joined via two different FK columns gets distinct column names."""
        joins = [
            DimensionJoin(
                dim_table_name="typed_sachkontenstamm",
                dim_duckdb_path="typed_sachkontenstamm",
                fact_fk_column="kontonummer_des_gegenkontos",
                dim_pk_column="kontonummer_des_kontos",
                include_columns=["beschriftung", "zusfkt"],
                relationship_id="rel-1",
            ),
            DimensionJoin(
                dim_table_name="typed_sachkontenstamm",
                dim_duckdb_path="typed_sachkontenstamm",
                fact_fk_column="kontonummer_des_kontos",
                dim_pk_column="kontonummer_des_kontos",
                include_columns=["beschriftung", "zusfkt"],
                relationship_id="rel-2",
            ),
        ]

        view_name, sql, dim_cols = build_enriched_view_sql(
            fact_table_name="kontobuchungen",
            fact_duckdb_path="typed_kontobuchungen",
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
