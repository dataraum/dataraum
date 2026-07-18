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
        assert [c.name for c in dim_cols] == ["customer_id__name", "customer_id__country"]
        # Each ref names its typed source relationally (dim table + source column) —
        # what the phase resolves to source_column_id, never parsed from the name.
        assert [(c.dim_table_name, c.source_column_name) for c in dim_cols] == [
            ("customers", "name"),
            ("customers", "country"),
        ]

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

        names = [c.name for c in dim_cols]
        assert sql.count("LEFT JOIN") == 2
        assert "customer_id__name" in names
        assert "product_id__product_name" in names
        assert "product_id__category" in names
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
        names = [c.name for c in dim_cols]
        assert len(names) == len(set(names))  # no duplicates
        assert names == ["org__city", "org__city_2"]
        # The collision the physical name cannot resolve (both are ``org__city``): the
        # forward refs map each disambiguated name to its DISTINCT source table, so the
        # phase links them correctly without reverse-parsing (the DAT-811/812 defect).
        assert [(c.name, c.dim_table_name, c.source_column_name) for c in dim_cols] == [
            ("org__city", "customers", "city"),
            ("org__city_2", "vendors", "city"),
        ]

    def test_dim_name_colliding_with_a_fact_column_is_disambiguated(self):
        """A fact column ALREADY named like a generated {fk}__{col} dim name must not
        collide with the f.* passthrough (DAT-811). Seeding the dedup with the fact's own
        columns pushes the dim column to a suffixed name, so the view never emits two
        identically-named columns (ambiguous ref + uq_table_column on registration)."""
        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path='lake.typed."csv__customers"',
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name"],
            )
        ]
        sql, dim_cols = build_enriched_view_sql(
            view_fqn='lake.typed."enriched_csv__orders"',
            fact_fqn='lake.typed."csv__orders"',
            dimension_joins=joins,
            # The fact ALREADY carries a column literally named "customer_id__name".
            fact_column_names=("order_id", "customer_id", "customer_id__name"),
        )
        # The dim column is suffixed; the f.* passthrough keeps the bare name.
        assert [c.name for c in dim_cols] == ["customer_id__name_2"]
        assert dim_cols[0].source_column_name == "name"
        assert 'AS "customer_id__name_2"' in sql

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
        names = [c.name for c in dim_cols]
        assert len(names) == len(set(names)), f"Duplicate column names: {names}"
        assert "kontonummer_des_gegenkontos__beschriftung" in names
        assert "kontonummer_des_gegenkontos__zusfkt" in names
        assert "kontonummer_des_kontos__beschriftung" in names
        assert "kontonummer_des_kontos__zusfkt" in names
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
