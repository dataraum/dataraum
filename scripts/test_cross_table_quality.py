#!/usr/bin/env python3
"""Evaluate cross-table quality analysis for confirmed relationships.

This script explores what data quality metrics we can extract when
analyzing tables joined on CONFIRMED relationships (post-semantic-agent).

Quality signals we want to detect:
1. Cross-table numeric correlations (columns in A correlated with columns in B)
2. Cross-table multicollinearity (VDP groups spanning multiple tables)
3. Redundant columns (same data stored in multiple places)
4. Derived columns across tables (A.col = f(B.col))
"""

import duckdb

from dataraum_context.analysis.correlation.cross_table import analyze_relationship_quality
from dataraum_context.analysis.correlation.models import EnrichedRelationship
from dataraum_context.core.models.base import RelationshipType


def main():
    print("=" * 70)
    print("Cross-Table Quality Analysis Evaluation")
    print("=" * 70)

    conn = duckdb.connect(":memory:")

    # Create test tables with known relationships and quality issues
    print("\n1. Creating test dataset with known quality issues...")
    print("-" * 50)

    # Table A: Orders (fact table)
    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            i AS order_id,
            (i % 100) + 1 AS customer_id,
            ROUND(RANDOM() * 1000, 2) AS amount,
            ROUND(RANDOM() * 1000 * 0.1, 2) AS tax,  -- tax = ~10% of amount (derived)
            ROUND(RANDOM() * 1000, 2) AS discount,
            DATE '2024-01-01' + INTERVAL (i % 365) DAY AS order_date
        FROM generate_series(1, 10000) AS t(i)
    """)

    # Fix the derived column: tax = amount * 0.1
    conn.execute("""
        UPDATE orders SET tax = ROUND(amount * 0.1, 2)
    """)

    # Table B: Customers (dimension table)
    conn.execute("""
        CREATE TABLE customers AS
        SELECT
            i AS customer_id,
            'Customer ' || i AS name,
            ROUND(RANDOM() * 10000, 2) AS credit_limit,
            ROUND(RANDOM() * 10000, 2) AS total_spent,
            (i % 5) + 1 AS region_id
        FROM generate_series(1, 100) AS t(i)
    """)

    # Add a redundant column: credit_limit_copy = credit_limit (quality issue)
    conn.execute("""
        ALTER TABLE customers ADD COLUMN credit_limit_copy DOUBLE
    """)
    conn.execute("""
        UPDATE customers SET credit_limit_copy = credit_limit
    """)

    # Table C: Regions (small dimension)
    conn.execute("""
        CREATE TABLE regions AS
        SELECT
            i AS region_id,
            'Region ' || i AS region_name,
            ROUND(RANDOM() * 100, 2) AS tax_rate
        FROM generate_series(1, 5) AS t(i)
    """)

    print("   Created 3 tables:")
    for table in ["orders", "customers", "regions"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        cols = conn.execute(f"SELECT * FROM {table} LIMIT 0").description
        print(f"   - {table}: {count} rows, {len(cols)} columns")

    # Simulate confirmed relationships (as EnrichedRelationship objects)
    print("\n2. Confirmed relationships (from semantic agent):")
    print("-" * 50)

    confirmed_relationships = [
        EnrichedRelationship(
            relationship_id="rel_1",
            from_table="orders",
            from_column="customer_id",
            from_column_id="col_1",
            from_table_id="tbl_1",
            to_table="customers",
            to_column="customer_id",
            to_column_id="col_2",
            to_table_id="tbl_2",
            relationship_type=RelationshipType.FOREIGN_KEY,
            confidence=0.95,
            detection_method="llm",
        ),
        EnrichedRelationship(
            relationship_id="rel_2",
            from_table="customers",
            from_column="region_id",
            from_column_id="col_3",
            from_table_id="tbl_2",
            to_table="regions",
            to_column="region_id",
            to_column_id="col_4",
            to_table_id="tbl_3",
            relationship_type=RelationshipType.FOREIGN_KEY,
            confidence=0.90,
            detection_method="llm",
        ),
    ]

    for rel in confirmed_relationships:
        print(f"   {rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}")

    # Analyze each confirmed relationship using the new API
    print("\n3. Per-relationship quality analysis (using new API):")
    print("=" * 70)

    table_paths = {
        "orders": "orders",
        "customers": "customers",
        "regions": "regions",
    }

    for rel in confirmed_relationships:
        print(f"\n### {rel.from_table} JOIN {rel.to_table} ###")
        print("-" * 50)

        result = analyze_relationship_quality(
            relationship=rel,
            duckdb_conn=conn,
            from_table_path=table_paths[rel.from_table],
            to_table_path=table_paths[rel.to_table],
        )

        if result is None:
            print("   Analysis failed or not enough data")
            continue

        print(f"   Joined rows: {result.joined_row_count}")
        print(f"   Numeric columns analyzed: {result.numeric_columns_analyzed}")

        # Cross-table correlations
        print("\n   Cross-table correlations:")
        if result.cross_table_correlations:
            for ctc in sorted(
                result.cross_table_correlations, key=lambda x: abs(x.pearson_r), reverse=True
            )[:5]:
                join_flag = " (JOIN COL)" if ctc.is_join_column else ""
                print(
                    f"      {ctc.from_table}.{ctc.from_column} <-> {ctc.to_table}.{ctc.to_column}: "
                    f"r={ctc.pearson_r:.3f} ({ctc.strength}){join_flag}"
                )
        else:
            print("      (none)")

        # Redundant columns
        print("\n   Redundant/Derived columns:")
        if result.redundant_columns:
            for rp in result.redundant_columns:
                print(f"      {rp.table}.{rp.column1} <-> {rp.column2}: r={rp.correlation:.3f}")
        else:
            print("      (none)")

        # Multicollinearity
        print(
            f"\n   Multicollinearity: CI={result.overall_condition_index:.1f} ({result.overall_severity})"
        )
        if result.dependency_groups:
            print(f"   Dependency groups: {len(result.dependency_groups)}")
            for i, group in enumerate(result.dependency_groups):
                cross_flag = " 🔗 CROSS-TABLE" if group.is_cross_table else ""
                cols = [f"{t}.{c}" for t, c in group.columns]
                print(
                    f"      Group {i + 1} (CI={group.condition_index:.1f}, {group.severity}):{cross_flag}"
                )
                print(f"         {', '.join(cols)}")

        # Quality issues
        print(f"\n   Quality issues detected: {len(result.issues)}")
        for issue in result.issues:
            print(f"      [{issue.severity.upper()}] {issue.message}")

    # Summary
    print("\n" + "=" * 70)
    print("4. Quality Issues Detected:")
    print("=" * 70)
    print("""
   Expected findings:
   1. orders.amount <-> orders.tax: r≈1.0 (derived column - tax = amount * 0.1)
   2. customers.credit_limit <-> customers.credit_limit_copy: r=1.0 (redundant)
   3. Multicollinearity groups should include the above pairs

   These are the kinds of quality signals we want to surface AFTER
   the semantic agent confirms relationships.
    """)

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
