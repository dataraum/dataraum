#!/usr/bin/env python3
"""Run staging/profiling pipeline on all CSV subsets and export query results.

This script:
1. Finds all CSV files in examples/subsets_* directories
2. Runs run_staging_profiling.py on each CSV
3. Queries the resulting SQLite databases
4. Exports results as CSV files

Usage:
    python scripts/run_subsets_pipeline.py
"""

from __future__ import annotations

import asyncio
import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd


# SQL queries to run against each SQLite database
QUERIES = {
    "categorical_associations": """
        SELECT 
            c.column_name as col1,
            sa.semantic_role as semantic_role1,
            sa.entity_type as entity_type1,
            sa.business_name as business_name1,
            sa.business_description as business_description1,
            c2.column_name as col2,
            sa2.semantic_role as semantic_role2,
            sa2.entity_type as entity_type2,
            sa2.business_name as business_name2,
            sa2.business_description as business_description2,
            ca.p_value,
            ca.cramers_v,
            ca.chi_square,
            ca.degrees_of_freedom,
            ca.association_strength,
            ca.is_significant
        FROM categorical_associations ca
        LEFT JOIN columns c ON ca.column1_id=c.column_id
        LEFT JOIN columns c2 ON ca.column2_id=c2.column_id
        LEFT JOIN semantic_annotations sa ON ca.column1_id=sa.column_id
        LEFT JOIN semantic_annotations sa2 ON ca.column2_id=sa2.column_id
        ORDER BY p_value DESC
    """,
    "correlations": """
        SELECT 
            sa.business_name as col1,
            sa2.business_name as col2,
            ca.pearson_r,
            ca.pearson_p_value,
            ca.spearman_rho,
            ca.spearman_p_value,
            ca.correlation_strength,
            ca.is_significant
        FROM column_correlations ca
        LEFT JOIN columns c ON ca.column1_id=c.column_id
        LEFT JOIN columns c2 ON ca.column2_id=c2.column_id
        LEFT JOIN semantic_annotations sa ON ca.column1_id=sa.column_id
        LEFT JOIN semantic_annotations sa2 ON ca.column2_id=sa2.column_id
        WHERE pearson_r IS NOT NULL
    """,
    "statistical_profiles": """
        SELECT 
            sa.business_name as col,
            sp.*
        FROM statistical_profiles sp
        LEFT JOIN columns c ON sp.column_id=c.column_id
        LEFT JOIN semantic_annotations sa ON sp.column_id=sa.column_id
    """,
}


def find_subset_csvs() -> list[Path]:
    """Find all CSV files in examples/subsets_* directories."""
    examples_dir = Path("examples")
    csv_files = []
    
    for subset_dir in examples_dir.glob("subsets_Wirt*"):
        if subset_dir.is_dir():
            csv_files.extend(subset_dir.glob("*.csv"))
    
    return sorted(csv_files)


def run_pipeline(csv_path: Path, table_name: str) -> tuple[bool, Path | None]:
    """Run the staging/profiling pipeline on a CSV file.
    
    Returns:
        Tuple of (success, sqlite_path)
    """
    print(f"\n{'='*60}")
    print(f"Processing: {csv_path}")
    print(f"Table name: {table_name}")
    print(f"{'='*60}")
    
    # Run the pipeline script
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_staging_profiling.py",
            str(csv_path),
            table_name,
        ],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        print(f"❌ Pipeline failed for {csv_path}")
        print(f"   Error: {result.stderr[:500] if result.stderr else 'Unknown error'}")
        return False, None
    
    # Find the SQLite database
    sqlite_path = Path("output") / f"{table_name}_metadata.sqlite"
    if not sqlite_path.exists():
        print(f"❌ SQLite database not found: {sqlite_path}")
        return False, None
    
    print(f"✅ Pipeline completed: {sqlite_path}")
    return True, sqlite_path


def query_database(sqlite_path: Path, output_dir: Path, table_name: str) -> dict[str, int]:
    """Run queries against SQLite database and export results as CSV.
    
    Returns:
        Dict mapping query name to row count
    """
    results = {}
    
    try:
        conn = sqlite3.connect(str(sqlite_path))
        
        for query_name, query in QUERIES.items():
            try:
                df = pd.read_sql_query(query, conn)
                
                # Save to CSV
                output_file = output_dir / f"{table_name}_{query_name}.csv"
                df.to_csv(output_file, index=False)
                
                results[query_name] = len(df)
                print(f"   ✅ {query_name}: {len(df)} rows → {output_file.name}")
                
            except Exception as e:
                print(f"   ⚠️ {query_name}: Query failed - {e}")
                results[query_name] = 0
        
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Database error: {e}")
    
    return results


def main():
    """Main entry point."""
    print("🔍 Finding CSV subsets...")
    csv_files = find_subset_csvs()
    
    if not csv_files:
        print("❌ No CSV files found in examples/subsets_* directories")
        print("   Run 'python examples/create_subsets.py' first")
        return
    
    print(f"   Found {len(csv_files)} CSV files")
    
    # Create output directory for query results
    query_output_dir = Path("output/query_results")
    query_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Track results
    processed = []
    failed = []
    
    for csv_path in csv_files:
        # Generate table name from file path
        # e.g., "subsets_Herkunftskennzeichen/Herkunftskennzeichen_SV.csv" 
        # -> "herkunftskennzeichen_sv"
        table_name = csv_path.stem.lower()
        
        # Run pipeline
        success, sqlite_path = run_pipeline(csv_path, table_name)
        
        if success and sqlite_path:
            # Query and export results
            print(f"\n📊 Exporting query results...")
            query_results = query_database(sqlite_path, query_output_dir, table_name)
            processed.append({
                "csv": csv_path,
                "sqlite": sqlite_path,
                "queries": query_results,
            })
        else:
            failed.append(csv_path)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"✅ Processed: {len(processed)}")
    print(f"❌ Failed: {len(failed)}")
    
    if processed:
        print(f"\n📁 Query results saved to: {query_output_dir.absolute()}")
        print("\nFiles generated:")
        for item in processed:
            print(f"  - {item['sqlite'].stem}:")
            for query_name, count in item['queries'].items():
                print(f"      {query_name}: {count} rows")
    
    if failed:
        print("\n❌ Failed files:")
        for f in failed:
            print(f"  - {f}")


if __name__ == "__main__":
    main()
