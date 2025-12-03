#!/usr/bin/env python3
"""Test script for financial domain analysis with real accounting data.

This script:
1. Loads financial CSV data from examples/
2. Runs topological analysis
3. Applies FinancialDomainAnalyzer to classify cycles
4. Prints detailed results for validation and tuning
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dataraum_context.quality.topological import (
    analyze_topological_quality,
    analyze_topological_quality_multi_table,
)
from dataraum_context.quality.domains.financial import FinancialDomainAnalyzer
from dataraum_context.storage.models_v2 import Base
from dataraum_context.storage.models_v2.core import Table, Column, Source


async def load_financial_tables(
    conn: duckdb.DuckDBPyConnection, session: AsyncSession, data_dir: Path
) -> list[str]:
    """Load all CSV files from examples/finance_csv_example into DuckDB and metadata.

    Returns:
        List of table names loaded
    """
    csv_files = {
        "chart_of_accounts": "chart_of_account_OB.csv",
        "customers": "customer_table.csv",
        "vendors": "vendor_table.csv",
        "employees": "employee_table.csv",
        "transactions": "Master_txn_table.csv",
        "products": "product_service_table.csv",
        "payment_methods": "payment_method.csv",
    }

    loaded_tables = []

    # Create a data source record
    data_source = Source(
        source_id="finance_csv_example",
        name="Financial CSV Example Data",
        source_type="csv",
        connection_config={"path": str(data_dir)},
    )
    session.add(data_source)
    await session.flush()

    for table_name, filename in csv_files.items():
        file_path = data_dir / filename
        if not file_path.exists():
            print(f"⚠️  Skipping {filename} - file not found")
            continue

        print(f"📥 Loading {filename} into table '{table_name}'...")

        # Load CSV with DuckDB's auto-detection
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{file_path}')
        """)

        # Get row count and columns
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        columns_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()

        print(f"   ✓ Loaded {row_count:,} rows")

        # Create metadata table record
        table_record = Table(
            table_id=table_name,
            table_name=table_name,
            source_id=data_source.source_id,
            layer="raw",  # Required field
            duckdb_path=table_name,  # Path to table in DuckDB
            row_count=row_count,
        )
        session.add(table_record)
        await session.flush()

        # Create column records
        for col_info in columns_info:
            column_record = Column(
                table_id=table_name,
                column_name=col_info[1],  # name
                raw_type=col_info[2],  # type
                column_position=col_info[0],  # cid
            )
            session.add(column_record)

        loaded_tables.append(table_name)

    await session.commit()
    return loaded_tables


async def create_mock_relationships(
    session: AsyncSession,
    table_ids: list[str],
) -> None:
    """Create mock relationships for financial tables (without LLM).

    Args:
        session: SQLAlchemy async session
        table_ids: List of table IDs to analyze
    """
    from uuid import uuid4

    from sqlalchemy import select

    from dataraum_context.storage.models_v2.relationship import Relationship as RelationshipModel

    print(f"\n{'=' * 80}")
    print("🧠 Creating mock relationships for financial schema...")
    print(f"{'=' * 80}")

    # Get column mappings
    stmt = (
        select(Table.table_name, Column.column_name, Column.column_id)
        .join(Column)
        .where(Table.table_id.in_(table_ids))
    )
    result = await session.execute(stmt)
    rows = result.all()

    column_map = {(table_name, col_name): col_id for table_name, col_name, col_id in rows}

    # Debug: Show available columns for transactions
    print(f"\n🔍 Available columns in transactions table:")
    txn_cols = [
        col_name for table_name, col_name in column_map.keys() if table_name == "transactions"
    ]
    for col in sorted(txn_cols)[:10]:
        print(f"   - {col}")
    if len(txn_cols) > 10:
        print(f"   ... and {len(txn_cols) - 10} more")
    table_map_stmt = select(Table.table_name, Table.table_id).where(Table.table_id.in_(table_ids))
    table_map_result = await session.execute(table_map_stmt)
    table_map = dict(table_map_result.tuples().all())

    # Define mock relationships based on ACTUAL financial schema column names
    # Note: Using "Customer name", "Vendor name" etc. as they appear in the actual CSVs
    mock_relationships = [
        # transactions → customers (via "Customer name")
        {
            "from_table": "transactions",
            "from_column": "Customer name",
            "to_table": "customers",
            "to_column": "Customer name",
            "cardinality": "many_to_one",
            "confidence": 0.95,
            "reasoning": "Customer name in transactions references customers.Customer name",
        },
        # transactions → vendors (via "Vendor name")
        {
            "from_table": "transactions",
            "from_column": "Vendor name",
            "to_table": "vendors",
            "to_column": "Vendor name",
            "cardinality": "many_to_one",
            "confidence": 0.95,
            "reasoning": "Vendor name in transactions references vendors.Vendor name",
        },
        # transactions → products (via "Product_Service")
        {
            "from_table": "transactions",
            "from_column": "Product_Service",
            "to_table": "products",
            "to_column": "Product/ Service",
            "cardinality": "many_to_one",
            "confidence": 0.90,
            "reasoning": "Product_Service in transactions references products.Product/ Service",
        },
        # transactions → chart_of_accounts (via "Account")
        {
            "from_table": "transactions",
            "from_column": "Account",
            "to_table": "chart_of_accounts",
            "to_column": "Account Name",
            "cardinality": "many_to_one",
            "confidence": 0.85,
            "reasoning": "Account in transactions references chart_of_accounts.Account Name",
        },
        # transactions → payment_methods (via "payment method")
        {
            "from_table": "transactions",
            "from_column": "payment method",
            "to_table": "payment_methods",
            "to_column": "Payment method name",
            "cardinality": "many_to_one",
            "confidence": 0.90,
            "reasoning": "payment method in transactions references payment_methods.Payment method name",
        },
        # CIRCULAR RELATIONSHIPS FOR TESTING CYCLE DETECTION:
        # Create an AR cycle: customers → transactions (simulating that customers have back-references to their transactions)
        # In reality, this would be through an invoice table, but for demo purposes we create a direct link
        {
            "from_table": "customers",
            "from_column": "Customer name",
            "to_table": "transactions",
            "to_column": "Customer name",
            "cardinality": "one_to_many",
            "confidence": 0.80,
            "reasoning": "Mock circular relationship to create AR cycle for testing (customers → transactions)",
        },
        # Create an AP cycle: vendors → transactions (simulating vendor-transaction linkage)
        {
            "from_table": "vendors",
            "from_column": "Vendor name",
            "to_table": "transactions",
            "to_column": "Vendor name",
            "cardinality": "one_to_many",
            "confidence": 0.80,
            "reasoning": "Mock circular relationship to create AP cycle for testing (vendors → transactions)",
        },
    ]

    created_count = 0
    print(f"\n📎 Creating {len(mock_relationships)} mock relationships:")

    for rel_data in mock_relationships:
        from_col_id = column_map.get((rel_data["from_table"], rel_data["from_column"]))
        to_col_id = column_map.get((rel_data["to_table"], rel_data["to_column"]))
        from_table_id = table_map.get(rel_data["from_table"])
        to_table_id = table_map.get(rel_data["to_table"])

        if not all([from_col_id, to_col_id, from_table_id, to_table_id]):
            print(
                f"   ⚠️  Skipping {rel_data['from_table']}.{rel_data['from_column']} "
                f"→ {rel_data['to_table']}.{rel_data['to_column']} (columns not found)"
            )
            continue

        db_rel = RelationshipModel(
            relationship_id=str(uuid4()),
            from_table_id=from_table_id,
            from_column_id=from_col_id,
            to_table_id=to_table_id,
            to_column_id=to_col_id,
            relationship_type="foreign_key",
            cardinality=rel_data["cardinality"],
            confidence=rel_data["confidence"],
            detection_method="mock",
            evidence={"source": "test_script", "reasoning": rel_data["reasoning"]},
        )
        session.add(db_rel)
        created_count += 1

        print(
            f"   ✓ {rel_data['from_table']}.{rel_data['from_column']} "
            f"→ {rel_data['to_table']}.{rel_data['to_column']} "
            f"({rel_data['cardinality']}, {rel_data['confidence']:.2f})"
        )

    await session.commit()
    print(f"\n✓ Created {created_count} mock relationships!")


async def analyze_table_topology(
    conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    table_name: str,
    use_domain_analyzer: bool = True,
):
    """Run topological analysis on a single table.

    Args:
        conn: DuckDB connection
        session: SQLAlchemy async session
        table_name: Name of table to analyze
        use_domain_analyzer: Whether to use FinancialDomainAnalyzer

    Returns:
        Result object containing TopologicalQualityResult
    """
    print(f"\n{'=' * 80}")
    print(f"🔍 Analyzing table: {table_name}")
    print(f"{'=' * 80}")

    # Get column info
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    print(f"\n📊 Table has {len(columns)} columns:")
    for col in columns[:5]:  # Show first 5
        print(f"   - {col[1]} ({col[2]})")
    if len(columns) > 5:
        print(f"   ... and {len(columns) - 5} more")

    # Run topological analysis
    domain_analyzer = FinancialDomainAnalyzer() if use_domain_analyzer else None

    result = await analyze_topological_quality(
        table_id=table_name,
        duckdb_conn=conn,
        session=session,
        domain_analyzer=domain_analyzer,
        temporal_context=None,  # Could add temporal analysis later
    )

    return result


def print_analysis_results(result, table_name: str):
    """Print detailed analysis results from Result object."""
    print(f"\n{'=' * 80}")
    print(f"📈 RESULTS for {table_name}")
    print(f"{'=' * 80}")

    # Check if result is success
    if not result.success:
        print(f"\n❌ Analysis failed: {result.error}")
        if result.warnings:
            print(f"⚠️  Warnings:")
            for warning in result.warnings:
                print(f"   - {warning}")
        return

    # Get the actual result data
    data = result.value  # Changed from result.data

    # Overall quality
    quality_score = data.quality_score if hasattr(data, "quality_score") else 0.0
    print(f"\n🎯 Overall Quality Score: {quality_score:.2%}")

    # Topological features (Betti numbers)
    if hasattr(data, "betti_numbers"):
        betti = data.betti_numbers
        print(f"\n🔢 Topological Features:")
        print(f"   Betti-0 (components): {betti.betti_0}")
        print(f"   Betti-1 (cycles): {betti.betti_1}")
        print(f"   Betti-2 (voids): {betti.betti_2}")
        print(f"   Total complexity: {betti.total_complexity}")

    # Historical complexity stats
    if hasattr(data, "complexity_mean") and data.complexity_mean is not None:
        print(f"\n📊 Historical Complexity Analysis:")
        print(f"   Baseline (30-day mean): {data.complexity_mean:.1f}")
        print(f"   Std deviation: {data.complexity_std:.1f}")
        print(f"   Z-score: {data.complexity_z_score:.2f}")

        # Interpretation
        if abs(data.complexity_z_score) > 2.0:
            print(
                f"   ⚠️  ANOMALY: Complexity is {abs(data.complexity_z_score):.1f} std deviations from baseline"
            )
        elif abs(data.complexity_z_score) > 1.5:
            print(f"   ⚠️  WARNING: Complexity slightly elevated")
        else:
            print(f"   ✓ Complexity within normal range")

    # Cycle detection
    cycles = data.cycles if hasattr(data, "cycles") else []
    print(f"\n🔄 Cycle Detection:")
    print(f"   Total cycles: {len(cycles)}")

    if cycles:
        # Group by cycle type
        cycle_types = {}
        for cycle in cycles:
            cycle_type = cycle.cycle_type if hasattr(cycle, "cycle_type") else None
            cycle_types[cycle_type or "unclassified"] = (
                cycle_types.get(cycle_type or "unclassified", 0) + 1
            )

        print(f"\n   Cycle Types:")
        for cycle_type, count in sorted(cycle_types.items(), key=lambda x: x[1], reverse=True):
            print(f"      {cycle_type}: {count}")

        # Show details of first few classified cycles
        classified = [c for c in cycles if hasattr(c, "cycle_type") and c.cycle_type]
        if classified:
            print(f"\n   📋 Sample Classified Cycles:")
            for cycle in classified[:3]:
                print(f"\n      Type: {cycle.cycle_type}")
                print(f"      Columns: {', '.join(cycle.involved_columns[:5])}")
                if len(cycle.involved_columns) > 5:
                    print(f"               ... and {len(cycle.involved_columns) - 5} more")
                print(f"      Persistence: {cycle.persistence:.3f}")

    # Anomalies
    anomalies = data.anomalies if hasattr(data, "anomalies") else []
    print(f"\n⚠️  Anomalies Detected: {len(anomalies)}")
    if anomalies:
        for anomaly in anomalies:
            print(f"\n   - {anomaly.anomaly_type}")
            print(f"     {anomaly.description}")
            if hasattr(anomaly, "severity"):
                print(f"     Severity: {anomaly.severity}")

    # Domain-specific results (if available)
    domain_analysis = data.domain_analysis if hasattr(data, "domain_analysis") else None
    if domain_analysis:
        print(f"\n💼 Financial Domain Analysis:")

        classified_cycles = domain_analysis.get("classified_cycles", [])
        classification_rate = (
            len([c for c in classified_cycles if c.cycle_type]) / len(classified_cycles)
            if classified_cycles
            else 0
        )
        print(f"   Classification rate: {classification_rate:.1%}")

        financial_anomalies = domain_analysis.get("financial_anomalies", [])
        print(f"   Financial-specific anomalies: {len(financial_anomalies)}")


async def main():
    """Main test script."""
    print("🧪 Financial Domain Analysis Test")
    print("=" * 80)

    # Find data directory
    data_dir = Path(__file__).parent.parent / "examples" / "finance_csv_example"
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return

    print(f"📁 Data directory: {data_dir}")

    # Create in-memory SQLite database for metadata
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create session factory
    async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Create DuckDB connection
    duckdb_conn = duckdb.connect(":memory:")

    try:
        # Load all tables
        print(f"\n{'=' * 80}")
        print("📥 Loading financial data...")
        print(f"{'=' * 80}")

        async with async_session_factory() as session:
            tables = await load_financial_tables(duckdb_conn, session, data_dir)
        print(f"\n✓ Loaded {len(tables)} tables: {', '.join(tables)}")

        # Create mock relationships (no LLM needed for testing)
        print(f"\n{'=' * 80}")
        print("🔍 Step 1: Relationship Detection (Mock)")
        print(f"{'=' * 80}")
        async with async_session_factory() as session:
            await create_mock_relationships(session, tables)

        # Analyze key tables
        key_tables = ["transactions", "customers", "vendors"]
        available_tables = [t for t in key_tables if t in tables]

        if not available_tables:
            print("\n⚠️  No key tables available for analysis")
            return

        print(f"\n{'=' * 80}")
        print("🔍 Step 2: Topological Analysis")
        print(f"{'=' * 80}")

        results = {}
        for table in available_tables:
            async with async_session_factory() as session:
                result = await analyze_table_topology(
                    duckdb_conn, session, table, use_domain_analyzer=True
                )
                results[table] = result
                print_analysis_results(result, table)

        # Summary comparison
        print(f"\n{'=' * 80}")
        print("📊 SUMMARY COMPARISON (Per-Table)")
        print(f"{'=' * 80}")

        print(f"\n{'Table':<20} {'Quality Score':<15} {'Cycles':<10} {'Anomalies':<10}")
        print("-" * 55)
        for table, result in results.items():
            if result.success:
                data = result.value
                quality = data.quality_score if hasattr(data, "quality_score") else 0.0
                cycles = len(data.cycles) if hasattr(data, "cycles") else 0
                anomalies = len(data.anomalies) if hasattr(data, "anomalies") else 0
                print(f"{table:<20} {quality:>14.1%} {cycles:>9} {anomalies:>9}")
            else:
                print(f"{table:<20} {'ERROR':<15} {'-':<10} {'-':<10}")

        # NEW: Multi-table analysis with relationship graph
        print(f"\n{'=' * 80}")
        print("🔍 Step 3: Multi-Table Relationship Analysis")
        print(f"{'=' * 80}")

        async with async_session_factory() as session:
            multi_result = await analyze_topological_quality_multi_table(
                table_ids=available_tables,
                duckdb_conn=duckdb_conn,
                session=session,
                domain_analyzer=FinancialDomainAnalyzer(),
            )

            if multi_result.success:
                multi_data = multi_result.value
                cross_table = multi_data["cross_table"]

                print(f"\n🌐 Cross-Table Topology:")
                print(f"   Relationships found: {multi_data['relationship_count']}")
                print(f"   Graph Betti-0 (connected components): {cross_table['betti_0']}")
                print(f"   Cross-table cycles detected: {cross_table['cycle_count']}")

                # If we have cross-table cycles, display them
                if cross_table["cycles"]:
                    print(f"\n🔄 Cross-Table Cycles (Raw):")
                    for i, cycle in enumerate(cross_table["cycles"], 1):
                        cycle_path = " → ".join(cycle) + f" → {cycle[0]}"
                        print(f"   {i}. {cycle_path}")

                # Display domain analysis with integrated cross-table classification
                if multi_data.get("domain_analysis"):
                    domain = multi_data["domain_analysis"]
                    print(f"\n📊 Domain Analysis Summary:")
                    print(f"   Single-table cycles: {len(domain['single_table_cycles'])}")
                    print(f"   Cross-table cycles: {len(domain['cross_table_cycles'])}")

                    # Display cross-table cycle classification if available
                    cross_table_classification = domain.get("cross_table_classification")
                    if cross_table_classification:
                        print(f"\n💼 Business Process Classification:")
                        print(
                            f"   Classification rate: {cross_table_classification['classification_rate']:.1%}"
                        )
                        print(f"   {cross_table_classification['quality_assessment']}")

                        if cross_table_classification["business_processes"]:
                            print(f"\n📋 Detected Business Processes:")
                            for bp in cross_table_classification["business_processes"]:
                                print(
                                    f"   • {bp['process_name']} (confidence: {bp['confidence']:.1%})"
                                )
                                print(
                                    f"     Tables: {' → '.join(bp['tables_involved'])} → {bp['tables_involved'][0]}"
                                )

                        if cross_table_classification["classified_cycles"]:
                            print(f"\n🏷️  Classified Cycles:")
                            for cycle_data in cross_table_classification["classified_cycles"]:
                                cycle_type = cycle_data["cycle_type"] or "unclassified"
                                table_path = (
                                    " → ".join(cycle_data["table_names"])
                                    + f" → {cycle_data['table_names'][0]}"
                                )
                                confidence = cycle_data["match_confidence"]
                                print(f"   • {cycle_type} (confidence: {confidence:.1%})")
                                print(f"     Path: {table_path}")
            else:
                print(f"\n❌ Multi-table analysis failed: {multi_result.error}")

        print(f"\n✅ Analysis complete!")

    finally:
        duckdb_conn.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
