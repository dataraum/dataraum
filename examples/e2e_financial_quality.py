#!/usr/bin/env python
"""End-to-end financial quality analysis example.

This script demonstrates:
1. Loading CSV files from the booksql dataset
2. Running per-table and cross-table financial quality analysis
3. Business cycle detection and classification (requires LLM)
4. Persistence of results to metadata database

Known Limitations:
- Data is loaded as VARCHAR (no type inference) - financial checks may fail
- Relationships require enrichment pipeline to be run first
- Business cycle persistence only occurs when LLM is available

Usage:
    # Without LLM (metrics only)
    uv run python examples/e2e_financial_quality.py

    # With LLM (full analysis)
    DATARAUM_LLM_PROVIDER=anthropic uv run python examples/e2e_financial_quality.py
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dataraum_context.domains.financial import (
    analyze_complete_financial_dataset_quality,
)
from dataraum_context.staging.loaders.csv import CSVLoader
from dataraum_context.storage import (
    Base,
    BusinessCycleClassification,
    MultiTableTopologyMetrics,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def create_database() -> tuple[AsyncSession, sessionmaker]:
    """Create in-memory SQLite database with schema."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return async_session


async def load_data(
    data_dir: Path,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> list[str]:
    """Load CSV files and return table IDs."""
    loader = CSVLoader()

    result = await loader.load_directory(
        directory_path=str(data_dir),
        source_name="booksql_financial",
        duckdb_conn=duckdb_conn,
        session=session,
        file_pattern="*.csv",
    )

    if not result.success:
        raise RuntimeError(f"Failed to load data: {result.error}")

    staging_result = result.unwrap()

    logger.info(f"Loaded {len(staging_result.tables)} tables:")
    for table in staging_result.tables:
        logger.info(f"  - {table.table_name}: {table.row_count} rows")
        # WORKAROUND: Create views without raw_ prefix for financial analysis
        # The financial module uses table.table_name but DuckDB tables have raw_ prefix
        # This is a known issue that should be fixed in the financial module
        duckdb_conn.execute(
            f"CREATE VIEW {table.table_name} AS SELECT * FROM {table.raw_table_name}"
        )

    if result.warnings:
        for warning in result.warnings:
            logger.warning(warning)

    return [t.table_id for t in staging_result.tables]


def get_llm_service():
    """Get LLM service if configured."""
    provider = os.environ.get("DATARAUM_LLM_PROVIDER")
    if not provider:
        logger.info("No LLM provider configured (set DATARAUM_LLM_PROVIDER)")
        return None

    try:
        from dataraum_context.llm import LLMConfig, LLMService

        config = LLMConfig(
            provider=provider,
            model=os.environ.get("DATARAUM_LLM_MODEL", "claude-sonnet-4-20250514"),
        )
        return LLMService(config)
    except Exception as e:
        logger.warning(f"Failed to initialize LLM: {e}")
        return None


async def verify_persistence(session: AsyncSession):
    """Verify persisted data."""
    # Check MultiTableTopologyMetrics
    stmt = select(MultiTableTopologyMetrics)
    result = await session.execute(stmt)
    topology_records = list(result.scalars().all())

    logger.info(f"\nPersisted {len(topology_records)} MultiTableTopologyMetrics records")

    for record in topology_records:
        logger.info(f"  Analysis {record.analysis_id}:")
        logger.info(f"    - Tables: {len(record.table_ids)}")
        logger.info(f"    - Relationships: {record.relationship_count}")
        logger.info(f"    - Cross-table cycles: {record.cross_table_cycles}")
        logger.info(f"    - Connected graph: {record.is_connected_graph}")

    # Check BusinessCycleClassification
    stmt = select(BusinessCycleClassification)
    result = await session.execute(stmt)
    cycle_records = list(result.scalars().all())

    logger.info(f"\nPersisted {len(cycle_records)} BusinessCycleClassification records")

    for record in cycle_records:
        logger.info(f"  Cycle {record.cycle_id[:8]}...:")
        logger.info(f"    - Type: {record.cycle_type}")
        logger.info(f"    - Confidence: {record.confidence:.1%}")
        logger.info(f"    - Business value: {record.business_value}")
        logger.info(f"    - Completeness: {record.completeness}")
        logger.info(f"    - Tables: {record.table_ids}")
        if record.explanation:
            logger.info(f"    - Explanation: {record.explanation[:100]}...")


async def main():
    """Run end-to-end financial quality analysis."""
    # Locate test data
    script_dir = Path(__file__).parent
    data_dir = script_dir / "finance_csv_example"

    if not data_dir.exists():
        # Try alternative path
        data_dir = Path(__file__).parent.parent.parent / "testdata/booksql/Tables"

    if not data_dir.exists():
        logger.error("Data directory not found. Tried:")
        logger.error(f"  - {script_dir / 'finance_csv_example'}")
        logger.error(f"  - {Path(__file__).parent.parent.parent / 'testdata/booksql/Tables'}")
        return

    logger.info(f"Using data from: {data_dir}")

    # Create database
    async_session = await create_database()

    # Create DuckDB connection
    duckdb_conn = duckdb.connect(":memory:")

    try:
        async with async_session() as session:
            # Step 1: Load data
            logger.info("\n=== STEP 1: Loading CSV files ===")
            table_ids = await load_data(data_dir, duckdb_conn, session)

            if not table_ids:
                logger.error("No tables loaded")
                return

            # Step 2: Get LLM service (optional)
            logger.info("\n=== STEP 2: Initializing LLM service ===")
            llm_service = get_llm_service()

            # Step 3: Run financial quality analysis
            logger.info("\n=== STEP 3: Running financial quality analysis ===")

            result = await analyze_complete_financial_dataset_quality(
                table_ids=table_ids,
                duckdb_conn=duckdb_conn,
                session=session,
                llm_service=llm_service,
            )

            if not result.success:
                logger.error(f"Analysis failed: {result.error}")
                return

            analysis = result.unwrap()

            # Step 4: Report results
            logger.info("\n=== STEP 4: Analysis Results ===")

            logger.info("\nPer-table metrics:")
            for table_id, metrics in analysis.get("per_table_metrics", {}).items():
                logger.info(f"  {table_id[:8]}...:")
                for key, value in metrics.items():
                    logger.info(f"    {key}: {value}")

            logger.info(f"\nRelationships: {len(analysis.get('relationships', []))}")
            for rel in analysis.get("relationships", [])[:5]:
                logger.info(
                    f"  {rel['from_table']}.{rel['from_column']} -> "
                    f"{rel['to_table']}.{rel['to_column']}"
                )

            logger.info(f"\nCross-table cycles: {len(analysis.get('cross_table_cycles', []))}")
            for cycle in analysis.get("cross_table_cycles", []):
                logger.info(f"  {' -> '.join(str(t)[:8] for t in cycle)}")

            logger.info(f"\nClassified cycles: {len(analysis.get('classified_cycles', []))}")
            for classified in analysis.get("classified_cycles", []):
                logger.info(f"  Type: {classified.get('primary_type', 'UNKNOWN')}")
                logger.info(f"    Business value: {classified.get('business_value', 'unknown')}")
                logger.info(f"    Completeness: {classified.get('completeness', 'unknown')}")
                if classified.get("explanation"):
                    logger.info(f"    Explanation: {classified['explanation'][:80]}...")

            if analysis.get("interpretation"):
                interp = analysis["interpretation"]
                logger.info("\nInterpretation:")
                logger.info(f"  Quality score: {interp.get('overall_quality_score', 'N/A')}")
                logger.info(f"  Summary: {interp.get('summary', 'N/A')}")

            # Step 5: Verify persistence
            logger.info("\n=== STEP 5: Verifying Persistence ===")
            await verify_persistence(session)

            logger.info("\n=== COMPLETE ===")
            logger.info(f"LLM available: {analysis.get('llm_available', False)}")
            if analysis.get("analysis_id"):
                logger.info(f"Analysis ID: {analysis['analysis_id']}")

    finally:
        duckdb_conn.close()


if __name__ == "__main__":
    asyncio.run(main())
