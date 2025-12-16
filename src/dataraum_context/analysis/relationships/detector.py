"""Async entry point for relationship detection."""

import time
from datetime import UTC, datetime

import duckdb
import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.relationships.finder import find_relationships
from dataraum_context.analysis.relationships.models import (
    JoinCandidate,
    RelationshipCandidate,
    RelationshipDetectionResult,
)
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Table


async def detect_relationships(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_confidence: float = 0.3,
    sample_percent: float = 10.0,
) -> Result[RelationshipDetectionResult]:
    """Detect relationships between tables.

    Args:
        table_ids: List of table IDs to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        min_confidence: Minimum confidence threshold
        sample_percent: Percentage of rows to sample (uses reservoir sampling)

    Returns:
        Result containing RelationshipDetectionResult
    """
    start_time = time.time()

    try:
        # Load tables with paths and sampled data for TDA
        tables_data = await _load_tables(session, duckdb_conn, table_ids, sample_percent)

        if len(tables_data) < 2:
            return Result.ok(
                RelationshipDetectionResult(
                    candidates=[],
                    total_tables=len(tables_data),
                    computed_at=datetime.now(UTC),
                    duration_seconds=time.time() - start_time,
                )
            )

        # Find relationships using DuckDB for accurate join detection
        raw_results = find_relationships(duckdb_conn, tables_data, min_confidence)

        # Convert to typed models
        candidates = [
            RelationshipCandidate(
                table1=r["table1"],
                table2=r["table2"],
                confidence=r["confidence"],
                topology_similarity=r["topology_similarity"],
                relationship_type=r["relationship_type"],
                join_candidates=[
                    JoinCandidate(
                        column1=j["column1"],
                        column2=j["column2"],
                        confidence=j["confidence"],
                        cardinality=j["cardinality"],
                    )
                    for j in r["join_columns"]
                ],
            )
            for r in raw_results
        ]

        return Result.ok(
            RelationshipDetectionResult(
                candidates=candidates,
                total_tables=len(tables_data),
                total_candidates=len(candidates),
                high_confidence_count=sum(1 for c in candidates if c.confidence > 0.7),
                computed_at=datetime.now(UTC),
                duration_seconds=time.time() - start_time,
            )
        )

    except Exception as e:
        return Result.fail(f"Relationship detection failed: {e}")


async def _load_tables(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    sample_percent: float,
) -> dict[str, tuple[str, pd.DataFrame]]:
    """Load table data from DuckDB.

    Returns duckdb_path for accurate join detection via SQL,
    plus sampled DataFrame for TDA topology analysis.
    """
    stmt = select(Table.table_name, Table.duckdb_path).where(Table.table_id.in_(table_ids))
    result = await session.execute(stmt)

    tables_data: dict[str, tuple[str, pd.DataFrame]] = {}
    for table_name, duckdb_path in result.all():
        try:
            # Sample for TDA only (join detection uses full data via SQL)
            df = duckdb_conn.execute(
                f"SELECT * FROM {duckdb_path} USING SAMPLE {sample_percent}%"
            ).df()
            tables_data[table_name] = (duckdb_path, df)
        except Exception:
            continue

    return tables_data
