"""Topological enrichment using TDA."""

from uuid import uuid4

import duckdb
import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import (
    Cardinality,
    JoinPath,
    JoinStep,
    Relationship,
    RelationshipType,
    Result,
    TopologyEnrichmentResult,
)
from dataraum_context.enrichment.tda import TableRelationshipFinder, TableTopologyExtractor
from dataraum_context.storage.models_v2 import Column, Table
from dataraum_context.storage.models_v2 import Relationship as RelationshipModel


async def enrich_topology(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
) -> Result[TopologyEnrichmentResult]:
    """Run topological analysis on tables using TDA.

    Steps:
    1. Load table data from DuckDB
    2. Extract topology for each table using TDA
    3. Find relationships between tables
    4. Store relationships in database
    5. Generate basic join paths

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        table_ids: List of table IDs to analyze

    Returns:
        Result containing topology enrichment data
    """
    # Load table data
    tables_data = await _load_tables_data(session, duckdb_conn, table_ids)

    if not tables_data:
        return Result.fail("No table data found")

    # Initialize TDA components
    extractor = TableTopologyExtractor()
    finder = TableRelationshipFinder()

    # Extract topology for each table
    topologies = {}
    for table_name, df in tables_data.items():
        try:
            topology = extractor.extract_topology(df)
            topologies[table_name] = topology

        except Exception as e:
            # Continue with other tables if one fails
            print(f"Warning: Failed to extract topology for {table_name}: {e}")
            continue

    # Find relationships between tables
    relationships = []

    if len(tables_data) > 1:
        try:
            tda_result = finder.find_relationships(tables_data)

            # Convert TDA relationships to our model
            # Store ALL candidate joins, not just the best one
            # This gives downstream models exploratory information
            for rel in tda_result.get("relationships", []):
                if not rel.get("join_columns"):
                    continue

                # Store ALL join candidates, not just the first one
                for join_candidate in rel["join_columns"]:
                    # Map cardinality
                    cardinality = _map_cardinality(join_candidate.get("join_type", ""))

                    # Use semantic relationship type instead of always FOREIGN_KEY
                    # This is more exploratory and less prescriptive
                    rel_type = RelationshipType.CORRELATION

                    relationship = Relationship(
                        relationship_id=str(uuid4()),
                        from_table=rel["table1"],
                        from_column=join_candidate["column1"],
                        to_table=rel["table2"],
                        to_column=join_candidate["column2"],
                        relationship_type=rel_type,
                        cardinality=cardinality,
                        confidence=join_candidate.get("confidence", 0.0),
                        detection_method="tda",
                        evidence={
                            "tda_confidence": rel.get("confidence", 0.0),
                            "join_type": join_candidate.get("join_type"),
                            "rank": rel["join_columns"].index(join_candidate),
                            "total_candidates": len(rel["join_columns"]),
                        },
                    )
                    relationships.append(relationship)

        except Exception as e:
            print(f"Warning: Failed to find relationships: {e}")

    # Store relationships in database
    await _store_relationships(session, relationships, table_ids)

    # Generate basic join paths (direct paths only for now)
    join_paths = _generate_join_paths(relationships)

    return Result.ok(
        TopologyEnrichmentResult(
            relationships=relationships,
            join_paths=join_paths,
        )
    )


async def _load_tables_data(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
) -> dict[str, pd.DataFrame]:
    """Load table data from DuckDB as pandas DataFrames."""
    # Get table names
    stmt = select(Table.table_name, Table.layer).where(Table.table_id.in_(table_ids))
    result = await session.execute(stmt)
    tables = result.all()

    # Load data from DuckDB
    tables_data = {}
    for table_name, layer in tables:
        # Use typed layer if available, otherwise raw
        actual_table = f"typed_{table_name}" if layer == "typed" else f"raw_{table_name}"

        try:
            # Limit to 10000 rows for performance
            df = duckdb_conn.execute(f"SELECT * FROM {actual_table} LIMIT 10000").df()
            tables_data[table_name] = df
        except Exception as e:
            print(f"Warning: Failed to load {actual_table}: {e}")
            continue

    return tables_data


async def _store_relationships(
    session: AsyncSession,
    relationships: list[Relationship],
    table_ids: list[str],
) -> None:
    """Store detected relationships in database."""
    # Load table and column mappings
    table_map = await _load_table_mappings(session, table_ids)
    column_map = await _load_column_mappings(session, table_ids)

    for rel in relationships:
        from_col_id = column_map.get((rel.from_table, rel.from_column))
        to_col_id = column_map.get((rel.to_table, rel.to_column))
        from_table_id = table_map.get(rel.from_table)
        to_table_id = table_map.get(rel.to_table)

        if not all([from_col_id, to_col_id, from_table_id, to_table_id]):
            continue

        db_rel = RelationshipModel(
            relationship_id=rel.relationship_id,
            from_table_id=from_table_id,
            from_column_id=from_col_id,
            to_table_id=to_table_id,
            to_column_id=to_col_id,
            relationship_type=rel.relationship_type.value,
            cardinality=rel.cardinality.value if rel.cardinality else None,
            confidence=rel.confidence,
            detection_method=rel.detection_method,
            evidence=rel.evidence,
        )
        session.add(db_rel)

    await session.commit()


async def _load_table_mappings(
    session: AsyncSession,
    table_ids: list[str],
) -> dict[str, str]:
    """Load mapping of table_name -> table_id."""
    stmt = select(Table.table_name, Table.table_id).where(Table.table_id.in_(table_ids))
    result = await session.execute(stmt)

    return dict(tuple(row) for row in result.all())


async def _load_column_mappings(
    session: AsyncSession,
    table_ids: list[str],
) -> dict[tuple[str, str], str]:
    """Load mapping of (table_name, column_name) -> column_id."""
    stmt = (
        select(Table.table_name, Column.column_name, Column.column_id)
        .join(Column)
        .where(Table.table_id.in_(table_ids))
    )
    result = await session.execute(stmt)

    return {(table_name, col_name): col_id for table_name, col_name, col_id in result.all()}


def _map_cardinality(join_type: str) -> Cardinality | None:
    """Map TDA join type to cardinality enum."""
    if "one-to-one" in join_type.lower():
        return Cardinality.ONE_TO_ONE
    elif "one-to-many" in join_type.lower():
        return Cardinality.ONE_TO_MANY
    elif "many-to-one" in join_type.lower():
        return Cardinality.ONE_TO_MANY  # Flip perspective
    elif "many-to-many" in join_type.lower():
        return Cardinality.MANY_TO_MANY
    return None


def _generate_join_paths(relationships: list[Relationship]) -> list[JoinPath]:
    """Generate direct join paths from relationships.

    For now, only generates single-step paths. Multi-step paths
    can be added later.
    """
    join_paths = []

    for rel in relationships:
        # Create a simple one-step join path
        path = JoinPath(
            from_table=rel.from_table,
            to_table=rel.to_table,
            steps=[
                JoinStep(
                    from_column=rel.from_column,
                    to_table=rel.to_table,
                    to_column=rel.to_column,
                    confidence=rel.confidence,
                )
            ],
            total_confidence=rel.confidence,
        )
        join_paths.append(path)

    return join_paths
