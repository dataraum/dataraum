"""Utility functions for relationship analysis.

Helper functions for loading and formatting relationship data.
"""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.storage import Column, Table


def load_relationship_candidates_for_semantic(
    session: Session,
    table_ids: list[str] | None = None,
    detection_method: str = "candidate",
) -> list[dict[str, Any]]:
    """Load relationship candidates from DB formatted for semantic agent.

    Groups relationships by table pair and includes all evaluation metrics
    from the evidence JSON field.

    Args:
        session: Database session
        table_ids: Optional list of table IDs to filter by. If None, loads all.
        detection_method: Filter by detection method (default: 'candidate')

    Returns:
        List of relationship candidates in the format expected by SemanticAgent:
        [
            {
                "table1": "...",
                "table2": "...",
                "topology_similarity": 0.8,
                "join_success_rate": 95.0,
                "introduces_duplicates": False,
                "join_columns": [
                    {
                        "column1": "...",
                        "column2": "...",
                        "confidence": 0.9,
                        "cardinality": "one-to-many",
                        "left_referential_integrity": 100.0,
                        "right_referential_integrity": 85.0,
                        "orphan_count": 5,
                        "cardinality_verified": True,
                    }
                ]
            }
        ]
    """
    # Build query
    stmt = select(Relationship).where(Relationship.detection_method == detection_method)

    if table_ids:
        stmt = stmt.where(
            (Relationship.from_table_id.in_(table_ids)) | (Relationship.to_table_id.in_(table_ids))
        )

    relationships = session.execute(stmt).scalars().all()

    if not relationships:
        return []

    # Load table and column metadata for names
    table_cache: dict[str, str] = {}  # table_id -> table_name
    column_cache: dict[str, str] = {}  # column_id -> column_name

    for rel in relationships:
        if rel.from_table_id not in table_cache:
            table = session.get(Table, rel.from_table_id)
            if table:
                table_cache[rel.from_table_id] = table.table_name
        if rel.to_table_id not in table_cache:
            table = session.get(Table, rel.to_table_id)
            if table:
                table_cache[rel.to_table_id] = table.table_name
        if rel.from_column_id not in column_cache:
            col = session.get(Column, rel.from_column_id)
            if col:
                column_cache[rel.from_column_id] = col.column_name
        if rel.to_column_id not in column_cache:
            col = session.get(Column, rel.to_column_id)
            if col:
                column_cache[rel.to_column_id] = col.column_name

    # Group by table pair
    # Key: (from_table_id, to_table_id) -> list of relationships
    grouped: dict[tuple[str, str], list[Relationship]] = {}
    for rel in relationships:
        key = (rel.from_table_id, rel.to_table_id)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(rel)

    # Build output format
    result = []
    for (from_table_id, to_table_id), rels in grouped.items():
        table1 = table_cache.get(from_table_id, "?")
        table2 = table_cache.get(to_table_id, "?")

        # Get relationship-level metrics from first relationship's evidence
        # (these are the same for all relationships in the group)
        first_evidence = rels[0].evidence or {}
        topology_similarity = first_evidence.get("topology_similarity", 0.0)
        join_success_rate = first_evidence.get("join_success_rate")
        introduces_duplicates = first_evidence.get("introduces_duplicates")

        # Build join columns list
        join_columns = []
        for rel in rels:
            col1 = column_cache.get(rel.from_column_id, "?")
            col2 = column_cache.get(rel.to_column_id, "?")
            evidence = rel.evidence or {}

            jc = {
                "column1": col1,
                "column2": col2,
                "confidence": rel.confidence,
                "cardinality": rel.cardinality or "unknown",
            }

            # Add evaluation metrics if present in evidence
            if "left_referential_integrity" in evidence:
                jc["left_referential_integrity"] = evidence["left_referential_integrity"]
            if "right_referential_integrity" in evidence:
                jc["right_referential_integrity"] = evidence["right_referential_integrity"]
            if "orphan_count" in evidence:
                jc["orphan_count"] = evidence["orphan_count"]
            if "cardinality_verified" in evidence:
                jc["cardinality_verified"] = evidence["cardinality_verified"]

            join_columns.append(jc)

        candidate = {
            "table1": table1,
            "table2": table2,
            "topology_similarity": topology_similarity,
            "join_columns": join_columns,
        }

        # Add optional relationship-level metrics
        if join_success_rate is not None:
            candidate["join_success_rate"] = join_success_rate
        if introduces_duplicates is not None:
            candidate["introduces_duplicates"] = introduces_duplicates

        result.append(candidate)

    return result
