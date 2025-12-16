"""Relationship gathering from database.

Load and filter relationships from the database with differentiated
confidence thresholds by relationship type.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Cardinality, RelationshipType
from dataraum_context.enrichment.db_models import Relationship as RelationshipModel
from dataraum_context.enrichment.relationships.models import EnrichedRelationship
from dataraum_context.storage.models_v2 import Column, Table

# Differentiated confidence thresholds by relationship type
CONFIDENCE_THRESHOLDS = {
    "foreign_key": 0.7,  # Stricter (high reliability expected)
    "semantic": 0.6,  # Medium (LLM-based, good but not perfect)
    "correlation": 0.5,  # More permissive (TDA-detected)
    "hierarchy": 0.6,  # Medium (similar to semantic)
}


async def gather_relationships(
    table_ids: list[str],
    session: AsyncSession,
) -> list[EnrichedRelationship]:
    """Gather and filter relationships from semantic + topology enrichment.

    Strategy:
    - Query relationships table for all combinations of input tables
    - Filter by differentiated confidence thresholds (FK: 0.7, Semantic: 0.6, Correlation: 0.5)
    - Merge/dedupe relationships from multiple sources
    - Resolve conflicts (prefer higher confidence, prefer FK over correlation)

    Args:
        table_ids: List of table IDs to analyze
        session: Async database session

    Returns:
        List of enriched relationships with metadata
    """
    # Build query for relationships between any pair of input tables
    # NOTE: We filter by confidence AFTER retrieval to apply differentiated thresholds
    stmt = (
        select(RelationshipModel)
        .where(
            (RelationshipModel.from_table_id.in_(table_ids))
            & (RelationshipModel.to_table_id.in_(table_ids))
        )
        .order_by(RelationshipModel.confidence.desc())
    )

    result = await session.execute(stmt)
    db_relationships = result.scalars().all()

    # Convert to enriched format with additional metadata
    enriched = []
    seen_pairs: set[tuple[str, str]] = set()  # (from_col_id, to_col_id) to dedupe

    for db_rel in db_relationships:
        # Apply differentiated confidence threshold
        threshold = CONFIDENCE_THRESHOLDS.get(db_rel.relationship_type, 0.5)
        if db_rel.confidence < threshold:
            continue  # Below threshold for this type

        # Check BOTH directions for duplicates (semantic may store A→B, topology B→A)
        pair_forward = (db_rel.from_column_id, db_rel.to_column_id)
        pair_reverse = (db_rel.to_column_id, db_rel.from_column_id)

        if pair_forward in seen_pairs or pair_reverse in seen_pairs:
            continue  # Skip duplicate in either direction (keep highest confidence)

        # Load column metadata for join construction
        from_col = await session.get(Column, db_rel.from_column_id)
        to_col = await session.get(Column, db_rel.to_column_id)
        from_table = await session.get(Table, db_rel.from_table_id)
        to_table = await session.get(Table, db_rel.to_table_id)

        # Skip if any metadata is missing
        if from_col is None or to_col is None or from_table is None or to_table is None:
            continue

        enriched.append(
            EnrichedRelationship(
                relationship_id=db_rel.relationship_id,
                from_table=from_table.table_name,
                from_column=from_col.column_name,
                from_column_id=db_rel.from_column_id,
                from_table_id=db_rel.from_table_id,
                to_table=to_table.table_name,
                to_column=to_col.column_name,
                to_column_id=db_rel.to_column_id,
                to_table_id=db_rel.to_table_id,
                relationship_type=RelationshipType(db_rel.relationship_type),
                cardinality=Cardinality(db_rel.cardinality) if db_rel.cardinality else None,
                confidence=db_rel.confidence,
                detection_method=db_rel.detection_method,
                evidence=db_rel.evidence or {},
            )
        )

        # Mark BOTH directions as seen to prevent reverse duplicates
        seen_pairs.add(pair_forward)
        seen_pairs.add(pair_reverse)

    return enriched
