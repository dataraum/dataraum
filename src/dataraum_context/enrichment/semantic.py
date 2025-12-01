"""Semantic enrichment using LLM analysis."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.enrichment.models import SemanticEnrichmentResult
from dataraum_context.llm import LLMService
from dataraum_context.storage.models_v2 import (
    Column,
    Table,
)
from dataraum_context.storage.models_v2 import (
    Relationship as RelationshipModel,
)
from dataraum_context.storage.models_v2 import (
    SemanticAnnotation as AnnotationModel,
)
from dataraum_context.storage.models_v2 import (
    TableEntity as EntityModel,
)


async def enrich_semantic(
    session: AsyncSession,
    llm_service: LLMService,
    table_ids: list[str],
    ontology: str = "general",
) -> Result[SemanticEnrichmentResult]:
    """Run semantic enrichment on tables.

    Steps:
    1. Call LLM service for semantic analysis
    2. Map column_refs to actual column_ids from database
    3. Store annotations in semantic_annotations table
    4. Store entity detections in table_entities table
    5. Store relationships in relationships table
    6. Return enrichment result

    Args:
        session: Database session
        llm_service: LLM service for semantic analysis
        table_ids: List of table IDs to enrich
        ontology: Ontology context for analysis

    Returns:
        Result containing semantic enrichment data
    """
    # Call LLM service
    llm_result = await llm_service.analyze_semantics(
        session=session,
        table_ids=table_ids,
        ontology=ontology,
    )

    if not llm_result.success:
        return llm_result

    enrichment = llm_result.value

    # Load column ID mappings
    column_map = await _load_column_mappings(session, table_ids)
    table_map = await _load_table_mappings(session, table_ids)

    # Store annotations
    for annotation in enrichment.annotations:
        column_id = column_map.get(
            (annotation.column_ref.table_name, annotation.column_ref.column_name)
        )
        if not column_id:
            continue  # Skip if column not found

        # Create or update semantic annotation
        db_annotation = AnnotationModel(
            column_id=column_id,
            semantic_role=annotation.semantic_role.value,
            entity_type=annotation.entity_type,
            business_name=annotation.business_name,
            business_description=annotation.business_description,
            annotation_source=annotation.annotation_source.value,
            annotated_by=annotation.annotated_by,
            confidence=annotation.confidence,
        )
        session.add(db_annotation)

    # Store entity detections
    for entity in enrichment.entity_detections:
        table_id = table_map.get(entity.table_name)
        if not table_id:
            continue

        db_entity = EntityModel(
            table_id=table_id,
            detected_entity_type=entity.entity_type,
            description=entity.description,
            confidence=entity.confidence,
            evidence=entity.evidence,
            grain_columns={"columns": entity.grain_columns},
            is_fact_table=entity.is_fact_table,
            is_dimension_table=entity.is_dimension_table,
            detection_source="llm",
        )
        session.add(db_entity)

    # Store relationships
    for rel in enrichment.relationships:
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

    return Result.ok(enrichment)


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


async def _load_table_mappings(
    session: AsyncSession,
    table_ids: list[str],
) -> dict[str, str]:
    """Load mapping of table_name -> table_id."""
    stmt = select(Table.table_name, Table.table_id).where(Table.table_id.in_(table_ids))
    result = await session.execute(stmt)

    return {table_name: table_id for table_name, table_id in result.all()}
