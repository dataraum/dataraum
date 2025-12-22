"""Semantic enrichment processor.

Orchestrates semantic analysis using the SemanticAgent and stores results.
"""

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.relationships.db_models import Relationship as RelationshipModel
from dataraum_context.analysis.semantic.agent import SemanticAgent
from dataraum_context.analysis.semantic.db_models import (
    SemanticAnnotation as AnnotationModel,
)
from dataraum_context.analysis.semantic.db_models import (
    TableEntity as EntityModel,
)
from dataraum_context.analysis.semantic.models import SemanticEnrichmentResult
from dataraum_context.analysis.semantic.utils import load_column_mappings, load_table_mappings
from dataraum_context.core.models.base import Result


async def enrich_semantic(
    session: AsyncSession,
    agent: SemanticAgent,
    table_ids: list[str],
    ontology: str = "general",
    relationship_candidates: list[dict[str, Any]] | None = None,
) -> Result[SemanticEnrichmentResult]:
    """Run semantic enrichment on tables.

    Steps:
    1. Call semantic agent for LLM analysis (with relationship candidates)
    2. Map column_refs to actual column_ids from database
    3. Store annotations in semantic_annotations table
    4. Store entity detections in table_entities table
    5. Store relationships in relationships table
    6. Return enrichment result

    Args:
        session: Database session
        agent: Semantic agent for LLM analysis
        table_ids: List of table IDs to enrich
        ontology: Ontology context for analysis
        relationship_candidates: Pre-computed relationship candidates from
            analysis/relationships module (TDA + join detection)

    Returns:
        Result containing semantic enrichment data
    """
    # Call semantic agent with relationship candidates
    llm_result = await agent.analyze(
        session=session,
        table_ids=table_ids,
        ontology=ontology,
        relationship_candidates=relationship_candidates,
    )

    if not llm_result.success:
        return Result.fail(llm_result.error or "Semantic analysis failed")

    enrichment = llm_result.unwrap()

    # Load column ID mappings
    column_map = await load_column_mappings(session, table_ids)
    table_map = await load_table_mappings(session, table_ids)

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
            business_domain=annotation.business_domain,
            ontology_term=annotation.ontology_term,
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

    # Store LLM-confirmed relationships (separate from Phase 6 candidates)
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
            cardinality=rel.cardinality,
            confidence=rel.confidence,
            detection_method="llm",  # Always 'llm' for semantic analysis
            evidence=rel.evidence,
        )
        session.add(db_rel)

    await session.commit()

    return Result.ok(enrichment)
