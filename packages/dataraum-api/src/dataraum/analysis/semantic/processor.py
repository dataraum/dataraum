"""Semantic enrichment processor.

Orchestrates semantic analysis using the SemanticAgent and stores results.
"""

from typing import Any

from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship as RelationshipModel
from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.db_models import (
    SemanticAnnotation as AnnotationModel,
)
from dataraum.analysis.semantic.db_models import (
    TableEntity as EntityModel,
)
from dataraum.analysis.semantic.models import SemanticEnrichmentResult
from dataraum.analysis.semantic.utils import load_column_mappings, load_table_mappings
from dataraum.core.models.base import Result


def _build_candidate_metrics_lookup(
    relationship_candidates: list[dict[str, Any]] | None,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    """Build lookup of evaluation metrics from relationship candidates.

    Returns a dict keyed by (from_table, from_column, to_table, to_column)
    containing the RI metrics for each candidate join.
    """
    if not relationship_candidates:
        return {}

    lookup: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for candidate in relationship_candidates:
        table1 = candidate.get("table1", "")
        table2 = candidate.get("table2", "")

        for jc in candidate.get("join_columns", []):
            col1 = jc.get("column1", "")
            col2 = jc.get("column2", "")

            # Extract evaluation metrics
            metrics: dict[str, Any] = {}
            if "left_referential_integrity" in jc:
                metrics["left_referential_integrity"] = jc["left_referential_integrity"]
            if "right_referential_integrity" in jc:
                metrics["right_referential_integrity"] = jc["right_referential_integrity"]
            if "orphan_count" in jc:
                metrics["orphan_count"] = jc["orphan_count"]
            if "cardinality_verified" in jc:
                metrics["cardinality_verified"] = jc["cardinality_verified"]

            # Add relationship-level metrics
            if "join_success_rate" in candidate:
                metrics["join_success_rate"] = candidate["join_success_rate"]
            if "introduces_duplicates" in candidate:
                metrics["introduces_duplicates"] = candidate["introduces_duplicates"]
            if "topology_similarity" in candidate:
                metrics["topology_similarity"] = candidate["topology_similarity"]

            if metrics:
                # Store for both directions (table1.col1 -> table2.col2 and reverse)
                lookup[(table1, col1, table2, col2)] = metrics
                lookup[(table2, col2, table1, col1)] = metrics

    return lookup


def enrich_semantic(
    session: Session,
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
    llm_result = agent.analyze(
        session=session,
        table_ids=table_ids,
        ontology=ontology,
        relationship_candidates=relationship_candidates,
    )

    if not llm_result.success:
        return Result.fail(llm_result.error or "Semantic analysis failed")

    enrichment = llm_result.unwrap()

    # Load column ID mappings
    column_map = load_column_mappings(session, table_ids)
    table_map = load_table_mappings(session, table_ids)

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
            business_concept=annotation.business_concept,
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

    # Build lookup of candidate metrics for merging into LLM relationships
    candidate_metrics = _build_candidate_metrics_lookup(relationship_candidates)

    # Store LLM-confirmed relationships (separate from Phase 6 candidates)
    for rel in enrichment.relationships:
        from_col_id = column_map.get((rel.from_table, rel.from_column))
        to_col_id = column_map.get((rel.to_table, rel.to_column))
        from_table_id = table_map.get(rel.from_table)
        to_table_id = table_map.get(rel.to_table)

        if not all([from_col_id, to_col_id, from_table_id, to_table_id]):
            continue

        # Merge candidate evaluation metrics into LLM evidence
        evidence = dict(rel.evidence) if rel.evidence else {}
        candidate_key = (rel.from_table, rel.from_column, rel.to_table, rel.to_column)
        if candidate_key in candidate_metrics:
            evidence.update(candidate_metrics[candidate_key])

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
            evidence=evidence,
        )
        session.add(db_rel)

    return Result.ok(enrichment)
