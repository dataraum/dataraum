"""Entropy context builder for graph execution integration.

This module provides the bridge between:
- Database (loads stored entropy from entropy_objects table)
- GraphExecutionContext (consumes entropy data)

Usage:
    from dataraum.entropy.context import build_entropy_context

    entropy_ctx = build_entropy_context(session, table_ids)
    # Use entropy_ctx.column_profiles, entropy_ctx.table_profiles, etc.

    # With LLM interpretation:
    entropy_ctx = build_entropy_context(
        session, table_ids,
        interpreter=interpreter,  # EntropyInterpreter instance
    )

Note: Entropy must be computed by the pipeline first (entropy phase).
This module only loads pre-computed entropy from the database.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.interpretation import (
    InterpretationInput,
)
from dataraum.entropy.models import (
    ColumnEntropyProfile,
    EntropyContext,
    RelationshipEntropyProfile,
    TableEntropyProfile,
)

if TYPE_CHECKING:
    from dataraum.entropy.interpretation import EntropyInterpreter

logger = get_logger(__name__)


def build_entropy_context(
    session: Session,
    table_ids: list[str],
    *,
    interpreter: EntropyInterpreter | None = None,
) -> EntropyContext:
    """Build entropy context for the given tables.

    Loads entropy data from the database (entropy_objects table).
    Raises an error if no entropy data exists - run the entropy phase first.

    Args:
        session: SQLAlchemy session
        table_ids: List of table IDs to process
        interpreter: Optional EntropyInterpreter for LLM-powered interpretation.
            If provided, generates interpretations for each column.

    Returns:
        EntropyContext with column, table, and relationship entropy profiles

    Raises:
        ValueError: If no entropy data exists for the given tables.
    """
    if not table_ids:
        return EntropyContext()

    # Load from database - this is the only source of truth
    entropy_context = _load_entropy_from_db(session, table_ids)
    if entropy_context is None:
        raise ValueError(
            "No entropy data found in database. Run the entropy pipeline phase first: "
            "`dataraum run <source> --phase entropy`"
        )

    logger.debug("Loaded entropy context from database")

    # Load analysis data for relationship entropy and interpretations
    analysis_data = _load_analysis_data(session, table_ids)

    # Compute relationship entropy (fast operation using stored relationships)
    relationship_profiles = _compute_relationship_entropy(session, table_ids, analysis_data)
    entropy_context.relationship_profiles = relationship_profiles

    # Load interpretations if interpreter provided
    if interpreter is not None:
        _build_interpretations(
            session=session,
            entropy_context=entropy_context,
            analysis_data=analysis_data,
            interpreter=interpreter,
        )

    return entropy_context


def _load_entropy_from_db(
    session: Session,
    table_ids: list[str],
) -> EntropyContext | None:
    """Load entropy context from database if data exists.

    Args:
        session: SQLAlchemy session
        table_ids: List of table IDs

    Returns:
        EntropyContext if data exists, None otherwise
    """
    from collections import defaultdict

    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.storage import Column, Table

    # Check if any entropy objects exist for these tables
    count_stmt = (
        select(EntropyObjectRecord).where(EntropyObjectRecord.table_id.in_(table_ids)).limit(1)
    )
    if session.execute(count_stmt).scalar_one_or_none() is None:
        return None

    # Load all entropy objects for these tables
    stmt = select(EntropyObjectRecord).where(EntropyObjectRecord.table_id.in_(table_ids))
    entropy_records = session.execute(stmt).scalars().all()

    # Load table and column info for building keys
    tables = session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars().all()
    table_map = {t.table_id: t for t in tables}

    columns = session.execute(select(Column).where(Column.table_id.in_(table_ids))).scalars().all()
    column_map = {c.column_id: c for c in columns}

    # Group entropy records by column
    records_by_column: dict[str, list[EntropyObjectRecord]] = defaultdict(list)
    for record in entropy_records:
        if record.column_id:
            records_by_column[record.column_id].append(record)

    # Build column profiles
    column_profiles: dict[str, ColumnEntropyProfile] = {}

    for column_id, records in records_by_column.items():
        col = column_map.get(column_id)
        if not col:
            continue
        table = table_map.get(col.table_id)
        if not table:
            continue

        key = f"{table.table_name}.{col.column_name}"

        # Build dimension scores from records
        dimension_scores: dict[str, float] = {}
        for record in records:
            dim_path = f"{record.layer}.{record.dimension}.{record.sub_dimension}"
            dimension_scores[dim_path] = record.score

        # Calculate layer-level scores
        structural_scores = [s for p, s in dimension_scores.items() if p.startswith("structural.")]
        semantic_scores = [s for p, s in dimension_scores.items() if p.startswith("semantic.")]
        value_scores = [s for p, s in dimension_scores.items() if p.startswith("value.")]
        computational_scores = [
            s for p, s in dimension_scores.items() if p.startswith("computational.")
        ]

        profile = ColumnEntropyProfile(
            table_name=table.table_name,
            column_name=col.column_name,
            column_id=column_id,
            dimension_scores=dimension_scores,
            structural_entropy=sum(structural_scores) / len(structural_scores)
            if structural_scores
            else 0.0,
            semantic_entropy=sum(semantic_scores) / len(semantic_scores)
            if semantic_scores
            else 0.0,
            value_entropy=sum(value_scores) / len(value_scores) if value_scores else 0.0,
            computational_entropy=sum(computational_scores) / len(computational_scores)
            if computational_scores
            else 0.0,
        )
        profile.calculate_composite()
        column_profiles[key] = profile

    # Build table profiles
    table_profiles: dict[str, TableEntropyProfile] = {}
    columns_by_table: dict[str, list[ColumnEntropyProfile]] = defaultdict(list)

    for key, profile in column_profiles.items():
        columns_by_table[profile.table_name].append(profile)

    for table_name, col_profiles in columns_by_table.items():
        table_profile = TableEntropyProfile(
            table_name=table_name,
            column_profiles=col_profiles,
        )
        table_profile.calculate_aggregates()
        table_profiles[table_name] = table_profile

    # Build context
    context = EntropyContext(
        column_profiles=column_profiles,
        table_profiles=table_profiles,
    )
    context.update_summary_stats()

    return context


def _load_analysis_data(
    session: Session,
    table_ids: list[str],
) -> dict[str, Any]:
    """Load all analysis data needed for entropy detection.

    Args:
        session: SQLAlchemy session
        table_ids: List of table IDs

    Returns:
        Dict mapping table_id to analysis data structure
    """
    # Lazy imports to avoid circular dependencies
    from dataraum.analysis.correlation.db_models import DerivedColumn
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.analysis.statistics.db_models import (
        StatisticalProfile,
        StatisticalQualityMetrics,
    )
    from dataraum.analysis.typing.db_models import TypeCandidate
    from dataraum.storage import Column, Table

    # Load tables
    tables_stmt = select(Table).where(Table.table_id.in_(table_ids))
    tables = session.execute(tables_stmt).scalars().all()
    table_map = {t.table_id: t for t in tables}

    # Load columns
    columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns = session.execute(columns_stmt).scalars().all()
    columns_by_table: dict[str, list[Column]] = {}
    for col in columns:
        if col.table_id not in columns_by_table:
            columns_by_table[col.table_id] = []
        columns_by_table[col.table_id].append(col)

    column_ids = [col.column_id for col in columns]

    # Load statistical profiles
    stat_profiles: dict[str, StatisticalProfile] = {}
    if column_ids:
        stat_stmt = select(StatisticalProfile).where(StatisticalProfile.column_id.in_(column_ids))
        for profile in session.execute(stat_stmt).scalars().all():
            stat_profiles[profile.column_id] = profile

    # Load statistical quality metrics
    stat_quality: dict[str, StatisticalQualityMetrics] = {}
    if column_ids:
        qual_stmt = select(StatisticalQualityMetrics).where(
            StatisticalQualityMetrics.column_id.in_(column_ids)
        )
        for metrics in session.execute(qual_stmt).scalars().all():
            stat_quality[metrics.column_id] = metrics

    # Load semantic annotations
    semantic: dict[str, SemanticAnnotation] = {}
    if column_ids:
        sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids))
        for ann in session.execute(sem_stmt).scalars().all():
            semantic[ann.column_id] = ann

    # Load type candidates (best by confidence)
    type_candidates: dict[str, TypeCandidate] = {}
    if column_ids:
        cand_stmt = select(TypeCandidate).where(TypeCandidate.column_id.in_(column_ids))
        for cand in session.execute(cand_stmt).scalars().all():
            if cand.column_id not in type_candidates:
                type_candidates[cand.column_id] = cand
            elif cand.confidence > type_candidates[cand.column_id].confidence:
                type_candidates[cand.column_id] = cand

    # Load derived columns
    derived_columns: dict[str, DerivedColumn] = {}
    if column_ids:
        derived_stmt = select(DerivedColumn).where(DerivedColumn.derived_column_id.in_(column_ids))
        for derived in session.execute(derived_stmt).scalars().all():
            derived_columns[derived.derived_column_id] = derived

    # Load LLM-confirmed relationships (not raw candidates) for join path entropy
    rel_stmt = select(Relationship).where(
        ((Relationship.from_table_id.in_(table_ids)) | (Relationship.to_table_id.in_(table_ids)))
        & (Relationship.detection_method == "llm")
    )
    relationships = session.execute(rel_stmt).scalars().all()

    # Build table_id -> table_name mapping
    table_names_map: dict[str, str] = {t.table_id: t.table_name for t in tables}

    # Build relationships by column with full context for join path determinism
    relationships_by_column: dict[str, list[dict[str, Any]]] = {}
    for rel in relationships:
        from_table = table_names_map.get(rel.from_table_id, "unknown")
        to_table = table_names_map.get(rel.to_table_id, "unknown")

        rel_dict: dict[str, Any] = {
            "relationship_type": rel.relationship_type,
            "confidence": rel.confidence,
            "detection_method": rel.detection_method,
            "from_table": from_table,
            "to_table": to_table,
            "cardinality": rel.cardinality,
        }

        if rel.from_column_id not in relationships_by_column:
            relationships_by_column[rel.from_column_id] = []
        relationships_by_column[rel.from_column_id].append(rel_dict)
        if rel.to_column_id not in relationships_by_column:
            relationships_by_column[rel.to_column_id] = []
        relationships_by_column[rel.to_column_id].append(rel_dict)

    # Build result structure
    result: dict[str, Any] = {}

    for table_id in table_ids:
        table = table_map.get(table_id)
        if not table:
            continue

        table_columns = columns_by_table.get(table_id, [])
        column_data: list[dict[str, Any]] = []

        for col in table_columns:
            # Build analysis_results dict for this column
            analysis_results: dict[str, Any] = {}

            # Typing analysis
            type_cand = type_candidates.get(col.column_id)
            if type_cand:
                analysis_results["typing"] = {
                    "parse_success_rate": type_cand.parse_success_rate,
                    "confidence": type_cand.confidence,
                    "data_type": type_cand.data_type,
                    "detected_unit": type_cand.detected_unit,
                    "pattern_match_rate": type_cand.pattern_match_rate,
                }

            # Statistics analysis
            stat_prof = stat_profiles.get(col.column_id)
            quality = stat_quality.get(col.column_id)
            if stat_prof or quality:
                stats_data: dict[str, Any] = {}
                if stat_prof:
                    stats_data["null_ratio"] = stat_prof.null_ratio
                    stats_data["cardinality_ratio"] = stat_prof.cardinality_ratio
                if quality:
                    stats_data["iqr_outlier_ratio"] = quality.iqr_outlier_ratio
                    stats_data["isolation_forest_anomaly_ratio"] = (
                        quality.isolation_forest_anomaly_ratio
                    )
                analysis_results["statistics"] = stats_data

            # Semantic analysis
            sem_ann = semantic.get(col.column_id)
            if sem_ann:
                analysis_results["semantic"] = {
                    "business_description": sem_ann.business_description,
                    "business_name": sem_ann.business_name,
                    "entity_type": sem_ann.entity_type,
                    "semantic_role": sem_ann.semantic_role,
                    "confidence": sem_ann.confidence,
                }

            # Correlation analysis (derived columns)
            derived_col = derived_columns.get(col.column_id)
            if derived_col:
                analysis_results["correlation"] = {
                    "derived_columns": [
                        {
                            "derived_column_name": col.column_name,
                            "formula": derived_col.formula,
                            "match_rate": derived_col.match_rate,
                            "derivation_type": derived_col.derivation_type,
                            "source_column_ids": derived_col.source_column_ids,
                        }
                    ]
                }

            # Relationships (already formatted with table names for join path entropy)
            if col.column_id in relationships_by_column:
                analysis_results["relationships"] = relationships_by_column[col.column_id]

            column_data.append(
                {
                    "name": col.column_name,
                    "column_id": col.column_id,
                    "analysis_results": analysis_results,
                }
            )

        result[table_id] = {
            "table_name": table.table_name,
            "columns": column_data,
        }

    return result


def _compute_relationship_entropy(
    session: Session,
    table_ids: list[str],
    analysis_data: dict[str, Any],
) -> dict[str, RelationshipEntropyProfile]:
    """Compute entropy for relationships between tables.

    Args:
        session: SQLAlchemy session
        table_ids: List of table IDs
        analysis_data: Pre-loaded analysis data

    Returns:
        Dict mapping relationship keys to entropy profiles
    """
    from dataraum.analysis.relationships.db_models import Relationship

    # Load relationships
    rel_stmt = select(Relationship).where(
        (Relationship.from_table_id.in_(table_ids)) & (Relationship.to_table_id.in_(table_ids))
    )
    relationships = session.execute(rel_stmt).scalars().all()

    # Get table names
    table_names: dict[str, str] = {}
    for table_id, data in analysis_data.items():
        table_names[table_id] = data["table_name"]

    # Build relationship profiles
    profiles: dict[str, RelationshipEntropyProfile] = {}

    for rel in relationships:
        from_table = table_names.get(rel.from_table_id, rel.from_table_id)
        to_table = table_names.get(rel.to_table_id, rel.to_table_id)

        # Get column names (would need to load from Column table)
        # For now use placeholder based on relationship
        from_column = rel.from_column_id[-8:] if rel.from_column_id else "unknown"
        to_column = rel.to_column_id[-8:] if rel.to_column_id else "unknown"

        profile = RelationshipEntropyProfile(
            from_table=from_table,
            from_column=from_column,
            to_table=to_table,
            to_column=to_column,
        )

        # Compute entropy based on relationship metadata
        # Cardinality entropy based on confidence
        profile.cardinality_entropy = 1.0 - (rel.confidence or 0.5)

        # Join path entropy - high if detection method is weak
        if rel.detection_method == "llm":
            profile.join_path_entropy = 0.2  # LLM-detected is reasonably reliable
        elif rel.detection_method == "exact_match":
            profile.join_path_entropy = 0.1
        else:
            profile.join_path_entropy = 0.5  # Statistical methods less certain

        # Referential integrity - would need to check for orphans
        # For now use confidence as proxy
        profile.referential_integrity_entropy = max(0.0, 0.5 - (rel.confidence or 0.5))

        # Semantic clarity - based on whether relationship type is known
        if rel.relationship_type and rel.relationship_type != "unknown":
            profile.semantic_clarity_entropy = 0.2
        else:
            profile.semantic_clarity_entropy = 0.7

        profile.calculate_composite()

        # Add warning if not deterministic
        if not profile.is_deterministic:
            profile.join_warning = (
                f"Join between {from_table} and {to_table} has "
                f"entropy {profile.composite_score:.2f} - verify join conditions"
            )

        profiles[profile.relationship_key] = profile

    return profiles


def get_column_entropy_summary(
    profile: ColumnEntropyProfile,
) -> dict[str, Any]:
    """Get a summary of column entropy suitable for graph context.

    Args:
        profile: Column entropy profile

    Returns:
        Dict with key entropy fields for context
    """
    summary: dict[str, Any] = {
        "composite_score": profile.composite_score,
        "readiness": profile.readiness,
        "structural_entropy": profile.structural_entropy,
        "semantic_entropy": profile.semantic_entropy,
        "value_entropy": profile.value_entropy,
        "computational_entropy": profile.computational_entropy,
        "high_entropy_dimensions": profile.high_entropy_dimensions,
        "resolution_hints": [
            {
                "action": hint.action,
                "description": hint.description,
                "expected_reduction": hint.expected_entropy_reduction,
                "effort": hint.effort,
            }
            for hint in profile.top_resolution_hints[:3]
        ],
    }

    # Include LLM interpretation if available
    if profile.interpretation is not None:
        summary["interpretation"] = {
            "explanation": profile.interpretation.explanation,
            "assumptions": [
                {
                    "dimension": a.dimension,
                    "assumption_text": a.assumption_text,
                    "confidence": a.confidence,
                    "impact": a.impact,
                    "basis": a.basis,
                }
                for a in profile.interpretation.assumptions
            ],
            "resolution_actions": [
                {
                    "action": r.action,
                    "description": r.description,
                    "priority": r.priority,
                    "effort": r.effort,
                    "expected_impact": r.expected_impact,
                }
                for r in profile.interpretation.resolution_actions
            ],
        }

    return summary


def get_table_entropy_summary(
    profile: TableEntropyProfile,
) -> dict[str, Any]:
    """Get a summary of table entropy suitable for graph context.

    Args:
        profile: Table entropy profile

    Returns:
        Dict with key entropy fields for context
    """
    return {
        "avg_composite_score": profile.avg_composite_score,
        "max_composite_score": profile.max_composite_score,
        "readiness": profile.readiness,
        "high_entropy_columns": profile.high_entropy_columns,
        "blocked_columns": profile.blocked_columns,
        "compound_risk_count": len(profile.compound_risks),
    }


def _build_interpretations(
    session: Session,
    entropy_context: EntropyContext,
    analysis_data: dict[str, Any],
    interpreter: EntropyInterpreter,
) -> None:
    """Build LLM interpretations for all column profiles in a single batch.

    Args:
        session: SQLAlchemy session
        entropy_context: The entropy context to populate with interpretations
        analysis_data: Pre-loaded analysis data (for type/description info)
        interpreter: EntropyInterpreter for LLM interpretation
    """
    # Build lookup for analysis data by column key
    column_analysis: dict[str, dict[str, Any]] = {}
    for _table_id, table_data in analysis_data.items():
        table_name = table_data["table_name"]
        for col_data in table_data["columns"]:
            col_name = col_data["name"]
            key = f"{table_name}.{col_name}"
            column_analysis[key] = col_data.get("analysis_results", {})

    # Build all interpretation inputs
    inputs: list[InterpretationInput] = []
    input_keys: list[str] = []

    for key, profile in entropy_context.column_profiles.items():
        # Get additional metadata from analysis
        analysis = column_analysis.get(key, {})
        typing_info = analysis.get("typing", {})
        semantic_info = analysis.get("semantic", {})

        detected_type = typing_info.get("data_type", "unknown")
        business_description = semantic_info.get("business_description")

        # Build raw metrics for interpretation
        raw_metrics: dict[str, Any] = {}
        if typing_info:
            raw_metrics["parse_success_rate"] = typing_info.get("parse_success_rate")
            raw_metrics["detected_unit"] = typing_info.get("detected_unit")
        stats_info = analysis.get("statistics", {})
        if stats_info:
            raw_metrics["null_ratio"] = stats_info.get("null_ratio")
            raw_metrics["outlier_ratio"] = stats_info.get("iqr_outlier_ratio")

        # Create interpretation input
        input_data = InterpretationInput.from_profile(
            profile=profile,
            detected_type=detected_type,
            business_description=business_description,
            raw_metrics=raw_metrics,
        )
        inputs.append(input_data)
        input_keys.append(key)

    if not inputs:
        return

    # Batch LLM interpretation
    interpretations: dict[str, Any] = {}
    result = interpreter.interpret_batch(session, inputs)
    if result.success and result.value:
        interpretations = result.value
    elif result.error:
        logger.warning("Batch LLM interpretation failed: %s", result.error)

    # Apply interpretations to profiles (no fallback - LLM or nothing)
    for key, _input_data in zip(input_keys, inputs, strict=True):
        interpretation = interpretations.get(key)

        # Store interpretation if available
        if interpretation is not None:
            profile = entropy_context.column_profiles[key]
            profile.interpretation = interpretation
            entropy_context.column_interpretations[key] = interpretation
