"""Entropy context builder for graph execution integration.

This module provides the bridge between:
- Database (loads analysis results)
- EntropyProcessor (computes entropy)
- GraphExecutionContext (consumes entropy data)

Usage:
    from dataraum_context.entropy.context import build_entropy_context

    entropy_ctx = await build_entropy_context(session, table_ids)
    # Use entropy_ctx.column_profiles, entropy_ctx.table_profiles, etc.

    # With LLM interpretation:
    entropy_ctx = await build_entropy_context(
        session, table_ids,
        interpreter=interpreter,  # EntropyInterpreter instance
    )
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.entropy.detectors import register_builtin_detectors
from dataraum_context.entropy.interpretation import (
    InterpretationInput,
    create_fallback_interpretation,
)
from dataraum_context.entropy.models import (
    ColumnEntropyProfile,
    EntropyContext,
    RelationshipEntropyProfile,
    TableEntropyProfile,
)
from dataraum_context.entropy.processor import EntropyProcessor

if TYPE_CHECKING:
    from dataraum_context.entropy.interpretation import EntropyInterpreter

logger = logging.getLogger(__name__)


async def build_entropy_context(
    session: AsyncSession,
    table_ids: list[str],
    *,
    interpreter: EntropyInterpreter | None = None,
    use_fallback_interpretation: bool = True,
) -> EntropyContext:
    """Build entropy context for the given tables.

    Loads analysis results from the database and runs entropy detectors
    to produce a complete EntropyContext.

    Args:
        session: SQLAlchemy async session
        table_ids: List of table IDs to process
        interpreter: Optional EntropyInterpreter for LLM-powered interpretation.
            If provided, generates interpretations for each column.
        use_fallback_interpretation: If True and interpreter fails or is None,
            generate basic fallback interpretations. Default True.

    Returns:
        EntropyContext with column, table, and relationship entropy profiles
    """
    if not table_ids:
        return EntropyContext()

    # Ensure builtin detectors are registered
    register_builtin_detectors()

    # Load all analysis data needed for entropy detection
    analysis_data = await _load_analysis_data(session, table_ids)

    # Create processor and process tables
    processor = EntropyProcessor()
    table_profiles: list[TableEntropyProfile] = []

    for table_id in table_ids:
        table_data = analysis_data.get(table_id)
        if not table_data:
            continue

        table_name = table_data["table_name"]
        columns = table_data["columns"]

        # Process each column
        table_profile = await processor.process_table(
            table_name=table_name,
            columns=columns,
            table_id=table_id,
        )
        table_profiles.append(table_profile)

    # Build the entropy context
    entropy_context = await processor.build_entropy_context(table_profiles)

    # Add relationship entropy
    relationship_profiles = await _compute_relationship_entropy(session, table_ids, analysis_data)
    entropy_context.relationship_profiles = relationship_profiles

    # Generate interpretations if requested
    if interpreter is not None or use_fallback_interpretation:
        await _build_interpretations(
            session=session,
            entropy_context=entropy_context,
            analysis_data=analysis_data,
            interpreter=interpreter,
            use_fallback=use_fallback_interpretation,
        )

    return entropy_context


async def _load_analysis_data(
    session: AsyncSession,
    table_ids: list[str],
) -> dict[str, Any]:
    """Load all analysis data needed for entropy detection.

    Args:
        session: SQLAlchemy async session
        table_ids: List of table IDs

    Returns:
        Dict mapping table_id to analysis data structure
    """
    # Lazy imports to avoid circular dependencies
    from dataraum_context.analysis.correlation.db_models import DerivedColumn
    from dataraum_context.analysis.relationships.db_models import Relationship
    from dataraum_context.analysis.semantic.db_models import SemanticAnnotation
    from dataraum_context.analysis.statistics.db_models import (
        StatisticalProfile,
        StatisticalQualityMetrics,
    )
    from dataraum_context.analysis.typing.db_models import TypeCandidate
    from dataraum_context.storage import Column, Table

    # Load tables
    tables_stmt = select(Table).where(Table.table_id.in_(table_ids))
    tables = (await session.execute(tables_stmt)).scalars().all()
    table_map = {t.table_id: t for t in tables}

    # Load columns
    columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns = (await session.execute(columns_stmt)).scalars().all()
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
        for profile in (await session.execute(stat_stmt)).scalars().all():
            stat_profiles[profile.column_id] = profile

    # Load statistical quality metrics
    stat_quality: dict[str, StatisticalQualityMetrics] = {}
    if column_ids:
        qual_stmt = select(StatisticalQualityMetrics).where(
            StatisticalQualityMetrics.column_id.in_(column_ids)
        )
        for metrics in (await session.execute(qual_stmt)).scalars().all():
            stat_quality[metrics.column_id] = metrics

    # Load semantic annotations
    semantic: dict[str, SemanticAnnotation] = {}
    if column_ids:
        sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids))
        for ann in (await session.execute(sem_stmt)).scalars().all():
            semantic[ann.column_id] = ann

    # Load type candidates (best by confidence)
    type_candidates: dict[str, TypeCandidate] = {}
    if column_ids:
        cand_stmt = select(TypeCandidate).where(TypeCandidate.column_id.in_(column_ids))
        for cand in (await session.execute(cand_stmt)).scalars().all():
            if cand.column_id not in type_candidates:
                type_candidates[cand.column_id] = cand
            elif cand.confidence > type_candidates[cand.column_id].confidence:
                type_candidates[cand.column_id] = cand

    # Load derived columns
    derived_columns: dict[str, DerivedColumn] = {}
    if column_ids:
        derived_stmt = select(DerivedColumn).where(DerivedColumn.derived_column_id.in_(column_ids))
        for derived in (await session.execute(derived_stmt)).scalars().all():
            derived_columns[derived.derived_column_id] = derived

    # Load relationships for table-level counts
    rel_stmt = select(Relationship).where(
        (Relationship.from_table_id.in_(table_ids)) | (Relationship.to_table_id.in_(table_ids))
    )
    relationships = (await session.execute(rel_stmt)).scalars().all()
    rel_count_by_table: dict[str, int] = {}
    for rel in relationships:
        rel_count_by_table[rel.from_table_id] = rel_count_by_table.get(rel.from_table_id, 0) + 1
        rel_count_by_table[rel.to_table_id] = rel_count_by_table.get(rel.to_table_id, 0) + 1

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

            # Relationships (for this table)
            rel_count = rel_count_by_table.get(table_id, 0)
            analysis_results["relationships"] = {
                "outgoing_count": rel_count // 2,  # Rough split
                "incoming_count": rel_count - rel_count // 2,
                "relationships": [],  # Detailed relationships not needed for count
            }

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


async def _compute_relationship_entropy(
    session: AsyncSession,
    table_ids: list[str],
    analysis_data: dict[str, Any],
) -> dict[str, RelationshipEntropyProfile]:
    """Compute entropy for relationships between tables.

    Args:
        session: SQLAlchemy async session
        table_ids: List of table IDs
        analysis_data: Pre-loaded analysis data

    Returns:
        Dict mapping relationship keys to entropy profiles
    """
    from dataraum_context.analysis.relationships.db_models import Relationship

    # Load relationships
    rel_stmt = select(Relationship).where(
        (Relationship.from_table_id.in_(table_ids)) & (Relationship.to_table_id.in_(table_ids))
    )
    relationships = (await session.execute(rel_stmt)).scalars().all()

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
    return {
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


async def _build_interpretations(
    session: AsyncSession,
    entropy_context: EntropyContext,
    analysis_data: dict[str, Any],
    interpreter: EntropyInterpreter | None,
    use_fallback: bool,
) -> None:
    """Build LLM interpretations for all column profiles.

    Args:
        session: SQLAlchemy async session
        entropy_context: The entropy context to populate with interpretations
        analysis_data: Pre-loaded analysis data (for type/description info)
        interpreter: Optional EntropyInterpreter for LLM interpretation
        use_fallback: Whether to use fallback when LLM fails/unavailable
    """
    # Build lookup for analysis data by column key
    column_analysis: dict[str, dict[str, Any]] = {}
    for _table_id, table_data in analysis_data.items():
        table_name = table_data["table_name"]
        for col_data in table_data["columns"]:
            col_name = col_data["name"]
            key = f"{table_name}.{col_name}"
            column_analysis[key] = col_data.get("analysis_results", {})

    # Process each column profile
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

        # Try LLM interpretation first
        interpretation = None
        if interpreter is not None:
            result = await interpreter.interpret(session, input_data)
            if result.success and result.value:
                interpretation = result.value
            elif result.error:
                logger.warning(
                    "LLM interpretation failed for %s: %s",
                    key,
                    result.error,
                )

        # Fall back to basic interpretation if needed
        if interpretation is None and use_fallback:
            interpretation = create_fallback_interpretation(input_data)

        # Store interpretation
        if interpretation is not None:
            profile.interpretation = interpretation
            entropy_context.column_interpretations[key] = interpretation
