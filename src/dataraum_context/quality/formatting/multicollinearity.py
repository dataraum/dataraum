"""Cross-table multicollinearity formatting for LLM consumption.

Formats cross-table multicollinearity analysis (VIF, Condition Index, dependency groups)
into structured, interpretable context for LLM consumption.

Note: Per-table multicollinearity has been removed. Multicollinearity analysis
only makes sense across tables where relationships exist.
"""

from typing import Any

from dataraum_context.analysis.correlation.models import (
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    SingleRelationshipJoin,
)
from dataraum_context.core.models.base import RelationshipType
from dataraum_context.quality.formatting.base import format_list_with_overflow
from dataraum_context.quality.formatting.config import FormatterConfig, get_default_config

# =============================================================================
# Cross-Table Multicollinearity Formatting
# =============================================================================


def _format_join_path(join_path: SingleRelationshipJoin) -> dict[str, Any]:
    """Format a join path for LLM consumption."""
    return {
        "from": f"{join_path.from_table}.{join_path.from_column}",
        "to": f"{join_path.to_table}.{join_path.to_column}",
        "relationship_type": join_path.relationship_type.value,
        "cardinality": join_path.cardinality.value if join_path.cardinality else None,
        "confidence": round(join_path.confidence, 2),
        "detection_method": join_path.detection_method,
    }


def _format_cross_table_dependency_groups(
    dependency_groups: list[CrossTableDependencyGroup],
) -> list[dict[str, Any]]:
    """Format cross-table dependency groups for LLM consumption."""
    formatted_groups = []
    for i, group in enumerate(dependency_groups, start=1):
        column_references = [f"{table}.{col}" for table, col in group.involved_columns]
        join_paths = [_format_join_path(jp) for jp in group.join_paths]
        recommendation = _get_cross_table_group_recommendation(group)

        formatted_groups.append(
            {
                "group_id": i,
                "severity": group.severity,
                "condition_index": round(group.condition_index, 1),
                "num_tables": group.num_tables,
                "num_columns": len(group.involved_columns),
                "columns": column_references,
                "variance_proportions": [round(vdp, 3) for vdp in group.variance_proportions],
                "join_paths": join_paths,
                "relationship_types": [rt.value for rt in group.relationship_types],
                "interpretation": group.interpretation,
                "recommendation": recommendation,
            }
        )

    return formatted_groups


def _get_cross_table_group_recommendation(group: CrossTableDependencyGroup) -> str:
    """Generate actionable recommendation for a cross-table dependency group."""
    if group.num_tables == 1:
        col_refs = [f"{table}.{col}" for table, col in group.involved_columns]
        if group.severity == "severe":
            return (
                f"Single-table redundancy: Remove redundant columns from {col_refs[0].split('.')[0]}. "
                f"Columns {', '.join(col_refs)} form a linear combination."
            )
        else:
            return (
                f"Single-table correlation: Review related columns in {col_refs[0].split('.')[0]}. "
                f"May indicate related metrics that could be simplified."
            )

    # Cross-table case
    table_names = list({table for table, _ in group.involved_columns})
    col_refs = [f"{table}.{col}" for table, col in group.involved_columns]
    col_list = format_list_with_overflow(col_refs, max_display=3)

    has_fk = any(rt == RelationshipType.FOREIGN_KEY for rt in group.relationship_types)
    has_semantic = any(rt == RelationshipType.SEMANTIC for rt in group.relationship_types)

    if group.severity == "severe":
        if has_fk:
            return (
                f"Severe cross-table redundancy across {', '.join(table_names)}: "
                f"{col_list} are highly correlated via foreign key relationships. "
                f"One column may be derived from others or represents duplicate data. "
                f"Consider consolidating to the most authoritative source."
            )
        elif has_semantic:
            return (
                f"Severe cross-table redundancy across {', '.join(table_names)}: "
                f"{col_list} are nearly identical despite being in different tables. "
                f"This may indicate denormalization or data duplication. "
                f"Verify if both copies are necessary."
            )
        else:
            return (
                f"Severe cross-table dependency across {', '.join(table_names)}: "
                f"{col_list} form a linear combination. "
                f"One may be calculable from the others. Review for redundancy."
            )
    else:  # moderate
        if has_fk:
            return (
                f"Moderate cross-table correlation across {', '.join(table_names)}: "
                f"{col_list} are related via foreign keys. "
                f"This is expected but verify data is not duplicated unnecessarily."
            )
        else:
            return (
                f"Moderate cross-table correlation across {', '.join(table_names)}: "
                f"{col_list} show shared variance. "
                f"May represent related business concepts. Review for potential data consolidation."
            )


def _get_cross_table_interpretation(
    analysis: CrossTableMulticollinearityAnalysis,
) -> str:
    """Generate natural language summary of cross-table multicollinearity analysis."""
    num_tables = len(analysis.table_ids)
    num_cross_table = analysis.num_cross_table_dependencies

    if analysis.overall_severity == "none":
        return (
            f"No significant cross-table multicollinearity detected across {num_tables} tables. "
            f"Columns across related tables are relatively independent."
        )

    if analysis.overall_severity == "moderate":
        if num_cross_table > 0:
            return (
                f"Moderate cross-table multicollinearity: Found {num_cross_table} dependency "
                f"group(s) spanning multiple tables out of {num_tables} tables analyzed. "
                f"Overall Condition Index of {analysis.overall_condition_index:.1f} indicates "
                f"some structural redundancy across related tables. This may be expected for "
                f"normalized schemas with foreign key relationships."
            )
        else:
            return (
                f"Moderate multicollinearity within individual tables, but minimal cross-table "
                f"dependencies. The {num_tables} tables maintain reasonable independence from each other."
            )

    # Severe
    severity_parts = []
    if num_cross_table > 0:
        severity_parts.append(
            f"{num_cross_table} severe dependency group(s) spanning multiple tables"
        )

    severity_parts.append(
        f"Overall Condition Index of {analysis.overall_condition_index:.1f} "
        f"indicates severe structural redundancy"
    )

    return (
        f"Severe cross-table multicollinearity detected across {num_tables} tables. "
        + ". ".join(severity_parts)
        + ". This suggests significant overlap in information across related tables, "
        "possibly from denormalization, derived columns, or data duplication. "
        "Consider consolidating redundant data before cross-table analysis."
    )


def _generate_cross_table_recommendations(
    analysis: CrossTableMulticollinearityAnalysis,
) -> list[str]:
    """Generate actionable recommendations based on cross-table multicollinearity analysis."""
    recommendations = []

    if analysis.overall_severity == "none":
        return [
            "No action needed - tables maintain sufficient independence for cross-table analysis"
        ]

    if analysis.num_cross_table_dependencies > 0:
        recommendations.append(
            f"Found {analysis.num_cross_table_dependencies} cross-table dependency "
            f"group(s). Review the detailed dependency groups below for specific "
            f"column-level recommendations."
        )

    severe_cross_table = [
        group for group in analysis.cross_table_groups if group.severity == "severe"
    ]

    if severe_cross_table:
        recommendations.append(
            f"Critical: {len(severe_cross_table)} severe cross-table dependency "
            f"group(s) detected. These represent significant redundancy across tables. "
            f"Prioritize investigating foreign key relationships and derived columns."
        )

    fk_groups = [
        group
        for group in analysis.cross_table_groups
        if any(rt == RelationshipType.FOREIGN_KEY for rt in group.relationship_types)
    ]

    if fk_groups and len(fk_groups) >= 2:
        recommendations.append(
            "Multiple dependency groups involve foreign key relationships. "
            "This may indicate aggressive denormalization. Verify that duplicated "
            "data serves a clear performance or business purpose."
        )

    semantic_groups = [
        group
        for group in analysis.cross_table_groups
        if any(rt == RelationshipType.SEMANTIC for rt in group.relationship_types)
        and not any(rt == RelationshipType.FOREIGN_KEY for rt in group.relationship_types)
    ]

    if semantic_groups:
        recommendations.append(
            "Semantically similar columns detected across tables without formal "
            "foreign key relationships. This may indicate shadow schemas or data "
            "duplication. Consider establishing explicit relationships or consolidating."
        )

    if analysis.overall_condition_index > 30:
        recommendations.append(
            "Severe unified Condition Index (>30) indicates high degree of "
            "linear dependency across the entire dataset. Cross-table regression "
            "or multi-table analysis may produce unstable estimates. Consider "
            "feature selection or dimensionality reduction before modeling."
        )
    elif analysis.overall_condition_index > 10:
        recommendations.append(
            "Moderate unified Condition Index (10-30) detected. This is common "
            "in normalized schemas with many relationships. Be cautious when using "
            "columns from multiple related tables in the same regression model."
        )

    for group in analysis.cross_table_groups:
        if group.severity == "severe":
            col_refs = [f"{table}.{col}" for table, col in group.involved_columns]
            col_list = format_list_with_overflow(col_refs, max_display=3)

            table_names = list({table for table, _ in group.involved_columns})
            recommendations.append(
                f"Severe Dependency: {col_list} across {', '.join(table_names)} "
                f"(CI={group.condition_index:.1f}). Investigate if one column is "
                f"derived from others or represents duplicate data."
            )

    if analysis.overall_severity == "moderate" and not severe_cross_table:
        recommendations.append(
            "Moderate cross-table multicollinearity is expected in normalized "
            "schemas with relationships. This doesn't invalidate analysis but be "
            "mindful when combining columns from related tables in statistical models."
        )

    return recommendations


def format_cross_table_multicollinearity_for_llm(
    analysis: CrossTableMulticollinearityAnalysis,
    config: FormatterConfig | None = None,
) -> dict[str, Any]:
    """Format cross-table multicollinearity analysis for LLM consumption.

    Transforms unified correlation matrix analysis into structured context with
    natural language interpretations, join paths, and actionable recommendations.

    Args:
        analysis: CrossTableMulticollinearityAnalysis from enrichment
        config: Optional formatter configuration for thresholds

    Returns:
        Dict with cross-table multicollinearity context for LLM
    """
    config = config or get_default_config()
    interpretation = _get_cross_table_interpretation(analysis)

    cross_table_groups_formatted = []
    if analysis.cross_table_groups:
        cross_table_groups_formatted = _format_cross_table_dependency_groups(
            analysis.cross_table_groups
        )

    all_groups_formatted = []
    if analysis.dependency_groups:
        all_groups_formatted = _format_cross_table_dependency_groups(analysis.dependency_groups)

    recommendations = _generate_cross_table_recommendations(analysis)

    # Get CI thresholds from config
    ci_threshold = config.get_threshold("multicollinearity", "condition_index")
    if ci_threshold:
        ci_thresholds = ci_threshold.thresholds
    else:
        ci_thresholds = {"moderate": 10, "severe": 30}

    return {
        "cross_table_multicollinearity_assessment": {
            "overall_severity": analysis.overall_severity,
            "summary": interpretation,
            "scope": {
                "num_tables": len(analysis.table_ids),
                "table_ids": analysis.table_ids,
                "table_names": analysis.table_names,
                "total_columns_analyzed": analysis.total_columns_analyzed,
                "total_relationships_used": analysis.total_relationships_used,
            },
            "unified_analysis": {
                "overall_condition_index": round(analysis.overall_condition_index, 1),
                "severity_level": analysis.overall_severity,
                "interpretation": (
                    f"Unified Condition Index of {analysis.overall_condition_index:.1f} "
                    f"across all {analysis.total_columns_analyzed} columns from "
                    f"{len(analysis.table_ids)} tables"
                ),
            },
            "cross_table_dependencies": {
                "count": analysis.num_cross_table_dependencies,
                "groups": cross_table_groups_formatted,
                "explanation": (
                    "Dependency groups where columns from multiple tables share "
                    "significant variance in near-singular dimensions (high VDP). "
                    "Indicates potential redundancy or derivation across tables."
                )
                if cross_table_groups_formatted
                else "No cross-table dependencies detected.",
            },
            "all_dependency_groups": {
                "count": len(analysis.dependency_groups),
                "groups": all_groups_formatted,
                "explanation": (
                    "All dependency groups including single-table and cross-table. "
                    "Provides complete picture of multicollinearity structure."
                ),
            },
            "recommendations": recommendations,
            "technical_details": {
                "analysis_method": "Belsley VDP on unified correlation matrix",
                "relationship_sources": "Semantic + Topology + Temporal enrichment",
                "vdp_threshold": 0.5,
                "ci_thresholds": ci_thresholds,
            },
        }
    }
