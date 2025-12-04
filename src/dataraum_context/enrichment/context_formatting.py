"""Context formatting for LLM consumption.

This module formats metadata from various pillars into structured, interpretable
context documents that LLMs can use to make data quality and analysis decisions.

Each formatter function takes raw metadata and produces:
- Natural language interpretations
- Actionable recommendations
- Structured evidence
- Severity assessments
"""

from typing import Any

from dataraum_context.core.models.base import RelationshipType
from dataraum_context.profiling.models import (
    ColumnVIF,
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    DependencyGroup,
    MulticollinearityAnalysis,
    SingleRelationshipJoin,
)


def _format_dependency_groups(
    dependency_groups: list[DependencyGroup], column_vifs: list[ColumnVIF]
) -> list[dict[str, Any]]:
    """Format dependency groups for LLM consumption.

    Args:
        dependency_groups: List of DependencyGroup objects
        column_vifs: List of ColumnVIF objects for column name lookup

    Returns:
        List of formatted dependency group dicts
    """
    # Create column ID to name mapping
    column_id_to_name = {vif.column_id: vif.column_ref.column_name for vif in column_vifs}

    formatted_groups = []
    for i, group in enumerate(dependency_groups, start=1):
        # Map column IDs to names
        column_names = [
            column_id_to_name.get(col_id, col_id) for col_id in group.involved_column_ids
        ]

        # Get VIF values for group members
        vif_values = {
            column_id_to_name.get(vif.column_id, vif.column_id): round(vif.vif, 2)
            for vif in column_vifs
            if vif.column_id in group.involved_column_ids
        }

        formatted_groups.append(
            {
                "group_id": i,
                "severity": group.severity,
                "condition_index": round(group.condition_index, 1),
                "columns": column_names,
                "vif_values": vif_values,
                "variance_proportions": [round(vdp, 3) for vdp in group.variance_proportions],
                "interpretation": group.interpretation,
                "recommendation": _get_dependency_group_recommendation(group, column_names),
            }
        )

    return formatted_groups


def _get_dependency_group_recommendation(group: DependencyGroup, column_names: list[str]) -> str:
    """Generate actionable recommendation for a dependency group.

    Args:
        group: DependencyGroup object
        column_names: Column names in the group

    Returns:
        Recommendation string
    """
    num_cols = len(column_names)

    if group.severity == "severe":
        if num_cols == 2:
            return (
                f"Remove one of {column_names[0]} or {column_names[1]}. "
                f"These columns are nearly identical or one is derived from the other."
            )
        else:
            col_list = ", ".join(column_names[:3])
            if num_cols > 3:
                col_list += f", and {num_cols - 3} others"
            return (
                f"Review {col_list}. These {num_cols} columns form a linear combination. "
                f"Consider consolidating or removing redundant columns."
            )
    else:  # moderate
        col_list = ", ".join(column_names)
        return (
            f"Investigate relationship between {col_list}. "
            f"These may represent related metrics that could be simplified."
        )


def format_multicollinearity_for_llm(
    analysis: MulticollinearityAnalysis,
) -> dict[str, Any]:
    """Format multicollinearity analysis for LLM consumption.

    Transforms VIF, Tolerance, and Condition Index metrics into structured
    context with natural language interpretations and actionable recommendations.

    Research-based thresholds:
    - VIF = 1: No multicollinearity
    - VIF 1-5: Low to moderate (acceptable)
    - VIF > 5: High, requires attention
    - VIF > 10: Serious, often problematic

    Args:
        analysis: MulticollinearityAnalysis from profiling

    Returns:
        Dict with multicollinearity context for LLM
    """
    # Generate overall interpretation
    interpretation = _get_multicollinearity_interpretation(analysis)

    # Format table-level assessment
    table_level = None
    if analysis.condition_index:
        table_level = {
            "condition_index": analysis.condition_index.condition_index,
            "severity": analysis.condition_index.severity,
            "assessment": analysis.condition_index.interpretation,
            "problematic_dimensions": analysis.condition_index.problematic_dimensions,
        }

        # Add dependency groups if available
        if analysis.condition_index.dependency_groups:
            table_level["dependency_groups"] = _format_dependency_groups(
                analysis.condition_index.dependency_groups, analysis.column_vifs
            )

    # Format problematic columns with detailed analysis
    problematic_columns = []
    for vif in analysis.column_vifs:
        if vif.vif > 4:  # Threshold for investigation
            column_info = {
                "column": vif.column_ref.column_name,
                "vif": round(vif.vif, 2),
                "tolerance": round(vif.tolerance, 3),
                "severity": _get_vif_severity_label(vif.vif),
                "interpretation": _get_vif_interpretation(vif.vif),
                "impact": _get_vif_impact(vif.vif),
            }

            # Add correlated columns if available
            if vif.correlated_with:
                column_info["highly_correlated_with"] = vif.correlated_with[:3]

            problematic_columns.append(column_info)

    # Generate recommendations
    recommendations = _generate_multicollinearity_recommendations(analysis)

    return {
        "multicollinearity_assessment": {
            "overall_severity": analysis.overall_severity,
            "summary": interpretation,
            "num_problematic_columns": analysis.num_problematic_columns,
            "table_level": table_level,
            "problematic_columns": problematic_columns,
            "recommendations": recommendations,
            "technical_details": {
                "total_columns_analyzed": len(analysis.column_vifs),
                "analysis_method": "VIF (Variance Inflation Factor) via OLS regression",
                "table_method": "Condition Index via eigenvalue decomposition",
            },
        }
    }


def _get_vif_severity_label(vif: float) -> str:
    """Get severity label based on research thresholds.

    Thresholds:
    - VIF = 1: No multicollinearity
    - VIF 1-5: Low to moderate
    - VIF 5-10: High
    - VIF > 10: Serious

    Args:
        vif: Variance Inflation Factor value

    Returns:
        Severity label
    """
    if vif <= 1.0:
        return "none"
    elif vif <= 5.0:
        return "low_to_moderate"
    elif vif <= 10.0:
        return "high"
    else:
        return "serious"


def _get_vif_interpretation(vif: float) -> str:
    """Get natural language interpretation of VIF value.

    Args:
        vif: Variance Inflation Factor value

    Returns:
        Natural language interpretation
    """
    if vif <= 1.0:
        return f"No multicollinearity detected (VIF={vif:.1f})"
    elif vif <= 5.0:
        return f"Low to moderate multicollinearity (VIF={vif:.1f}) - generally acceptable"
    elif vif <= 10.0:
        return f"High multicollinearity (VIF={vif:.1f}) - requires attention"
    else:
        return f"Serious multicollinearity (VIF={vif:.1f}) - often considered problematic"


def _get_vif_impact(vif: float) -> str:
    """Explain the practical impact of the VIF value.

    Args:
        vif: Variance Inflation Factor value

    Returns:
        Impact description
    """
    if vif <= 1.0:
        return "Column variance is not inflated by correlation with other columns"
    elif vif <= 5.0:
        variance_inflation = (vif - 1) * 100
        return f"Column variance inflated by ~{variance_inflation:.0f}% due to correlation with other columns"
    elif vif <= 10.0:
        return f"Column is {vif:.1f}x more variable than it would be if uncorrelated - may indicate redundancy"
    else:
        redundancy = (1 - 1 / vif) * 100
        return f"Column is ~{redundancy:.0f}% redundant with other columns - likely derived or duplicate data"


def _get_multicollinearity_interpretation(
    analysis: MulticollinearityAnalysis,
) -> str:
    """Generate natural language summary of multicollinearity analysis.

    Args:
        analysis: MulticollinearityAnalysis

    Returns:
        Natural language summary
    """
    if analysis.overall_severity == "none":
        return (
            "No significant multicollinearity detected. Columns are relatively "
            "independent and each provides unique information."
        )

    if analysis.overall_severity == "moderate":
        parts = []
        if analysis.num_problematic_columns > 0:
            parts.append(
                f"{analysis.num_problematic_columns} column(s) show elevated "
                f"correlation with other columns"
            )
        if analysis.condition_index and analysis.condition_index.has_multicollinearity:
            parts.append(
                f"Table-level analysis indicates moderate structural redundancy "
                f"(Condition Index: {analysis.condition_index.condition_index:.1f})"
            )

        summary = "Moderate multicollinearity: " + ". ".join(parts) + "."
        return (
            summary
            + " This may indicate some overlap in information but is not necessarily problematic."
        )

    # Severe
    severity_desc = []
    if analysis.num_problematic_columns > 0:
        severity_desc.append(
            f"{analysis.num_problematic_columns} columns are highly redundant with other columns"
        )

    if analysis.condition_index:
        severity_desc.append(
            f"Condition Index of {analysis.condition_index.condition_index:.1f} "
            f"indicates severe structural redundancy"
        )

    return (
        "Severe multicollinearity detected. "
        + ". ".join(severity_desc)
        + ". This suggests significant overlap in information, possibly from "
        "derived columns, duplicate data, or mathematically related fields. "
        "Consider data consolidation before analysis."
    )


def _generate_multicollinearity_recommendations(
    analysis: MulticollinearityAnalysis,
) -> list[str]:
    """Generate actionable recommendations based on multicollinearity analysis.

    Args:
        analysis: MulticollinearityAnalysis

    Returns:
        List of actionable recommendations
    """
    recommendations = []

    if analysis.overall_severity == "none":
        return ["No action needed - columns are sufficiently independent for analysis"]

    # Identify severe cases (VIF > 10)
    severe_cols = [vif for vif in analysis.column_vifs if vif.vif > 10]

    if severe_cols:
        col_names = [vif.column_ref.column_name for vif in severe_cols[:3]]
        if len(severe_cols) > 3:
            col_list = f"{', '.join(col_names)}, and {len(severe_cols) - 3} others"
        else:
            col_list = ", ".join(col_names)

        recommendations.append(
            f"🔴 Critical: Investigate {col_list}. These columns show serious "
            f"redundancy (VIF > 10). Consider removing or consolidating them."
        )

    # Identify high cases (VIF 5-10)
    high_cols = [vif for vif in analysis.column_vifs if 5 < vif.vif <= 10]

    if high_cols:
        recommendations.append(
            f"🟡 Attention: {len(high_cols)} column(s) have high multicollinearity "
            f"(VIF 5-10). Review if these represent the same underlying concept."
        )

    # Check for derived columns (very high VIF with multiple correlations)
    derived_candidates = [
        vif for vif in analysis.column_vifs if vif.vif > 15 and len(vif.correlated_with) >= 2
    ]

    if derived_candidates:
        recommendations.append(
            "💡 Some columns may be calculated from others (e.g., total = sum of parts). "
            "Use derived columns for validation only, not as independent variables."
        )

    # Dependency group recommendations (VDP-based - most specific)
    if analysis.condition_index and analysis.condition_index.dependency_groups:
        # Create column ID to name mapping
        column_id_to_name = {
            vif.column_id: vif.column_ref.column_name for vif in analysis.column_vifs
        }

        for i, group in enumerate(analysis.condition_index.dependency_groups, start=1):
            column_names = [
                column_id_to_name.get(col_id, col_id) for col_id in group.involved_column_ids
            ]
            col_list = ", ".join(column_names[:3])
            if len(column_names) > 3:
                col_list += f", and {len(column_names) - 3} others"

            if group.severity == "severe":
                recommendations.append(
                    f"🔴 Dependency Group {i}: {col_list} form a severe linear dependency "
                    f"(CI={group.condition_index:.1f}). Remove redundant columns from this group."
                )
            else:
                recommendations.append(
                    f"🟡 Dependency Group {i}: {col_list} show shared variance "
                    f"(CI={group.condition_index:.1f}). Review for potential simplification."
                )

    # Table-level severity (CI thresholds: <10=none, 10-30=moderate, >30=severe)
    if analysis.condition_index and analysis.condition_index.condition_index > 30:
        recommendations.append(
            "⚠️ Severe table-level multicollinearity (Condition Index > 30) suggests "
            "high degree of linear dependency that makes regression estimates unstable. "
            "Consider a comprehensive review of column selection before statistical analysis."
        )

    # Moderate table-level
    if analysis.condition_index and 10 <= analysis.condition_index.condition_index <= 30:
        recommendations.append(
            "ℹ️ Moderate table-level multicollinearity detected (CI 10-30). This warrants "
            "investigation but may not prevent analysis. Review column relationships carefully."
        )

    # General advice for moderate cases
    if analysis.overall_severity == "moderate" and not recommendations:
        recommendations.append(
            "Consider reviewing column relationships to ensure each provides unique "
            "information. Multicollinearity doesn't invalidate data but may affect "
            "certain types of analysis (e.g., regression coefficient interpretation)."
        )

    return recommendations


# === Cross-Table Multicollinearity Formatting ===


def _format_join_path(join_path: SingleRelationshipJoin) -> dict[str, Any]:
    """Format a join path for LLM consumption.

    Args:
        join_path: SingleRelationshipJoin object describing a relationship

    Returns:
        Formatted join path dict
    """
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
    """Format cross-table dependency groups for LLM consumption.

    Args:
        dependency_groups: List of CrossTableDependencyGroup objects

    Returns:
        List of formatted dependency group dicts
    """
    formatted_groups = []
    for i, group in enumerate(dependency_groups, start=1):
        # Format columns as "table.column"
        column_references = [f"{table}.{col}" for table, col in group.involved_columns]

        # Format join paths
        join_paths = [_format_join_path(jp) for jp in group.join_paths]

        # Generate recommendation
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
    """Generate actionable recommendation for a cross-table dependency group.

    Args:
        group: CrossTableDependencyGroup object

    Returns:
        Recommendation string
    """
    if group.num_tables == 1:
        # Single-table case
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
    col_refs = [f"{table}.{col}" for table, col in group.involved_columns[:3]]
    if len(group.involved_columns) > 3:
        col_list = f"{', '.join(col_refs)}, and {len(group.involved_columns) - 3} others"
    else:
        col_list = ", ".join(col_refs)

    # Identify relationship types (relationship_types are already enum values)
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
    """Generate natural language summary of cross-table multicollinearity analysis.

    Args:
        analysis: CrossTableMulticollinearityAnalysis

    Returns:
        Natural language summary
    """
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
    """Generate actionable recommendations based on cross-table multicollinearity analysis.

    Args:
        analysis: CrossTableMulticollinearityAnalysis

    Returns:
        List of actionable recommendations
    """
    recommendations = []

    if analysis.overall_severity == "none":
        return [
            "No action needed - tables maintain sufficient independence for cross-table analysis"
        ]

    # Cross-table specific recommendations
    if analysis.num_cross_table_dependencies > 0:
        recommendations.append(
            f"🔍 Found {analysis.num_cross_table_dependencies} cross-table dependency "
            f"group(s). Review the detailed dependency groups below for specific "
            f"column-level recommendations."
        )

    # Identify severe cross-table groups
    severe_cross_table = [
        group for group in analysis.cross_table_groups if group.severity == "severe"
    ]

    if severe_cross_table:
        recommendations.append(
            f"🔴 Critical: {len(severe_cross_table)} severe cross-table dependency "
            f"group(s) detected. These represent significant redundancy across tables. "
            f"Prioritize investigating foreign key relationships and derived columns."
        )

    # Check for denormalization patterns
    fk_groups = [
        group
        for group in analysis.cross_table_groups
        if any(rt == RelationshipType.FOREIGN_KEY for rt in group.relationship_types)
    ]

    if fk_groups and len(fk_groups) >= 2:
        recommendations.append(
            "⚠️ Multiple dependency groups involve foreign key relationships. "
            "This may indicate aggressive denormalization. Verify that duplicated "
            "data serves a clear performance or business purpose."
        )

    # Check for semantic duplication
    semantic_groups = [
        group
        for group in analysis.cross_table_groups
        if any(rt == RelationshipType.SEMANTIC for rt in group.relationship_types)
        and not any(rt == RelationshipType.FOREIGN_KEY for rt in group.relationship_types)
    ]

    if semantic_groups:
        recommendations.append(
            "💡 Semantically similar columns detected across tables without formal "
            "foreign key relationships. This may indicate shadow schemas or data "
            "duplication. Consider establishing explicit relationships or consolidating."
        )

    # Overall condition index guidance
    if analysis.overall_condition_index > 30:
        recommendations.append(
            "⚠️ Severe unified Condition Index (>30) indicates high degree of "
            "linear dependency across the entire dataset. Cross-table regression "
            "or multi-table analysis may produce unstable estimates. Consider "
            "feature selection or dimensionality reduction before modeling."
        )
    elif analysis.overall_condition_index > 10:
        recommendations.append(
            "ℹ️ Moderate unified Condition Index (10-30) detected. This is common "
            "in normalized schemas with many relationships. Be cautious when using "
            "columns from multiple related tables in the same regression model."
        )

    # Group-specific recommendations (most actionable)
    for group in analysis.cross_table_groups:
        if group.severity == "severe":
            col_refs = [f"{table}.{col}" for table, col in group.involved_columns[:3]]
            col_list = ", ".join(col_refs)
            if len(group.involved_columns) > 3:
                col_list += f", and {len(group.involved_columns) - 3} others"

            table_names = list({table for table, _ in group.involved_columns})
            recommendations.append(
                f"🔴 Severe Dependency: {col_list} across {', '.join(table_names)} "
                f"(CI={group.condition_index:.1f}). Investigate if one column is "
                f"derived from others or represents duplicate data."
            )

    # General advice for moderate severity
    if analysis.overall_severity == "moderate" and not severe_cross_table:
        recommendations.append(
            "✅ Moderate cross-table multicollinearity is expected in normalized "
            "schemas with relationships. This doesn't invalidate analysis but be "
            "mindful when combining columns from related tables in statistical models."
        )

    return recommendations


def format_cross_table_multicollinearity_for_llm(
    analysis: CrossTableMulticollinearityAnalysis,
) -> dict[str, Any]:
    """Format cross-table multicollinearity analysis for LLM consumption.

    Transforms unified correlation matrix analysis into structured context with
    natural language interpretations, join paths, and actionable recommendations.

    This extends single-table multicollinearity to detect dependencies across
    related tables using a unified correlation matrix built from enrichment
    relationships (FK, semantic, correlation, hierarchy).

    Args:
        analysis: CrossTableMulticollinearityAnalysis from enrichment

    Returns:
        Dict with cross-table multicollinearity context for LLM
    """
    # Generate overall interpretation
    interpretation = _get_cross_table_interpretation(analysis)

    # Format cross-table dependency groups
    cross_table_groups_formatted = []
    if analysis.cross_table_groups:
        cross_table_groups_formatted = _format_cross_table_dependency_groups(
            analysis.cross_table_groups
        )

    # Format all dependency groups (including single-table for context)
    all_groups_formatted = []
    if analysis.dependency_groups:
        all_groups_formatted = _format_cross_table_dependency_groups(analysis.dependency_groups)

    # Generate recommendations
    recommendations = _generate_cross_table_recommendations(analysis)

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
                "ci_thresholds": {"moderate": 10, "severe": 30},
            },
        }
    }
