"""LLM Filtering Agent - The Bridge (Phase 8).

This module bridges quality context (System 2) and data filtering (System 1).

Flow:
1. Format quality context from metadata tables
2. Add business cycles context (which process does this table belong to?)
3. Add schema mapping context (which calculations use these columns?)
4. Analyze context with LLM → generate scope + quality filters
5. Return FilteringRecommendations for merger with user rules (Phase 9)

Key Design:
- LLM analyzes quality context from all pillars (statistical, topological, temporal, domain)
- Business cycles inform which business process this table participates in
- Schema mapping informs downstream calculation impact
- Generates both SCOPE filters (row selection) and QUALITY filters (data cleaning)
- Provides clear rationale for each recommendation
- User rules (Phase 9) can override/extend recommendations
"""

import json
import logging
from typing import Any

import duckdb
import yaml
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.llm import LLMService
from dataraum_context.llm.providers.base import LLMRequest
from dataraum_context.quality.context import format_table_quality_context
from dataraum_context.quality.filtering.models import (
    CalculationImpact,
    FilterDefinition,
    FilteringRecommendations,
    FilterType,
    QualityFlag,
)
from dataraum_context.quality.formatting.business_cycles import BusinessCyclesOutput

logger = logging.getLogger(__name__)

# Cache for loaded prompts
_PROMPTS_CACHE: dict[str, Any] | None = None


def _load_prompts() -> dict[str, Any]:
    """Load filtering analysis prompts from YAML config.

    Returns:
        Dictionary of prompt templates

    Raises:
        FileNotFoundError: If prompts file not found
        yaml.YAMLError: If prompts file is invalid
    """
    global _PROMPTS_CACHE
    if _PROMPTS_CACHE is not None:
        return _PROMPTS_CACHE

    from pathlib import Path

    # Try common locations
    prompt_paths = [
        Path("config/prompts/filtering_analysis.yaml"),
        Path.cwd() / "config/prompts/filtering_analysis.yaml",
    ]

    for prompt_path in prompt_paths:
        if prompt_path.exists():
            with open(prompt_path) as f:
                _PROMPTS_CACHE = yaml.safe_load(f)
                return _PROMPTS_CACHE

    raise FileNotFoundError("filtering_analysis.yaml prompts file not found")


def _format_business_cycles_context(
    cycles: BusinessCyclesOutput | None,
    table_name: str,
) -> str:
    """Format business cycles context for LLM prompt.

    Args:
        cycles: Business cycles output from formatter
        table_name: Current table name

    Returns:
        Formatted context string or empty string if no cycles
    """
    if not cycles or not cycles.has_cycles:
        return "No business cycles detected for this table."

    # Find cycles that include this table
    relevant_cycles = [c for c in cycles.cycles if table_name in c.tables]

    if not relevant_cycles:
        return "This table is not part of any detected business cycles."

    lines = [f"This table participates in {len(relevant_cycles)} business cycle(s):"]
    for cycle in relevant_cycles:
        lines.append(f"  - {cycle.cycle_type}")
        lines.append(f"    Business value: {cycle.business_value}")
        lines.append(f"    Completeness: {cycle.completeness}")
        lines.append(f"    Related tables: {', '.join(cycle.tables)}")
        if cycle.missing_elements:
            lines.append(f"    Missing: {', '.join(cycle.missing_elements[:3])}")

    if cycles.high_value_cycles:
        lines.append(f"\nHigh-value cycles: {', '.join(cycles.high_value_cycles)}")

    return "\n".join(lines)


def _parse_filter_definitions(
    filters_data: list[dict[str, Any]],
    filter_type: FilterType,
) -> list[FilterDefinition]:
    """Parse filter definitions from LLM response.

    Args:
        filters_data: List of filter dicts from LLM
        filter_type: Type to assign (SCOPE or QUALITY)

    Returns:
        List of FilterDefinition objects
    """
    results = []
    for f in filters_data:
        if not isinstance(f, dict):
            continue
        try:
            results.append(
                FilterDefinition(
                    column=f.get("column", "unknown"),
                    condition=f.get("condition", ""),
                    filter_type=filter_type,
                    reason=f.get("reason", ""),
                    rows_affected_pct=f.get("rows_affected_pct"),
                    auto_approve=f.get("auto_approve", True),
                    review_note=f.get("review_note"),
                )
            )
        except Exception:
            continue
    return results


def _parse_quality_flags(flags_data: list[dict[str, Any]]) -> list[QualityFlag]:
    """Parse quality flags from LLM response.

    Args:
        flags_data: List of flag dicts from LLM

    Returns:
        List of QualityFlag objects
    """
    results = []
    for f in flags_data:
        if not isinstance(f, dict):
            continue
        try:
            results.append(
                QualityFlag(
                    issue_type=f.get("issue_type", "unknown"),
                    column=f.get("column", "unknown"),
                    description=f.get("description", ""),
                    severity=f.get("severity", "moderate"),
                    recommendation=f.get("recommendation"),
                )
            )
        except Exception:
            continue
    return results


def _parse_calculation_impacts(
    impacts_data: list[dict[str, Any]],
) -> list[CalculationImpact]:
    """Parse calculation impacts from LLM response.

    Args:
        impacts_data: List of impact dicts from LLM

    Returns:
        List of CalculationImpact objects
    """
    results = []
    for i in impacts_data:
        if not isinstance(i, dict):
            continue
        try:
            results.append(
                CalculationImpact(
                    calculation_id=i.get("calculation_id", "unknown"),
                    abstract_field=i.get("abstract_field", "unknown"),
                    impact_severity=i.get("impact_severity", "moderate"),
                    explanation=i.get("explanation", ""),
                )
            )
        except Exception:
            continue
    return results


def _format_schema_mapping_context(
    mapping_context: dict[str, Any] | None,
) -> str:
    """Format schema mapping context for LLM prompt.

    Args:
        mapping_context: Schema mapping context dict

    Returns:
        Formatted context string or empty string if no mapping
    """
    if not mapping_context:
        return "No calculation mappings for this table."

    lines = []

    # Mapped calculations
    calculations = mapping_context.get("mapped_calculations", [])
    if calculations:
        lines.append(f"This table feeds into calculations: {', '.join(calculations)}")

    # Column mappings
    col_mappings = mapping_context.get("column_mappings", {})
    if col_mappings:
        lines.append("\nColumn → Calculation Field mappings:")
        for col, mapping in col_mappings.items():
            field = mapping.get("abstract_field", "unknown")
            required = "REQUIRED" if mapping.get("is_required", False) else "optional"
            lines.append(f"  - {col} → {field} ({required})")

    # Downstream impact
    impact = mapping_context.get("downstream_impact")
    if impact:
        lines.append(f"\nDownstream impact: {impact}")

    return "\n".join(lines) if lines else "No calculation mappings for this table."


async def analyze_quality_for_filtering(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    llm_service: LLMService | None,
    *,
    business_cycles: BusinessCyclesOutput | None = None,
    schema_mapping_context: dict[str, Any] | None = None,
) -> Result[FilteringRecommendations]:
    """Analyze quality context and generate filtering recommendations.

    This is the LLM bridge between quality context and data filtering.

    Args:
        table_id: Table to analyze
        duckdb_conn: DuckDB connection (for row counts)
        session: SQLAlchemy session for metadata queries
        llm_service: LLM service (optional - returns empty recommendations if None)
        business_cycles: Optional business cycles context (from format_business_cycles_for_llm)
        schema_mapping_context: Optional schema mapping context with structure:
            {
                "mapped_calculations": ["dso", "cash_runway"],
                "column_mappings": {
                    "amount": {"abstract_field": "revenue", "is_required": True},
                    "ar_balance": {"abstract_field": "accounts_receivable", "is_required": True},
                },
                "downstream_impact": "Quality issues in 'amount' affect DSO calculation"
            }

    Returns:
        Result containing FilteringRecommendations:
        - scope_filters: Filters for calculation scope (row selection)
        - quality_filters: Filters for data quality (cleaning)
        - flags: Issues that can't be filtered
        - calculation_impacts: How quality issues affect calculations
        - clean_view_filters: Combined SQL WHERE clauses (backward compatible)
        - rationale: Explanation for each recommendation

    Example:
        >>> recs = await analyze_quality_for_filtering(
        ...     table_id, duckdb_conn, session, llm_service,
        ...     business_cycles=cycles_output,
        ...     schema_mapping_context=mapping_ctx,
        ... )
        >>> if recs.success:
        ...     for f in recs.value.scope_filters:
        ...         print(f"Scope: {f.condition} - {f.reason}")
        ...     for f in recs.value.quality_filters:
        ...         print(f"Quality: {f.condition} - {f.reason}")
    """
    try:
        # Get quality context using the new context formatter
        table_context = await format_table_quality_context(table_id, session, duckdb_conn)

        if not table_context:
            return Result.fail(f"Table {table_id} not found")

        # If no LLM, return empty recommendations
        if llm_service is None:
            logger.warning("LLM service unavailable, returning empty recommendations")
            return Result.ok(
                FilteringRecommendations(
                    clean_view_filters=[],
                    quarantine_criteria=[],
                    column_exclusions=[],
                    rationale={},
                )
            )

        # Build column metrics from context
        column_metrics = []
        for col in table_context.columns:
            col_info = {
                "column_name": col.column_name,
                "null_ratio": col.null_ratio,
                "cardinality_ratio": col.cardinality_ratio,
                "outlier_ratio": col.outlier_ratio,
                "parse_success_rate": col.parse_success_rate,
                "benford_compliant": col.benford_compliant,
                "is_stale": col.is_stale,
                "data_freshness_days": col.data_freshness_days,
                "has_seasonality": col.has_seasonality,
                "has_trend": col.has_trend,
                "flags": col.flags,
                "issue_count": len(col.issues),
            }
            column_metrics.append(col_info)

        # Count issues by type
        all_issues = table_context.issues + [
            issue for col in table_context.columns for issue in col.issues
        ]
        critical_issues = sum(1 for i in all_issues if i.severity.value == "critical")
        total_issues = len(all_issues)

        # Count specific issue types
        benford_violations = sum(
            1 for col in table_context.columns if col.benford_compliant is False
        )
        stale_columns = sum(1 for col in table_context.columns if col.is_stale is True)

        # Build problematic columns summary from context
        problematic_columns = []
        for col in table_context.columns:
            if col.flags or col.issues:
                issue_parts = []
                if col.null_ratio and col.null_ratio > 0.3:
                    issue_parts.append(f"High nulls: {col.null_ratio:.1%}")
                if col.benford_compliant is False:
                    issue_parts.append("Benford violation")
                if col.outlier_ratio and col.outlier_ratio > 0.1:
                    issue_parts.append(f"Outliers: {col.outlier_ratio:.1%}")
                if col.is_stale is True:
                    issue_parts.append("Stale data")
                if "high_nulls" in col.flags:
                    if not any("null" in p.lower() for p in issue_parts):
                        issue_parts.append("High nulls (flagged)")
                if "high_outliers" in col.flags:
                    if not any("outlier" in p.lower() for p in issue_parts):
                        issue_parts.append("High outliers (flagged)")

                if issue_parts:
                    problematic_columns.append(f"- {col.column_name}: {', '.join(issue_parts)}")

        # Build business cycles context
        business_cycles_context = _format_business_cycles_context(
            business_cycles, table_context.table_name
        )

        # Build schema mapping context
        schema_mapping_summary = _format_schema_mapping_context(schema_mapping_context)

        # Load prompts
        try:
            prompts = _load_prompts()
            filtering_prompt = prompts.get("filtering_analysis", {})
        except Exception as e:
            logger.warning(f"Failed to load prompts: {e}")
            return Result.ok(
                FilteringRecommendations(
                    clean_view_filters=[],
                    quarantine_criteria=[],
                    column_exclusions=[],
                    rationale={"error": f"Prompts unavailable: {e}"},
                )
            )

        # Format context for LLM
        system_prompt = filtering_prompt.get("system", "")
        user_template = filtering_prompt.get("user", "")

        # Prepare template variables
        template_vars = {
            "table_name": table_context.table_name,
            "row_count": table_context.row_count or 0,
            "column_count": table_context.column_count,
            "critical_issues": critical_issues,
            "total_issues": total_issues,
            "benford_violations": benford_violations,
            "stale_columns": stale_columns,
            "has_cycles": table_context.betti_1 is not None and table_context.betti_1 > 0,
            "column_metrics_json": json.dumps(column_metrics, indent=2, default=str),
            "problematic_columns_summary": (
                "\n".join(problematic_columns)
                if problematic_columns
                else "No significant issues detected"
            ),
            # New context fields
            "business_cycles_context": business_cycles_context,
            "schema_mapping_context": schema_mapping_summary,
        }

        # Format user prompt - handle missing template vars gracefully
        try:
            user_prompt = user_template.format(**template_vars)
        except KeyError as e:
            # Template may not have new fields yet - try without them
            logger.debug(f"Template missing field {e}, using basic format")
            user_prompt = user_template.format(
                table_name=table_context.table_name,
                row_count=table_context.row_count or 0,
                column_count=table_context.column_count,
                critical_issues=critical_issues,
                total_issues=total_issues,
                benford_violations=benford_violations,
                stale_columns=stale_columns,
                has_cycles=table_context.betti_1 is not None and table_context.betti_1 > 0,
                column_metrics_json=json.dumps(column_metrics, indent=2, default=str),
                problematic_columns_summary=(
                    "\n".join(problematic_columns)
                    if problematic_columns
                    else "No significant issues detected"
                ),
            )
            # Append context manually if template doesn't support it
            user_prompt += f"\n\n## Business Cycles Context\n{business_cycles_context}"
            user_prompt += f"\n\n## Schema Mapping Context\n{schema_mapping_summary}"

        # Call LLM
        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        request = LLMRequest(
            prompt=full_prompt,
            max_tokens=1500,  # Filtering recommendations need space
            temperature=0.3,  # Lower temperature for precise SQL generation
            response_format="json",
        )

        response_result = await llm_service.provider.complete(request)

        if not response_result.success or not response_result.value:
            logger.error(f"LLM request failed: {response_result.error}")
            return Result.ok(
                FilteringRecommendations(
                    clean_view_filters=[],
                    quarantine_criteria=[],
                    column_exclusions=[],
                    rationale={"error": "LLM request failed"},
                )
            )

        # Parse JSON response
        response_text = response_result.value.content.strip()
        try:
            recommendations_dict = json.loads(response_text)

            # Parse extended filter format
            scope_filters = _parse_filter_definitions(
                recommendations_dict.get("scope_filters", []),
                FilterType.SCOPE,
            )
            quality_filters = _parse_filter_definitions(
                recommendations_dict.get("quality_filters", []),
                FilterType.QUALITY,
            )
            flags = _parse_quality_flags(recommendations_dict.get("flags", []))
            calculation_impacts = _parse_calculation_impacts(
                recommendations_dict.get("calculation_impacts", [])
            )

            # Build backward-compatible clean_view_filters
            all_conditions = [f.condition for f in scope_filters + quality_filters]
            legacy_filters = recommendations_dict.get("clean_view_filters", [])
            combined_filters = list(set(all_conditions + legacy_filters))

            # Extract business cycles from context for storage
            business_cycle_names = []
            if business_cycles and business_cycles.has_cycles:
                relevant = [
                    c.cycle_type
                    for c in business_cycles.cycles
                    if table_context.table_name in c.tables
                ]
                business_cycle_names = relevant

            # Validate structure
            recommendations = FilteringRecommendations(
                # New structured fields
                scope_filters=scope_filters,
                quality_filters=quality_filters,
                flags=flags,
                calculation_impacts=calculation_impacts,
                # Backward-compatible fields
                clean_view_filters=combined_filters,
                quarantine_criteria=recommendations_dict.get("quarantine_criteria", []),
                column_exclusions=recommendations_dict.get("column_exclusions", []),
                rationale=recommendations_dict.get("rationale", {}),
                # Metadata
                source="llm",
                confidence=recommendations_dict.get("confidence", 0.7),
                requires_acknowledgment=recommendations_dict.get(
                    "requires_acknowledgment", len(flags) > 0
                ),
                business_cycles=business_cycle_names,
            )

            logger.info(
                f"Generated {len(scope_filters)} scope filters, "
                f"{len(quality_filters)} quality filters, "
                f"{len(flags)} flags for {table_context.table_name}"
            )

            return Result.ok(recommendations)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            logger.debug(f"Response text: {response_text[:500]}")
            return Result.fail(f"Invalid JSON response from LLM: {e}")

    except Exception as e:
        logger.error(f"Failed to analyze quality for filtering: {e}")
        return Result.fail(f"Filtering analysis failed: {e}")
