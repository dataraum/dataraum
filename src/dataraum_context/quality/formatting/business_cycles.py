"""Business cycles formatter.

Formats business cycle information into contextualized output for LLM consumption.
Business cycles represent cross-table process flows like AR, AP, Revenue, Expense cycles.

This formatter is used by:
- Filter generation (provides context for scope filters)
- Quality assessment (scoping by business process)
- Calculation context (linking calculations to business processes)

Usage:
    from dataraum_context.quality.formatting.business_cycles import (
        format_business_cycles_for_llm,
        format_single_cycle,
        BusinessCycleContext,
    )

    result = format_business_cycles_for_llm(
        cycles=detected_cycles,
        config=formatter_config,
    )
"""

from dataclasses import dataclass, field
from typing import Any

from dataraum_context.core.formatting.config import FormatterConfig, get_default_config


@dataclass
class BusinessCycleContext:
    """Contextualized business cycle information."""

    cycle_type: str
    tables: list[str]
    confidence: float
    business_value: str  # high, medium, low
    completeness: str  # complete, partial, incomplete
    severity: str  # Based on completeness and value
    interpretation: str
    explanation: str | None = None
    missing_elements: list[str] | None = None
    recommendations: list[str] = field(default_factory=list)


@dataclass
class BusinessCyclesOutput:
    """Complete business cycles formatted output."""

    has_cycles: bool
    cycle_count: int
    overall_severity: str
    summary: str
    cycles: list[BusinessCycleContext]
    high_value_cycles: list[str]
    incomplete_cycles: list[str]
    recommendations: list[str]


# =============================================================================
# Interpretation Templates
# =============================================================================

CYCLE_TYPE_DESCRIPTIONS = {
    "accounts_receivable_cycle": "Tracks customer credit sales through invoicing and collection",
    "accounts_payable_cycle": "Tracks vendor purchases through payment processing",
    "revenue_cycle": "Tracks revenue recognition from order to cash",
    "expense_cycle": "Tracks expense processing from requisition to payment",
    "inventory_cycle": "Tracks inventory from procurement to consumption/sale",
    "payroll_cycle": "Tracks employee compensation processing",
    "fixed_asset_cycle": "Tracks capital asset acquisition and depreciation",
    "cash_cycle": "Tracks cash movements across accounts",
    "UNKNOWN": "Unclassified business process cycle",
    "CUSTOM": "Custom business process specific to this dataset",
}

COMPLETENESS_INTERPRETATIONS = {
    "complete": "All expected tables and relationships present",
    "partial": "Some expected elements missing but cycle is functional",
    "incomplete": "Key elements missing, cycle may not be fully operational",
    "unknown": "Completeness could not be determined",
}

BUSINESS_VALUE_INTERPRETATIONS = {
    "high": "Critical business process, quality issues have significant impact",
    "medium": "Important business process, quality issues should be monitored",
    "low": "Supporting business process, lower priority for quality focus",
    "unknown": "Business value could not be determined",
}


# =============================================================================
# Severity Mapping
# =============================================================================


def _determine_cycle_severity(
    business_value: str,
    completeness: str,
    confidence: float,
) -> str:
    """Determine severity based on business value and completeness.

    High-value incomplete cycles are more severe than low-value complete ones.
    """
    # Completeness weight
    completeness_scores = {
        "complete": 0,
        "partial": 1,
        "incomplete": 2,
        "unknown": 1,
    }

    # Business value weight
    value_scores = {
        "high": 2,
        "medium": 1,
        "low": 0,
        "unknown": 1,
    }

    completeness_score = completeness_scores.get(completeness, 1)
    value_score = value_scores.get(business_value, 1)

    # Combined score: incomplete high-value = worst
    combined_score = completeness_score * (value_score + 1)

    # Low confidence increases severity
    if confidence < 0.5:
        combined_score += 1

    if combined_score == 0:
        return "none"
    elif combined_score <= 1:
        return "low"
    elif combined_score <= 3:
        return "moderate"
    elif combined_score <= 5:
        return "high"
    else:
        return "severe"


def _get_cycle_interpretation(
    cycle_type: str,
    completeness: str,
    business_value: str,
) -> str:
    """Generate interpretation for a single cycle."""
    type_desc = CYCLE_TYPE_DESCRIPTIONS.get(cycle_type, f"Business process: {cycle_type}")
    completeness_desc = COMPLETENESS_INTERPRETATIONS.get(completeness, completeness)
    value_desc = BUSINESS_VALUE_INTERPRETATIONS.get(business_value, business_value)

    return f"{type_desc}. {completeness_desc}. {value_desc}."


def _get_cycle_recommendations(
    cycle_type: str,
    completeness: str,
    business_value: str,
    missing_elements: list[str] | None,
) -> list[str]:
    """Generate recommendations for a cycle."""
    recommendations = []

    if completeness == "incomplete":
        recommendations.append(
            f"Investigate missing elements in {cycle_type} for data completeness"
        )
        if missing_elements:
            recommendations.append(
                f"Missing: {', '.join(missing_elements[:3])}"
                + (f" (+{len(missing_elements) - 3} more)" if len(missing_elements) > 3 else "")
            )

    if completeness == "partial" and business_value == "high":
        recommendations.append(
            f"High-value {cycle_type} is only partially represented; verify data coverage"
        )

    if business_value == "high":
        recommendations.append(f"Prioritize quality assessment for {cycle_type} tables")

    return recommendations


# =============================================================================
# Main Formatters
# =============================================================================


def format_single_cycle(
    cycle_type: str,
    tables: list[str],
    confidence: float,
    business_value: str,
    completeness: str,
    explanation: str | None = None,
    missing_elements: list[str] | None = None,
    config: FormatterConfig | None = None,
) -> BusinessCycleContext:
    """Format a single business cycle.

    Args:
        cycle_type: Type of cycle (accounts_receivable_cycle, etc.)
        tables: Tables involved in the cycle
        confidence: Classification confidence (0-1)
        business_value: Business importance (high/medium/low)
        completeness: Cycle completeness (complete/partial/incomplete)
        explanation: Optional explanation from LLM classification
        missing_elements: Optional list of missing expected elements
        config: Formatter configuration (uses defaults if not provided)

    Returns:
        BusinessCycleContext with severity and interpretation
    """
    if config is None:
        config = get_default_config()

    severity = _determine_cycle_severity(business_value, completeness, confidence)
    interpretation = _get_cycle_interpretation(cycle_type, completeness, business_value)
    recommendations = _get_cycle_recommendations(
        cycle_type, completeness, business_value, missing_elements
    )

    return BusinessCycleContext(
        cycle_type=cycle_type,
        tables=tables,
        confidence=confidence,
        business_value=business_value,
        completeness=completeness,
        severity=severity,
        interpretation=interpretation,
        explanation=explanation,
        missing_elements=missing_elements,
        recommendations=recommendations,
    )


def format_business_cycles_for_llm(
    cycles: list[dict[str, Any]],
    config: FormatterConfig | None = None,
) -> BusinessCyclesOutput:
    """Format business cycles for LLM consumption.

    Args:
        cycles: List of cycle dicts with keys:
            - cycle_type: str
            - tables: list[str]
            - confidence: float
            - business_value: str
            - completeness: str
            - explanation: str (optional)
            - missing_elements: list[str] (optional)
        config: Formatter configuration

    Returns:
        BusinessCyclesOutput with all cycles contextualized

    Example:
        cycles = [
            {
                "cycle_type": "accounts_receivable_cycle",
                "tables": ["invoices", "customers", "payments"],
                "confidence": 0.85,
                "business_value": "high",
                "completeness": "complete",
            }
        ]
        result = format_business_cycles_for_llm(cycles)
    """
    if config is None:
        config = get_default_config()

    if not cycles:
        return BusinessCyclesOutput(
            has_cycles=False,
            cycle_count=0,
            overall_severity="none",
            summary="No business cycles detected in the dataset",
            cycles=[],
            high_value_cycles=[],
            incomplete_cycles=[],
            recommendations=[
                "Consider analyzing table relationships for business process patterns"
            ],
        )

    # Format each cycle
    formatted_cycles = []
    for cycle_data in cycles:
        ctx = format_single_cycle(
            cycle_type=cycle_data.get("cycle_type", "UNKNOWN"),
            tables=cycle_data.get("tables", []),
            confidence=cycle_data.get("confidence", 0.5),
            business_value=cycle_data.get("business_value", "unknown"),
            completeness=cycle_data.get("completeness", "unknown"),
            explanation=cycle_data.get("explanation"),
            missing_elements=cycle_data.get("missing_elements"),
            config=config,
        )
        formatted_cycles.append(ctx)

    # Aggregate insights
    high_value_cycles = [c.cycle_type for c in formatted_cycles if c.business_value == "high"]
    incomplete_cycles = [
        c.cycle_type for c in formatted_cycles if c.completeness in ("incomplete", "partial")
    ]

    # Determine overall severity (worst case)
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    severities = [c.severity for c in formatted_cycles]
    max_severity_idx = max(severity_order.index(s) for s in severities) if severities else 0
    overall_severity = severity_order[max_severity_idx]

    # Generate summary
    summary_parts = [f"{len(cycles)} business cycle(s) detected"]
    if high_value_cycles:
        summary_parts.append(f"{len(high_value_cycles)} high-value")
    if incomplete_cycles:
        summary_parts.append(f"{len(incomplete_cycles)} incomplete/partial")
    summary = ". ".join(summary_parts) + "."

    # Aggregate recommendations
    all_recommendations = []
    for c in formatted_cycles:
        all_recommendations.extend(c.recommendations)
    # Deduplicate while preserving order
    seen = set()
    unique_recommendations = []
    for r in all_recommendations:
        if r not in seen:
            seen.add(r)
            unique_recommendations.append(r)

    return BusinessCyclesOutput(
        has_cycles=True,
        cycle_count=len(cycles),
        overall_severity=overall_severity,
        summary=summary,
        cycles=formatted_cycles,
        high_value_cycles=high_value_cycles,
        incomplete_cycles=incomplete_cycles,
        recommendations=unique_recommendations[:5],  # Limit to top 5
    )


def format_business_cycles_as_context_string(
    cycles_output: BusinessCyclesOutput,
) -> str:
    """Format business cycles output as a context string for LLM prompts.

    Args:
        cycles_output: Formatted business cycles output

    Returns:
        String suitable for inclusion in LLM prompts
    """
    if not cycles_output.has_cycles:
        return "Business Cycles: None detected"

    lines = [
        f"Business Cycles: {cycles_output.cycle_count} detected",
        f"Overall Severity: {cycles_output.overall_severity}",
        f"Summary: {cycles_output.summary}",
        "",
        "Detected Cycles:",
    ]

    for cycle in cycles_output.cycles:
        lines.append(f"  - {cycle.cycle_type}")
        lines.append(f"    Tables: {', '.join(cycle.tables)}")
        lines.append(
            f"    Business Value: {cycle.business_value}, Completeness: {cycle.completeness}"
        )
        lines.append(f"    Confidence: {cycle.confidence:.0%}")
        if cycle.explanation:
            lines.append(f"    Note: {cycle.explanation}")

    if cycles_output.recommendations:
        lines.append("")
        lines.append("Recommendations:")
        for rec in cycles_output.recommendations:
            lines.append(f"  - {rec}")

    return "\n".join(lines)
