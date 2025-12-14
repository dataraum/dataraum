"""Topological quality formatter.

Formats topological quality metrics into contextualized output for LLM consumption.
Groups related metrics into interpretation units:
- Structure: Betti numbers (components, cycles, voids)
- Cycles: Persistent cycles, anomalous cycles, business cycles
- Complexity: Structural complexity, persistent entropy, complexity trends
- Stability: Homological stability, bottleneck distance, changes

Usage:
    from dataraum_context.quality.formatting.topological import (
        format_topological_quality,
        format_structure_group,
        format_cycles_group,
        format_complexity_group,
        format_topological_stability_group,
    )

    result = format_topological_quality(
        betti_0=1,
        betti_1=3,
        structural_complexity=4,
        config=formatter_config,
    )
"""

from dataclasses import dataclass
from typing import Any

from dataraum_context.quality.formatting.config import FormatterConfig, get_default_config


@dataclass
class MetricContext:
    """Contextualized metric with severity and interpretation."""

    value: Any
    severity: str
    interpretation: str
    details: dict[str, Any] | None = None


@dataclass
class GroupContext:
    """Contextualized metric group."""

    group_name: str
    overall_severity: str
    interpretation: str
    metrics: dict[str, MetricContext]
    samples: list[Any] | None = None


# =============================================================================
# Interpretation Templates
# =============================================================================

STRUCTURE_INTERPRETATIONS = {
    "none": "Well-connected structure with expected topology",
    "low": "Minor structural irregularities detected",
    "moderate": "Moderate structural complexity or fragmentation",
    "high": "High structural complexity, multiple components or cycles",
    "severe": "Severe structural issues, fragmented or overly complex topology",
    "critical": "Critical structural problems, topology fundamentally broken",
}

CYCLES_INTERPRETATIONS = {
    "none": "No unexpected cycles detected in data relationships",
    "low": "Minor cyclical patterns, likely normal business flows",
    "moderate": "Moderate cycles detected, warrant review",
    "high": "Significant cyclical structures, potential data integrity concern",
    "severe": "Severe cyclical anomalies, possible circular dependencies or errors",
    "critical": "Critical cycle issues requiring immediate investigation",
}

COMPLEXITY_INTERPRETATIONS = {
    "none": "Complexity within expected bounds",
    "low": "Slightly elevated complexity, within acceptable range",
    "moderate": "Moderate complexity, may indicate rich relationships",
    "high": "High structural complexity, review recommended",
    "severe": "Severe complexity, potential over-connection or noise",
    "critical": "Critical complexity levels, likely data quality issue",
}

STABILITY_INTERPRETATIONS = {
    "none": "Topology is stable over time",
    "low": "Minor topological changes detected",
    "moderate": "Moderate topological evolution observed",
    "high": "Significant topological changes, structure evolving",
    "severe": "Severe topological instability, major structural changes",
    "critical": "Critical instability, fundamental topology changes",
}


# =============================================================================
# Group Formatters
# =============================================================================


def format_structure_group(
    betti_0: int | None = None,
    betti_1: int | None = None,
    betti_2: int | None = None,
    is_connected: bool | None = None,
    has_cycles: bool | None = None,
    orphaned_components: int | None = None,
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> GroupContext:
    """Format structural topology metrics into contextualized group.

    Args:
        betti_0: Connected components count
        betti_1: Cycles/holes count
        betti_2: Voids/cavities count
        is_connected: Whether graph is fully connected
        has_cycles: Whether cycles exist
        orphaned_components: Number of isolated components
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        GroupContext with structure analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Betti 0 - Connected components
    if betti_0 is not None:
        severity = config.get_severity("topology", "betti_0", betti_0, table_name)
        severities.append(severity)

        if betti_0 == 1:
            interp = "Fully connected structure (1 component)"
        elif betti_0 <= 3:
            interp = f"{betti_0} connected components (minor fragmentation)"
        else:
            interp = f"{betti_0} connected components (fragmented structure)"

        metrics["betti_0"] = MetricContext(
            value=betti_0,
            severity=severity,
            interpretation=interp,
            details={"is_connected": is_connected},
        )

    # Betti 1 - Cycles
    if betti_1 is not None:
        severity = config.get_severity("topology", "betti_1", betti_1, table_name)
        severities.append(severity)

        if betti_1 == 0:
            interp = "No topological cycles (tree-like structure)"
        elif betti_1 <= 2:
            interp = f"{betti_1} topological cycle(s) detected"
        else:
            interp = f"{betti_1} topological cycles (complex loop structure)"

        metrics["betti_1"] = MetricContext(
            value=betti_1,
            severity=severity,
            interpretation=interp,
            details={"has_cycles": has_cycles},
        )

    # Betti 2 - Voids (usually 0 in data contexts)
    if betti_2 is not None and betti_2 > 0:
        # Voids are unusual in typical data, flag if present
        severity = "moderate" if betti_2 <= 2 else "high"
        severities.append(severity)

        metrics["betti_2"] = MetricContext(
            value=betti_2,
            severity=severity,
            interpretation=f"{betti_2} higher-dimensional void(s) detected (unusual)",
            details=None,
        )

    # Orphaned components
    if orphaned_components is not None:
        severity = config.get_severity(
            "topology", "orphaned_components", orphaned_components, table_name
        )
        severities.append(severity)

        if orphaned_components == 0:
            interp = "No orphaned components"
        elif orphaned_components <= 2:
            interp = f"{orphaned_components} orphaned component(s)"
        else:
            interp = f"{orphaned_components} orphaned components (disconnected data)"

        metrics["orphaned_components"] = MetricContext(
            value=orphaned_components,
            severity=severity,
            interpretation=interp,
            details=None,
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="structure",
        overall_severity=overall_severity,
        interpretation=STRUCTURE_INTERPRETATIONS.get(overall_severity, "Unknown structure status"),
        metrics=metrics,
    )


def format_cycles_group(
    cycle_count: int | None = None,
    anomalous_cycle_count: int | None = None,
    cycles: list[dict[str, Any]] | None = None,
    anomalous_cycles: list[dict[str, Any]] | None = None,
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> GroupContext:
    """Format cycle detection metrics into contextualized group.

    Args:
        cycle_count: Total persistent cycles detected
        anomalous_cycle_count: Cycles flagged as anomalous
        cycles: Cycle details
        anomalous_cycles: Anomalous cycle details
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        GroupContext with cycle analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Total cycles
    if cycle_count is not None:
        severity = config.get_severity("topology", "cycle_count", cycle_count, table_name)
        severities.append(severity)

        if cycle_count == 0:
            interp = "No persistent cycles detected"
        elif cycle_count <= 3:
            interp = f"{cycle_count} persistent cycle(s) detected"
        else:
            interp = f"{cycle_count} persistent cycles (complex flow patterns)"

        # Format cycle samples
        cycle_details = None
        if cycles:
            cycle_details = [
                {
                    "type": c.get("cycle_type"),
                    "columns": c.get("involved_columns", [])[:3],
                    "persistence": c.get("persistence"),
                }
                for c in cycles[:3]
            ]

        metrics["cycle_count"] = MetricContext(
            value=cycle_count,
            severity=severity,
            interpretation=interp,
            details={"cycles": cycle_details} if cycle_details else None,
        )

    # Anomalous cycles
    if anomalous_cycle_count is not None:
        severity = config.get_severity(
            "topology", "anomalous_cycle_count", anomalous_cycle_count, table_name
        )
        severities.append(severity)

        if anomalous_cycle_count == 0:
            interp = "No anomalous cycles"
        elif anomalous_cycle_count == 1:
            interp = "1 anomalous cycle detected - requires investigation"
        else:
            interp = f"{anomalous_cycle_count} anomalous cycles - review required"

        # Format anomalous cycle samples
        anomaly_details = None
        if anomalous_cycles:
            anomaly_details = [
                {
                    "type": c.get("cycle_type"),
                    "reason": c.get("anomaly_reason"),
                    "columns": c.get("involved_columns", [])[:3],
                }
                for c in anomalous_cycles[:3]
            ]

        metrics["anomalous_cycle_count"] = MetricContext(
            value=anomalous_cycle_count,
            severity=severity,
            interpretation=interp,
            details={"anomalous_cycles": anomaly_details} if anomaly_details else None,
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="cycles",
        overall_severity=overall_severity,
        interpretation=CYCLES_INTERPRETATIONS.get(overall_severity, "Unknown cycles status"),
        metrics=metrics,
    )


def format_complexity_group(
    structural_complexity: int | None = None,
    persistent_entropy: float | None = None,
    complexity_z_score: float | None = None,
    complexity_within_bounds: bool | None = None,
    complexity_trend: str | None = None,
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> GroupContext:
    """Format complexity metrics into contextualized group.

    Args:
        structural_complexity: Total complexity (sum of Betti numbers)
        persistent_entropy: Entropy of persistence diagram
        complexity_z_score: How many std devs from historical mean
        complexity_within_bounds: Whether complexity is acceptable
        complexity_trend: 'increasing', 'decreasing', 'stable'
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        GroupContext with complexity analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Structural complexity
    if structural_complexity is not None:
        severity = config.get_severity(
            "topology", "structural_complexity", structural_complexity, table_name
        )
        severities.append(severity)

        if structural_complexity <= 2:
            interp = f"Low structural complexity ({structural_complexity})"
        elif structural_complexity <= 5:
            interp = f"Moderate structural complexity ({structural_complexity})"
        elif structural_complexity <= 10:
            interp = f"High structural complexity ({structural_complexity})"
        else:
            interp = f"Very high structural complexity ({structural_complexity})"

        metrics["structural_complexity"] = MetricContext(
            value=structural_complexity,
            severity=severity,
            interpretation=interp,
            details={
                "within_bounds": complexity_within_bounds,
                "trend": complexity_trend,
            },
        )

    # Persistent entropy
    if persistent_entropy is not None:
        # Entropy is informational - higher can indicate richer structure
        if persistent_entropy < 0.5:
            entropy_interp = "Low persistence entropy (simple structure)"
        elif persistent_entropy < 1.5:
            entropy_interp = f"Moderate persistence entropy ({persistent_entropy:.2f})"
        else:
            entropy_interp = f"High persistence entropy ({persistent_entropy:.2f}) - rich topology"

        metrics["persistent_entropy"] = MetricContext(
            value=persistent_entropy,
            severity="none",  # Informational
            interpretation=entropy_interp,
            details=None,
        )

    # Complexity z-score (deviation from historical)
    if complexity_z_score is not None:
        abs_z = abs(complexity_z_score)
        if abs_z < 1:
            severity = "none"
            interp = f"Complexity within normal range (z={complexity_z_score:.2f})"
        elif abs_z < 2:
            severity = "low"
            direction = "higher" if complexity_z_score > 0 else "lower"
            interp = f"Complexity slightly {direction} than usual (z={complexity_z_score:.2f})"
        elif abs_z < 3:
            severity = "moderate"
            direction = "higher" if complexity_z_score > 0 else "lower"
            interp = f"Complexity notably {direction} than usual (z={complexity_z_score:.2f})"
        else:
            severity = "high"
            direction = "higher" if complexity_z_score > 0 else "lower"
            interp = f"Complexity significantly {direction} than usual (z={complexity_z_score:.2f})"

        severities.append(severity)

        metrics["complexity_z_score"] = MetricContext(
            value=complexity_z_score,
            severity=severity,
            interpretation=interp,
            details=None,
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="complexity",
        overall_severity=overall_severity,
        interpretation=COMPLEXITY_INTERPRETATIONS.get(
            overall_severity, "Unknown complexity status"
        ),
        metrics=metrics,
    )


def format_topological_stability_group(
    bottleneck_distance: float | None = None,
    is_stable: bool | None = None,
    stability_level: str | None = None,
    components_added: int | None = None,
    components_removed: int | None = None,
    cycles_added: int | None = None,
    cycles_removed: int | None = None,
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> GroupContext:
    """Format topological stability metrics into contextualized group.

    Args:
        bottleneck_distance: Distance between persistence diagrams
        is_stable: Whether topology is considered stable
        stability_level: 'stable', 'minor_changes', 'significant_changes', 'unstable'
        components_added: Components added since last analysis
        components_removed: Components removed since last analysis
        cycles_added: Cycles added since last analysis
        cycles_removed: Cycles removed since last analysis
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        GroupContext with stability analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Bottleneck distance
    if bottleneck_distance is not None:
        severity = config.get_severity(
            "topology", "bottleneck_distance", bottleneck_distance, table_name
        )
        severities.append(severity)

        if bottleneck_distance < 0.05:
            interp = f"Very stable topology (distance={bottleneck_distance:.3f})"
        elif bottleneck_distance < 0.1:
            interp = f"Stable topology (distance={bottleneck_distance:.3f})"
        elif bottleneck_distance < 0.2:
            interp = f"Minor topological changes (distance={bottleneck_distance:.3f})"
        else:
            interp = f"Significant topological changes (distance={bottleneck_distance:.3f})"

        metrics["bottleneck_distance"] = MetricContext(
            value=bottleneck_distance,
            severity=severity,
            interpretation=interp,
            details={
                "is_stable": is_stable,
                "stability_level": stability_level,
            },
        )

    # Component changes
    total_component_changes = (components_added or 0) + (components_removed or 0)
    if total_component_changes > 0:
        if total_component_changes <= 2:
            severity = "low"
        elif total_component_changes <= 5:
            severity = "moderate"
        else:
            severity = "high"
        severities.append(severity)

        parts = []
        if components_added:
            parts.append(f"+{components_added} added")
        if components_removed:
            parts.append(f"-{components_removed} removed")

        metrics["component_changes"] = MetricContext(
            value=total_component_changes,
            severity=severity,
            interpretation=f"Component changes: {', '.join(parts)}",
            details={
                "added": components_added,
                "removed": components_removed,
            },
        )

    # Cycle changes
    total_cycle_changes = (cycles_added or 0) + (cycles_removed or 0)
    if total_cycle_changes > 0:
        if total_cycle_changes <= 2:
            severity = "low"
        elif total_cycle_changes <= 5:
            severity = "moderate"
        else:
            severity = "high"
        severities.append(severity)

        parts = []
        if cycles_added:
            parts.append(f"+{cycles_added} added")
        if cycles_removed:
            parts.append(f"-{cycles_removed} removed")

        metrics["cycle_changes"] = MetricContext(
            value=total_cycle_changes,
            severity=severity,
            interpretation=f"Cycle changes: {', '.join(parts)}",
            details={
                "added": cycles_added,
                "removed": cycles_removed,
            },
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="topological_stability",
        overall_severity=overall_severity,
        interpretation=STABILITY_INTERPRETATIONS.get(overall_severity, "Unknown stability status"),
        metrics=metrics,
    )


# =============================================================================
# Main Formatter
# =============================================================================


def format_topological_quality(
    # Structure metrics
    betti_0: int | None = None,
    betti_1: int | None = None,
    betti_2: int | None = None,
    is_connected: bool | None = None,
    has_cycles: bool | None = None,
    orphaned_components: int | None = None,
    # Cycle metrics
    cycle_count: int | None = None,
    anomalous_cycle_count: int | None = None,
    cycles: list[dict[str, Any]] | None = None,
    anomalous_cycles: list[dict[str, Any]] | None = None,
    # Complexity metrics
    structural_complexity: int | None = None,
    persistent_entropy: float | None = None,
    complexity_z_score: float | None = None,
    complexity_within_bounds: bool | None = None,
    complexity_trend: str | None = None,
    # Stability metrics
    bottleneck_distance: float | None = None,
    is_stable: bool | None = None,
    stability_level: str | None = None,
    components_added: int | None = None,
    components_removed: int | None = None,
    cycles_added: int | None = None,
    cycles_removed: int | None = None,
    # Anomalies
    has_anomalies: bool | None = None,
    anomalies: list[dict[str, Any]] | None = None,
    quality_warnings: list[str] | None = None,
    # Configuration
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> dict[str, Any]:
    """Format all topological quality metrics into contextualized output.

    This is the main entry point for topological formatting. It groups
    related metrics and produces a structured output suitable for LLM
    consumption or human review.

    Args:
        betti_0: Connected components count
        betti_1: Cycles/holes count
        betti_2: Voids count
        is_connected: Whether fully connected
        has_cycles: Whether cycles exist
        orphaned_components: Isolated component count
        cycle_count: Persistent cycle count
        anomalous_cycle_count: Anomalous cycle count
        cycles: Cycle details
        anomalous_cycles: Anomalous cycle details
        structural_complexity: Total complexity
        persistent_entropy: Persistence entropy
        complexity_z_score: Historical deviation
        complexity_within_bounds: Bounds check
        complexity_trend: Trend direction
        bottleneck_distance: Stability distance
        is_stable: Stability flag
        stability_level: Stability category
        components_added: Components added
        components_removed: Components removed
        cycles_added: Cycles added
        cycles_removed: Cycles removed
        has_anomalies: Anomaly flag
        anomalies: Anomaly details
        quality_warnings: Warning messages
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        Dict with contextualized topological quality assessment
    """
    config = config or get_default_config()

    # Format each group
    structure = format_structure_group(
        betti_0=betti_0,
        betti_1=betti_1,
        betti_2=betti_2,
        is_connected=is_connected,
        has_cycles=has_cycles,
        orphaned_components=orphaned_components,
        config=config,
        table_name=table_name,
    )

    cycles_group = format_cycles_group(
        cycle_count=cycle_count,
        anomalous_cycle_count=anomalous_cycle_count,
        cycles=cycles,
        anomalous_cycles=anomalous_cycles,
        config=config,
        table_name=table_name,
    )

    complexity = format_complexity_group(
        structural_complexity=structural_complexity,
        persistent_entropy=persistent_entropy,
        complexity_z_score=complexity_z_score,
        complexity_within_bounds=complexity_within_bounds,
        complexity_trend=complexity_trend,
        config=config,
        table_name=table_name,
    )

    stability = format_topological_stability_group(
        bottleneck_distance=bottleneck_distance,
        is_stable=is_stable,
        stability_level=stability_level,
        components_added=components_added,
        components_removed=components_removed,
        cycles_added=cycles_added,
        cycles_removed=cycles_removed,
        config=config,
        table_name=table_name,
    )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    all_severities = [
        structure.overall_severity,
        cycles_group.overall_severity,
        complexity.overall_severity,
        stability.overall_severity,
    ]
    overall_severity = max(
        all_severities, key=lambda s: severity_order.index(s) if s in severity_order else 0
    )

    # Include anomalies if present
    anomaly_summary = None
    if has_anomalies and anomalies:
        anomaly_summary = [
            {
                "type": a.get("anomaly_type"),
                "severity": a.get("severity"),
                "description": a.get("description"),
            }
            for a in anomalies[:5]
        ]

    # Build output
    tq_result: dict[str, Any] = {
        "overall_severity": overall_severity,
        "groups": {
            "structure": _group_to_dict(structure),
            "cycles": _group_to_dict(cycles_group),
            "complexity": _group_to_dict(complexity),
            "stability": _group_to_dict(stability),
        },
        "table_name": table_name,
    }

    if anomaly_summary:
        tq_result["anomalies"] = anomaly_summary

    if quality_warnings:
        tq_result["warnings"] = quality_warnings[:5]

    return {"topological_quality": tq_result}


def _group_to_dict(group: GroupContext) -> dict[str, Any]:
    """Convert GroupContext to dictionary."""
    result: dict[str, Any] = {
        "severity": group.overall_severity,
        "interpretation": group.interpretation,
        "metrics": {},
    }

    for name, metric in group.metrics.items():
        result["metrics"][name] = {
            "value": metric.value,
            "severity": metric.severity,
            "interpretation": metric.interpretation,
        }
        if metric.details:
            result["metrics"][name]["details"] = metric.details

    if group.samples:
        result["samples"] = group.samples

    return result
