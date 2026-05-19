"""Configuration loader for business cycle detection.

Loads domain vocabulary from config/verticals/<vertical>/cycles.yaml
to enhance cycle detection.
"""

from __future__ import annotations

from typing import Any

import yaml


def get_cycles_config(vertical: str) -> dict[str, Any]:
    """Load the cycles configuration for a vertical.

    Args:
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Configuration dictionary, or empty dict if not found
    """
    from dataraum.core.vertical import VerticalConfig

    config_path = VerticalConfig(vertical).cycles_path
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def get_cycle_types(vertical: str) -> dict[str, Any]:
    """Get cycle type definitions.

    Args:
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Dictionary of cycle_type_name -> cycle definition
    """
    config = get_cycles_config(vertical)
    result: dict[str, Any] = config.get("cycle_types", {})
    return result


def map_to_canonical_type(cycle_type: str, vertical: str) -> tuple[str | None, bool]:
    """Map an LLM-returned cycle_type to a canonical vocabulary type.

    Handles aliases (e.g., "ar_cycle" -> "accounts_receivable") and
    case-insensitive matching. For unknown types, preserves the LLM's
    type as the canonical type so it can still participate in health
    scoring with universal validations.

    Args:
        cycle_type: The cycle type string from LLM output
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Tuple of (canonical_type, is_known_type):
        - canonical_type: The vocabulary key if matched, or the normalized
          LLM type if not. None only if cycle_type is empty.
        - is_known_type: True if the type matches vocabulary
    """
    if not cycle_type:
        return None, False

    cycle_types = get_cycle_types(vertical)
    cycle_type_lower = cycle_type.lower().strip()

    # Direct match (case-insensitive)
    for canonical in cycle_types:
        if cycle_type_lower == canonical.lower():
            return canonical, True

    # Check aliases
    for canonical, config in cycle_types.items():
        aliases = config.get("aliases", [])
        for alias in aliases:
            if cycle_type_lower == alias.lower():
                return canonical, True

    # No vocabulary match — preserve the LLM's type as canonical so the cycle
    # can still be health-scored using universal validations.
    return cycle_type_lower, False


def format_cycle_vocabulary_for_context(*, vertical: str) -> str:
    """Format cycle vocabulary as readable context for the LLM.

    Args:
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Formatted string suitable for LLM context
    """
    lines = []
    config = get_cycles_config(vertical)

    if not config:
        return ""

    # Cycle types
    cycle_types = config.get("cycle_types", {})
    if cycle_types:
        lines.append("## KNOWN BUSINESS CYCLE TYPES")
        lines.append("")

        for cycle_name, cycle_def in cycle_types.items():
            business_value = cycle_def.get("business_value", "medium")
            description = cycle_def.get("description", "")
            aliases = cycle_def.get("aliases", [])

            lines.append(f"### {cycle_name} (value: {business_value})")
            lines.append(f"Description: {description}")
            if aliases:
                lines.append(f"Also known as: {', '.join(aliases)}")

            # Stages
            stages = cycle_def.get("typical_stages", [])
            if stages:
                lines.append("Typical stages:")
                for stage in stages:
                    stage_name = stage.get("name", "")
                    indicators = stage.get("indicators", [])
                    lines.append(
                        f"  {stage['order']}. {stage_name} - indicators: {', '.join(indicators)}"
                    )

            # Completion indicators
            completion = cycle_def.get("completion_indicators", [])
            if completion:
                lines.append(f"Completion indicators: {', '.join(completion)}")

            # Downstream cycles
            feeds_into = cycle_def.get("feeds_into", [])
            if feeds_into:
                lines.append(f"Feeds into: {', '.join(feeds_into)}")

            lines.append("")

    # Analysis hints
    hints = config.get("analysis_hints", {})
    if hints:
        lines.append("## ANALYSIS GUIDANCE")

        strong = hints.get("strong_indicators", [])
        if strong:
            lines.append("Strong indicators of cycles:")
            for hint in strong:
                lines.append(f"  - {hint}")

        health = hints.get("health_factors", [])
        if health:
            lines.append("Healthy cycle indicators:")
            for hint in health:
                lines.append(f"  - {hint}")

        warnings = hints.get("warning_signs", [])
        if warnings:
            lines.append("Warning signs:")
            for hint in warnings:
                lines.append(f"  - {hint}")

    return "\n".join(lines)
