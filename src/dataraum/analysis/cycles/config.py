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


def get_domain_config(domain: str, vertical: str) -> dict[str, Any]:
    """Get domain-specific configuration.

    Args:
        domain: Domain name (financial, retail, manufacturing)
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Domain-specific configuration, or empty dict if not found
    """
    config = get_cycles_config(vertical)
    domains: dict[str, Any] = config.get("domains", {})
    result: dict[str, Any] = domains.get(domain, {})
    return result


def map_to_canonical_type(
    cycle_type: str, vertical: str
) -> tuple[str | None, bool]:
    """Map an LLM-returned cycle_type to a canonical vocabulary type.

    Handles aliases (e.g., "ar_cycle" -> "accounts_receivable") and
    case-insensitive matching.

    Args:
        cycle_type: The cycle type string from LLM output
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Tuple of (canonical_type, is_known_type):
        - canonical_type: The vocabulary key if matched, None otherwise
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

    # No match found
    return None, False


def format_cycle_vocabulary_for_context(
    domain: str | None = None, *, vertical: str
) -> str:
    """Format cycle vocabulary as readable context for the LLM.

    Args:
        domain: Optional domain to include domain-specific cycles
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

            lines.append("")

    # Domain-specific cycles
    if domain:
        domain_config = get_domain_config(domain, vertical)
        if domain_config:
            lines.append(f"## {domain.upper()} DOMAIN SPECIFICS")

            expected = domain_config.get("expected_cycles", [])
            if expected:
                lines.append(f"Expected cycles: {', '.join(expected)}")

            additional = domain_config.get("additional_cycles", {})
            if additional:
                lines.append("")
                lines.append("Additional domain cycles:")
                for cycle_name, cycle_def in additional.items():
                    description = cycle_def.get("description", "")
                    lines.append(f"  - {cycle_name}: {description}")

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
