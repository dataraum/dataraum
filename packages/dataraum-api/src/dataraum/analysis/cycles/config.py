"""Configuration loader for business cycle detection.

Loads domain vocabulary from config/cycles/ to enhance cycle detection.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# Module-level cache
_CYCLE_CONFIG_CACHE: dict[str, Any] | None = None


def get_cycles_config() -> dict[str, Any]:
    """Load the cycles configuration.

    Searches for config/cycles/cycle_vocabulary.yaml in:
    1. Current working directory
    2. Project root (relative to this file)

    Returns:
        Configuration dictionary, or empty dict if not found
    """
    global _CYCLE_CONFIG_CACHE

    if _CYCLE_CONFIG_CACHE is not None:
        return _CYCLE_CONFIG_CACHE

    config_filename = "cycle_vocabulary.yaml"

    # Search paths (config/ is in packages/api/, 5 levels up from this file)
    search_paths = [
        Path.cwd() / "config" / "cycles" / config_filename,
        Path(__file__).parent.parent.parent.parent.parent / "config" / "cycles" / config_filename,
    ]

    for config_path in search_paths:
        if config_path.exists():
            with open(config_path) as f:
                _CYCLE_CONFIG_CACHE = yaml.safe_load(f) or {}
                return _CYCLE_CONFIG_CACHE

    # Not found - return empty config
    _CYCLE_CONFIG_CACHE = {}
    return _CYCLE_CONFIG_CACHE


def clear_config_cache() -> None:
    """Clear the configuration cache (useful for testing)."""
    global _CYCLE_CONFIG_CACHE
    _CYCLE_CONFIG_CACHE = None


def get_cycle_types() -> dict[str, Any]:
    """Get cycle type definitions.

    Returns:
        Dictionary of cycle_type_name -> cycle definition
    """
    config = get_cycles_config()
    result: dict[str, Any] = config.get("cycle_types", {})
    return result


def get_completion_indicators() -> dict[str, list[str]]:
    """Get completion indicator patterns.

    Returns:
        Dictionary of indicator_category -> list of indicator values
    """
    config = get_cycles_config()
    result: dict[str, list[str]] = config.get("completion_indicators", {})
    return result


def get_entity_roles() -> dict[str, list[str]]:
    """Get entity role mappings.

    Returns:
        Dictionary of role_type -> list of entity types
    """
    config = get_cycles_config()
    result: dict[str, list[str]] = config.get("entity_roles", {})
    return result


def get_domain_config(domain: str = "financial") -> dict[str, Any]:
    """Get domain-specific configuration.

    Args:
        domain: Domain name (financial, retail, manufacturing)

    Returns:
        Domain-specific configuration, or empty dict if not found
    """
    config = get_cycles_config()
    domains: dict[str, Any] = config.get("domains", {})
    result: dict[str, Any] = domains.get(domain, {})
    return result


def get_analysis_hints() -> dict[str, list[str]]:
    """Get analysis hints for the agent.

    Returns:
        Dictionary of hint_category -> list of hints
    """
    config = get_cycles_config()
    result: dict[str, list[str]] = config.get("analysis_hints", {})
    return result


def map_to_canonical_type(cycle_type: str) -> tuple[str | None, bool]:
    """Map an LLM-returned cycle_type to a canonical vocabulary type.

    Handles aliases (e.g., "ar_cycle" -> "accounts_receivable") and
    case-insensitive matching.

    Args:
        cycle_type: The cycle type string from LLM output

    Returns:
        Tuple of (canonical_type, is_known_type):
        - canonical_type: The vocabulary key if matched, None otherwise
        - is_known_type: True if the type matches vocabulary
    """
    if not cycle_type:
        return None, False

    cycle_types = get_cycle_types()
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


def format_cycle_vocabulary_for_context(domain: str | None = None) -> str:
    """Format cycle vocabulary as readable context for the LLM.

    Args:
        domain: Optional domain to include domain-specific cycles

    Returns:
        Formatted string suitable for LLM context
    """
    lines = []
    config = get_cycles_config()

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
        domain_config = get_domain_config(domain)
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
