"""Configuration loader for business cycle detection.

Loads the cycle vocabulary from ``config/verticals/<vertical>/cycles.yaml``,
layered with workspace ``cycle`` overlay teach rows (DAT-455) — the same
OntologyLoader dual-path pattern validation uses: the production path is
overlay-aware (teach rows upsert over the shipped vertical's ``cycle_types``;
a *framed* vertical with no on-disk file resolves overlay-only), while an
explicit ``verticals_dir`` (tests / fixtures) reads raw YAML and bypasses the
overlay.

The vocabulary IS the declared set for the cycle lifecycle family: each
``cycle_types`` key (canonical name + stages/aliases/completion indicators) is
declared as one ``cycle`` lifecycle artifact, then bound + executed against the
workspace. The engine induces nothing — declares come from the vertical now;
user declares arrive via frame-2 teach rows.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from dataraum.core.logging import get_logger

logger = get_logger(__name__)


def get_cycles_config(vertical: str, verticals_dir: Path | None = None) -> dict[str, Any]:
    """Load a vertical's cycles config, layered with ``cycle`` overlay rows.

    Production path (``verticals_dir`` is ``None``): read the shipped vertical's
    ``cycles.yaml`` (empty base when the vertical is framed — declared via the
    cockpit, no on-disk file), then merge active ``cycle`` overlay rows via
    :func:`dataraum.core.overlay.apply_overlay` (upsert by cycle name into
    ``cycle_types``). An unknown vertical resolves to an EMPTY dict, never
    raises — "no declared cycles" is a loud, explicit outcome at the phase
    tier, not a loader crash.

    Test path (explicit ``verticals_dir``): read
    ``<verticals_dir>/<vertical>/cycles.yaml`` raw, bypassing the overlay —
    deterministic for unit tests (mirrors ``OntologyLoader`` /
    ``load_all_validation_specs``).

    Args:
        vertical: Vertical name (e.g. ``'finance'``).
        verticals_dir: Root verticals directory override (tests only).

    Returns:
        The cycles config dict (``{"cycle_types": {...}, "analysis_hints": ...}``),
        or an empty dict when neither file nor overlay declares anything.
    """
    if verticals_dir is not None:
        config_path = verticals_dir / vertical / "cycles.yaml"
        if not config_path.is_file():
            return {}
        with open(config_path) as f:
            return yaml.safe_load(f) or {}

    from dataraum.core.config import get_config_file
    from dataraum.core.overlay import apply_overlay

    relative_path = f"verticals/{vertical}/cycles.yaml"
    base: dict[str, Any] = {}
    try:
        path = get_config_file(relative_path)
    except FileNotFoundError:
        # Framed vertical (no on-disk file) or a vertical without a shipped
        # cycle vocabulary — the overlay rows ARE the declared set.
        path = None
    if path is not None:
        with open(path) as f:
            base = yaml.safe_load(f) or {}
    return apply_overlay(relative_path, base)


def get_cycle_types(vertical: str, verticals_dir: Path | None = None) -> dict[str, Any]:
    """Get cycle type definitions (overlay-aware).

    Args:
        vertical: Vertical name (e.g. 'finance').
        verticals_dir: Root verticals directory override (tests only).

    Returns:
        Dictionary of cycle_type_name -> cycle definition.
    """
    config = get_cycles_config(vertical, verticals_dir)
    result: dict[str, Any] = config.get("cycle_types") or {}
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
