"""Fiscal stability rules (deterministic, auditable).

Domain rules that provide deterministic, auditable outputs.
These complement the LLM's interpretive capabilities.
"""

from typing import Any


def assess_fiscal_stability(
    stability: Any,  # StabilityAnalysis from topological analysis
    temporal_context: dict[str, Any],
) -> dict[str, Any]:
    """Enhance stability analysis with fiscal period awareness.

    Distinguishes between:
    - Fiscal period effects (expected, recurring changes)
    - Structural changes (unexpected, permanent topology shifts)

    Args:
        stability: StabilityAnalysis object from topological analysis
        temporal_context: Dict with fiscal calendar information

    Returns:
        Dict with enhanced stability assessment including fiscal context
    """
    if stability is None:
        return {
            "stability_level": "unknown",
            "fiscal_context": None,
            "is_fiscal_period_effect": False,
            "pattern_type": "unknown",
        }

    # Extract fiscal calendar info
    current_period = temporal_context.get("current_fiscal_period")
    is_period_end = temporal_context.get("is_period_end", False)
    is_quarter_end = temporal_context.get("is_quarter_end", False)
    is_year_end = temporal_context.get("is_year_end", False)

    # Determine if changes are fiscal period effects
    fiscal_context = None
    is_fiscal_period_effect = False
    pattern_type = "structural_change"  # Default assumption

    stability_level = getattr(stability, "stability_level", "unknown")

    if is_year_end and stability_level in ["significant_changes", "unstable"]:
        fiscal_context = "fiscal_year_end_close"
        is_fiscal_period_effect = True
        pattern_type = "recurring_spike"

    elif is_quarter_end and stability_level in ["minor_changes", "significant_changes"]:
        fiscal_context = "quarter_end_close"
        is_fiscal_period_effect = True
        pattern_type = "recurring_spike"

    elif is_period_end and stability_level == "minor_changes":
        fiscal_context = "month_end_close"
        is_fiscal_period_effect = True
        pattern_type = "recurring_spike"

    elif stability_level in ["significant_changes", "unstable"]:
        fiscal_context = "mid_period"
        is_fiscal_period_effect = False
        pattern_type = "structural_change"

    # Generate interpretation
    interpretation = _interpret_fiscal_stability(
        stability_level, is_fiscal_period_effect, fiscal_context
    )

    return {
        "original_stability_level": stability_level,
        "fiscal_context": fiscal_context,
        "is_fiscal_period_effect": is_fiscal_period_effect,
        "pattern_type": pattern_type,
        "affected_periods": [current_period] if current_period else [],
        "components_added": getattr(stability, "components_added", 0),
        "components_removed": getattr(stability, "components_removed", 0),
        "cycles_added": getattr(stability, "cycles_added", 0),
        "cycles_removed": getattr(stability, "cycles_removed", 0),
        "interpretation": interpretation,
    }


def _interpret_fiscal_stability(
    stability_level: str, is_fiscal_effect: bool, fiscal_context: str | None
) -> str:
    """Generate human-readable interpretation of stability assessment."""
    if is_fiscal_effect:
        if fiscal_context == "fiscal_year_end_close":
            return (
                "Expected topology changes due to fiscal year-end close. "
                "Increased activity and relationship complexity is normal."
            )
        elif fiscal_context == "quarter_end_close":
            return (
                "Recurring topology changes due to quarter-end close. "
                "Period-end spikes are expected."
            )
        elif fiscal_context == "month_end_close":
            return "Minor topology changes due to month-end close. Normal recurring pattern."
        else:
            return "Changes appear related to fiscal period effects."
    else:
        if stability_level == "unstable":
            return (
                "ALERT: Significant structural changes detected outside normal fiscal periods. "
                "Investigate data quality or business process changes."
            )
        elif stability_level == "significant_changes":
            return (
                "WARNING: Notable structural changes detected mid-period. "
                "May indicate data quality issues or business changes."
            )
        else:
            return "Topology is stable with minor expected variations."
