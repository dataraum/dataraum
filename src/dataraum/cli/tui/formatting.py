"""Shared formatting utilities for TUI screens."""

from __future__ import annotations

from typing import Any


def escape_markup(text: str) -> str:
    """Escape ALL square brackets for Textual/Rich markup.

    Unlike rich.markup.escape which only escapes brackets that look like
    Rich tags (lowercase start), Textual's Content parser is case-insensitive.
    This escapes every ``[`` to prevent any markup interpretation.
    """
    return text.replace("[", "\\[")


def format_evidence_field(key: str, value: Any) -> str:
    """Format a single evidence field with human-readable label and valShouldue.

    Args:
        key: Evidence field name (e.g. "null_rate", "type_confidence")
        value: Field value (float, bool, int, list, dict, or str)

    Returns:
        Rich-markup formatted string for display
    """
    label = escape_markup(key.replace("_", " ").title())

    if isinstance(value, float):
        if key.endswith(("_rate", "_ratio", "_confidence", "confidence")):
            return f"[dim]{label}:[/dim] {value:.1%}"
        return f"[dim]{label}:[/dim] {value:.3f}"
    elif isinstance(value, bool):
        return f"[dim]{label}:[/dim] {'yes' if value else 'no'}"
    elif isinstance(value, int):
        return f"[dim]{label}:[/dim] {value:,}"
    elif isinstance(value, list):
        if not value:
            return f"[dim]{label}:[/dim] (none)"
        items = [str(v) for v in value[:5]]
        suffix = f" ... +{len(value) - 5}" if len(value) > 5 else ""
        return f"[dim]{label}:[/dim] {escape_markup(', '.join(items))}{suffix}"
    elif isinstance(value, dict):
        items = [f"{k}={v}" for k, v in list(value.items())[:3]]
        return f"[dim]{label}:[/dim] {escape_markup(', '.join(items))}"
    else:
        val_str = str(value)
        if len(val_str) > 80:
            val_str = val_str[:77] + "..."
        return f"[dim]{label}:[/dim] {escape_markup(val_str)}"


def format_score_color(score: float) -> str:
    """Return a Rich color name based on entropy score threshold.

    Args:
        score: Entropy score (0.0-1.0)

    Returns:
        Color name: "red", "yellow", or "green"
    """
    if score > 0.3:
        return "red"
    elif score > 0.15:
        return "yellow"
    return "green"


def format_priority_color(priority: str) -> str:
    """Return a Rich color name for action priority level.

    Args:
        priority: Priority string ("high", "medium", "low")

    Returns:
        Color name: "red", "yellow", or "green"
    """
    return {"high": "red", "medium": "yellow", "low": "green"}.get(str(priority).lower(), "white")
