"""Fix handler registry — standalone registry for config-level fix handlers.

Decouples fix handlers from pipeline phases. Each handler declares which
action it handles, which config file it writes, and which phase to re-run.

Detectors declare fixable_actions (what they can propose).
Handlers register here (how to apply the fix).
The scheduler and gate handler query this registry — no phase scanning needed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dataraum.pipeline.fixes import ConfigPatch, FixInput, FixResult


@dataclass
class FixHandler:
    """A registered fix handler with its metadata."""

    action: str  # FixAction enum value (str-compatible)
    handler: Callable[[FixInput, dict[str, Any]], FixResult]
    phase_name: str  # which phase config to pass and which to re-run


class FixRegistry:
    """Registry mapping action names to fix handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, FixHandler] = {}

    def register(self, entry: FixHandler) -> None:
        """Register a fix handler for an action."""
        self._handlers[str(entry.action)] = entry

    def find(self, action_name: str) -> FixHandler | None:
        """Look up a handler by action name."""
        return self._handlers.get(action_name)

    def actions_for_phase(self, phase_name: str) -> list[str]:
        """Return action names handled by a given phase."""
        return [
            name
            for name, entry in self._handlers.items()
            if entry.phase_name == phase_name
        ]

    @property
    def all_actions(self) -> dict[str, str]:
        """Return action_name -> phase_name for all registered handlers."""
        return {name: entry.phase_name for name, entry in self._handlers.items()}


# ---------------------------------------------------------------------------
# Default registry with built-in handlers
# ---------------------------------------------------------------------------

_default_registry: FixRegistry | None = None


def get_default_fix_registry() -> FixRegistry:
    """Return the singleton fix registry, populating on first call."""
    global _default_registry
    if _default_registry is None:
        _default_registry = FixRegistry()
        _register_builtin_handlers(_default_registry)
    return _default_registry


def _register_builtin_handlers(registry: FixRegistry) -> None:
    """Register all built-in fix handlers."""
    registry.register(
        FixHandler(
            action="transform_exclude_outliers",
            handler=_handle_exclude_outliers,
            phase_name="statistical_quality",
        )
    )


# ---------------------------------------------------------------------------
# Built-in handlers
# ---------------------------------------------------------------------------


def _handle_exclude_outliers(
    fix_input: FixInput, config: dict[str, Any]
) -> FixResult:
    """Write exclude_outlier_columns to statistical_quality config.

    Appends each affected column to the exclusion list so that on re-run
    the phase skips outlier detection for those columns.
    """
    patches: list[ConfigPatch] = []
    for col in fix_input.affected_columns:
        patches.append(
            ConfigPatch(
                config_path="phases/statistical_quality.yaml",
                operation="append",
                key_path=["exclude_outlier_columns"],
                value=col,
                reason=fix_input.interpretation or f"Exclude outliers for {col}",
            )
        )

    return FixResult(
        config_patches=patches,
        requires_rerun="statistical_quality",
        summary=f"Excluded outlier columns: {', '.join(fix_input.affected_columns)}",
    )
