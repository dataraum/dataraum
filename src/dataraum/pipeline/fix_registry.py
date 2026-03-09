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

from dataraum.core.logging import get_logger
from dataraum.pipeline.fixes import ConfigPatch, FixInput, FixResult

logger = get_logger(__name__)


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

    def validate(self) -> list[str]:
        """Cross-validate against detector and phase registries.

        Checks:
        1. Every fixable_action declared by a detector has a registered handler.
        2. Every handler's phase_name refers to a registered pipeline phase.

        Returns:
            List of warning messages (empty if all valid).
        """
        from dataraum.entropy.detectors.base import get_default_registry
        from dataraum.pipeline.registry import get_registry

        warnings: list[str] = []

        # Check: every detector fixable_action has a handler
        detector_registry = get_default_registry()
        for detector in detector_registry.get_all_detectors():
            for action in detector.fixable_actions:
                action_str = str(action)
                if action_str not in self._handlers:
                    warnings.append(
                        f"Detector '{detector.detector_id}' declares fixable "
                        f"action '{action_str}' but no handler is registered"
                    )

        # Check: every handler's phase_name is a registered phase
        phase_registry = get_registry()
        for action_name, entry in self._handlers.items():
            if entry.phase_name not in phase_registry:
                warnings.append(
                    f"Fix handler '{action_name}' targets phase "
                    f"'{entry.phase_name}' which is not registered"
                )

        return warnings


# ---------------------------------------------------------------------------
# Default registry with built-in handlers
# ---------------------------------------------------------------------------

_default_registry: FixRegistry | None = None


def get_default_fix_registry() -> FixRegistry:
    """Return the singleton fix registry, populating and validating on first call."""
    global _default_registry
    if _default_registry is None:
        _default_registry = FixRegistry()
        _register_builtin_handlers(_default_registry)
        for warning in _default_registry.validate():
            logger.warning("fix_registry_validation", message=warning)
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
