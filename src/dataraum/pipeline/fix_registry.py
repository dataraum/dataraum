"""Fix handler registry — standalone registry for config-level fix handlers.

Decouples fix handlers from pipeline phases. Each handler declares which
action it handles, which config file it writes, and which phase to re-run.

Detectors declare fixable_actions (what they can propose).
Handlers register here (how to apply the fix).
The scheduler and gate handler query this registry — no phase scanning needed.

Handler categories:
  accept_finding — Generic "reviewed, expected" pattern.  Writes to
      entropy/thresholds.yaml so the detector reads accepted_columns and
      returns a floor score.  Works for benford, outlier_rate, null_ratio.
  document_business_meaning — Writes semantic overrides (business_name,
      entity_type, description) to phases/semantic.yaml.
  declare_unit — Writes unit declaration to phases/semantic.yaml.
  confirm_relationship — Confirms a detected relationship in
      phases/relationships.yaml.
  resolve_join_ambiguity — Sets preferred join path in
      phases/relationships.yaml.
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
            action="accept_finding",
            handler=_handle_accept_finding,
            phase_name="quality_review",
        )
    )
    registry.register(
        FixHandler(
            action="document_business_meaning",
            handler=_handle_document_business_meaning,
            phase_name="semantic",
        )
    )
    registry.register(
        FixHandler(
            action="declare_unit",
            handler=_handle_declare_unit,
            phase_name="semantic",
        )
    )
    registry.register(
        FixHandler(
            action="confirm_relationship",
            handler=_handle_confirm_relationship,
            phase_name="relationships",
        )
    )
    registry.register(
        FixHandler(
            action="resolve_join_ambiguity",
            handler=_handle_resolve_join_ambiguity,
            phase_name="relationships",
        )
    )


# ---------------------------------------------------------------------------
# Built-in handlers
# ---------------------------------------------------------------------------


def _handle_accept_finding(
    fix_input: FixInput, config: dict[str, Any]
) -> FixResult:
    """Mark an entropy finding as reviewed and accepted.

    Appends each affected column to the detector's ``accepted_columns``
    list in the entropy thresholds config.  On next measurement the
    detector reads this list and returns ``score_accepted`` instead of
    the computed score.

    Parameters in fix_input:
        detector_id (str): Which detector config section to write to.
        reason (str, optional): Why the finding was accepted.
    """
    detector_id = fix_input.parameters.get("detector_id", "unknown")
    patches: list[ConfigPatch] = []
    for col in fix_input.affected_columns:
        patches.append(
            ConfigPatch(
                config_path="entropy/thresholds.yaml",
                operation="append",
                key_path=["detectors", detector_id, "accepted_columns"],
                value=col,
                reason=fix_input.interpretation
                or f"Accepted {detector_id} finding for {col}",
            )
        )

    return FixResult(
        config_patches=patches,
        requires_rerun="quality_review",  # re-measure so scores update this run
        summary=f"Accepted {detector_id} findings: {', '.join(fix_input.affected_columns)}",
    )


def _handle_document_business_meaning(
    fix_input: FixInput, config: dict[str, Any]
) -> FixResult:
    """Write business meaning overrides to semantic phase config.

    Parameters in fix_input:
        business_name (str, optional): Human-readable name.
        entity_type (str, optional): Entity classification.
        description (str, optional): Business description.
    """
    patches: list[ConfigPatch] = []
    fields = ["business_name", "entity_type", "description"]

    for col in fix_input.affected_columns:
        override: dict[str, str] = {}
        for field in fields:
            value = fix_input.parameters.get(field)
            if value:
                override[field] = value

        if override:
            patches.append(
                ConfigPatch(
                    config_path="phases/semantic.yaml",
                    operation="merge",
                    key_path=["overrides", "business_meaning", col],
                    value=override,
                    reason=fix_input.interpretation
                    or f"Document business meaning for {col}",
                )
            )

    return FixResult(
        config_patches=patches,
        requires_rerun="semantic",
        summary=f"Documented business meaning: {', '.join(fix_input.affected_columns)}",
    )


def _handle_declare_unit(
    fix_input: FixInput, config: dict[str, Any]
) -> FixResult:
    """Write unit declaration to semantic phase config.

    Parameters in fix_input:
        unit (str): The unit to declare (e.g. "EUR", "kg", "dimensionless").
        unit_source_column (str, optional): Column that defines the unit.
    """
    patches: list[ConfigPatch] = []
    unit = fix_input.parameters.get("unit", "")
    unit_source = fix_input.parameters.get("unit_source_column")

    for col in fix_input.affected_columns:
        override: dict[str, Any] = {"unit": unit}
        if unit_source:
            override["unit_source_column"] = unit_source
        patches.append(
            ConfigPatch(
                config_path="phases/semantic.yaml",
                operation="merge",
                key_path=["overrides", "units", col],
                value=override,
                reason=fix_input.interpretation or f"Declare unit '{unit}' for {col}",
            )
        )

    return FixResult(
        config_patches=patches,
        requires_rerun="semantic",
        summary=f"Declared unit '{unit}': {', '.join(fix_input.affected_columns)}",
    )


def _handle_confirm_relationship(
    fix_input: FixInput, config: dict[str, Any]
) -> FixResult:
    """Confirm a detected relationship in relationships phase config.

    Parameters in fix_input:
        from_table (str): Source table.
        to_table (str): Target table.
        relationship_type (str, optional): Confirmed type (e.g. "foreign_key").
        cardinality (str, optional): Confirmed cardinality (e.g. "many_to_one").
    """
    patches: list[ConfigPatch] = []
    from_table = fix_input.parameters.get("from_table", "")
    to_table = fix_input.parameters.get("to_table", "")
    rel_key = f"{from_table}->{to_table}"

    confirmation: dict[str, Any] = {"confirmed": True}
    if fix_input.parameters.get("relationship_type"):
        confirmation["relationship_type"] = fix_input.parameters["relationship_type"]
    if fix_input.parameters.get("cardinality"):
        confirmation["cardinality"] = fix_input.parameters["cardinality"]

    patches.append(
        ConfigPatch(
            config_path="phases/relationships.yaml",
            operation="merge",
            key_path=["overrides", "confirmed_relationships", rel_key],
            value=confirmation,
            reason=fix_input.interpretation
            or f"Confirmed relationship {rel_key}",
        )
    )

    return FixResult(
        config_patches=patches,
        requires_rerun="relationships",
        summary=f"Confirmed relationship: {rel_key}",
    )


def _handle_resolve_join_ambiguity(
    fix_input: FixInput, config: dict[str, Any]
) -> FixResult:
    """Set preferred join path for ambiguous table connections.

    Parameters in fix_input:
        table (str): Source table with ambiguous paths.
        target_table (str): Target table.
        preferred_column (str): Column to use for the join.
    """
    patches: list[ConfigPatch] = []
    table = fix_input.parameters.get("table", "")
    target = fix_input.parameters.get("target_table", "")
    preferred_col = fix_input.parameters.get("preferred_column", "")
    path_key = f"{table}->{target}"

    patches.append(
        ConfigPatch(
            config_path="phases/relationships.yaml",
            operation="merge",
            key_path=["overrides", "preferred_joins", path_key],
            value={"column": preferred_col},
            reason=fix_input.interpretation
            or f"Resolved join ambiguity: {path_key} via {preferred_col}",
        )
    )

    return FixResult(
        config_patches=patches,
        requires_rerun="relationships",
        summary=f"Resolved join ambiguity: {path_key} via {preferred_col}",
    )
