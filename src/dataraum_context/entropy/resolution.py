"""Resolution tracking and cascade management.

This module handles:
- Finding resolution cascades (single fixes that improve multiple dimensions)
- Prioritizing resolutions by impact-to-effort ratio
- Tracking resolution history and effectiveness
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from dataraum_context.entropy.models import (
    ColumnEntropyProfile,
    EntropyObject,
    ResolutionCascade,
    ResolutionOption,
    TableEntropyProfile,
)

logger = logging.getLogger(__name__)


@dataclass
class ResolutionFinder:
    """Finds and prioritizes resolution options across entropy objects.

    Groups related resolutions, identifies cascades, and ranks
    by impact-to-effort ratio.
    """

    # Resolution options grouped by action type
    options_by_action: dict[str, list[ResolutionOption]] = field(default_factory=dict)

    # Cascade relationships discovered
    cascades: list[ResolutionCascade] = field(default_factory=list)

    def analyze_entropy_objects(
        self,
        entropy_objects: list[EntropyObject],
    ) -> list[ResolutionCascade]:
        """Analyze entropy objects to find resolution cascades.

        A cascade occurs when multiple entropy objects can be fixed
        by a single action.

        Args:
            entropy_objects: List of entropy objects with resolution options

        Returns:
            List of resolution cascades, sorted by priority
        """
        self.options_by_action = defaultdict(list)
        self.cascades = []

        # Group options by action type
        for obj in entropy_objects:
            for option in obj.resolution_options:
                key = self._action_key(option)
                self.options_by_action[key].append(option)

        # Find cascades (actions that appear multiple times)
        for action_key, options in self.options_by_action.items():
            if len(options) > 1:
                cascade = self._build_cascade(action_key, options)
                if cascade:
                    self.cascades.append(cascade)

        # Also look for resolutions with cascade_dimensions
        for obj in entropy_objects:
            for option in obj.resolution_options:
                if option.cascade_dimensions:
                    cascade = self._build_cascade_from_option(obj, option)
                    if cascade and not self._cascade_exists(cascade):
                        self.cascades.append(cascade)

        # Calculate priorities and sort
        for cascade in self.cascades:
            cascade.calculate_priority()

        self.cascades.sort(key=lambda c: c.priority_score, reverse=True)

        return self.cascades

    def _action_key(self, option: ResolutionOption) -> str:
        """Create a unique key for an action based on type and parameters."""
        # Create a stable key from action and key parameters
        params_str = ""
        if "column" in option.parameters:
            params_str = f":{option.parameters['column']}"
        elif "table" in option.parameters:
            params_str = f":{option.parameters['table']}"
        return f"{option.action}{params_str}"

    def _build_cascade(
        self,
        action_key: str,
        options: list[ResolutionOption],
    ) -> ResolutionCascade | None:
        """Build a cascade from multiple related options."""
        if not options:
            return None

        # Use first option as template
        template = options[0]

        # Collect all affected targets and entropy reductions
        affected_targets: list[str] = []
        entropy_reductions: dict[str, float] = {}

        # Get highest reduction per dimension
        for option in options:
            for dim in option.cascade_dimensions or [template.action]:
                if dim not in entropy_reductions:
                    entropy_reductions[dim] = option.expected_entropy_reduction
                else:
                    entropy_reductions[dim] = max(
                        entropy_reductions[dim], option.expected_entropy_reduction
                    )

        # If no cascade dimensions, use the action as dimension key
        if not entropy_reductions:
            entropy_reductions[action_key] = sum(
                o.expected_entropy_reduction for o in options
            ) / len(options)

        return ResolutionCascade(
            action=template.action,
            parameters=template.parameters,
            affected_targets=affected_targets,
            entropy_reductions=entropy_reductions,
            effort=template.effort,
            description=template.description or f"Apply {template.action}",
        )

    def _build_cascade_from_option(
        self,
        entropy_object: EntropyObject,
        option: ResolutionOption,
    ) -> ResolutionCascade:
        """Build a cascade from a single option with cascade_dimensions."""
        entropy_reductions = dict.fromkeys(
            option.cascade_dimensions, option.expected_entropy_reduction
        )

        return ResolutionCascade(
            action=option.action,
            parameters=option.parameters,
            affected_targets=[entropy_object.target],
            entropy_reductions=entropy_reductions,
            effort=option.effort,
            description=option.description or f"Apply {option.action}",
        )

    def _cascade_exists(self, new_cascade: ResolutionCascade) -> bool:
        """Check if an equivalent cascade already exists."""
        for existing in self.cascades:
            if (
                existing.action == new_cascade.action
                and existing.parameters == new_cascade.parameters
            ):
                return True
        return False


def find_top_resolutions(
    entropy_objects: list[EntropyObject],
    limit: int = 5,
) -> list[ResolutionCascade]:
    """Find the top resolution cascades by priority.

    Args:
        entropy_objects: List of entropy objects with resolution options
        limit: Maximum number of resolutions to return

    Returns:
        Top resolution cascades sorted by priority score
    """
    finder = ResolutionFinder()
    cascades = finder.analyze_entropy_objects(entropy_objects)
    return cascades[:limit]


def get_resolutions_for_column(
    profile: ColumnEntropyProfile,
    entropy_objects: list[EntropyObject] | None = None,
    limit: int = 3,
) -> list[ResolutionOption]:
    """Get top resolution options for a column.

    Uses the column profile's top_resolution_hints if available,
    otherwise analyzes entropy objects.

    Args:
        profile: Column entropy profile
        entropy_objects: Optional entropy objects for deeper analysis
        limit: Maximum number of resolutions

    Returns:
        List of resolution options sorted by priority
    """
    # First try profile's pre-computed hints
    if profile.top_resolution_hints:
        return profile.top_resolution_hints[:limit]

    # Fall back to analyzing entropy objects
    if entropy_objects:
        all_options: list[ResolutionOption] = []
        for obj in entropy_objects:
            all_options.extend(obj.resolution_options)

        # Sort by priority and deduplicate
        all_options.sort(key=lambda o: o.priority_score(), reverse=True)
        seen_actions: set[str] = set()
        unique_options: list[ResolutionOption] = []
        for opt in all_options:
            key = f"{opt.action}:{opt.parameters}"
            if key not in seen_actions:
                seen_actions.add(key)
                unique_options.append(opt)

        return unique_options[:limit]

    return []


def get_resolutions_for_table(
    table_profile: TableEntropyProfile,
    entropy_objects_by_column: dict[str, list[EntropyObject]] | None = None,
    limit: int = 5,
) -> list[ResolutionCascade]:
    """Get top resolution cascades for a table.

    Analyzes all columns to find resolutions with the broadest impact.

    Args:
        table_profile: Table entropy profile
        entropy_objects_by_column: Optional dict of column to entropy objects
        limit: Maximum number of cascades

    Returns:
        List of resolution cascades sorted by priority
    """
    all_objects: list[EntropyObject] = []

    if entropy_objects_by_column:
        for objects in entropy_objects_by_column.values():
            all_objects.extend(objects)

    return find_top_resolutions(all_objects, limit=limit)


@dataclass
class ResolutionEffectiveness:
    """Tracks the effectiveness of a resolution over time.

    Used to learn which resolutions actually work and
    improve recommendations.
    """

    resolution_action: str
    resolution_parameters: dict[str, Any]

    # Before/after metrics
    dimension: str
    before_score: float
    after_score: float
    reduction_achieved: float = 0.0

    # Expected vs actual
    expected_reduction: float = 0.0
    effectiveness_ratio: float = 0.0  # actual / expected

    # Feedback
    user_confirmed: bool = False
    user_feedback: str | None = None

    def calculate_effectiveness(self) -> None:
        """Calculate reduction achieved and effectiveness ratio."""
        self.reduction_achieved = self.before_score - self.after_score
        if self.expected_reduction > 0:
            self.effectiveness_ratio = self.reduction_achieved / self.expected_reduction
        else:
            self.effectiveness_ratio = 1.0 if self.reduction_achieved > 0 else 0.0


def estimate_resolution_impact(
    option: ResolutionOption,
    current_scores: dict[str, float],
) -> dict[str, float]:
    """Estimate the impact of a resolution on entropy scores.

    Args:
        option: Resolution option to evaluate
        current_scores: Current dimension scores

    Returns:
        Estimated scores after applying resolution
    """
    estimated = dict(current_scores)

    # Apply expected reduction to affected dimensions
    affected_dims = option.cascade_dimensions or []

    for dim in affected_dims:
        if dim in estimated:
            new_score = max(0.0, estimated[dim] - option.expected_entropy_reduction)
            estimated[dim] = new_score

    return estimated


def format_resolution_for_display(option: ResolutionOption) -> str:
    """Format a resolution option for human-readable display.

    Args:
        option: Resolution option to format

    Returns:
        Formatted string describing the resolution
    """
    effort_emoji = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(option.effort, "⚪")

    desc = option.description or option.action
    reduction = f"-{option.expected_entropy_reduction:.0%}"

    return f"{effort_emoji} {desc} ({reduction} entropy)"


def format_cascade_for_display(cascade: ResolutionCascade) -> str:
    """Format a resolution cascade for human-readable display.

    Args:
        cascade: Resolution cascade to format

    Returns:
        Formatted string describing the cascade
    """
    effort_emoji = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(cascade.effort, "⚪")

    dims_improved = cascade.dimensions_improved
    total_reduction = cascade.total_reduction

    desc = cascade.description or cascade.action

    return (
        f"{effort_emoji} {desc} (improves {dims_improved} dimensions, -{total_reduction:.0%} total)"
    )
