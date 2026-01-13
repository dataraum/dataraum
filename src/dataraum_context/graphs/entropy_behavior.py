"""Entropy behavior configuration for graph agent responses.

Defines how the graph agent should behave when encountering different
entropy levels, per the specification in ENTROPY_QUERY_BEHAVIOR.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class BehaviorMode(str, Enum):
    """Agent behavior mode when encountering data entropy."""

    STRICT = "strict"  # Ask clarification for ANY entropy > 0.3
    BALANCED = "balanced"  # Default - ask for high entropy (> 0.6)
    LENIENT = "lenient"  # Only refuse for critical entropy (> 0.8)


class EntropyAction(str, Enum):
    """Action to take based on entropy level."""

    ANSWER_CONFIDENTLY = "answer_confidently"  # Direct answer, no caveats
    ANSWER_WITH_ASSUMPTIONS = "answer_with_assumptions"  # Include assumptions
    ASK_OR_CAVEAT = "ask_or_caveat"  # Prefer clarification; if answering, strong caveat
    REFUSE = "refuse"  # Explain what needs resolution first


class CompoundRiskAction(str, Enum):
    """Action for compound risks."""

    REFUSE = "refuse"
    WARN_STRONGLY = "warn_strongly"
    NOTE_IN_RESPONSE = "note_in_response"


@dataclass
class DimensionBehavior:
    """Behavior override for a specific entropy dimension."""

    dimension: str  # e.g., "semantic.units", "structural.relations"
    clarification_threshold: float  # Override threshold for this dimension
    always_disclose: bool = False  # Always show in response even at low entropy


@dataclass
class CompoundRiskBehavior:
    """Behavior configuration for compound risks."""

    critical_action: CompoundRiskAction = CompoundRiskAction.REFUSE
    critical_explain: bool = True
    high_action: CompoundRiskAction = CompoundRiskAction.WARN_STRONGLY
    high_require_confirmation: bool = True
    medium_action: CompoundRiskAction = CompoundRiskAction.NOTE_IN_RESPONSE


@dataclass
class EntropyBehaviorConfig:
    """Configuration for entropy-aware agent behavior.

    Controls how the agent responds to data uncertainty at query time.
    """

    mode: BehaviorMode = BehaviorMode.BALANCED

    # Thresholds for behavior decisions
    clarification_threshold: float = 0.6  # Ask for clarification above this
    refusal_threshold: float = 0.8  # Refuse to answer above this

    # Assumption handling
    auto_assume: bool = True  # Make reasonable assumptions automatically
    show_entropy_scores: bool = False  # Show raw entropy scores in response
    assumption_disclosure: str = "when_made"  # always, when_made, minimal

    # Dimension-specific overrides
    dimension_overrides: list[DimensionBehavior] = field(default_factory=list)

    # Compound risk handling
    compound_risk_behavior: CompoundRiskBehavior = field(default_factory=CompoundRiskBehavior)

    @classmethod
    def strict(cls) -> EntropyBehaviorConfig:
        """Create strict mode configuration."""
        return cls(
            mode=BehaviorMode.STRICT,
            clarification_threshold=0.3,
            refusal_threshold=0.6,
            auto_assume=False,
            show_entropy_scores=True,
            assumption_disclosure="always",
        )

    @classmethod
    def balanced(cls) -> EntropyBehaviorConfig:
        """Create balanced mode configuration (default)."""
        return cls(
            mode=BehaviorMode.BALANCED,
            clarification_threshold=0.6,
            refusal_threshold=0.8,
            auto_assume=True,
            show_entropy_scores=False,
            assumption_disclosure="when_made",
        )

    @classmethod
    def lenient(cls) -> EntropyBehaviorConfig:
        """Create lenient mode configuration."""
        return cls(
            mode=BehaviorMode.LENIENT,
            clarification_threshold=0.8,
            refusal_threshold=0.95,
            auto_assume=True,
            show_entropy_scores=False,
            assumption_disclosure="minimal",
        )

    def get_threshold_for_dimension(self, dimension: str) -> float:
        """Get the clarification threshold for a specific dimension.

        Some dimensions (like currency/units) should have lower thresholds
        because errors in those dimensions are particularly problematic.
        """
        for override in self.dimension_overrides:
            if override.dimension == dimension:
                return override.clarification_threshold
        return self.clarification_threshold

    def determine_action(
        self,
        max_entropy: float,
        has_critical_compound_risk: bool = False,
        has_high_compound_risk: bool = False,
    ) -> EntropyAction:
        """Determine what action to take based on entropy level.

        Args:
            max_entropy: Maximum entropy score encountered
            has_critical_compound_risk: Whether critical compound risks exist
            has_high_compound_risk: Whether high compound risks exist

        Returns:
            The recommended action for the agent
        """
        # Compound risks override normal thresholds
        if has_critical_compound_risk:
            if self.compound_risk_behavior.critical_action == CompoundRiskAction.REFUSE:
                return EntropyAction.REFUSE
            return EntropyAction.ASK_OR_CAVEAT

        if has_high_compound_risk:
            if self.compound_risk_behavior.high_action == CompoundRiskAction.REFUSE:
                return EntropyAction.REFUSE
            if self.compound_risk_behavior.high_action == CompoundRiskAction.WARN_STRONGLY:
                return EntropyAction.ASK_OR_CAVEAT

        # Standard entropy-based decisions
        if max_entropy >= self.refusal_threshold:
            return EntropyAction.REFUSE

        if max_entropy >= self.clarification_threshold:
            return EntropyAction.ASK_OR_CAVEAT

        if max_entropy >= 0.3:  # Medium entropy
            if self.auto_assume:
                return EntropyAction.ANSWER_WITH_ASSUMPTIONS
            return EntropyAction.ASK_OR_CAVEAT

        # Low entropy
        return EntropyAction.ANSWER_CONFIDENTLY


# Default dimension-specific thresholds per ENTROPY_QUERY_BEHAVIOR.md
DEFAULT_DIMENSION_OVERRIDES = [
    DimensionBehavior(
        dimension="semantic.units",
        clarification_threshold=0.4,  # Always ask about currency
        always_disclose=True,
    ),
    DimensionBehavior(
        dimension="structural.relations",
        clarification_threshold=0.5,  # Always ask about join paths
        always_disclose=True,
    ),
]


def get_default_config(mode: str = "balanced") -> EntropyBehaviorConfig:
    """Get default entropy behavior configuration.

    Args:
        mode: One of "strict", "balanced", "lenient"

    Returns:
        Configuration for the specified mode with default dimension overrides
    """
    if mode == "strict":
        config = EntropyBehaviorConfig.strict()
    elif mode == "lenient":
        config = EntropyBehaviorConfig.lenient()
    else:
        config = EntropyBehaviorConfig.balanced()

    config.dimension_overrides = DEFAULT_DIMENSION_OVERRIDES.copy()
    return config


def format_entropy_sql_comments(
    entropy_score: float,
    assumptions: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
) -> str:
    """Format entropy information as SQL comments.

    Per ENTROPY_QUERY_BEHAVIOR.md, generated SQL should include comments
    noting the entropy level and any assumptions made.

    Args:
        entropy_score: The composite entropy score
        assumptions: List of assumptions made (dicts with 'dimension', 'assumption')
        warnings: List of warning messages

    Returns:
        SQL comment block to prepend to generated SQL
    """
    lines = []

    # Entropy level indicator
    if entropy_score < 0.3:
        lines.append(f"-- Generated with high confidence (entropy: {entropy_score:.2f})")
    elif entropy_score < 0.6:
        lines.append(f"-- Generated with assumptions (entropy: {entropy_score:.2f})")
    else:
        lines.append(f"-- ⚠️ HIGH ENTROPY WARNING (entropy: {entropy_score:.2f})")

    # Add assumptions as comments
    if assumptions:
        for assumption in assumptions:
            dimension = assumption.get("dimension", "unknown")
            text = assumption.get("assumption", "")
            lines.append(f"-- ASSUMPTION ({dimension}): {text}")

    # Add warnings
    if warnings:
        for warning in warnings:
            lines.append(f"-- WARNING: {warning}")

    # Add verification note for high entropy
    if entropy_score >= 0.6:
        lines.append("-- VERIFY: Results before using in reports")

    if lines:
        lines.append("")  # Empty line before SQL

    return "\n".join(lines)


def format_assumptions_for_response(
    assumptions: list[dict[str, Any]],
    disclosure_mode: str = "when_made",
) -> str:
    """Format assumptions for inclusion in user-facing response.

    Args:
        assumptions: List of assumptions made
        disclosure_mode: "always", "when_made", or "minimal"

    Returns:
        Formatted assumptions section for response
    """
    if not assumptions:
        return ""

    if disclosure_mode == "minimal":
        # Just note that assumptions were made
        return f"\n*({len(assumptions)} assumptions made)*"

    lines = ["\n**Assumptions made:**"]
    for assumption in assumptions:
        text = assumption.get("assumption", "")
        confidence = assumption.get("confidence", 0.0)

        if disclosure_mode == "always":
            lines.append(f"- {text} (confidence: {confidence:.0%})")
        else:
            lines.append(f"- {text}")

    return "\n".join(lines)
