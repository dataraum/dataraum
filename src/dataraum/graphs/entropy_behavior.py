"""Entropy behavior configuration for graph agent responses.

Defines how the graph agent should behave when encountering different
entropy levels, per the specification in ENTROPY_QUERY_BEHAVIOR.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


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


@dataclass
class DimensionBehavior:
    """Behavior override for a specific entropy dimension."""

    dimension: str  # e.g., "semantic.units", "structural.relations"
    clarification_threshold: float  # Override threshold for this dimension
    always_disclose: bool = False  # Always show in response even at low entropy


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
    ) -> EntropyAction:
        """Determine what action to take based on entropy level.

        Args:
            max_entropy: Maximum entropy score encountered

        Returns:
            The recommended action for the agent
        """
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
