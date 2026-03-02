"""Pipeline entropy state tracking.

Tracks hard detector scores at runtime for gate checking.
This is a lightweight in-memory structure — not a DB model.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class DimensionScore:
    """A single hard entropy dimension score at a point in time."""

    sub_dimension: str
    score: float
    measured_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    target_count: int = 0  # How many targets were measured


@dataclass
class PipelineEntropyState:
    """Runtime entropy state for gate checking.

    Tracks the latest hard detector scores aggregated across all targets.
    Updated after each phase that produces entropy measurements.
    """

    # Current hard scores: sub_dimension -> aggregated score
    hard_scores: dict[str, DimensionScore] = field(default_factory=dict)

    # History of snapshots (for before/after comparisons)
    snapshots: list[dict[str, DimensionScore]] = field(default_factory=list)

    def update_score(
        self,
        sub_dimension: str,
        score: float,
        target_count: int = 0,
    ) -> None:
        """Update a hard dimension score."""
        self.hard_scores[sub_dimension] = DimensionScore(
            sub_dimension=sub_dimension,
            score=score,
            target_count=target_count,
        )

    def get_score(self, sub_dimension: str) -> float | None:
        """Get the current score for a hard dimension, or None if not measured."""
        ds = self.hard_scores.get(sub_dimension)
        return ds.score if ds else None

    def check_preconditions(
        self,
        preconditions: dict[str, float],
        producible_dimensions: set[str] | None = None,
    ) -> dict[str, tuple[float, float]]:
        """Check which preconditions are violated.

        Args:
            preconditions: sub_dimension -> max_allowed_score
            producible_dimensions: Dimensions that some phase in this pipeline
                run is known to produce via post_verification.  When a required
                dimension is not yet measured *and* it appears in this set, it
                is treated as a violation (sentinel score ``-1.0``) because the
                producing phase hasn't run yet and the gate should wait.

        Returns:
            Dict of violated preconditions: sub_dimension -> (current_score, threshold).
            Empty dict means all preconditions pass.
        """
        violations: dict[str, tuple[float, float]] = {}
        producible = producible_dimensions or set()
        for sub_dim, threshold in preconditions.items():
            current = self.get_score(sub_dim)
            if current is not None and current > threshold:
                violations[sub_dim] = (current, threshold)
            elif current is None and sub_dim in producible:
                # Not yet measured but a pipeline phase will produce it — block
                violations[sub_dim] = (-1.0, threshold)
        return violations

    def take_snapshot(self) -> dict[str, DimensionScore]:
        """Take a snapshot of current hard scores for before/after comparison."""
        snapshot = dict(self.hard_scores)
        self.snapshots.append(snapshot)
        return snapshot

    def to_dict(self) -> dict[str, float]:
        """Export current hard scores as a simple dict."""
        return {k: v.score for k, v in self.hard_scores.items()}
