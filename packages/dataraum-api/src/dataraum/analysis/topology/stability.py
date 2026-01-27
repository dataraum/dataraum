"""Topological stability assessment.

This module provides functions for assessing topological stability:
- Homological stability (changes between time periods)
- Bottleneck distance computation
"""

import numpy as np

from dataraum.analysis.topology.models import StabilityAnalysis
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result

logger = get_logger(__name__)


def compute_bottleneck_distance(
    dgm1: np.ndarray,
    dgm2: np.ndarray,
) -> float:
    """Compute bottleneck distance between two persistence diagrams.

    The bottleneck distance measures the maximum displacement needed
    to transform one diagram into another.

    Args:
        dgm1: First persistence diagram
        dgm2: Second persistence diagram

    Returns:
        Bottleneck distance value
    """
    try:
        from persim import bottleneck

        # Filter to finite points only
        dgm1_finite = dgm1[dgm1[:, 1] < np.inf] if len(dgm1) > 0 else np.array([])
        dgm2_finite = dgm2[dgm2[:, 1] < np.inf] if len(dgm2) > 0 else np.array([])

        # Handle empty diagrams
        if len(dgm1_finite) == 0 and len(dgm2_finite) == 0:
            return 0.0
        if len(dgm1_finite) == 0 or len(dgm2_finite) == 0:
            # One is empty - return max persistence from non-empty
            non_empty = dgm1_finite if len(dgm1_finite) > 0 else dgm2_finite
            return float(np.max(non_empty[:, 1] - non_empty[:, 0]))

        return float(bottleneck(dgm1_finite, dgm2_finite))

    except ImportError:
        logger.warning("persim not installed, using fallback distance")
        return 0.0
    except Exception as e:
        logger.warning(f"Bottleneck distance computation failed: {e}")
        return 0.0


def assess_homological_stability(
    current_diagrams: list[np.ndarray],
    previous_diagrams: list[np.ndarray] | None = None,
    threshold: float = 0.1,
) -> Result[StabilityAnalysis | None]:
    """Assess homological stability between current and previous topology.

    Compares persistence diagrams to detect structural changes.

    Args:
        current_diagrams: Current persistence diagrams
        previous_diagrams: Previous persistence diagrams
        threshold: Distance threshold for stability

    Returns:
        Result containing StabilityAnalysis or None if no previous data
    """
    try:
        # No previous data - return None (first analysis)
        if previous_diagrams is None:
            return Result.ok(None)

        # Compute bottleneck distance for each dimension
        max_distance = 0.0

        for dim in range(min(len(current_diagrams), len(previous_diagrams))):
            distance = compute_bottleneck_distance(
                current_diagrams[dim],
                previous_diagrams[dim],
            )
            max_distance = max(max_distance, distance)

        # Determine stability level
        if max_distance < threshold:
            stability_level = "stable"
        elif max_distance < threshold * 2:
            stability_level = "minor_changes"
        elif max_distance < threshold * 5:
            stability_level = "significant_changes"
        else:
            stability_level = "unstable"

        # Count component and cycle changes
        components_added = 0
        components_removed = 0
        cycles_added = 0
        cycles_removed = 0

        if len(current_diagrams) > 0 and len(previous_diagrams) > 0:
            # Count dimension 0 (components)
            curr_count = int(
                np.sum(current_diagrams[0][:, 1] < np.inf) if len(current_diagrams[0]) > 0 else 0
            )
            prev_count = int(
                np.sum(previous_diagrams[0][:, 1] < np.inf) if len(previous_diagrams[0]) > 0 else 0
            )
            if curr_count > prev_count:
                components_added = curr_count - prev_count
            else:
                components_removed = prev_count - curr_count

        if len(current_diagrams) > 1 and len(previous_diagrams) > 1:
            # Count dimension 1 (cycles)
            curr_count = int(
                np.sum(current_diagrams[1][:, 1] < np.inf) if len(current_diagrams[1]) > 0 else 0
            )
            prev_count = int(
                np.sum(previous_diagrams[1][:, 1] < np.inf) if len(previous_diagrams[1]) > 0 else 0
            )
            if curr_count > prev_count:
                cycles_added = curr_count - prev_count
            else:
                cycles_removed = prev_count - curr_count

        stability = StabilityAnalysis(
            bottleneck_distance=max_distance,
            is_stable=max_distance < threshold,
            stability_threshold=threshold,
            stability_level=stability_level,
            components_added=components_added,
            components_removed=components_removed,
            cycles_added=cycles_added,
            cycles_removed=cycles_removed,
        )

        return Result.ok(stability)

    except Exception as e:
        return Result.fail(f"Stability assessment failed: {e}")
