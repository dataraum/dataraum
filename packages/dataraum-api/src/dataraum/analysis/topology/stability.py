"""Topological stability assessment.

This module provides functions for assessing topological stability:
- Homological stability (changes between time periods)
- Historical complexity tracking
- Bottleneck distance computation
"""

from typing import Any

import numpy as np
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from dataraum.analysis.topology.db_models import TopologicalQualityMetrics
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
    table_id: str | None = None,
    session: Session | None = None,
    previous_diagrams: list[np.ndarray] | None = None,
    threshold: float = 0.1,
) -> Result[StabilityAnalysis | None]:
    """Assess homological stability between current and previous topology.

    Compares persistence diagrams to detect structural changes.

    Args:
        current_diagrams: Current persistence diagrams
        table_id: Table ID (used to fetch previous diagrams if session provided)
        session: Database session (used to fetch previous diagrams)
        previous_diagrams: Previous persistence diagrams (overrides database lookup)
        threshold: Distance threshold for stability

    Returns:
        Result containing StabilityAnalysis or None if no previous data
    """
    try:
        # Try to get previous diagrams from database if not provided
        if previous_diagrams is None and session is not None and table_id is not None:
            previous_diagrams = get_previous_topology(session, table_id)

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


def compute_historical_complexity(
    session: Session,
    table_id: str,
    current_complexity: int,
    window_size: int = 10,
) -> Result[dict[str, Any]]:
    """Compute historical complexity statistics.

    Retrieves past complexity values and computes statistics
    to contextualize the current complexity.

    Args:
        session: Database session
        table_id: Table to analyze
        current_complexity: Current structural complexity
        window_size: Number of historical records to consider

    Returns:
        Result containing dict with mean, std, z_score, trend
    """
    try:
        # Fetch historical complexity values
        stmt = (
            select(TopologicalQualityMetrics.structural_complexity)
            .where(TopologicalQualityMetrics.table_id == table_id)
            .order_by(desc(TopologicalQualityMetrics.computed_at))
            .limit(window_size)
        )

        result = session.execute(stmt)
        historical_values = [row[0] for row in result.fetchall() if row[0] is not None]

        if len(historical_values) < 3:
            # Not enough history
            return Result.ok(
                {
                    "mean": None,
                    "std": None,
                    "z_score": None,
                    "trend": None,
                    "within_bounds": True,
                }
            )

        # Compute statistics
        mean = float(np.mean(historical_values))
        std = float(np.std(historical_values))

        # Z-score for current value
        z_score = None
        if std > 0:
            z_score = (current_complexity - mean) / std

        # Simple trend detection (linear regression slope)
        trend = None
        if len(historical_values) >= 3:
            x = np.arange(len(historical_values))
            coeffs = np.polyfit(x, historical_values, 1)
            slope = coeffs[0]
            if slope > 0.1:
                trend = "increasing"
            elif slope < -0.1:
                trend = "decreasing"
            else:
                trend = "stable"

        # Check if within 2 standard deviations
        within_bounds = z_score is None or abs(z_score) < 2

        return Result.ok(
            {
                "mean": mean,
                "std": std,
                "z_score": z_score,
                "trend": trend,
                "within_bounds": within_bounds,
            }
        )

    except Exception as e:
        return Result.fail(f"Historical complexity computation failed: {e}")


def get_previous_topology(
    session: Session,
    table_id: str,
) -> list[np.ndarray] | None:
    """Retrieve the most recent previous topology for a table.

    Args:
        session: Database session
        table_id: Table to get previous topology for

    Returns:
        Previous persistence diagrams or None if no history
    """
    try:
        stmt = (
            select(TopologicalQualityMetrics.topology_data)
            .where(TopologicalQualityMetrics.table_id == table_id)
            .order_by(desc(TopologicalQualityMetrics.computed_at))
            .limit(1)
        )

        result = session.execute(stmt)
        row = result.fetchone()

        if row is None or row[0] is None:
            return None

        topology_data = row[0]

        # Extract persistence diagrams from stored data
        diagrams_data = topology_data.get("persistence_diagrams", [])
        if not diagrams_data:
            return None

        # Convert back to numpy arrays
        diagrams = []
        for dgm_dict in diagrams_data:
            points = dgm_dict.get("points", [])
            if points:
                dgm = np.array([[p["birth"], p["death"]] for p in points])
                diagrams.append(dgm)
            else:
                diagrams.append(np.array([]).reshape(0, 2))

        return diagrams if diagrams else None

    except Exception as e:
        logger.warning(f"Failed to retrieve previous topology: {e}")
        return None
