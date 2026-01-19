"""Topological feature extraction.

This module provides functions for extracting topological features:
- Betti numbers (connected components, cycles, voids)
- Persistence diagrams
- Persistent entropy
- Cycle detection
"""

from datetime import UTC, datetime
from uuid import uuid4

import numpy as np
from scipy.stats import entropy as scipy_entropy

from dataraum_context.analysis.topology.models import (
    BettiNumbers,
    CycleDetection,
    PersistenceDiagram,
    PersistencePoint,
)
from dataraum_context.core.models.base import Result


def extract_betti_numbers(
    persistence_diagrams: list[np.ndarray],
) -> Result[BettiNumbers]:
    """Extract Betti numbers from persistence diagrams.

    Betti numbers are topological invariants:
    - β₀: Number of connected components
    - β₁: Number of cycles (holes)
    - β₂: Number of voids (cavities)

    Args:
        persistence_diagrams: List of persistence diagrams from ripser

    Returns:
        Result containing BettiNumbers
    """
    try:
        if not persistence_diagrams:
            return Result.fail("No persistence diagrams provided")

        # Extract Betti numbers by counting features in each dimension
        # Note: We count features with finite persistence (death < inf)
        betti_0 = 0
        betti_1 = 0
        betti_2 = None

        if len(persistence_diagrams) > 0:
            # Dimension 0: connected components
            dgm_0 = persistence_diagrams[0]
            # Count finite persistence features
            finite_mask = dgm_0[:, 1] < np.inf
            betti_0 = int(np.sum(finite_mask))
            # Add 1 for the infinite component (the whole dataset)
            betti_0 += 1

        if len(persistence_diagrams) > 1:
            # Dimension 1: cycles
            dgm_1 = persistence_diagrams[1]
            finite_mask = dgm_1[:, 1] < np.inf
            betti_1 = int(np.sum(finite_mask))

        if len(persistence_diagrams) > 2:
            # Dimension 2: voids
            dgm_2 = persistence_diagrams[2]
            finite_mask = dgm_2[:, 1] < np.inf
            betti_2 = int(np.sum(finite_mask))

        total_complexity = betti_0 + betti_1 + (betti_2 or 0)

        betti_numbers = BettiNumbers(
            betti_0=betti_0,
            betti_1=betti_1,
            betti_2=betti_2 or 0,
            total_complexity=total_complexity,
            is_connected=betti_0 == 1,  # Single connected component
            has_cycles=betti_1 > 0,
        )

        return Result.ok(betti_numbers)

    except Exception as e:
        return Result.fail(f"Betti number extraction failed: {e}")


def process_persistence_diagrams(
    persistence_diagrams: list[np.ndarray],
) -> Result[list[PersistenceDiagram]]:
    """Convert raw persistence diagrams to structured format.

    Args:
        persistence_diagrams: Raw diagrams from ripser

    Returns:
        Result containing list of PersistenceDiagram objects
    """
    try:
        if not persistence_diagrams:
            return Result.ok([])

        diagrams = []

        for dimension, dgm in enumerate(persistence_diagrams):
            if len(dgm) == 0:
                continue

            # Filter out infinite persistence for statistics
            finite_mask = dgm[:, 1] < np.inf
            _finite_dgm = dgm[finite_mask]

            # Create persistence points
            points = []
            for birth, death in dgm:
                if death < np.inf:  # Only include finite features
                    persistence = float(death - birth)
                    points.append(
                        PersistencePoint(
                            dimension=dimension,
                            birth=float(birth),
                            death=float(death),
                            persistence=persistence,
                        )
                    )

            if not points:
                continue

            # Calculate statistics
            max_persistence = max(p.persistence for p in points)

            # Compute persistent entropy for this dimension
            lifetimes = [p.persistence for p in points]
            total_lifetime = sum(lifetimes)
            dim_entropy = 0.0
            if total_lifetime > 0:
                probabilities = [lt / total_lifetime for lt in lifetimes]
                dim_entropy = float(scipy_entropy(probabilities))

            diagram = PersistenceDiagram(
                dimension=dimension,
                points=points,
                max_persistence=max_persistence,
                num_features=len(points),  # Count of features (avoids len(points) everywhere)
                persistent_entropy=dim_entropy,
            )

            diagrams.append(diagram)

        return Result.ok(diagrams)

    except Exception as e:
        return Result.fail(f"Persistence diagram processing failed: {e}")


def compute_persistent_entropy(persistence_diagrams: list[np.ndarray]) -> float:
    """Compute persistent entropy as a measure of topological complexity.

    Persistent entropy quantifies the distribution of lifetimes in the
    persistence diagram. Higher entropy = more complex topology.

    Args:
        persistence_diagrams: Raw diagrams from ripser

    Returns:
        Persistent entropy value
    """
    try:
        all_lifetimes = []

        for dgm in persistence_diagrams:
            if len(dgm) == 0:
                continue

            # Get finite lifetimes
            finite_mask = dgm[:, 1] < np.inf
            births = dgm[finite_mask, 0]
            deaths = dgm[finite_mask, 1]
            lifetimes = deaths - births

            all_lifetimes.extend(lifetimes)

        if not all_lifetimes:
            return 0.0

        # Normalize lifetimes to probabilities
        lifetimes = np.array(all_lifetimes)
        total = np.sum(lifetimes)

        if total == 0:
            return 0.0

        probabilities = lifetimes / total

        # Compute Shannon entropy
        return float(scipy_entropy(probabilities))

    except Exception:
        return 0.0


def detect_persistent_cycles(
    persistence_diagrams: list[np.ndarray],
    min_persistence: float = 0.1,
) -> Result[list[CycleDetection]]:
    """Detect significant persistent cycles (dimension 1 features).

    Cycles represent circular relationships or flows in the data.

    Args:
        persistence_diagrams: Raw diagrams from ripser
        min_persistence: Minimum persistence to consider significant

    Returns:
        Result containing list of detected cycles
    """
    try:
        cycles = []

        if len(persistence_diagrams) < 2:
            return Result.ok([])

        # Get dimension 1 persistence diagram (cycles)
        dgm_1 = persistence_diagrams[1]

        if len(dgm_1) == 0:
            return Result.ok([])

        # Filter for finite, significant cycles
        finite_mask = dgm_1[:, 1] < np.inf
        finite_dgm = dgm_1[finite_mask]

        for birth, death in finite_dgm:
            persistence = death - birth

            if persistence < min_persistence:
                continue

            now = datetime.now(UTC)
            cycle = CycleDetection(
                cycle_id=str(uuid4()),  # Unique identifier for tracking
                dimension=1,
                birth=float(birth),
                death=float(death),
                persistence=float(persistence),
                involved_columns=[],  # Would need additional analysis
                cycle_type=None,  # CRITICAL: Would be inferred from domain analysis
                is_anomalous=False,
                anomaly_reason=None,
                first_detected=now,  # When cycle first appeared
                last_seen=now,  # Temporal tracking
            )

            cycles.append(cycle)

        return Result.ok(cycles)

    except Exception as e:
        return Result.fail(f"Cycle detection failed: {e}")
