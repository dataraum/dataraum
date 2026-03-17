"""Entropy detectors for measuring uncertainty across dimensions.

This module provides the detector infrastructure for the entropy layer:
- EntropyDetector: Abstract base class for all detectors
- DetectorRegistry: Registry for detector discovery and management
- Built-in detectors for structural, semantic, value, and computational entropy

Usage:
    from dataraum.entropy.detectors import (
        EntropyDetector,
        DetectorRegistry,
        get_default_registry,
        register_builtin_detectors,
    )

    # Register all built-in detectors
    register_builtin_detectors()

    # Get all registered detectors
    registry = get_default_registry()
    detectors = registry.get_all_detectors()

    # Run a detector
    for detector in detectors:
        results = detector.detect(context)
"""

from dataraum.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    EntropyDetector,
    get_default_registry,
)

# Computational layer detectors
from dataraum.entropy.detectors.computational import (
    DerivedValueDetector,
)

# Semantic layer detectors
from dataraum.entropy.detectors.semantic import (
    BusinessMeaningDetector,
    ColumnQualityDetector,
    DimensionalEntropyDetector,
    DimensionCoverageDetector,
    TemporalEntropyDetector,
    UnitEntropyDetector,
)

# Structural layer detectors
from dataraum.entropy.detectors.structural import (
    JoinPathDeterminismDetector,
    RelationshipEntropyDetector,
    TypeFidelityDetector,
)

# Value layer detectors
from dataraum.entropy.detectors.value import (
    BenfordDetector,
    NullRatioDetector,
    OutlierRateDetector,
    TemporalDriftDetector,
)

# All built-in detector classes (column, table, and view scoped)
BUILTIN_DETECTORS: list[type[EntropyDetector]] = [
    # Structural
    TypeFidelityDetector,
    JoinPathDeterminismDetector,
    RelationshipEntropyDetector,
    # Value
    NullRatioDetector,
    OutlierRateDetector,
    TemporalDriftDetector,
    BenfordDetector,
    # Semantic (column-scoped)
    BusinessMeaningDetector,
    UnitEntropyDetector,
    TemporalEntropyDetector,
    # Semantic (table-scoped)
    DimensionalEntropyDetector,
    ColumnQualityDetector,
    # Semantic (view-scoped)
    DimensionCoverageDetector,
    # Computational
    DerivedValueDetector,
]


def register_builtin_detectors(registry: DetectorRegistry | None = None) -> None:
    """Register all built-in detectors to a registry.

    Args:
        registry: Registry to register detectors to. If None, uses default registry.
    """
    if registry is None:
        registry = get_default_registry()

    for detector_class in BUILTIN_DETECTORS:
        detector = detector_class()
        if detector.detector_id not in registry.get_detector_ids():
            registry.register(detector)


__all__ = [
    # Base classes
    "EntropyDetector",
    "DetectorContext",
    "DetectorRegistry",
    "get_default_registry",
    # Registration
    "BUILTIN_DETECTORS",
    "register_builtin_detectors",
    # Structural detectors
    "TypeFidelityDetector",
    "JoinPathDeterminismDetector",
    "RelationshipEntropyDetector",
    # Value detectors
    "BenfordDetector",
    "NullRatioDetector",
    "OutlierRateDetector",
    "TemporalDriftDetector",
    # Semantic detectors
    "BusinessMeaningDetector",
    "UnitEntropyDetector",
    "TemporalEntropyDetector",
    "ColumnQualityDetector",
    "DimensionalEntropyDetector",
    "DimensionCoverageDetector",
    # Computational detectors
    "DerivedValueDetector",
]
