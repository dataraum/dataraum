"""Entropy detectors for measuring uncertainty across dimensions.

This module provides the detector infrastructure for the entropy layer:
- EntropyDetector: Abstract base class for all detectors
- DetectorRegistry: Registry for detector discovery and management
- Built-in detectors for structural, semantic, value, and computational entropy

Usage:
    from dataraum_context.entropy.detectors import (
        EntropyDetector,
        DetectorRegistry,
        get_default_registry,
        register_builtin_detectors,
    )

    # Register all built-in detectors
    register_builtin_detectors()

    # Get all registered detectors
    registry = get_default_registry()
    detectors = registry.get_detectors_for_layer("structural")

    # Run a specific detector
    detector = registry.get_detector("type_fidelity")
    results = detector.detect(context)
"""

from dataraum_context.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    EntropyDetector,
    get_default_registry,
)

# Computational layer detectors
from dataraum_context.entropy.detectors.computational import (
    DerivedValueDetector,
)

# Semantic layer detectors
from dataraum_context.entropy.detectors.semantic import (
    BusinessMeaningDetector,
)

# Structural layer detectors
from dataraum_context.entropy.detectors.structural import (
    JoinPathDeterminismDetector,
    TypeFidelityDetector,
)

# Value layer detectors
from dataraum_context.entropy.detectors.value import (
    NullRatioDetector,
    OutlierRateDetector,
)

# All built-in detector classes
BUILTIN_DETECTORS: list[type[EntropyDetector]] = [
    # Structural
    TypeFidelityDetector,
    JoinPathDeterminismDetector,
    # Value
    NullRatioDetector,
    OutlierRateDetector,
    # Semantic
    BusinessMeaningDetector,
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
    # Value detectors
    "NullRatioDetector",
    "OutlierRateDetector",
    # Semantic detectors
    "BusinessMeaningDetector",
    # Computational detectors
    "DerivedValueDetector",
]
