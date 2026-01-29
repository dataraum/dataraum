"""Base classes for entropy detection.

This module provides:
- EntropyDetector: Abstract base class for all entropy detectors
- DetectorRegistry: Registry for detector discovery and management
- DetectorContext: Context passed to detectors with analysis data

Each detector focuses on a specific sub-dimension of entropy and
produces EntropyObject instances with scores, evidence, and resolution options.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from dataraum.entropy.models import (
    EntropyObject,
    HumanContext,
    LLMContext,
    ResolutionOption,
)


@dataclass
class DetectorContext:
    """Context passed to detectors containing analysis results.

    Detectors read from existing analysis modules (typing, statistics,
    semantic, etc.) rather than re-analyzing raw data.
    """

    # Target identification
    source_id: str | None = None
    table_id: str | None = None
    table_name: str = ""
    column_id: str | None = None
    column_name: str = ""

    # Analysis results from other modules (keyed by module name)
    # e.g., {"typing": TypeCandidate, "statistics": ColumnProfile, ...}
    analysis_results: dict[str, Any] = field(default_factory=dict)

    # Configuration overrides
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def target_ref(self) -> str:
        """Get the target reference string."""
        if self.column_name:
            return f"column:{self.table_name}.{self.column_name}"
        elif self.table_name:
            return f"table:{self.table_name}"
        return "unknown"

    def get_analysis(self, module: str, default: Any = None) -> Any:
        """Get analysis result for a module."""
        return self.analysis_results.get(module, default)


class EntropyDetector(ABC):
    """Abstract base class for entropy detectors.

    Each detector focuses on a specific sub-dimension of entropy:
    - Layer: structural, semantic, value, computational
    - Dimension: types, relations, units, aggregations, etc.
    - Sub-dimension: type_fidelity, naming_clarity, etc.

    Detectors read from existing analysis modules and produce
    EntropyObject instances with:
    - Score (0.0 = deterministic, 1.0 = maximum uncertainty)
    - Evidence (what led to this score)
    - Resolution options (how to fix it)
    - Context for LLM and human consumers
    """

    # Detector identity (override in subclasses)
    detector_id: str = "base"
    layer: str = ""  # structural, semantic, value, computational
    dimension: str = ""  # types, relations, units, etc.
    sub_dimension: str = ""  # type_fidelity, naming_clarity, etc.

    # What analysis modules this detector requires
    required_analyses: list[str] = []

    # Human-readable description
    description: str = ""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize detector with optional configuration.

        Args:
            config: Detector-specific configuration overrides
        """
        self.config = config or {}

    @abstractmethod
    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Run detection and return entropy objects.

        Args:
            context: Detection context with analysis results

        Returns:
            List of EntropyObject instances (usually one per target)
        """
        pass

    def can_run(self, context: DetectorContext) -> bool:
        """Check if this detector can run with the given context.

        Verifies that all required analyses are available.

        Args:
            context: Detection context

        Returns:
            True if detector can run, False otherwise
        """
        for module in self.required_analyses:
            if module not in context.analysis_results:
                return False
        return True

    def create_entropy_object(
        self,
        context: DetectorContext,
        score: float,
        evidence: list[dict[str, Any]] | None = None,
        resolution_options: list[ResolutionOption] | None = None,
        llm_context: LLMContext | None = None,
        human_context: HumanContext | None = None,
    ) -> EntropyObject:
        """Helper to create an EntropyObject with detector metadata.

        Args:
            context: Detection context
            score: Entropy score (0.0-1.0)
            evidence: Evidence supporting the score
            resolution_options: Ways to reduce entropy
            llm_context: Context for LLM agents
            human_context: Context for human users

        Returns:
            Configured EntropyObject
        """
        return EntropyObject(
            layer=self.layer,
            dimension=self.dimension,
            sub_dimension=self.sub_dimension,
            target=context.target_ref,
            score=score,
            evidence=evidence or [],
            resolution_options=resolution_options or [],
            llm_context=llm_context or LLMContext(),
            human_context=human_context or HumanContext(),
            detector_id=self.detector_id,
            source_analysis_ids=[],
        )

    @property
    def dimension_path(self) -> str:
        """Get full dimension path."""
        return f"{self.layer}.{self.dimension}.{self.sub_dimension}"


@dataclass
class DetectorRegistry:
    """Registry for entropy detector discovery and management.

    Detectors register themselves and can be looked up by:
    - Detector ID
    - Layer (structural, semantic, value, computational)
    - Dimension
    - Required analysis modules
    """

    detectors: dict[str, EntropyDetector] = field(default_factory=dict)

    def register(self, detector: EntropyDetector) -> None:
        """Register a detector.

        Args:
            detector: Detector instance to register
        """
        self.detectors[detector.detector_id] = detector

    def unregister(self, detector_id: str) -> None:
        """Unregister a detector by ID.

        Args:
            detector_id: ID of detector to remove
        """
        self.detectors.pop(detector_id, None)

    def get_detector(self, detector_id: str) -> EntropyDetector | None:
        """Get a detector by ID.

        Args:
            detector_id: Detector ID

        Returns:
            Detector instance or None if not found
        """
        return self.detectors.get(detector_id)

    def get_all_detectors(self) -> list[EntropyDetector]:
        """Get all registered detectors.

        Returns:
            List of all detector instances
        """
        return list(self.detectors.values())

    def get_detectors_for_layer(self, layer: str) -> list[EntropyDetector]:
        """Get all detectors for a specific layer.

        Args:
            layer: Layer name (structural, semantic, value, computational)

        Returns:
            List of detectors for that layer
        """
        return [d for d in self.detectors.values() if d.layer == layer]

    def get_detectors_for_dimension(self, layer: str, dimension: str) -> list[EntropyDetector]:
        """Get all detectors for a specific dimension.

        Args:
            layer: Layer name
            dimension: Dimension name

        Returns:
            List of detectors for that dimension
        """
        return [d for d in self.detectors.values() if d.layer == layer and d.dimension == dimension]

    def get_runnable_detectors(self, context: DetectorContext) -> list[EntropyDetector]:
        """Get all detectors that can run with the given context.

        Args:
            context: Detection context with available analyses

        Returns:
            List of detectors that have required analyses available
        """
        return [d for d in self.detectors.values() if d.can_run(context)]

    def get_detector_ids(self) -> list[str]:
        """Get list of all registered detector IDs.

        Returns:
            List of detector IDs
        """
        return list(self.detectors.keys())

    def get_layers(self) -> list[str]:
        """Get list of unique layers with registered detectors.

        Returns:
            List of layer names
        """
        return list({d.layer for d in self.detectors.values()})

    def get_dimensions(self, layer: str) -> list[str]:
        """Get list of dimensions for a layer.

        Args:
            layer: Layer name

        Returns:
            List of dimension names
        """
        return list({d.dimension for d in self.detectors.values() if d.layer == layer})


# Global registry instance
_default_registry: DetectorRegistry | None = None


def get_default_registry() -> DetectorRegistry:
    """Get the default detector registry.

    Creates and populates the registry on first call.

    Returns:
        Default DetectorRegistry instance
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = DetectorRegistry()
        _register_builtin_detectors(_default_registry)
    return _default_registry


def _register_builtin_detectors(registry: DetectorRegistry) -> None:
    """Register built-in detectors with the registry.

    This is called once when the default registry is created.
    Detectors are imported here to avoid circular imports.
    """
    # Structural layer detectors
    from dataraum.entropy.detectors.structural.relations import JoinPathDeterminismDetector
    from dataraum.entropy.detectors.structural.relationship_entropy import (
        RelationshipEntropyDetector,
    )
    from dataraum.entropy.detectors.structural.types import TypeFidelityDetector

    registry.register(TypeFidelityDetector())
    registry.register(JoinPathDeterminismDetector())
    registry.register(RelationshipEntropyDetector())

    # Value layer detectors
    from dataraum.entropy.detectors.value.null_semantics import NullRatioDetector
    from dataraum.entropy.detectors.value.outliers import OutlierRateDetector

    registry.register(NullRatioDetector())
    registry.register(OutlierRateDetector())

    # Semantic layer detectors
    from dataraum.entropy.detectors.semantic.business_meaning import BusinessMeaningDetector
    from dataraum.entropy.detectors.semantic.temporal_entropy import TemporalEntropyDetector
    from dataraum.entropy.detectors.semantic.unit_entropy import UnitEntropyDetector

    registry.register(BusinessMeaningDetector())
    registry.register(UnitEntropyDetector())
    registry.register(TemporalEntropyDetector())

    # Computational layer detectors
    from dataraum.entropy.detectors.computational.derived_values import (
        DerivedValueDetector,
    )

    registry.register(DerivedValueDetector())
