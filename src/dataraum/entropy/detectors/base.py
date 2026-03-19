"""Base classes for entropy detection.

This module provides:
- EntropyDetector: Abstract base class for all entropy detectors
- DetectorRegistry: Registry for detector discovery and management
- DetectorContext: Context passed to detectors with analysis data

Each detector focuses on a specific sub-dimension of entropy and
produces EntropyObject instances with scores, evidence, and resolution options.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import (
    EntropyObject,
    ResolutionOption,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.pipeline.fixes.models import FixSchema


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
    view_name: str = ""

    # SQLAlchemy session for detector-driven data loading
    session: Session | None = None

    # DuckDB connection for data queries
    duckdb_conn: Any = None

    # Analysis results from other modules (keyed by module name)
    # e.g., {"typing": TypeCandidate, "statistics": ColumnProfile, ...}
    analysis_results: dict[str, Any] = field(default_factory=dict)

    # Configuration overrides
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def target_ref(self) -> str:
        """Get the target reference string."""
        if self.view_name:
            return f"view:{self.view_name}"
        elif self.column_name:
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
    layer: Layer  # structural, semantic, value, computational
    dimension: Dimension  # types, relations, units, etc.
    sub_dimension: SubDimension  # subclasses must set this

    # Target scope: "column" (per-column analysis) or "table" (cross-column analysis)
    scope: str = "column"

    # What analysis modules this detector requires
    required_analyses: list[AnalysisKey] = []

    # Human-readable description
    description: str = ""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize detector with optional configuration.

        Args:
            config: Detector-specific configuration overrides
        """
        self.config = config or {}

    def load_data(self, context: DetectorContext) -> None:  # noqa: B027
        """Load analysis data into context.analysis_results.

        Override in subclasses to query DB via context.session and populate
        context.analysis_results[key]. Default is a no-op so pre-populated
        contexts (e.g. in tests) still work.
        """

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
    ) -> EntropyObject:
        """Helper to create an EntropyObject with detector metadata.

        Args:
            context: Detection context
            score: Entropy score (0.0-1.0)
            evidence: Evidence supporting the score
            resolution_options: Ways to reduce entropy

        Returns:
            Configured EntropyObject
        """
        # Inject context identifiers into evidence for self-identification
        enriched_evidence = evidence or []
        for ev in enriched_evidence:
            ev["_column_name"] = context.column_name
            ev["_table_name"] = context.table_name

        return EntropyObject(
            layer=self.layer,
            dimension=self.dimension,
            sub_dimension=self.sub_dimension,
            target=context.target_ref,
            score=score,
            evidence=enriched_evidence,
            resolution_options=resolution_options or [],
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

        Validates that layer, dimension, and sub_dimension use the
        correct enum types to catch typos at startup.

        Args:
            detector: Detector instance to register

        Raises:
            TypeError: If layer, dimension, or sub_dimension are not enum instances.
        """
        if not isinstance(detector.layer, Layer):
            raise TypeError(
                f"Detector {detector.detector_id!r}: layer must be a Layer enum, "
                f"got {type(detector.layer).__name__} ({detector.layer!r})"
            )
        if not isinstance(detector.dimension, Dimension):
            raise TypeError(
                f"Detector {detector.detector_id!r}: dimension must be a Dimension enum, "
                f"got {type(detector.dimension).__name__} ({detector.dimension!r})"
            )
        if not isinstance(detector.sub_dimension, SubDimension):
            raise TypeError(
                f"Detector {detector.detector_id!r}: sub_dimension must be a SubDimension enum, "
                f"got {type(detector.sub_dimension).__name__} ({detector.sub_dimension!r})"
            )
        self.detectors[detector.detector_id] = detector

    def get_all_detectors(self) -> list[EntropyDetector]:
        """Get all registered detectors.

        Returns:
            List of all detector instances
        """
        return list(self.detectors.values())

    def get_detector_ids(self) -> list[str]:
        """Get list of all registered detector IDs.

        Returns:
            List of detector IDs
        """
        return list(self.detectors.keys())

    def get_fix_schema(
        self, action_name: str, dimension_path: str | None = None
    ) -> FixSchema | None:
        """Find a FixSchema by action name, optionally scoped by dimension.

        Delegates to the YAML fix schema loader.

        Each action name is now unique (e.g. ``document_accepted_null_ratio``).
        *dimension_path* can still be used to scope the search to a specific
        detector.

        Args:
            action_name: The action to look up.
            dimension_path: If provided, only consider detectors whose
                dimension_path matches.

        Returns:
            The matching FixSchema, or None if not found.
        """
        from dataraum.entropy.fix_schemas import get_fix_schema as yaml_get_fix_schema

        return yaml_get_fix_schema(action_name, dimension_path=dimension_path)


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
    from dataraum.entropy.detectors.value.benford import BenfordDetector
    from dataraum.entropy.detectors.value.null_semantics import NullRatioDetector
    from dataraum.entropy.detectors.value.outliers import OutlierRateDetector
    from dataraum.entropy.detectors.value.slice_variance import SliceVarianceDetector
    from dataraum.entropy.detectors.value.temporal_drift import TemporalDriftDetector

    registry.register(NullRatioDetector())
    registry.register(OutlierRateDetector())
    registry.register(BenfordDetector())
    registry.register(TemporalDriftDetector())
    registry.register(SliceVarianceDetector())

    # Semantic layer detectors
    from dataraum.entropy.detectors.semantic.business_meaning import BusinessMeaningDetector
    from dataraum.entropy.detectors.semantic.dimensional_entropy import DimensionalEntropyDetector
    from dataraum.entropy.detectors.semantic.temporal_entropy import TemporalEntropyDetector
    from dataraum.entropy.detectors.semantic.unit_entropy import UnitEntropyDetector

    registry.register(BusinessMeaningDetector())
    registry.register(UnitEntropyDetector())
    registry.register(TemporalEntropyDetector())
    registry.register(DimensionalEntropyDetector())

    from dataraum.entropy.detectors.semantic.column_quality import ColumnQualityDetector
    from dataraum.entropy.detectors.semantic.dimension_coverage import DimensionCoverageDetector

    registry.register(ColumnQualityDetector())
    registry.register(DimensionCoverageDetector())

    # Semantic layer detectors (table-scoped, Zone 3)
    from dataraum.entropy.detectors.semantic.business_cycle_health import (
        BusinessCycleHealthDetector,
    )

    registry.register(BusinessCycleHealthDetector())

    # Computational layer detectors
    from dataraum.entropy.detectors.computational.cross_table_consistency import (
        CrossTableConsistencyDetector,
    )
    from dataraum.entropy.detectors.computational.derived_values import (
        DerivedValueDetector,
    )

    registry.register(CrossTableConsistencyDetector())
    registry.register(DerivedValueDetector())
