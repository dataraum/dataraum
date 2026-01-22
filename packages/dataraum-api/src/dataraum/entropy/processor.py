"""Entropy processor for running detectors and aggregating results.

This module orchestrates entropy detection:
- Runs all applicable detectors for a given context
- Aggregates results into column/table profiles
- Detects compound risks
- Finds top resolution cascades

Usage:
    from dataraum.entropy.processor import EntropyProcessor

    processor = EntropyProcessor()
    context = processor.process_column(
        table_name="orders",
        column_name="amount",
        analysis_results={
            "typing": type_candidate,
            "statistics": column_profile,
            "semantic": semantic_annotation,
        }
    )
"""

from dataclasses import dataclass
from typing import Any

from dataraum.core.logging import get_logger
from dataraum.entropy.compound_risk import detect_compound_risks_for_column
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    get_default_registry,
)
from dataraum.entropy.models import (
    ColumnEntropyProfile,
    EntropyContext,
    EntropyObject,
    TableEntropyProfile,
)
from dataraum.entropy.resolution import find_top_resolutions

logger = get_logger(__name__)


@dataclass
class ProcessorConfig:
    """Configuration for entropy processor.

    Defaults are loaded from config/entropy/thresholds.yaml.
    Can be overridden by passing explicit values.
    """

    # Score weights for composite calculation
    layer_weights: dict[str, float] | None = None

    # Thresholds
    high_entropy_threshold: float | None = None
    critical_entropy_threshold: float | None = None

    # Resolution settings
    max_resolution_hints: int = 3

    # Parallel execution
    max_concurrent_detectors: int = 10

    def __post_init__(self) -> None:
        """Load defaults from config if not explicitly set."""
        config = get_entropy_config()

        if self.layer_weights is None:
            self.layer_weights = config.composite_weights

        if self.high_entropy_threshold is None:
            self.high_entropy_threshold = config.high_entropy_threshold

        if self.critical_entropy_threshold is None:
            self.critical_entropy_threshold = config.critical_entropy_threshold


class EntropyProcessor:
    """Orchestrates entropy detection across detectors.

    Runs detectors, aggregates results into profiles, and detects
    compound risks.
    """

    def __init__(
        self,
        registry: DetectorRegistry | None = None,
        config: ProcessorConfig | None = None,
    ):
        """Initialize processor.

        Args:
            registry: Detector registry to use. Defaults to global registry.
            config: Processor configuration.
        """
        self.registry = registry or get_default_registry()
        self.config = config or ProcessorConfig()

    def process_column(
        self,
        table_name: str,
        column_name: str,
        analysis_results: dict[str, Any],
        source_id: str | None = None,
        table_id: str | None = None,
        column_id: str | None = None,
    ) -> ColumnEntropyProfile:
        """Process a single column and return its entropy profile.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            analysis_results: Dict of module name to analysis result
            source_id: Optional source ID
            table_id: Optional table ID
            column_id: Optional column ID

        Returns:
            ColumnEntropyProfile with aggregated entropy
        """
        # Create detector context
        context = DetectorContext(
            source_id=source_id,
            table_id=table_id,
            table_name=table_name,
            column_id=column_id,
            column_name=column_name,
            analysis_results=analysis_results,
        )

        # Run all applicable detectors
        entropy_objects = self._run_detectors(context)

        # Aggregate into profile
        profile = self._aggregate_to_profile(
            table_name=table_name,
            column_name=column_name,
            column_id=column_id or "",
            entropy_objects=entropy_objects,
        )

        # Detect compound risks
        profile.compound_risks = detect_compound_risks_for_column(profile, entropy_objects)

        # Find top resolution hints
        cascades = find_top_resolutions(entropy_objects, limit=self.config.max_resolution_hints)
        # Convert cascades to resolution options for the profile
        for cascade in cascades:
            if cascade.entropy_reductions:
                from dataraum.entropy.models import ResolutionOption

                opt = ResolutionOption(
                    action=cascade.action,
                    parameters=cascade.parameters,
                    expected_entropy_reduction=cascade.total_reduction,
                    effort=cascade.effort,
                    description=cascade.description,
                    cascade_dimensions=list(cascade.entropy_reductions.keys()),
                )
                profile.top_resolution_hints.append(opt)

        return profile

    def process_table(
        self,
        table_name: str,
        columns: list[dict[str, Any]],
        source_id: str | None = None,
        table_id: str | None = None,
    ) -> TableEntropyProfile:
        """Process all columns in a table and return table entropy profile.

        Args:
            table_name: Name of the table
            columns: List of column specs with 'name' and 'analysis_results'
            source_id: Optional source ID
            table_id: Optional table ID

        Returns:
            TableEntropyProfile with aggregated entropy
        """
        # Process each column
        column_profiles: list[ColumnEntropyProfile] = []

        for col_spec in columns:
            column_name = col_spec.get("name", "")
            analysis_results = col_spec.get("analysis_results", {})
            column_id = col_spec.get("column_id")

            profile = self.process_column(
                table_name=table_name,
                column_name=column_name,
                analysis_results=analysis_results,
                source_id=source_id,
                table_id=table_id,
                column_id=column_id,
            )
            column_profiles.append(profile)

        # Create table profile
        table_profile = TableEntropyProfile(
            table_id=table_id or "",
            table_name=table_name,
            column_profiles=column_profiles,
        )
        table_profile.calculate_aggregates()

        return table_profile

    def build_entropy_context(
        self,
        tables: list[TableEntropyProfile],
    ) -> EntropyContext:
        """Build complete entropy context from table profiles.

        Args:
            tables: List of table entropy profiles

        Returns:
            EntropyContext for graph agent consumption
        """
        context = EntropyContext()

        for table_profile in tables:
            # Add table profile
            context.table_profiles[table_profile.table_name] = table_profile

            # Add column profiles
            for col_profile in table_profile.column_profiles:
                key = f"{table_profile.table_name}.{col_profile.column_name}"
                context.column_profiles[key] = col_profile

                # Collect compound risks
                context.compound_risks.extend(col_profile.compound_risks)

        # Update summary stats
        context.update_summary_stats()

        return context

    def _run_detectors(self, context: DetectorContext) -> list[EntropyObject]:
        """Run all applicable detectors and collect results.

        Args:
            context: Detector context

        Returns:
            List of EntropyObject instances from all detectors
        """
        # Get detectors that can run with this context
        detectors = self.registry.get_runnable_detectors(context)

        if not detectors:
            logger.debug(f"No detectors can run for {context.target_ref}")
            return []

        logger.debug(f"Running {len(detectors)} detectors for {context.target_ref}")

        # Run detectors (could be parallel in future)
        all_objects: list[EntropyObject] = []

        for detector in detectors:
            try:
                objects = detector.detect(context)
                all_objects.extend(objects)
            except Exception as e:
                logger.error(f"Detector {detector.detector_id} failed: {e}")
                # Continue with other detectors

        return all_objects

    def _aggregate_to_profile(
        self,
        table_name: str,
        column_name: str,
        column_id: str,
        entropy_objects: list[EntropyObject],
    ) -> ColumnEntropyProfile:
        """Aggregate entropy objects into a column profile.

        Args:
            table_name: Table name
            column_name: Column name
            column_id: Column ID
            entropy_objects: List of entropy objects

        Returns:
            ColumnEntropyProfile with aggregated scores
        """
        profile = ColumnEntropyProfile(
            column_id=column_id,
            column_name=column_name,
            table_name=table_name,
        )

        # Group by layer and calculate layer scores
        layer_scores: dict[str, list[float]] = {
            "structural": [],
            "semantic": [],
            "value": [],
            "computational": [],
        }

        for obj in entropy_objects:
            if obj.layer in layer_scores:
                layer_scores[obj.layer].append(obj.score)

            # Also track dimension-level scores
            profile.dimension_scores[obj.dimension_path] = obj.score

        # Calculate layer averages (or 0 if no detectors ran)
        profile.structural_entropy = (
            sum(layer_scores["structural"]) / len(layer_scores["structural"])
            if layer_scores["structural"]
            else 0.0
        )
        profile.semantic_entropy = (
            sum(layer_scores["semantic"]) / len(layer_scores["semantic"])
            if layer_scores["semantic"]
            else 0.0
        )
        profile.value_entropy = (
            sum(layer_scores["value"]) / len(layer_scores["value"])
            if layer_scores["value"]
            else 0.0
        )
        profile.computational_entropy = (
            sum(layer_scores["computational"]) / len(layer_scores["computational"])
            if layer_scores["computational"]
            else 0.0
        )

        # Calculate composite and update readiness
        profile.calculate_composite(self.config.layer_weights)
        profile.update_high_entropy_dimensions(self.config.high_entropy_threshold)
        profile.update_readiness()

        return profile


def process_column_entropy(
    table_name: str,
    column_name: str,
    analysis_results: dict[str, Any],
    **kwargs: Any,
) -> ColumnEntropyProfile:
    """Convenience function to process a single column.

    Args:
        table_name: Table name
        column_name: Column name
        analysis_results: Analysis results from other modules
        **kwargs: Additional arguments for process_column

    Returns:
        ColumnEntropyProfile
    """
    processor = EntropyProcessor()
    return processor.process_column(
        table_name=table_name,
        column_name=column_name,
        analysis_results=analysis_results,
        **kwargs,
    )


def process_table_entropy(
    table_name: str,
    columns: list[dict[str, Any]],
    **kwargs: Any,
) -> TableEntropyProfile:
    """Convenience function to process a table.

    Args:
        table_name: Table name
        columns: List of column specs
        **kwargs: Additional arguments for process_table

    Returns:
        TableEntropyProfile
    """
    processor = EntropyProcessor()
    return processor.process_table(
        table_name=table_name,
        columns=columns,
        **kwargs,
    )
