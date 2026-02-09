"""Compound risk detection for dangerous entropy combinations.

Some dimension pairs create multiplicative risk that exceeds the sum
of individual scores. This module detects these dangerous combinations
and assigns appropriate risk levels.

Configuration is loaded from config/entropy/thresholds.yaml (compound_risks section).
This module uses fail-fast: if no config is found, it raises an error rather than
using hardcoded defaults (which can become stale).
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from dataraum.core.config import get_config_file
from dataraum.core.logging import get_logger
from dataraum.entropy.models import (
    CompoundRisk,
    CompoundRiskDefinition,
    EntropyObject,
    ResolutionOption,
)

if TYPE_CHECKING:
    from dataraum.entropy.analysis.aggregator import ColumnSummary

logger = get_logger(__name__)


@dataclass
class CompoundRiskDetector:
    """Detects dangerous combinations of entropy dimensions.

    Loads risk definitions from configuration and evaluates
    entropy summaries against those definitions.
    """

    risk_definitions: list[CompoundRiskDefinition] = field(default_factory=list)
    config_loaded: bool = False

    def load_config(self, config_path: Path | None = None) -> None:
        """Load compound risk definitions from YAML config.

        Loads from thresholds.yaml (primary) or compound_risks.yaml (override).
        The compound_risks section in thresholds.yaml is the default location.

        Args:
            config_path: Optional path to override config file.
        """
        # First, try loading from thresholds.yaml (the standard location)
        if config_path is None:
            self._load_from_thresholds()
            if self.config_loaded:
                return

        # If explicit path provided, or thresholds.yaml didn't have compound_risks,
        # try the dedicated compound_risks.yaml file
        if config_path is None:
            config_path = get_config_file("system/entropy/compound_risks.yaml")

        if not config_path.exists():
            if not self.config_loaded:
                # Fail fast - require configuration instead of using stale hardcoded defaults
                raise FileNotFoundError(
                    f"Compound risk config not found: {config_path}. "
                    "Add 'compound_risks' section to config/entropy/thresholds.yaml"
                )
            return

        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)

            self.risk_definitions = []
            for risk_type, definition in config.get("compound_risks", {}).items():
                self.risk_definitions.append(
                    CompoundRiskDefinition(
                        risk_type=risk_type,
                        dimensions=definition.get("dimensions", []),
                        threshold=definition.get("threshold", 0.5),
                        risk_level=definition.get("risk_level", "high"),
                        impact_template=definition.get("impact_template", ""),
                        multiplier=definition.get("multiplier", 1.5),
                    )
                )
            self.config_loaded = True
            logger.info(
                f"Loaded {len(self.risk_definitions)} compound risk definitions from {config_path}"
            )

        except Exception as e:
            # Fail fast on config errors
            raise RuntimeError(f"Error loading compound risk config: {e}") from e

    def _load_from_thresholds(self) -> None:
        """Load compound risk definitions from thresholds.yaml."""
        from dataraum.entropy.config import get_entropy_config

        config = get_entropy_config()

        if config.compound_risks:
            self.risk_definitions = [
                CompoundRiskDefinition(
                    risk_type=risk_config.risk_type,
                    dimensions=risk_config.dimensions,
                    threshold=risk_config.threshold,
                    risk_level=risk_config.risk_level,
                    impact_template=risk_config.impact_template,
                    multiplier=risk_config.multiplier,
                )
                for risk_config in config.compound_risks.values()
            ]
            self.config_loaded = True
            logger.debug(
                f"Loaded {len(self.risk_definitions)} compound risk definitions from thresholds.yaml"
            )

    def detect_risks(
        self,
        summary: ColumnSummary,
        entropy_objects: list[EntropyObject] | None = None,
    ) -> list[CompoundRisk]:
        """Detect compound risks for a column.

        Args:
            summary: Column entropy summary with dimension scores
            entropy_objects: Optional list of entropy objects for resolution extraction

        Returns:
            List of detected compound risks
        """
        if not self.config_loaded:
            self.load_config()

        detected: list[CompoundRisk] = []

        for definition in self.risk_definitions:
            risk = self._evaluate_definition(summary, definition, entropy_objects)
            if risk:
                detected.append(risk)

        return detected

    def _evaluate_definition(
        self,
        summary: ColumnSummary,
        definition: CompoundRiskDefinition,
        entropy_objects: list[EntropyObject] | None = None,
    ) -> CompoundRisk | None:
        """Evaluate a single risk definition against a summary.

        Returns CompoundRisk if the definition matches, None otherwise.
        """
        # Check if all dimensions exceed threshold
        dimension_scores: dict[str, float] = {}
        all_above_threshold = True

        for dimension in definition.dimensions:
            # Look for matching dimension in summary
            score = self._get_dimension_score(summary, dimension)
            dimension_scores[dimension] = score

            if score < definition.threshold:
                all_above_threshold = False

        if not all_above_threshold:
            return None

        # Risk detected - create CompoundRisk
        impact = definition.impact_template

        # Calculate combined score with multiplier
        avg_score = sum(dimension_scores.values()) / len(dimension_scores)
        combined_score = min(1.0, avg_score * definition.multiplier)

        # Extract relevant resolution options from entropy objects
        mitigation_options: list[ResolutionOption] = []
        if entropy_objects:
            for obj in entropy_objects:
                if any(d in obj.dimension_path for d in definition.dimensions):
                    mitigation_options.extend(obj.resolution_options[:1])  # Top option only

        return CompoundRisk(
            target=f"column:{summary.table_name}.{summary.column_name}",
            dimensions=definition.dimensions,
            dimension_scores=dimension_scores,
            risk_level=definition.risk_level,
            impact=impact,
            multiplier=definition.multiplier,
            combined_score=combined_score,
            mitigation_options=mitigation_options,
        )

    def _get_dimension_score(self, summary: ColumnSummary, dimension: str) -> float:
        """Get score for a dimension from summary.

        Handles both full paths (structural.types.type_fidelity) and
        partial paths (structural.types).
        """
        # First try exact match in dimension_scores
        if dimension in summary.dimension_scores:
            return summary.dimension_scores[dimension]

        # Try partial match (e.g., "semantic.units" matches "semantic.units.unit_declared")
        for dim_path, score in summary.dimension_scores.items():
            if dim_path.startswith(dimension):
                return score

        # Fall back to layer-level scores
        layer = dimension.split(".")[0] if "." in dimension else dimension
        return summary.layer_scores.get(layer, 0.0)


def detect_compound_risks_for_column(
    summary: ColumnSummary,
    entropy_objects: list[EntropyObject] | None = None,
    config_path: Path | None = None,
) -> list[CompoundRisk]:
    """Convenience function to detect compound risks for a column.

    Args:
        summary: Column entropy summary
        entropy_objects: Optional entropy objects for resolution extraction
        config_path: Optional path to compound_risks.yaml

    Returns:
        List of detected compound risks
    """
    detector = CompoundRiskDetector()
    if config_path:
        detector.load_config(config_path)
    return detector.detect_risks(summary, entropy_objects)
