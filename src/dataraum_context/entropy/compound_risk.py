"""Compound risk detection for dangerous entropy combinations.

Some dimension pairs create multiplicative risk that exceeds the sum
of individual scores. This module detects these dangerous combinations
and assigns appropriate risk levels.

Key compound risks:
- semantic.units + computational.aggregations -> Critical
- structural.relations + computational.filters -> High
- value.nulls + computational.aggregations -> High
- semantic.temporal + value.ranges -> Medium
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dataraum_context.entropy.models import (
    ColumnEntropyProfile,
    CompoundRisk,
    CompoundRiskDefinition,
    EntropyObject,
    ResolutionOption,
)

logger = logging.getLogger(__name__)

# Default config directory
DEFAULT_CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "config" / "entropy"


@dataclass
class CompoundRiskDetector:
    """Detects dangerous combinations of entropy dimensions.

    Loads risk definitions from configuration and evaluates
    entropy profiles against those definitions.
    """

    risk_definitions: list[CompoundRiskDefinition] = field(default_factory=list)
    config_loaded: bool = False

    def load_config(self, config_path: Path | None = None) -> None:
        """Load compound risk definitions from YAML config.

        Args:
            config_path: Path to compound_risks.yaml. Defaults to config/entropy/compound_risks.yaml.
        """
        config_path = config_path or (DEFAULT_CONFIG_DIR / "compound_risks.yaml")

        if not config_path.exists():
            logger.warning(f"Compound risk config not found: {config_path}. Using defaults.")
            self._load_default_definitions()
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
            logger.info(f"Loaded {len(self.risk_definitions)} compound risk definitions")

        except Exception as e:
            logger.error(f"Error loading compound risk config: {e}")
            self._load_default_definitions()

    def _load_default_definitions(self) -> None:
        """Load built-in default risk definitions."""
        self.risk_definitions = [
            # Critical: Units + Aggregations
            CompoundRiskDefinition(
                risk_type="units_aggregations",
                dimensions=["semantic.units", "computational.aggregations"],
                threshold=0.5,
                risk_level="critical",
                impact_template=(
                    "Unknown currencies/units being summed without conversion. "
                    "Results could be off by 20-40%."
                ),
                multiplier=2.0,
            ),
            # High: Relations + Filters
            CompoundRiskDefinition(
                risk_type="relations_filters",
                dimensions=["structural.relations", "computational.filters"],
                threshold=0.5,
                risk_level="high",
                impact_template=(
                    "Non-deterministic join paths combined with filtering. "
                    "Different query paths may give different results."
                ),
                multiplier=1.8,
            ),
            # High: Nulls + Aggregations
            CompoundRiskDefinition(
                risk_type="nulls_aggregations",
                dimensions=["value.nulls", "computational.aggregations"],
                threshold=0.5,
                risk_level="high",
                impact_template=(
                    "High null ratio in aggregated columns. "
                    "Results may exclude significant data or misrepresent averages."
                ),
                multiplier=1.5,
            ),
            # Medium: Temporal + Ranges
            CompoundRiskDefinition(
                risk_type="temporal_ranges",
                dimensions=["semantic.temporal", "value.ranges"],
                threshold=0.5,
                risk_level="medium",
                impact_template=(
                    "Unclear time periods combined with outlier values. "
                    "May include or exclude data incorrectly."
                ),
                multiplier=1.3,
            ),
            # High: Types + Aggregations
            CompoundRiskDefinition(
                risk_type="types_aggregations",
                dimensions=["structural.types", "computational.aggregations"],
                threshold=0.6,
                risk_level="high",
                impact_template=(
                    "Type inference issues in columns being aggregated. "
                    "Some values may fail to parse, skewing results."
                ),
                multiplier=1.5,
            ),
        ]
        self.config_loaded = True
        logger.info(f"Loaded {len(self.risk_definitions)} default compound risk definitions")

    def detect_risks(
        self,
        profile: ColumnEntropyProfile,
        entropy_objects: list[EntropyObject] | None = None,
    ) -> list[CompoundRisk]:
        """Detect compound risks for a column.

        Args:
            profile: Column entropy profile with dimension scores
            entropy_objects: Optional list of entropy objects for resolution extraction

        Returns:
            List of detected compound risks
        """
        if not self.config_loaded:
            self.load_config()

        detected: list[CompoundRisk] = []

        for definition in self.risk_definitions:
            risk = self._evaluate_definition(profile, definition, entropy_objects)
            if risk:
                detected.append(risk)

        return detected

    def _evaluate_definition(
        self,
        profile: ColumnEntropyProfile,
        definition: CompoundRiskDefinition,
        entropy_objects: list[EntropyObject] | None = None,
    ) -> CompoundRisk | None:
        """Evaluate a single risk definition against a profile.

        Returns CompoundRisk if the definition matches, None otherwise.
        """
        # Check if all dimensions exceed threshold
        dimension_scores: dict[str, float] = {}
        all_above_threshold = True

        for dimension in definition.dimensions:
            # Look for matching dimension in profile
            score = self._get_dimension_score(profile, dimension)
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
            target=f"column:{profile.table_name}.{profile.column_name}",
            dimensions=definition.dimensions,
            dimension_scores=dimension_scores,
            risk_level=definition.risk_level,
            impact=impact,
            multiplier=definition.multiplier,
            combined_score=combined_score,
            mitigation_options=mitigation_options,
        )

    def _get_dimension_score(self, profile: ColumnEntropyProfile, dimension: str) -> float:
        """Get score for a dimension from profile.

        Handles both full paths (structural.types.type_fidelity) and
        partial paths (structural.types).
        """
        # First try exact match
        if dimension in profile.dimension_scores:
            return profile.dimension_scores[dimension]

        # Try partial match (e.g., "semantic.units" matches "semantic.units.unit_declared")
        for dim_path, score in profile.dimension_scores.items():
            if dim_path.startswith(dimension):
                return score

        # Fall back to layer-level scores
        layer = dimension.split(".")[0] if "." in dimension else dimension
        layer_map = {
            "structural": profile.structural_entropy,
            "semantic": profile.semantic_entropy,
            "value": profile.value_entropy,
            "computational": profile.computational_entropy,
        }
        return layer_map.get(layer, 0.0)


def detect_compound_risks_for_column(
    profile: ColumnEntropyProfile,
    entropy_objects: list[EntropyObject] | None = None,
    config_path: Path | None = None,
) -> list[CompoundRisk]:
    """Convenience function to detect compound risks for a column.

    Args:
        profile: Column entropy profile
        entropy_objects: Optional entropy objects for resolution extraction
        config_path: Optional path to compound_risks.yaml

    Returns:
        List of detected compound risks
    """
    detector = CompoundRiskDetector()
    if config_path:
        detector.load_config(config_path)
    return detector.detect_risks(profile, entropy_objects)


def detect_compound_risks_for_table(
    column_profiles: list[ColumnEntropyProfile],
    entropy_objects_by_column: dict[str, list[EntropyObject]] | None = None,
    config_path: Path | None = None,
) -> list[CompoundRisk]:
    """Detect compound risks across all columns in a table.

    Args:
        column_profiles: List of column entropy profiles
        entropy_objects_by_column: Optional dict of column name to entropy objects
        config_path: Optional path to compound_risks.yaml

    Returns:
        List of all detected compound risks
    """
    detector = CompoundRiskDetector()
    if config_path:
        detector.load_config(config_path)

    all_risks: list[CompoundRisk] = []

    for profile in column_profiles:
        objects = None
        if entropy_objects_by_column:
            key = f"{profile.table_name}.{profile.column_name}"
            objects = entropy_objects_by_column.get(key)

        risks = detector.detect_risks(profile, objects)
        all_risks.extend(risks)

    return all_risks


def get_compound_risk_config_template() -> dict[str, Any]:
    """Return a template for compound_risks.yaml configuration.

    Useful for generating initial configuration file.
    """
    return {
        "compound_risks": {
            "units_aggregations": {
                "dimensions": ["semantic.units", "computational.aggregations"],
                "threshold": 0.5,
                "risk_level": "critical",
                "impact_template": (
                    "Unknown currencies/units being summed without conversion. "
                    "Results could be off by 20-40%."
                ),
                "multiplier": 2.0,
            },
            "relations_filters": {
                "dimensions": ["structural.relations", "computational.filters"],
                "threshold": 0.5,
                "risk_level": "high",
                "impact_template": (
                    "Non-deterministic join paths combined with filtering. "
                    "Different query paths may give different results."
                ),
                "multiplier": 1.8,
            },
            "nulls_aggregations": {
                "dimensions": ["value.nulls", "computational.aggregations"],
                "threshold": 0.5,
                "risk_level": "high",
                "impact_template": (
                    "High null ratio in aggregated columns. Results may exclude significant data."
                ),
                "multiplier": 1.5,
            },
        },
    }
