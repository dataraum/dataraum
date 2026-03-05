"""Dimensional entropy detector for cross-column patterns.

Measures uncertainty in cross-column relationships based on slice variance.
Detects patterns like:
- Mutual exclusivity (debit/credit columns)
- Conditional dependencies (field A populated only when field B = X)
- Correlated variance (columns that vary together across slices)
- Temporal correlations (columns that spike/drift together over time)

This is Stage 2 of the AI Entropy Framework - synthesizing business rules
from INTERESTING columns identified by slice variance filtering.

Source: quality_summary variance analysis (slice_data, temporal_data)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import log2
from typing import Any

from dataraum.core.logging import get_logger
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, DetectorTrust, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption

logger = get_logger(__name__)


@dataclass
class ColumnVariancePattern:
    """Detected variance pattern for a column."""

    column_name: str
    classification: str  # empty, constant, stable, interesting
    null_spread: float = 0.0
    distinct_ratio: float = 1.0
    outlier_spread: float = 0.0
    exceeded_thresholds: list[str] = field(default_factory=list)


@dataclass
class TemporalColumnPattern:
    """Detected temporal pattern for a column."""

    column_name: str
    is_interesting: bool = False
    reasons: list[str] = field(default_factory=list)

    # Temporal metrics
    completeness_ratio: float | None = None
    period_end_spike_ratio: float | None = None
    gap_count: int = 0
    has_drift: bool = False
    drift_periods: list[str] = field(default_factory=list)


@dataclass
class CrossColumnPattern:
    """Detected cross-column relationship pattern."""

    pattern_type: (
        str  # mutual_exclusivity, conditional_dependency, correlated_variance, temporal_correlation
    )
    columns: list[str]
    confidence: float
    description: str
    business_rule_hypothesis: str
    evidence: dict[str, Any] = field(default_factory=dict)
    # For entropy calculation
    uncertainty_bits: float = 0.0  # Information-theoretic uncertainty


@dataclass
class DimensionalEntropyScore:
    """Overall dimensional entropy score combining all detected patterns.

    The score represents how much uncertainty exists in understanding
    the business rules and relationships between columns.

    Entropy formula:
    H = -Σ p_i * log2(p_i) for each uncertainty source

    In practice, we use a weighted sum of pattern severities:
    - Each undocumented pattern adds uncertainty
    - Higher confidence patterns = more certain the rule exists but is undocumented
    - More patterns = more complexity = higher entropy
    """

    # Overall score (0.0 = fully documented, 1.0 = maximum uncertainty)
    total_score: float = 0.0

    # Component scores by pattern type
    categorical_entropy: float = 0.0  # From slice-based patterns
    temporal_entropy: float = 0.0  # From time-based patterns

    # Pattern counts
    mutual_exclusivity_count: int = 0
    conditional_dependency_count: int = 0
    correlated_variance_count: int = 0
    temporal_correlation_count: int = 0
    temporal_drift_count: int = 0

    # Total patterns detected
    total_patterns: int = 0

    # Uncertainty bits (information-theoretic measure)
    total_uncertainty_bits: float = 0.0

    # Interpretation
    interpretation: str = ""

    def calculate_total(
        self,
        weights: dict[str, float] | None = None,
    ) -> None:
        """Calculate total entropy score from components.

        Args:
            weights: Optional pattern type weights. Defaults to built-in values
                     if not provided (or loaded from config by caller).
        """
        if weights is None:
            weights = {
                "mutual_exclusivity": 0.8,
                "conditional_dependency": 0.6,
                "correlated_variance": 0.4,
                "temporal_correlation": 0.5,
                "temporal_drift": 0.3,
            }

        # Calculate weighted pattern score
        pattern_score = (
            self.mutual_exclusivity_count * weights["mutual_exclusivity"]
            + self.conditional_dependency_count * weights["conditional_dependency"]
            + self.correlated_variance_count * weights["correlated_variance"]
            + self.temporal_correlation_count * weights["temporal_correlation"]
            + self.temporal_drift_count * weights["temporal_drift"]
        )

        # Normalize: more patterns = higher entropy, but with diminishing returns
        # Using log to compress: H = log2(1 + pattern_score)
        if pattern_score > 0:
            self.total_uncertainty_bits = log2(1 + pattern_score)
            # Normalize to 0-1 scale (assume max ~10 patterns = ~3.5 bits)
            self.total_score = min(1.0, self.total_uncertainty_bits / 3.5)
        else:
            self.total_score = 0.0
            self.total_uncertainty_bits = 0.0

        self.total_patterns = (
            self.mutual_exclusivity_count
            + self.conditional_dependency_count
            + self.correlated_variance_count
            + self.temporal_correlation_count
            + self.temporal_drift_count
        )

        # Set interpretation
        if self.total_score < 0.2:
            self.interpretation = "Low dimensional entropy - relationships are well understood"
        elif self.total_score < 0.5:
            self.interpretation = "Moderate dimensional entropy - some undocumented business rules"
        elif self.total_score < 0.8:
            self.interpretation = "High dimensional entropy - significant undocumented complexity"
        else:
            self.interpretation = "Very high dimensional entropy - many undocumented relationships"


class DimensionalEntropyDetector(EntropyDetector):
    """Detector for cross-column dimensional uncertainty.

    Analyzes INTERESTING columns (those with variance across slices) to
    identify business rules and relationships that create semantic entropy.

    Key patterns detected:
    1. Mutual Exclusivity: Two columns that are never both populated
       (e.g., debit_amount / credit_amount)
    2. Conditional Dependencies: Column A varies based on Column B's value
       (e.g., payment_method affects fee_structure)
    3. Correlated Variance: Columns whose variance patterns track together
       (e.g., quantity and total_price)
    4. Temporal Correlations: Columns that spike/drift together over time
       (e.g., columns affected by same business event)

    Source: quality_summary.variance (slice_data, temporal_data from aggregation)
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "dimensional_entropy"
    layer = "semantic"
    dimension = "dimensional"
    sub_dimension = "cross_column_patterns"
    trust_level = DetectorTrust.HARD
    required_analyses = ["slice_variance"]  # temporal_variance is optional
    description = "Detects cross-column business rules from slice and temporal variance patterns"

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect cross-column pattern entropy.

        Analyzes variance metrics across INTERESTING columns to find
        undocumented business rules that create interpretation uncertainty.

        Args:
            context: Detector context with slice_variance analysis results
                Expected structure:
                {
                    "columns": {
                        "column_name": {
                            "classification": "interesting",
                            "null_spread": 0.15,
                            "distinct_ratio": 2.5,
                            ...
                        }
                    },
                    "slice_data": {...},  # Raw slice metrics
                    "temporal_columns": {...},  # Optional: temporal column results
                    "temporal_drift": {...},  # Optional: drift analysis results
                }

        Returns:
            List of EntropyObject instances for detected patterns
        """
        config = get_entropy_config()
        detector_config = config.detector("dimensional_entropy")

        # Configurable scores
        score_undocumented_rule = detector_config.get("score_undocumented_rule", 0.7)
        score_partial_pattern = detector_config.get("score_partial_pattern", 0.5)
        correlation_threshold = detector_config.get("correlation_threshold", 0.8)
        mutual_exclusivity_threshold = detector_config.get("mutual_exclusivity_threshold", 0.95)
        pattern_weights = detector_config.get("pattern_weights", {})

        slice_variance = context.get_analysis("slice_variance", {})
        columns_data = slice_variance.get("columns", {})
        slice_data = slice_variance.get("slice_data", {})

        # Optional temporal data
        temporal_columns = slice_variance.get("temporal_columns", {})
        temporal_drift = slice_variance.get("temporal_drift", [])

        # Initialize entropy score tracker
        entropy_score = DimensionalEntropyScore()

        if not columns_data:
            return []

        # Extract INTERESTING columns (categorical)
        interesting_columns = self._get_interesting_columns(columns_data)

        # Extract INTERESTING temporal columns
        interesting_temporal = self._get_interesting_temporal_columns(temporal_columns)

        # Resolve weights (config or defaults)
        w_me = pattern_weights.get("mutual_exclusivity", 0.8)
        w_cd = pattern_weights.get("conditional_dependency", 0.6)
        w_cv = pattern_weights.get("correlated_variance", 0.4)
        w_tc = pattern_weights.get("temporal_correlation", 0.5)
        w_td = pattern_weights.get("temporal_drift", 0.3)

        # Detect categorical patterns
        patterns: list[CrossColumnPattern] = []

        if len(interesting_columns) >= 2:
            # 1. Check for mutual exclusivity patterns
            mutual_patterns = self._detect_mutual_exclusivity(
                interesting_columns, slice_data, mutual_exclusivity_threshold
            )
            patterns.extend(mutual_patterns)
            entropy_score.mutual_exclusivity_count = len(mutual_patterns)

            # 2. Check for correlated variance patterns
            correlated_patterns = self._detect_correlated_variance(
                interesting_columns, columns_data, correlation_threshold
            )
            patterns.extend(correlated_patterns)
            entropy_score.correlated_variance_count = len(correlated_patterns)

            # 3. Check for conditional dependencies
            conditional_patterns = self._detect_conditional_dependencies(
                interesting_columns, slice_data
            )
            patterns.extend(conditional_patterns)
            entropy_score.conditional_dependency_count = len(conditional_patterns)

        # Detect temporal patterns
        if len(interesting_temporal) >= 2:
            # 4. Check for temporal correlations (columns that spike/drift together)
            temporal_corr_patterns = self._detect_temporal_correlations(
                interesting_temporal, temporal_drift
            )
            patterns.extend(temporal_corr_patterns)
            entropy_score.temporal_correlation_count = len(temporal_corr_patterns)

        # 5. Count significant drift patterns (separate from correlation)
        drift_patterns = self._detect_significant_drift(temporal_drift)
        patterns.extend(drift_patterns)
        entropy_score.temporal_drift_count = len(drift_patterns)

        # Calculate overall entropy score using configurable weights
        entropy_score.categorical_entropy = (
            entropy_score.mutual_exclusivity_count * w_me
            + entropy_score.conditional_dependency_count * w_cd
            + entropy_score.correlated_variance_count * w_cv
        ) / max(len(interesting_columns), 1)

        entropy_score.temporal_entropy = (
            entropy_score.temporal_correlation_count * w_tc
            + entropy_score.temporal_drift_count * w_td
        ) / max(len(interesting_temporal), 1)

        entropy_score.calculate_total(weights=pattern_weights or None)

        # Create entropy objects for each pattern
        entropy_objects: list[EntropyObject] = []

        for pattern in patterns:
            score = score_undocumented_rule if pattern.confidence > 0.9 else score_partial_pattern

            evidence = [
                {
                    "pattern_type": pattern.pattern_type,
                    "columns": pattern.columns,
                    "confidence": pattern.confidence,
                    "description": pattern.description,
                    "business_rule_hypothesis": pattern.business_rule_hypothesis,
                    "raw_evidence": pattern.evidence,
                    "uncertainty_bits": pattern.uncertainty_bits,
                }
            ]

            resolution_options = [
                ResolutionOption(
                    action="document_business_rule",
                    parameters={
                        "pattern_type": pattern.pattern_type,
                        "columns": pattern.columns,
                        "hypothesis": pattern.business_rule_hypothesis,
                    },
                    effort="medium",
                    description=f"Document business rule: {pattern.description}",
                ),
                ResolutionOption(
                    action="create_constraint",
                    parameters={
                        "pattern_type": pattern.pattern_type,
                        "columns": pattern.columns,
                    },
                    effort="high",
                    description="Add database constraint to enforce this rule",
                ),
            ]

            entropy_objects.append(
                self.create_entropy_object(
                    context=context,
                    score=score,
                    evidence=evidence,
                    resolution_options=resolution_options,
                )
            )

        # Add summary entropy object with overall score
        if patterns:
            summary_evidence = [
                {
                    "dimensional_entropy_score": {
                        "total_score": entropy_score.total_score,
                        "total_uncertainty_bits": entropy_score.total_uncertainty_bits,
                        "categorical_entropy": entropy_score.categorical_entropy,
                        "temporal_entropy": entropy_score.temporal_entropy,
                        "total_patterns": entropy_score.total_patterns,
                        "pattern_breakdown": {
                            "mutual_exclusivity": entropy_score.mutual_exclusivity_count,
                            "conditional_dependency": entropy_score.conditional_dependency_count,
                            "correlated_variance": entropy_score.correlated_variance_count,
                            "temporal_correlation": entropy_score.temporal_correlation_count,
                            "temporal_drift": entropy_score.temporal_drift_count,
                        },
                        "interpretation": entropy_score.interpretation,
                    }
                }
            ]

            entropy_objects.append(
                EntropyObject(
                    layer=self.layer,
                    dimension=self.dimension,
                    sub_dimension="overall_score",
                    target=context.target_ref,
                    score=entropy_score.total_score,
                    evidence=summary_evidence,
                    resolution_options=[
                        ResolutionOption(
                            action="document_business_rule",
                            parameters={"pattern_count": entropy_score.total_patterns},
                            effort="high",
                            description=f"Document all {entropy_score.total_patterns} detected business rules",
                        )
                    ],
                    detector_id=f"{self.detector_id}_summary",
                    source_analysis_ids=[],
                )
            )

        return entropy_objects

    def detect_with_details(
        self, context: DetectorContext
    ) -> tuple[
        list[EntropyObject], list[CrossColumnPattern], DimensionalEntropyScore, dict[str, Any]
    ]:
        """Detect patterns and return detailed results for summary generation.

        Same as detect(), but also returns intermediate results needed for
        generating dataset-level summaries.

        Args:
            context: Detector context with slice_variance analysis results

        Returns:
            Tuple of (entropy_objects, patterns, entropy_score, analysis_data) where:
                - entropy_objects: List of EntropyObject instances
                - patterns: List of CrossColumnPattern instances detected
                - entropy_score: DimensionalEntropyScore with calculated metrics
                - analysis_data: Dict with columns_data, temporal_columns, etc.
        """
        config = get_entropy_config()
        detector_config = config.detector("dimensional_entropy")

        # Configurable scores
        score_undocumented_rule = detector_config.get("score_undocumented_rule", 0.7)
        score_partial_pattern = detector_config.get("score_partial_pattern", 0.5)
        correlation_threshold = detector_config.get("correlation_threshold", 0.8)
        mutual_exclusivity_threshold = detector_config.get("mutual_exclusivity_threshold", 0.95)
        pattern_weights = detector_config.get("pattern_weights", {})

        slice_variance = context.get_analysis("slice_variance", {})
        columns_data = slice_variance.get("columns", {})
        slice_data = slice_variance.get("slice_data", {})

        # Optional temporal data
        temporal_columns = slice_variance.get("temporal_columns", {})
        temporal_drift = slice_variance.get("temporal_drift", [])

        # Initialize entropy score tracker
        entropy_score = DimensionalEntropyScore()

        if not columns_data:
            return [], [], entropy_score, {"columns_data": {}, "temporal_columns": {}}

        # Extract INTERESTING columns (categorical)
        interesting_columns = self._get_interesting_columns(columns_data)

        # Extract INTERESTING temporal columns
        interesting_temporal = self._get_interesting_temporal_columns(temporal_columns)

        # Resolve weights (config or defaults)
        w_me = pattern_weights.get("mutual_exclusivity", 0.8)
        w_cd = pattern_weights.get("conditional_dependency", 0.6)
        w_cv = pattern_weights.get("correlated_variance", 0.4)
        w_tc = pattern_weights.get("temporal_correlation", 0.5)
        w_td = pattern_weights.get("temporal_drift", 0.3)

        # Detect categorical patterns
        patterns: list[CrossColumnPattern] = []

        if len(interesting_columns) >= 2:
            # 1. Check for mutual exclusivity patterns
            mutual_patterns = self._detect_mutual_exclusivity(
                interesting_columns, slice_data, mutual_exclusivity_threshold
            )
            patterns.extend(mutual_patterns)
            entropy_score.mutual_exclusivity_count = len(mutual_patterns)

            # 2. Check for correlated variance patterns
            correlated_patterns = self._detect_correlated_variance(
                interesting_columns, columns_data, correlation_threshold
            )
            patterns.extend(correlated_patterns)
            entropy_score.correlated_variance_count = len(correlated_patterns)

            # 3. Check for conditional dependencies
            conditional_patterns = self._detect_conditional_dependencies(
                interesting_columns, slice_data
            )
            patterns.extend(conditional_patterns)
            entropy_score.conditional_dependency_count = len(conditional_patterns)

        # Detect temporal patterns
        if len(interesting_temporal) >= 2:
            # 4. Check for temporal correlations (columns that spike/drift together)
            temporal_corr_patterns = self._detect_temporal_correlations(
                interesting_temporal, temporal_drift
            )
            patterns.extend(temporal_corr_patterns)
            entropy_score.temporal_correlation_count = len(temporal_corr_patterns)

        # 5. Count significant drift patterns (separate from correlation)
        drift_patterns = self._detect_significant_drift(temporal_drift)
        patterns.extend(drift_patterns)
        entropy_score.temporal_drift_count = len(drift_patterns)

        # Calculate overall entropy score using configurable weights
        entropy_score.categorical_entropy = (
            entropy_score.mutual_exclusivity_count * w_me
            + entropy_score.conditional_dependency_count * w_cd
            + entropy_score.correlated_variance_count * w_cv
        ) / max(len(interesting_columns), 1)

        entropy_score.temporal_entropy = (
            entropy_score.temporal_correlation_count * w_tc
            + entropy_score.temporal_drift_count * w_td
        ) / max(len(interesting_temporal), 1)

        entropy_score.calculate_total(weights=pattern_weights or None)

        # Create entropy objects for each pattern
        entropy_objects: list[EntropyObject] = []

        for pattern in patterns:
            score = score_undocumented_rule if pattern.confidence > 0.9 else score_partial_pattern

            evidence = [
                {
                    "pattern_type": pattern.pattern_type,
                    "columns": pattern.columns,
                    "confidence": pattern.confidence,
                    "description": pattern.description,
                    "business_rule_hypothesis": pattern.business_rule_hypothesis,
                    "raw_evidence": pattern.evidence,
                    "uncertainty_bits": pattern.uncertainty_bits,
                }
            ]

            resolution_options = [
                ResolutionOption(
                    action="document_business_rule",
                    parameters={
                        "pattern_type": pattern.pattern_type,
                        "columns": pattern.columns,
                        "hypothesis": pattern.business_rule_hypothesis,
                    },
                    effort="medium",
                    description=f"Document business rule: {pattern.description}",
                ),
                ResolutionOption(
                    action="create_constraint",
                    parameters={
                        "pattern_type": pattern.pattern_type,
                        "columns": pattern.columns,
                    },
                    effort="high",
                    description="Add database constraint to enforce this rule",
                ),
            ]

            entropy_objects.append(
                self.create_entropy_object(
                    context=context,
                    score=score,
                    evidence=evidence,
                    resolution_options=resolution_options,
                )
            )

        # Add summary entropy object with overall score
        if patterns:
            summary_evidence = [
                {
                    "dimensional_entropy_score": {
                        "total_score": entropy_score.total_score,
                        "total_uncertainty_bits": entropy_score.total_uncertainty_bits,
                        "categorical_entropy": entropy_score.categorical_entropy,
                        "temporal_entropy": entropy_score.temporal_entropy,
                        "total_patterns": entropy_score.total_patterns,
                        "pattern_breakdown": {
                            "mutual_exclusivity": entropy_score.mutual_exclusivity_count,
                            "conditional_dependency": entropy_score.conditional_dependency_count,
                            "correlated_variance": entropy_score.correlated_variance_count,
                            "temporal_correlation": entropy_score.temporal_correlation_count,
                            "temporal_drift": entropy_score.temporal_drift_count,
                        },
                        "interpretation": entropy_score.interpretation,
                    }
                }
            ]

            entropy_objects.append(
                EntropyObject(
                    layer=self.layer,
                    dimension=self.dimension,
                    sub_dimension="overall_score",
                    target=context.target_ref,
                    score=entropy_score.total_score,
                    evidence=summary_evidence,
                    resolution_options=[
                        ResolutionOption(
                            action="document_business_rule",
                            parameters={"pattern_count": entropy_score.total_patterns},
                            effort="high",
                            description=f"Document all {entropy_score.total_patterns} detected business rules",
                        )
                    ],
                    detector_id=f"{self.detector_id}_summary",
                    source_analysis_ids=[],
                )
            )

        # Return analysis data for summary generation
        analysis_data = {
            "columns_data": columns_data,
            "temporal_columns": temporal_columns,
            "temporal_drift": temporal_drift,
        }

        return entropy_objects, patterns, entropy_score, analysis_data

    def _get_interesting_columns(self, columns_data: dict[str, Any]) -> list[ColumnVariancePattern]:
        """Extract columns classified as INTERESTING from categorical analysis."""
        interesting = []
        for col_name, metrics in columns_data.items():
            classification = metrics.get("classification", "stable")
            if classification == "interesting":
                interesting.append(
                    ColumnVariancePattern(
                        column_name=col_name,
                        classification=classification,
                        null_spread=metrics.get("null_spread", 0.0),
                        distinct_ratio=metrics.get("distinct_ratio", 1.0),
                        outlier_spread=metrics.get("outlier_spread", 0.0),
                        exceeded_thresholds=metrics.get("exceeded_thresholds", []),
                    )
                )
        return interesting

    def _get_interesting_temporal_columns(
        self, temporal_columns: dict[str, Any]
    ) -> list[TemporalColumnPattern]:
        """Extract columns classified as INTERESTING from temporal analysis."""
        interesting = []
        for col_name, result in temporal_columns.items():
            if result.get("is_interesting", False):
                interesting.append(
                    TemporalColumnPattern(
                        column_name=col_name,
                        is_interesting=True,
                        reasons=result.get("reasons", []),
                        completeness_ratio=result.get("completeness_ratio"),
                        period_end_spike_ratio=result.get("period_end_spike_ratio"),
                        gap_count=result.get("gap_count", 0),
                    )
                )
        return interesting

    def _detect_mutual_exclusivity(
        self,
        columns: list[ColumnVariancePattern],
        slice_data: dict[str, Any],
        threshold: float,
    ) -> list[CrossColumnPattern]:
        """Detect pairs of columns that are mutually exclusive.

        Two columns are mutually exclusive if when one has data,
        the other is typically NULL (like debit/credit amounts).
        """
        patterns = []

        # Find columns with high null_spread (varies a lot)
        high_null_spread = [c for c in columns if c.null_spread > 0.1]

        for i, col_a in enumerate(high_null_spread):
            for col_b in high_null_spread[i + 1 :]:
                # Check if null patterns are inverse
                # This requires per-slice null ratios from slice_data
                inverse_score = self._compute_inverse_null_correlation(
                    col_a.column_name, col_b.column_name, slice_data
                )

                if inverse_score > threshold:
                    patterns.append(
                        CrossColumnPattern(
                            pattern_type="mutual_exclusivity",
                            columns=[col_a.column_name, col_b.column_name],
                            confidence=inverse_score,
                            description=(
                                f"{col_a.column_name} and {col_b.column_name} "
                                "are mutually exclusive"
                            ),
                            business_rule_hypothesis=(
                                f"When {col_a.column_name} has a value, "
                                f"{col_b.column_name} should be NULL and vice versa. "
                                "This suggests a business constraint (e.g., debit vs credit)."
                            ),
                            evidence={
                                "inverse_null_correlation": inverse_score,
                                "col_a_null_spread": col_a.null_spread,
                                "col_b_null_spread": col_b.null_spread,
                            },
                        )
                    )

        return patterns

    def _detect_correlated_variance(
        self,
        columns: list[ColumnVariancePattern],
        columns_data: dict[str, Any],
        threshold: float,
    ) -> list[CrossColumnPattern]:
        """Detect columns whose variance patterns correlate.

        Columns that exceed the same thresholds in the same slices
        likely have a business relationship.
        """
        patterns = []

        for i, col_a in enumerate(columns):
            for col_b in columns[i + 1 :]:
                # Check if they exceed the same thresholds
                common_thresholds = set(col_a.exceeded_thresholds) & set(col_b.exceeded_thresholds)

                if len(common_thresholds) >= 2:
                    # Both columns vary on multiple dimensions together
                    confidence = len(common_thresholds) / max(
                        len(col_a.exceeded_thresholds),
                        len(col_b.exceeded_thresholds),
                        1,
                    )

                    if confidence >= threshold:
                        patterns.append(
                            CrossColumnPattern(
                                pattern_type="correlated_variance",
                                columns=[col_a.column_name, col_b.column_name],
                                confidence=confidence,
                                description=(
                                    f"{col_a.column_name} and {col_b.column_name} "
                                    f"vary together on: {', '.join(common_thresholds)}"
                                ),
                                business_rule_hypothesis=(
                                    "These columns have a business relationship - "
                                    "changes in one likely affect the other."
                                ),
                                evidence={
                                    "common_thresholds": list(common_thresholds),
                                    "col_a_thresholds": col_a.exceeded_thresholds,
                                    "col_b_thresholds": col_b.exceeded_thresholds,
                                },
                            )
                        )

        return patterns

    def _detect_conditional_dependencies(
        self,
        columns: list[ColumnVariancePattern],
        slice_data: dict[str, Any],
    ) -> list[CrossColumnPattern]:
        """Detect columns that vary conditionally on slice value.

        Looks for columns where:
        1. Null ratio spikes only in specific slices (conditionally optional)
        2. Distinct count dramatically changes by slice (different value sets)
        3. A column is only populated when another column has specific values

        Args:
            columns: List of INTERESTING columns
            slice_data: Per-slice metrics keyed by slice_name -> column_name -> metrics

        Returns:
            List of detected conditional dependency patterns
        """
        patterns = []

        # For each interesting column, check if its variance is slice-specific
        for col in columns:
            col_name = col.column_name

            # Collect per-slice metrics for this column
            slice_null_ratios: dict[str, float] = {}
            slice_distinct_counts: dict[str, int] = {}

            for slice_name, slice_metrics in slice_data.items():
                col_metrics = slice_metrics.get(col_name, {})
                if col_metrics:
                    null_r = col_metrics.get("null_ratio")
                    distinct_c = col_metrics.get("distinct_count")
                    if null_r is not None:
                        slice_null_ratios[slice_name] = null_r
                    if distinct_c is not None:
                        slice_distinct_counts[slice_name] = distinct_c

            if len(slice_null_ratios) < 2:
                continue

            # Pattern 1: Conditional optionality
            # Field is mostly populated in some slices but mostly NULL in others
            high_null_slices = [s for s, nr in slice_null_ratios.items() if nr > 0.8]
            low_null_slices = [s for s, nr in slice_null_ratios.items() if nr < 0.2]

            if high_null_slices and low_null_slices:
                confidence = (
                    min(
                        len(high_null_slices) / len(slice_null_ratios),
                        len(low_null_slices) / len(slice_null_ratios),
                    )
                    * 2
                )  # Scale to 0-1

                if confidence > 0.3:
                    patterns.append(
                        CrossColumnPattern(
                            pattern_type="conditional_dependency",
                            columns=[col_name],
                            confidence=min(confidence, 1.0),
                            description=(
                                f"{col_name} is conditionally optional: "
                                f"mostly NULL in [{', '.join(high_null_slices[:3])}] "
                                f"but populated in [{', '.join(low_null_slices[:3])}]"
                            ),
                            business_rule_hypothesis=(
                                f"Field '{col_name}' is only applicable/required for certain "
                                f"categories. This suggests a business rule where the field "
                                f"is conditional on the slice dimension value."
                            ),
                            evidence={
                                "high_null_slices": high_null_slices,
                                "low_null_slices": low_null_slices,
                                "slice_null_ratios": slice_null_ratios,
                            },
                        )
                    )

            # Pattern 2: Value set changes by slice
            # Different distinct counts suggest different allowed values per category
            if slice_distinct_counts and len(slice_distinct_counts) >= 2:
                distinct_values = list(slice_distinct_counts.values())
                min_distinct = min(distinct_values)
                max_distinct = max(distinct_values)

                if min_distinct > 0 and max_distinct / min_distinct > 3.0:
                    # 3x+ difference in cardinality by slice
                    low_card_slices = [
                        s for s, dc in slice_distinct_counts.items() if dc <= min_distinct * 1.5
                    ]
                    high_card_slices = [
                        s for s, dc in slice_distinct_counts.items() if dc >= max_distinct * 0.7
                    ]

                    patterns.append(
                        CrossColumnPattern(
                            pattern_type="conditional_dependency",
                            columns=[col_name],
                            confidence=0.7,
                            description=(
                                f"{col_name} has different value sets by slice: "
                                f"{min_distinct} distinct values in [{', '.join(low_card_slices[:2])}] "
                                f"vs {max_distinct} in [{', '.join(high_card_slices[:2])}]"
                            ),
                            business_rule_hypothesis=(
                                f"Field '{col_name}' has different allowed values depending on "
                                f"the category. This may indicate a lookup table or validation "
                                f"rule that varies by context."
                            ),
                            evidence={
                                "slice_distinct_counts": slice_distinct_counts,
                                "ratio": max_distinct / min_distinct,
                                "low_card_slices": low_card_slices,
                                "high_card_slices": high_card_slices,
                            },
                        )
                    )

        return patterns

    def _compute_inverse_null_correlation(
        self,
        col_a: str,
        col_b: str,
        slice_data: dict[str, Any],
    ) -> float:
        """Compute inverse correlation of null ratios across slices.

        Returns 1.0 if perfectly inverse (when A is null, B is not, and vice versa).
        Returns 0.0 if not correlated.
        """
        # Extract per-slice null ratios for both columns
        col_a_nulls: list[float] = []
        col_b_nulls: list[float] = []

        for _slice_name, slice_metrics in slice_data.items():
            a_null = slice_metrics.get(col_a, {}).get("null_ratio")
            b_null = slice_metrics.get(col_b, {}).get("null_ratio")

            if a_null is not None and b_null is not None:
                col_a_nulls.append(float(a_null))
                col_b_nulls.append(float(b_null))

        if len(col_a_nulls) < 2:
            return 0.0

        # Check for inverse correlation: when A is high, B should be low
        # Simple heuristic: sum of (a_null + b_null) should be ~1.0 if inverse
        inverse_scores = [a + b for a, b in zip(col_a_nulls, col_b_nulls, strict=True)]

        # If inverse, sum should be close to 1.0 for each slice
        avg_sum = sum(inverse_scores) / len(inverse_scores)
        variance = sum((s - avg_sum) ** 2 for s in inverse_scores) / len(inverse_scores)

        # Score: high if avg_sum is close to 1.0 and variance is low
        if avg_sum < 0.5 or avg_sum > 1.5:
            return 0.0

        closeness_to_one = 1.0 - abs(1.0 - avg_sum)
        consistency = 1.0 / (1.0 + variance * 10)

        return closeness_to_one * consistency

    # =========================================================================
    # TEMPORAL PATTERN DETECTION
    # =========================================================================

    def _detect_temporal_correlations(
        self,
        temporal_columns: list[TemporalColumnPattern],
        temporal_drift: list[dict[str, Any]],
    ) -> list[CrossColumnPattern]:
        """Detect columns that show correlated temporal behavior.

        Looks for:
        1. Columns with same temporal reasons (both have gaps, both have spikes)
        2. Columns that drift at the same change points

        Args:
            temporal_columns: List of INTERESTING temporal columns
            temporal_drift: List of drift summary records with change_points

        Returns:
            List of detected temporal correlation patterns
        """
        patterns = []

        # Build drift lookup: column -> set of change points
        drift_by_column: dict[str, set[str]] = {}
        for drift in temporal_drift:
            if drift.get("has_significant_drift") or drift.get("has_category_changes"):
                col = drift.get("column_name", "")
                change_points = drift.get("change_points", [])
                if col and change_points:
                    drift_by_column[col] = set(change_points)

        # Check for correlated temporal patterns between column pairs
        for i, col_a in enumerate(temporal_columns):
            for col_b in temporal_columns[i + 1 :]:
                correlation_evidence: dict[str, Any] = {}
                confidence = 0.0

                # Pattern 1: Same temporal reasons
                common_reasons = set(col_a.reasons) & set(col_b.reasons)
                if common_reasons:
                    confidence += 0.3 * len(common_reasons)
                    correlation_evidence["common_temporal_reasons"] = list(common_reasons)

                # Pattern 2: Both have period-end spikes
                if (
                    col_a.period_end_spike_ratio
                    and col_a.period_end_spike_ratio > 1.5
                    and col_b.period_end_spike_ratio
                    and col_b.period_end_spike_ratio > 1.5
                ):
                    confidence += 0.4
                    correlation_evidence["both_have_period_end_spikes"] = {
                        col_a.column_name: col_a.period_end_spike_ratio,
                        col_b.column_name: col_b.period_end_spike_ratio,
                    }

                # Pattern 3: Drift at same change points
                drift_a = drift_by_column.get(col_a.column_name, set())
                drift_b = drift_by_column.get(col_b.column_name, set())
                common_change_points = drift_a & drift_b

                if common_change_points:
                    confidence += 0.3 * min(len(common_change_points), 3) / 3
                    correlation_evidence["common_change_points"] = list(common_change_points)

                # Pattern 4: Similar completeness (both have gaps or both complete)
                if col_a.completeness_ratio is not None and col_b.completeness_ratio is not None:
                    completeness_diff = abs(col_a.completeness_ratio - col_b.completeness_ratio)
                    if completeness_diff < 0.1:
                        confidence += 0.2
                        correlation_evidence["similar_completeness"] = {
                            col_a.column_name: col_a.completeness_ratio,
                            col_b.column_name: col_b.completeness_ratio,
                        }

                # Create pattern if confidence is high enough
                if confidence >= 0.5:
                    patterns.append(
                        CrossColumnPattern(
                            pattern_type="temporal_correlation",
                            columns=[col_a.column_name, col_b.column_name],
                            confidence=min(confidence, 1.0),
                            description=(
                                f"{col_a.column_name} and {col_b.column_name} "
                                f"show correlated temporal behavior"
                            ),
                            business_rule_hypothesis=(
                                "These columns are affected by the same temporal factors "
                                "(e.g., same data source, same business process, same fiscal calendar). "
                                "Changes to one likely affect the other."
                            ),
                            evidence=correlation_evidence,
                            uncertainty_bits=log2(1 + confidence),
                        )
                    )

        return patterns

    def _detect_significant_drift(
        self,
        temporal_drift: list[dict[str, Any]],
    ) -> list[CrossColumnPattern]:
        """Detect significant drift patterns that indicate business rule changes.

        Uses drift summaries (one per column) with change_points to detect
        systemic changes — multiple columns drifting at the same change point.

        Args:
            temporal_drift: List of drift summary records with change_points

        Returns:
            List of detected drift patterns
        """
        patterns = []

        # Group columns by change points to detect systemic changes
        drift_by_change_point: dict[str, list[dict[str, Any]]] = {}
        for drift in temporal_drift:
            if not drift.get("has_significant_drift") and not drift.get("has_category_changes"):
                continue

            change_points = drift.get("change_points", [])
            col_name = drift.get("column_name", "")

            for cp in change_points:
                if cp not in drift_by_change_point:
                    drift_by_change_point[cp] = []
                drift_by_change_point[cp].append(drift)

            # If no change points but has drift, use "unknown" as period
            if not change_points and col_name:
                if "__no_change_point__" not in drift_by_change_point:
                    drift_by_change_point["__no_change_point__"] = []
                drift_by_change_point["__no_change_point__"].append(drift)

        # Detect systemic drift (multiple columns at same change point)
        for change_point, drifts in drift_by_change_point.items():
            if change_point == "__no_change_point__" or len(drifts) < 2:
                continue

            columns = [d.get("column_name", "") for d in drifts]
            avg_divergence = sum(d.get("js_divergence", 0) or 0 for d in drifts) / len(drifts)

            patterns.append(
                CrossColumnPattern(
                    pattern_type="temporal_drift",
                    columns=columns,
                    confidence=min(0.5 + 0.1 * len(drifts), 1.0),
                    description=(
                        f"Systemic drift at {change_point}: {len(drifts)} columns changed together"
                    ),
                    business_rule_hypothesis=(
                        f"Multiple columns changed at {change_point}. This may indicate: "
                        f"(1) Business rule change, (2) Data migration, "
                        f"(3) New data source, or (4) Seasonal business pattern."
                    ),
                    evidence={
                        "change_point": change_point,
                        "affected_columns": columns,
                        "avg_js_divergence": avg_divergence,
                    },
                    uncertainty_bits=log2(1 + len(drifts) * avg_divergence),
                )
            )

        return patterns


# Convenience function to compute overall dimensional entropy
def compute_dimensional_entropy(
    categorical_patterns: int,
    temporal_patterns: int,
    column_count: int,
) -> float:
    """Compute overall dimensional entropy score.

    Simple formula:
    H = log2(1 + patterns) / log2(1 + columns)

    This gives:
    - 0 if no patterns
    - Higher if more patterns relative to columns
    - Normalized by column count (more columns = more potential patterns)

    Args:
        categorical_patterns: Number of categorical cross-column patterns
        temporal_patterns: Number of temporal patterns
        column_count: Total columns analyzed

    Returns:
        Normalized entropy score (0.0 - 1.0)
    """
    total_patterns = categorical_patterns + temporal_patterns
    if total_patterns == 0:
        return 0.0
    if column_count == 0:
        return 1.0

    # Maximum possible patterns scales with column pairs
    max_patterns = column_count * (column_count - 1) / 2

    # Entropy: how many patterns vs how many possible
    pattern_ratio = total_patterns / max(max_patterns, 1)

    # Use log to compress: more patterns = diminishing returns
    entropy = log2(1 + pattern_ratio * 10) / log2(11)  # Normalize to 0-1

    return min(1.0, entropy)
