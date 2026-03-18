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

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
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
    layer = Layer.SEMANTIC
    dimension = Dimension.DIMENSIONAL
    sub_dimension = SubDimension.CROSS_COLUMN_PATTERNS
    scope = "table"
    required_analyses = [AnalysisKey.SLICE_VARIANCE]  # temporal_variance is optional
    description = "Detects cross-column business rules from slice and temporal variance patterns"

    def load_data(self, context: DetectorContext) -> None:
        """Load slice variance data for this table."""
        if context.session is None or context.table_id is None:
            return

        result = self._load_slice_variance(context.session, context.table_id, context.table_name)
        if result is not None:
            context.analysis_results["slice_variance"] = result["slice_variance"]
            context.analysis_results["drift_summaries"] = result["drift_summaries"]

    @staticmethod
    def _load_slice_variance(
        session: Session,
        table_id: str,
        table_name: str,
    ) -> dict[str, Any] | None:
        """Load table-scoped slice variance data.

        Returns dict with slice_variance and drift_summaries keys,
        or None if no slice profiles exist.
        """
        from dataraum.analysis.quality_summary.db_models import ColumnSliceProfile
        from dataraum.analysis.slicing.db_models import SliceDefinition
        from dataraum.analysis.slicing.slice_runner import _get_slice_table_name
        from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary
        from dataraum.storage import Column, Table

        # Get columns for this typed table
        table_columns = list(
            session.execute(select(Column).where(Column.table_id == table_id)).scalars().all()
        )
        table_column_ids = [c.column_id for c in table_columns]

        # Check for slicing_view table (FK-based scoping)
        sv_table = session.execute(
            select(Table).where(
                Table.table_name == f"slicing_{table_name}",
                Table.layer == "slicing_view",
            )
        ).scalar_one_or_none()

        if sv_table:
            sv_cols = (
                session.execute(select(Column).where(Column.table_id == sv_table.table_id))
                .scalars()
                .all()
            )
            lookup_column_ids = [c.column_id for c in sv_cols]
        else:
            lookup_column_ids = table_column_ids

        # Load column slice profiles
        profiles = list(
            session.execute(
                select(ColumnSliceProfile).where(
                    ColumnSliceProfile.source_column_id.in_(lookup_column_ids)
                )
            )
            .scalars()
            .all()
        )

        if not profiles:
            return None

        # Build slice_data: slice_value -> column_name -> metrics
        slice_data: dict[str, dict[str, dict[str, Any]]] = {}
        columns_data: dict[str, dict[str, Any]] = {}

        for profile in profiles:
            slice_val = profile.slice_value
            col_name = profile.column_name

            if slice_val not in slice_data:
                slice_data[slice_val] = {}

            slice_data[slice_val][col_name] = {
                "null_ratio": profile.null_ratio,
                "distinct_count": profile.distinct_count,
                "row_count": profile.row_count,
                "quality_score": profile.quality_score,
                "has_issues": profile.has_issues,
            }

            if col_name not in columns_data:
                columns_data[col_name] = {
                    "classification": profile.variance_classification or "stable",
                    "null_ratios": [],
                    "distinct_counts": [],
                    "exceeded_thresholds": [],
                }
            if profile.null_ratio is not None:
                columns_data[col_name]["null_ratios"].append(profile.null_ratio)
            if profile.distinct_count is not None:
                columns_data[col_name]["distinct_counts"].append(profile.distinct_count)

        # Calculate variance metrics per column
        for col_metrics in columns_data.values():
            null_ratios = col_metrics.get("null_ratios", [])
            distinct_counts = col_metrics.get("distinct_counts", [])

            if null_ratios and len(null_ratios) > 1:
                col_metrics["null_spread"] = max(null_ratios) - min(null_ratios)
            else:
                col_metrics["null_spread"] = 0.0

            if distinct_counts and len(distinct_counts) > 1 and min(distinct_counts) > 0:
                col_metrics["distinct_ratio"] = max(distinct_counts) / min(distinct_counts)
            else:
                col_metrics["distinct_ratio"] = 1.0

            if col_metrics["null_spread"] > 0.1 or col_metrics["distinct_ratio"] > 2.0:
                col_metrics["classification"] = "interesting"
                if col_metrics["null_spread"] > 0.1:
                    col_metrics["exceeded_thresholds"].append("null_spread")
                if col_metrics["distinct_ratio"] > 2.0:
                    col_metrics["exceeded_thresholds"].append("distinct_ratio")

        # Load drift summaries for slice tables
        col_name_by_id = {c.column_id: c.column_name for c in table_columns}
        slice_defs = list(
            session.execute(select(SliceDefinition).where(SliceDefinition.table_id == table_id))
            .scalars()
            .all()
        )

        slice_table_names: list[str] = []
        for sd in slice_defs:
            sd_col_name = sd.column_name or col_name_by_id.get(sd.column_id)
            if sd_col_name and sd.distinct_values:
                for value in sd.distinct_values:
                    slice_table_names.append(_get_slice_table_name(table_name, sd_col_name, value))

        drift_summaries: list[Any] = []
        if slice_table_names:
            drift_summaries = list(
                session.execute(
                    select(ColumnDriftSummary).where(
                        ColumnDriftSummary.slice_table_name.in_(slice_table_names)
                    )
                )
                .scalars()
                .all()
            )

        # Build temporal_drift from drift summaries
        temporal_drift: list[dict[str, Any]] = []
        for ds in drift_summaries:
            if ds.max_js_divergence > 0:
                evidence = ds.drift_evidence_json or {}
                change_points = evidence.get("change_points", [])
                temporal_drift.append(
                    {
                        "column_name": ds.column_name,
                        "js_divergence": ds.max_js_divergence,
                        "has_significant_drift": ds.periods_with_drift > 0,
                        "has_category_changes": bool(
                            evidence.get("emerged_categories")
                            or evidence.get("vanished_categories")
                        ),
                        "change_points": change_points,
                    }
                )

        # Load temporal analyses
        temporal_columns: dict[str, dict[str, Any]] = {}
        if slice_table_names:
            from dataraum.analysis.temporal_slicing.db_models import TemporalSliceAnalysis

            period_analyses = list(
                session.execute(
                    select(TemporalSliceAnalysis).where(
                        TemporalSliceAnalysis.slice_table_name.in_(slice_table_names)
                    )
                )
                .scalars()
                .all()
            )

            for ta in period_analyses:
                col_name = ta.time_column
                if col_name not in temporal_columns:
                    temporal_columns[col_name] = {
                        "is_interesting": False,
                        "reasons": [],
                        "coverage_ratio": ta.coverage_ratio,
                        "last_day_ratio": ta.last_day_ratio,
                        "is_volume_anomaly": bool(ta.is_volume_anomaly),
                    }
                if (
                    (ta.coverage_ratio is not None and ta.coverage_ratio < 0.5)
                    or (ta.last_day_ratio is not None and ta.last_day_ratio > 1.5)
                    or ta.is_volume_anomaly
                ):
                    temporal_columns[col_name]["is_interesting"] = True
                    if ta.coverage_ratio is not None and ta.coverage_ratio < 0.5:
                        temporal_columns[col_name]["reasons"].append("low_coverage")
                    if ta.last_day_ratio is not None and ta.last_day_ratio > 1.5:
                        temporal_columns[col_name]["reasons"].append("period_end_spike")
                    if ta.is_volume_anomaly:
                        temporal_columns[col_name]["reasons"].append("volume_anomaly")

        return {
            "slice_variance": {
                "columns": columns_data,
                "slice_data": slice_data,
                "temporal_columns": temporal_columns,
                "temporal_drift": temporal_drift,
            },
            "drift_summaries": drift_summaries,
        }

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
        accepted_tables: list[str] = self.config.get("accepted_tables") or detector_config.get(
            "accepted_tables", []
        )
        # Documented patterns: list of {"table", "columns", "pattern_type"} dicts.
        # Patterns matching a documented entry are excluded from scoring.
        documented_patterns: list[dict[str, Any]] = self.config.get(
            "documented_patterns"
        ) or detector_config.get("documented_patterns", [])

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
                interesting_columns, correlation_threshold
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

        # 6. Value-based mutual exclusivity for numeric column pairs.
        # The null-based check (step 1) requires columns to be "interesting"
        # (high null_spread). For debit/credit-style columns where one is
        # always zero when the other has a value, null_spread is 0.0 but the
        # mutual exclusivity is real. Check directly via DuckDB.
        if context.duckdb_conn is not None and entropy_score.mutual_exclusivity_count == 0:
            value_mutex_patterns = self._detect_value_mutual_exclusivity(
                context, mutual_exclusivity_threshold
            )
            patterns.extend(value_mutex_patterns)
            entropy_score.mutual_exclusivity_count += len(value_mutex_patterns)

        # Filter out documented patterns — these are known business rules,
        # not undocumented complexity
        if documented_patterns:
            documented_keys = set()
            for dp in documented_patterns:
                if dp.get("table") == context.table_name:
                    raw_cols = dp.get("columns", [])
                    # Handle both list and comma-separated string formats
                    if isinstance(raw_cols, str):
                        raw_cols = [c.strip() for c in raw_cols.split(",")]
                    cols = frozenset(raw_cols)
                    documented_keys.add((dp.get("pattern_type", ""), cols))

            undocumented: list[CrossColumnPattern] = []
            for p in patterns:
                key = (p.pattern_type, frozenset(p.columns))
                if key not in documented_keys:
                    undocumented.append(p)
                else:
                    logger.info(
                        "documented_pattern_skipped",
                        table=context.table_name,
                        pattern_type=p.pattern_type,
                        columns=p.columns,
                    )

            # Recount after filtering
            patterns = undocumented
            entropy_score.mutual_exclusivity_count = sum(
                1 for p in patterns if p.pattern_type == "mutual_exclusivity"
            )
            entropy_score.correlated_variance_count = sum(
                1 for p in patterns if p.pattern_type == "correlated_variance"
            )
            entropy_score.conditional_dependency_count = sum(
                1 for p in patterns if p.pattern_type == "conditional_dependency"
            )
            entropy_score.temporal_correlation_count = sum(
                1 for p in patterns if p.pattern_type == "temporal_correlation"
            )
            entropy_score.temporal_drift_count = sum(
                1 for p in patterns if p.pattern_type == "temporal_drift"
            )

        # Calculate overall entropy score using configurable weights
        num_categorical = max(len(interesting_columns), entropy_score.mutual_exclusivity_count, 1)
        entropy_score.categorical_entropy = (
            entropy_score.mutual_exclusivity_count * w_me
            + entropy_score.conditional_dependency_count * w_cd
            + entropy_score.correlated_variance_count * w_cv
        ) / num_categorical

        entropy_score.temporal_entropy = (
            entropy_score.temporal_correlation_count * w_tc
            + entropy_score.temporal_drift_count * w_td
        ) / max(len(interesting_temporal), 1)

        entropy_score.calculate_total(weights=pattern_weights or None)

        # Check if this table has been accepted (documented business rules)
        is_accepted = context.table_name in accepted_tables

        # Create entropy objects for each pattern
        entropy_objects: list[EntropyObject] = []

        for pattern in patterns:
            score = score_undocumented_rule if pattern.confidence > 0.9 else score_partial_pattern
            ev_dict: dict[str, Any] = {
                "pattern_type": pattern.pattern_type,
                "columns": pattern.columns,
                "confidence": pattern.confidence,
                "description": pattern.description,
                "business_rule_hypothesis": pattern.business_rule_hypothesis,
                "raw_evidence": pattern.evidence,
                "uncertainty_bits": pattern.uncertainty_bits,
            }
            if is_accepted:
                ev_dict["accepted"] = True
            evidence = [ev_dict]

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
            summary_ev: dict[str, Any] = {
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
                },
            }
            if is_accepted:
                summary_ev["accepted"] = True
            summary_evidence = [summary_ev]
            overall_score = entropy_score.total_score
            entropy_objects.append(
                EntropyObject(
                    layer=self.layer,
                    dimension=self.dimension,
                    sub_dimension="overall_score",
                    target=context.target_ref,
                    score=overall_score,
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

    def _detect_value_mutual_exclusivity(
        self,
        context: DetectorContext,
        threshold: float,
    ) -> list[CrossColumnPattern]:
        """Detect value-based mutual exclusivity via DuckDB.

        Finds numeric column pairs where one is zero/NULL whenever the other
        has a non-zero value (e.g., debit/credit in journal lines).
        Unlike _detect_mutual_exclusivity which checks NULL spread across
        slices, this checks actual row-level value patterns.
        """
        from dataraum.storage import Column

        if context.session is None or context.table_id is None:
            return []

        # Get numeric columns for this table
        numeric_types = {"DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "INTEGER", "BIGINT"}
        numeric_cols = list(
            context.session.execute(
                select(Column).where(
                    Column.table_id == context.table_id,
                    Column.resolved_type.in_(list(numeric_types)),
                )
            )
            .scalars()
            .all()
        )

        if len(numeric_cols) < 2:
            return []

        patterns: list[CrossColumnPattern] = []

        for i, col_a in enumerate(numeric_cols):
            for col_b in numeric_cols[i + 1 :]:
                # Query: what fraction of rows have both columns non-zero?
                try:
                    sql = f"""
                        SELECT
                            COUNT(*) AS total,
                            SUM(CASE WHEN "{col_a.column_name}" != 0
                                      AND "{col_b.column_name}" != 0 THEN 1 ELSE 0 END) AS both_nonzero,
                            SUM(CASE WHEN "{col_a.column_name}" = 0
                                      AND "{col_b.column_name}" = 0 THEN 1 ELSE 0 END) AS both_zero
                        FROM "typed_{context.table_name}"
                        WHERE "{col_a.column_name}" IS NOT NULL
                          AND "{col_b.column_name}" IS NOT NULL
                    """
                    row = context.duckdb_conn.execute(sql).fetchone()
                except Exception:
                    continue

                if not row or row[0] == 0:
                    continue

                total, both_nonzero, both_zero = row[0], row[1], row[2]

                # Mutual exclusivity: very few rows have both non-zero
                # AND the columns aren't both-zero everywhere (trivial case)
                mutex_ratio = 1.0 - (both_nonzero / total)
                nontrivial = (total - both_zero) / total  # fraction with at least one non-zero

                if mutex_ratio >= threshold and nontrivial > 0.5:
                    patterns.append(
                        CrossColumnPattern(
                            pattern_type="mutual_exclusivity",
                            columns=[col_a.column_name, col_b.column_name],
                            confidence=mutex_ratio,
                            description=(
                                f"{col_a.column_name} and {col_b.column_name} "
                                f"are value-mutually-exclusive ({mutex_ratio:.1%} of rows)"
                            ),
                            business_rule_hypothesis=(
                                f"When {col_a.column_name} has a non-zero value, "
                                f"{col_b.column_name} is zero, and vice versa. "
                                "This suggests a business constraint (e.g., debit vs credit)."
                            ),
                            evidence={
                                "mutex_ratio": mutex_ratio,
                                "both_nonzero_count": both_nonzero,
                                "total_rows": total,
                                "detection_method": "value_based",
                            },
                        )
                    )

        return patterns

    def _detect_correlated_variance(
        self,
        columns: list[ColumnVariancePattern],
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
