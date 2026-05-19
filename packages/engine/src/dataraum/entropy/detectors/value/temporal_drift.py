"""Temporal drift entropy detector.

Measures uncertainty from distribution drift over time.
Uses max Jensen-Shannon divergence from ColumnDriftSummary records.
"""

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class TemporalDriftDetector(EntropyDetector):
    """Detector for temporal distribution drift uncertainty.

    Uses drift summaries (ColumnDriftSummary) to score columns based on
    their max JS divergence across time periods.

    Score mapping (max_js_divergence):
    - 0.0       -> 0.0  (no drift)
    - 0.1       -> 0.3  (mild drift)
    - 0.3       -> 0.7  (moderate drift)
    - 0.5+      -> 1.0  (severe drift)
    """

    detector_id = "temporal_drift"
    layer = Layer.VALUE
    dimension = Dimension.TEMPORAL
    sub_dimension = SubDimension.TEMPORAL_DRIFT
    required_analyses = [AnalysisKey.DRIFT_SUMMARIES, AnalysisKey.SEMANTIC]
    description = "Measures uncertainty from distribution drift over time"

    # Only measure columns benefit from temporal drift analysis.
    # IDs naturally differ across periods (JS divergence = ln(2) guaranteed).
    # Dimensions/categories naturally vary — that's business, not drift.
    # Attributes (descriptions, notes) are free text that changes by definition.
    _APPLICABLE_ROLES = frozenset({"measure"})

    # Columns with cardinality ratio above this are near-unique (IDs, references)
    # and naturally produce max JS divergence — skip to avoid false positives.
    _CARDINALITY_SKIP_THRESHOLD = 0.90

    def load_data(self, context: DetectorContext) -> None:
        """Load drift summaries, semantic annotation, and statistics for this column."""
        if context.session is None or context.column_id is None or context.table_id is None:
            return
        from dataraum.entropy.detectors.loaders import (
            load_drift_summaries,
            load_semantic,
            load_statistics,
        )

        drift = load_drift_summaries(
            context.session, context.column_id, context.table_id, context.table_name
        )
        if drift is not None:
            context.analysis_results["drift_summaries"] = drift
        sem = load_semantic(context.session, context.column_id)
        if sem is not None:
            context.analysis_results["semantic"] = sem
        stats = load_statistics(context.session, context.column_id)
        if stats is not None:
            context.analysis_results["statistics"] = stats

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect temporal drift entropy for a column.

        Args:
            context: Detector context with drift_summaries in analysis_results

        Returns:
            List with single EntropyObject for drift score, or empty if no data
        """
        # Only apply to measure columns — drift on IDs, dimensions, attributes,
        # and text is expected business behavior, not a data quality signal
        semantic = context.get_analysis("semantic", {})
        if hasattr(semantic, "semantic_role"):
            role = semantic.semantic_role
        else:
            role = semantic.get("semantic_role")
        if role not in self._APPLICABLE_ROLES:
            return []

        # Skip high-cardinality columns — near-unique values (references, codes)
        # produce max JS divergence by construction, not from real drift
        stats = context.get_analysis("statistics", {})
        cardinality = getattr(stats, "cardinality_ratio", None)
        if cardinality is None and isinstance(stats, dict):
            cardinality = stats.get("cardinality_ratio")
        if cardinality is not None and cardinality > self._CARDINALITY_SKIP_THRESHOLD:
            return []

        drift_summaries = context.get_analysis("drift_summaries", [])
        if not drift_summaries:
            return []

        # Find drift summary for this column
        col_summary = None
        for s in drift_summaries:
            if s.column_name == context.column_name:
                col_summary = s
                break

        if col_summary is None:
            return []

        max_js = col_summary.max_js_divergence
        mean_js = col_summary.mean_js_divergence

        # Use mean JS divergence for scoring. Mean reflects the average
        # drift across all periods, which distinguishes natural temporal
        # change (low mean, high max — one bad period) from genuine
        # distribution shift (high mean — consistent change).
        js = mean_js

        # Score mapping: piecewise linear
        if js <= 0.0:
            score = 0.0
        elif js <= 0.1:
            score = js * 3.0  # 0->0, 0.1->0.3
        elif js <= 0.3:
            score = 0.3 + (js - 0.1) * 2.0  # 0.1->0.3, 0.3->0.7
        elif js <= 0.5:
            score = 0.7 + (js - 0.3) * 1.5  # 0.3->0.7, 0.5->1.0
        else:
            score = 1.0

        score = min(score, 1.0)

        # Build evidence
        evidence_data: dict[str, object] = {
            "max_js_divergence": max_js,
            "mean_js_divergence": col_summary.mean_js_divergence,
            "periods_analyzed": col_summary.periods_analyzed,
            "periods_with_drift": col_summary.periods_with_drift,
        }

        # Add drift evidence details if available
        if col_summary.drift_evidence_json:
            de = col_summary.drift_evidence_json
            evidence_data["worst_period"] = de.get("worst_period")
            top_shifts = de.get("top_shifts", [])
            if top_shifts:
                evidence_data["top_shifts"] = top_shifts

        evidence = [evidence_data]

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
            )
        ]
