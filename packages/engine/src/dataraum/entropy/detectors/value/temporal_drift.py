"""Temporal drift entropy detector.

Scores distribution drift as the periods' DISAGREEMENT about the value
distribution: the generalized Jensen–Shannon divergence of the per-period
distributions from their pooled mixture (``ColumnDriftSummary.drift_divergence``,
already normalized to [0, 1] — the same quantity as the pooling engine's conflict
``C`` with time periods as the witnesses). Severity per intent lives in the loss
table (entropy/loss.yaml); this detector feeds the loss path, not a network node.
"""

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class TemporalDriftDetector(EntropyDetector):
    """Detector for temporal distribution drift uncertainty.

    Scores ``drift_divergence`` (the normalized generalized JSD across periods)
    directly — no boost curve. The reference is the pooled distribution, so a
    permanent level shift or a slow ramp both register; the old consecutive-pair
    score diluted them.
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
            context.session, context.column_id, context.table_id, run_id=context.run_id
        )
        if drift is not None:
            context.analysis_results["drift_summaries"] = drift
        sem = load_semantic(
            context.session, context.column_id, context.run_id, base_runs=context.base_runs
        )
        if sem is not None:
            context.analysis_results["semantic"] = sem
        stats = load_statistics(
            context.session, context.column_id, context.run_id, base_runs=context.base_runs
        )
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

        # Skip point-in-time measures (period balances, snapshot levels) — their
        # per-period distribution shifts BY THE DATA MODEL as the business moves
        # (DAT-405: clean trial_balance debit/credit_balance scored 0.45–0.54).
        # Drift detection is meaningful for additive measures (transaction
        # amounts), where the generating process should be period-stationary.
        if hasattr(semantic, "temporal_behavior"):
            behavior = semantic.temporal_behavior
        else:
            behavior = semantic.get("temporal_behavior")
        if behavior == "point_in_time":
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

        # Score = the normalized generalized JSD across periods (already [0, 1]).
        # Nullable for pre-existing rows written before the field existed → 0.
        drift_divergence = getattr(col_summary, "drift_divergence", None)
        score = max(0.0, min(1.0, float(drift_divergence))) if drift_divergence is not None else 0.0

        # Build evidence (max/mean consecutive JS kept for interpretation)
        evidence_data: dict[str, object] = {
            "drift_divergence": drift_divergence,
            "max_js_divergence": col_summary.max_js_divergence,
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
