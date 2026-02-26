"""Temporal drift entropy detector.

Measures uncertainty from distribution drift over time.
Uses max Jensen-Shannon divergence from ColumnDriftSummary records.
"""

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


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
    layer = "value"
    dimension = "temporal"
    sub_dimension = "temporal_drift"
    required_analyses = ["drift_summaries", "semantic"]
    description = "Measures uncertainty from distribution drift over time"

    # Semantic roles where drift detection is meaningless —
    # IDs naturally differ across periods (JS divergence = ln(2) guaranteed).
    _SKIP_ROLES = frozenset({"key", "foreign_key", "identifier"})

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect temporal drift entropy for a column.

        Args:
            context: Detector context with drift_summaries in analysis_results

        Returns:
            List with single EntropyObject for drift score, or empty if no data
        """
        # Skip identifier columns — IDs naturally differ across periods
        semantic = context.get_analysis("semantic", {})
        if hasattr(semantic, "semantic_role"):
            role = semantic.semantic_role
        else:
            role = semantic.get("semantic_role")
        if role in self._SKIP_ROLES:
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

        # Score mapping: piecewise linear
        if max_js <= 0.0:
            score = 0.0
        elif max_js <= 0.1:
            score = max_js * 3.0  # 0->0, 0.1->0.3
        elif max_js <= 0.3:
            score = 0.3 + (max_js - 0.1) * 2.0  # 0.1->0.3, 0.3->0.7
        elif max_js <= 0.5:
            score = 0.7 + (max_js - 0.3) * 1.5  # 0.3->0.7, 0.5->1.0
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
                evidence_data["top_shifts"] = top_shifts[:3]

        evidence = [evidence_data]

        # Resolution options
        resolution_options: list[ResolutionOption] = []
        if score > 0.3:
            resolution_options.append(
                ResolutionOption(
                    action="investigate_drift",
                    parameters={
                        "column": context.column_name,
                        "worst_period": evidence_data.get("worst_period", ""),
                    },
                    effort="medium",
                    description="Investigate the cause of distribution drift",
                )
            )

        if score > 0.7:
            resolution_options.append(
                ResolutionOption(
                    action="transform_add_time_filter",
                    parameters={
                        "column": context.column_name,
                        "strategy": "use_recent_periods_only",
                    },
                    effort="low",
                    description="Filter to recent stable periods to reduce drift impact",
                )
            )

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
