"""Temporal drift entropy detector.

Measures uncertainty from distribution drift over time.
Uses max Jensen-Shannon divergence from ColumnDriftSummary records.
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption
from dataraum.pipeline.fixes.models import FixSchema, FixSchemaField


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

    # Semantic roles where drift detection is meaningless —
    # IDs naturally differ across periods (JS divergence = ln(2) guaranteed).
    # Dimensions (counterparty, category) naturally vary across periods — that's
    # expected business behavior, not a data quality problem.
    _SKIP_ROLES = frozenset({"key", "foreign_key", "identifier", "dimension", "category"})

    # Columns with cardinality ratio above this are near-unique (IDs, references)
    # and naturally produce max JS divergence — skip to avoid false positives.
    _CARDINALITY_SKIP_THRESHOLD = 0.90

    @property
    def fix_schemas(self) -> list[FixSchema]:
        """Fix schemas for temporal drift."""
        return [
            FixSchema(
                action="accept_finding",
                target="config",
                description="Accept temporal drift as expected (e.g., seasonal patterns, business growth)",
                config_path="entropy/thresholds.yaml",
                key_path=["detectors", "temporal_drift", "accepted_columns"],
                operation="append",
                requires_rerun="analysis_review",
                guidance=(
                    "The column shows distribution drift over time. This may be "
                    "expected (seasonal patterns, business growth, price changes) "
                    "or a real data quality issue (schema change, data corruption). "
                    "Show the user the drift evidence: which period had the biggest "
                    "shift, the JS divergence magnitude, and the top value shifts. "
                    "Ask whether the drift is expected business behavior."
                ),
                fields={
                    "reason": FixSchemaField(
                        type="string",
                        required=True,
                        description="Why this drift is expected (e.g., 'seasonal revenue pattern')",
                    ),
                },
            ),
        ]

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
        # Skip identifier columns — IDs naturally differ across periods
        semantic = context.get_analysis("semantic", {})
        if hasattr(semantic, "semantic_role"):
            role = semantic.semantic_role
        else:
            role = semantic.get("semantic_role")
        if role in self._SKIP_ROLES:
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

        # Load config for accepted_columns
        config = get_entropy_config()
        detector_config = config.detector("temporal_drift")
        score_accepted = self.config.get("score_accepted") or detector_config.get(
            "score_accepted", 0.2
        )
        accepted_columns: list[str] = self.config.get("accepted_columns") or detector_config.get(
            "accepted_columns", []
        )

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
                evidence_data["top_shifts"] = top_shifts

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

        # Apply acceptance floor if this column was previously accepted
        target_key = f"{context.table_name}.{context.column_name}"
        if target_key in accepted_columns:
            score = score_accepted
            evidence[0]["accepted"] = True

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
