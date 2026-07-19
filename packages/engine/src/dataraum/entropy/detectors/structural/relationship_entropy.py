"""Relationship quality entropy detector.

The measurement is the raw referential-integrity ORPHAN RATE (``stats.orphan_rate``):
20% orphans scores 0.20, no boost. That single rate IS the score — the old
cardinality / semantic-clarity component scores and their max() aggregation are gone
(DAT-442 two-table). Cardinality + confirmation are relationship CONTEXT carried in
evidence, not score; an absent RI metric is ignorance, not a fabricated mid-score.
The eval asserts the ordering vs clean; severity per intent lives in the loss table.
Teach: define / fix the FK.

Source: relationships.Relationship.evidence (contains JoinCandidate metrics)
"""

from typing import Any

from dataraum.entropy import stats
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class RelationshipEntropyDetector(EntropyDetector):
    """Detector for relationship quality entropy.

    Computes entropy from actual relationship metrics rather than
    hardcoded values. Uses:
    - Referential integrity (% FK values with matching PK)
    - Orphan count (FK values with no match)
    - Cardinality verification (does detected cardinality match actual)
    - Confirmation status (human verified vs auto-detected)

    The evidence JSON from Relationship.evidence contains JoinCandidate
    evaluation metrics populated by analysis/relationships/evaluator.py.

    Source: relationships analysis (Relationship.evidence)
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "relationship_entropy"
    layer = Layer.STRUCTURAL
    dimension = Dimension.RELATIONS
    sub_dimension = SubDimension.RELATIONSHIP_QUALITY
    required_analyses = [AnalysisKey.RELATIONSHIPS]
    scope = "relationship"
    description = "Measures relationship quality from evaluation metrics"

    def load_data(self, context: DetectorContext) -> None:
        """Load the focal relationship for this directional column pair (DAT-408)."""
        if (
            context.session is None
            or context.from_column_id is None
            or context.to_column_id is None
        ):
            return
        from dataraum.entropy.detectors.loaders import load_relationship_for_pair

        rel = load_relationship_for_pair(
            context.session,
            context.from_column_id,
            context.to_column_id,
            run_id=context.run_id,
        )
        if rel is not None:
            # Store under the declared RELATIONSHIPS key so can_run() (which gates on
            # required_analyses) finds it — detect() reads the same key. Storing under
            # a different key ("relationship") left can_run permanently False → the
            # detector was silently skipped (zero recall). Found by DAT-405 calibration.
            context.analysis_results[AnalysisKey.RELATIONSHIPS] = rel

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect relationship-quality entropy for the focal relationship (DAT-408).

        Relationship-scoped: measures the one directional column pair in context →
        a single EntropyObject keyed ``relationship:{from}::{to}``. Components:
        The measurement is the raw referential-integrity ORPHAN RATE
        (``stats.orphan_rate`` over the evaluator's metrics) — the honest tunable
        entropy a teach (define / fix the FK) closes. The old card_entropy /
        semantic_entropy components and their invented constants + max() are gone
        (DAT-442 two-table): cardinality + confirmation are relationship CONTEXT,
        not score; an absent RI metric is ignorance, not a fabricated 0.5.

        DAT-372/409: confirmation is read from ``ConfigOverlay(type='relationship')``
        via ``load_confirmed_relationship_pairs`` (keyed on the focal column pair).
        """
        rel = context.get_analysis(AnalysisKey.RELATIONSHIPS, None)
        if not rel:
            return []

        evidence = self._get_value(rel, "evidence", {}) or {}
        left_ri = evidence.get("left_referential_integrity")
        left_orphan_count = evidence.get("left_orphan_count")
        total_count = evidence.get("total_count") or evidence.get("left_total_count")

        if left_ri is not None:
            score = max(0.0, min(1.0, 1.0 - left_ri / 100.0))
        elif left_orphan_count is not None and total_count:
            score = stats.rate(left_orphan_count, total_count)
        else:
            # No referential-integrity metric → nothing measurable. Absence is ignorance,
            # not a fabricated mid-score (the old score_unknown_ri=0.5 and the
            # 0.3 + orphan/1000 count fallback are deleted).
            return []

        rel_evidence: dict[str, Any] = {
            "from_table": context.from_table_name or self._get_value(rel, "from_table", "unknown"),
            "to_table": context.to_table_name or self._get_value(rel, "to_table", "unknown"),
            "relationship_type": self._get_value(rel, "relationship_type", "unknown"),
            "cardinality": self._get_value(rel, "cardinality", None),
            "confidence": self._get_value(rel, "confidence", 0.5),
            "is_confirmed": self._is_confirmed_via_overlay(context),
            "orphan_rate": round(score, 3),
            "ri_entropy": round(score, 3),  # canonical evidence key (back-compat)
            "evaluation_metrics": {
                "left_referential_integrity": left_ri,
                "left_orphan_count": left_orphan_count,
                "total_count": total_count,
                "cardinality_verified": evidence.get("cardinality_verified"),
            },
        }

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=[rel_evidence],
            )
        ]

    def _is_confirmed_via_overlay(self, context: DetectorContext) -> bool:
        """True iff the focal column pair has an active confirm overlay (DAT-372/409).

        Keyed on the directional column pair (the relationship's identity), not the
        table-name pair — confirmation is per-relationship, and the overlay payload
        carries ``{action, from_column_id, to_column_id}`` (DAT-409).
        """
        if context.session is None or not context.from_column_id or not context.to_column_id:
            return False
        from dataraum.analysis.relationships.utils import load_confirmed_relationship_pairs

        confirmed = load_confirmed_relationship_pairs(context.session)
        return frozenset({context.from_column_id, context.to_column_id}) in confirmed

    def _get_value(self, obj: Any, key: str, default: Any = None) -> Any:
        """Get value from object or dict."""
        if hasattr(obj, key):
            return getattr(obj, key)
        if isinstance(obj, dict):
            return obj.get(key, default)
        return default
