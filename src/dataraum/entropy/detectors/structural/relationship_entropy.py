"""Relationship quality entropy detector.

Measures uncertainty in relationships based on actual evaluation metrics:
- Referential integrity (orphan ratio) — primary signal, sqrt-boosted
- Cardinality verification
- Semantic clarity (relationship type, confirmation status)

Uses max aggregation (not weighted average) so the worst component drives
the score. RI is sqrt-boosted because orphan rates >5% are genuinely bad
for FK relationships, not noise.

Source: relationships.Relationship.evidence (contains JoinCandidate metrics)
"""

import math
from typing import Any

from dataraum.entropy.config import get_entropy_config
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
    description = "Measures relationship quality from evaluation metrics"

    def load_data(self, context: DetectorContext) -> None:
        """Load relationships for this column."""
        if context.session is None or context.column_id is None or context.table_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_relationships

        result = load_relationships(context.session, context.column_id, context.table_id)
        if result is not None:
            context.analysis_results["relationships"] = result

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect relationship quality entropy.

        Computes entropy components from actual relationship data:
        - ri_entropy: from referential integrity percentage
        - card_entropy: from cardinality_verified flag
        - semantic_entropy: from is_confirmed and relationship_type

        Args:
            context: Detector context with relationship analysis

        Returns:
            List of EntropyObjects (one per relationship involving this column),
            or empty list if no relationships
        """
        config = get_entropy_config()
        detector_config = config.detector("relationship_entropy")

        # Configurable scores for unknown values
        score_unknown_ri = detector_config.get("score_unknown_ri", 0.5)
        score_unverified_cardinality = detector_config.get("score_unverified_cardinality", 0.4)
        score_cardinality_mismatch = detector_config.get("score_cardinality_mismatch", 0.7)
        score_unconfirmed = detector_config.get("score_unconfirmed", 0.3)
        score_unknown_type = detector_config.get("score_unknown_type", 0.6)

        # RI boost factor: sqrt amplifies small orphan rates
        ri_boost = detector_config.get("ri_boost", True)

        relationships = context.get_analysis("relationships", [])

        # Handle different input formats
        if isinstance(relationships, dict):
            rels = relationships.get("relationships", [])
        elif isinstance(relationships, list):
            rels = relationships
        else:
            rels = []

        if not rels:
            return []

        objects: list[EntropyObject] = []

        for rel in rels:
            # Extract relationship metadata
            rel_type = self._get_value(rel, "relationship_type", "unknown")
            is_confirmed = self._get_value(rel, "is_confirmed", False)
            confidence = self._get_value(rel, "confidence", 0.5)
            evidence = self._get_value(rel, "evidence", {}) or {}
            cardinality = self._get_value(rel, "cardinality", None)

            # Get evaluation metrics from evidence (JoinCandidate fields)
            left_ri = evidence.get("left_referential_integrity")
            orphan_count = evidence.get("orphan_count")
            total_count = evidence.get("total_count") or evidence.get("left_total_count")
            cardinality_verified = evidence.get("cardinality_verified")

            # 1. Compute referential integrity entropy
            if left_ri is not None:
                ri_entropy = 1.0 - (left_ri / 100.0)
            elif orphan_count is not None and total_count and total_count > 0:
                ri_entropy = min(1.0, orphan_count / total_count)
            elif orphan_count is not None and orphan_count > 0:
                ri_entropy = min(1.0, 0.3 + (orphan_count / 1000.0))
            else:
                ri_entropy = score_unknown_ri

            # Boost RI: sqrt amplifies small-but-real orphan rates.
            # 5% orphans → 0.22, 10% → 0.32, 20% → 0.45, 50% → 0.71
            if ri_boost and ri_entropy > 0:
                ri_entropy = min(1.0, math.sqrt(ri_entropy))

            # 2. Compute cardinality entropy
            if cardinality_verified is True:
                card_entropy = 0.1
            elif cardinality_verified is False:
                card_entropy = score_cardinality_mismatch
            else:
                card_entropy = score_unverified_cardinality

            # 3. Compute semantic clarity entropy
            if is_confirmed and rel_type not in ("unknown", "candidate"):
                semantic_entropy = 0.1
            elif rel_type == "foreign_key" and ri_entropy < 0.05:
                # High-RI foreign key: the semantic agent classified it as FK
                # and RI proves the data matches. No human confirmation needed.
                semantic_entropy = 0.1
            elif rel_type not in ("unknown", "candidate"):
                semantic_entropy = score_unconfirmed
            else:
                semantic_entropy = score_unknown_type

            # Max aggregation: worst component drives the score.
            # Weighted average was too forgiving — it diluted real RI problems.
            score = max(ri_entropy, card_entropy, semantic_entropy)

            # Build evidence
            from_table = self._get_value(rel, "from_table", "unknown")
            to_table = self._get_value(rel, "to_table", "unknown")

            rel_evidence: dict[str, Any] = {
                "from_table": from_table,
                "to_table": to_table,
                "relationship_type": rel_type,
                "cardinality": cardinality,
                "confidence": confidence,
                "is_confirmed": is_confirmed,
                "ri_entropy": round(ri_entropy, 3),
                "card_entropy": round(card_entropy, 3),
                "semantic_entropy": round(semantic_entropy, 3),
                "aggregation_method": "max",
                "ri_boosted": ri_boost,
                "evaluation_metrics": {
                    "left_referential_integrity": left_ri,
                    "orphan_count": orphan_count,
                    "cardinality_verified": cardinality_verified,
                },
            }

            objects.append(
                self.create_entropy_object(
                    context=context,
                    score=score,
                    evidence=[rel_evidence],
                )
            )

        return objects

    def _get_value(self, obj: Any, key: str, default: Any = None) -> Any:
        """Get value from object or dict."""
        if hasattr(obj, key):
            return getattr(obj, key)
        if isinstance(obj, dict):
            return obj.get(key, default)
        return default
