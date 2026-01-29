"""Relationship quality entropy detector.

Measures uncertainty in relationships based on actual evaluation metrics:
- Referential integrity (orphan ratio)
- Cardinality verification
- Semantic clarity (relationship type, confirmation status)

Source: relationships.Relationship.evidence (contains JoinCandidate metrics)
"""

from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


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
    layer = "structural"
    dimension = "relations"
    sub_dimension = "relationship_quality"
    required_analyses = ["relationships"]
    description = "Measures relationship quality from evaluation metrics"

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
        reduction_verify = detector_config.get("reduction_verify_relationship", 0.6)

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
            cardinality_verified = evidence.get("cardinality_verified")

            # 1. Compute referential integrity entropy
            if left_ri is not None:
                # 100% integrity = 0 entropy, 0% integrity = 1.0 entropy
                ri_entropy = 1.0 - (left_ri / 100.0)
            elif orphan_count is not None and orphan_count > 0:
                # High orphan count indicates referential integrity issues
                # Without knowing total count, use orphan presence as indicator
                ri_entropy = min(1.0, 0.3 + (orphan_count / 1000.0))
            else:
                # Unknown referential integrity
                ri_entropy = score_unknown_ri

            # 2. Compute cardinality entropy
            if cardinality_verified is True:
                card_entropy = 0.1  # Verified cardinality = low entropy
            elif cardinality_verified is False:
                card_entropy = score_cardinality_mismatch  # Cardinality mismatch
            else:
                card_entropy = score_unverified_cardinality  # Unknown

            # 3. Compute semantic clarity entropy
            if is_confirmed and rel_type not in ("unknown", "candidate"):
                semantic_entropy = 0.1  # Confirmed with known type
            elif rel_type not in ("unknown", "candidate"):
                semantic_entropy = score_unconfirmed  # Known type but unconfirmed
            else:
                semantic_entropy = score_unknown_type  # Unknown type

            # Overall score is the maximum component (worst case)
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
                "evaluation_metrics": {
                    "left_referential_integrity": left_ri,
                    "orphan_count": orphan_count,
                    "cardinality_verified": cardinality_verified,
                },
            }

            # Resolution options
            resolution_options: list[ResolutionOption] = []

            if score > 0.3:
                if not is_confirmed:
                    resolution_options.append(
                        ResolutionOption(
                            action="confirm_relationship",
                            parameters={
                                "from_table": from_table,
                                "to_table": to_table,
                                "column": context.column_name,
                            },
                            expected_entropy_reduction=reduction_verify,
                            effort="low",
                            description=f"Confirm relationship between {from_table} and {to_table}",
                        )
                    )

                if ri_entropy > 0.3:
                    resolution_options.append(
                        ResolutionOption(
                            action="fix_referential_integrity",
                            parameters={
                                "from_table": from_table,
                                "to_table": to_table,
                                "orphan_count": orphan_count,
                            },
                            expected_entropy_reduction=ri_entropy * 0.8,
                            effort="high",
                            description="Fix referential integrity issues (orphan records)",
                        )
                    )

            objects.append(
                self.create_entropy_object(
                    context=context,
                    score=score,
                    evidence=[rel_evidence],
                    resolution_options=resolution_options,
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
