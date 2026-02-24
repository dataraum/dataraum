"""Business meaning entropy detector.

Measures uncertainty in business meaning/description.
Columns without clear business descriptions are harder to interpret correctly.

Scoring formula (additive):
  score = base_score + confidence_weight * (1 - confidence) - concept_bonus

- base_score: from presence of description/metadata fields (0.0 to 1.0)
- confidence_weight * (1 - confidence): low LLM confidence adds entropy independently
- concept_bonus: business_concept presence reduces entropy (ontology alignment)
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


class BusinessMeaningDetector(EntropyDetector):
    """Detector for business meaning clarity.

    Measures semantic clarity using an additive formula:
      score = base_score + confidence_weight * (1 - confidence) - concept_bonus

    - base_score: from presence of description, business_name, entity_type
    - confidence_weight: how much LLM confidence affects score (additive penalty)
    - concept_bonus: business_concept presence reduces entropy (ontology alignment)

    Source: semantic/SemanticAnnotation
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "business_meaning"
    layer = "semantic"
    dimension = "business_meaning"
    sub_dimension = "naming_clarity"
    required_analyses = ["semantic"]
    description = "Measures clarity of business meaning and description"

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect business meaning entropy.

        Score calculation (additive):
        1. Base score from presence of description/metadata (0.0 to 1.0)
        2. Confidence penalty: confidence_weight * (1 - confidence)
        3. Ontology bonus: business_concept presence reduces entropy

        Final: score = base_score + confidence_penalty - concept_bonus

        Args:
            context: Detector context with semantic analysis results

        Returns:
            List with single EntropyObject for business meaning
        """
        # Load configuration
        config = get_entropy_config()
        detector_config = config.detector("business_meaning")

        # Get configurable scores and reductions
        score_missing = detector_config.get("score_missing", 1.0)
        score_partial = detector_config.get("score_partial", 0.6)
        score_documented = detector_config.get("score_documented", 0.2)
        score_fully_documented = detector_config.get("score_fully_documented", 0.0)

        # Confidence weighting and ontology bonus
        confidence_weight = detector_config.get("confidence_weight", 0.3)
        ontology_bonus = detector_config.get("ontology_bonus", 0.1)

        semantic = context.get_analysis("semantic", {})

        # Extract raw metrics from semantic annotation
        if hasattr(semantic, "business_description"):
            description = semantic.business_description or ""
            business_name = getattr(semantic, "business_name", None)
            entity_type = getattr(semantic, "entity_type", None)
            semantic_role = getattr(semantic, "semantic_role", None)
            confidence = getattr(semantic, "confidence", None) or 1.0
            business_concept = getattr(semantic, "business_concept", None)
        else:
            description = semantic.get("business_description", "") or ""
            business_name = semantic.get("business_name")
            entity_type = semantic.get("entity_type")
            semantic_role = semantic.get("semantic_role")
            confidence = semantic.get("confidence") or 1.0
            business_concept = semantic.get("business_concept")

        # Collect raw metrics (factual, not interpreted)
        raw_metrics = {
            "description": description.strip(),
            "description_length": len(description.strip()),
            "has_description": bool(description.strip()),
            "business_name": business_name,
            "has_business_name": bool(business_name),
            "entity_type": entity_type,
            "has_entity_type": bool(entity_type),
            "semantic_role": str(semantic_role) if semantic_role else None,
            "semantic_confidence": confidence,
            "business_concept": business_concept,
            "has_business_concept": bool(business_concept),
        }

        # 1. Base score from documentation presence
        if not raw_metrics["has_description"]:
            base_score = score_missing  # No description = high entropy
        elif not raw_metrics["has_business_name"] and not raw_metrics["has_entity_type"]:
            base_score = score_partial  # Description but no other context
        elif raw_metrics["has_business_name"] and raw_metrics["has_entity_type"]:
            base_score = score_fully_documented  # All metadata present
        else:
            base_score = score_documented  # Has description + one of the two

        # 2. Confidence penalty: low confidence adds entropy independently
        # When confidence=1.0, penalty=0; when confidence=0.0, penalty=confidence_weight
        confidence_penalty = confidence_weight * max(0.0, 1.0 - confidence)

        # 3. Ontology bonus: having business_concept = ontology alignment = lower entropy
        concept_bonus = ontology_bonus if business_concept else 0.0

        # Calculate final score (additive: base + penalty - bonus)
        # This avoids the multiplicative bug where 0.0 * anything = 0.0
        score = base_score + confidence_penalty - concept_bonus
        score = max(0.0, min(1.0, score))  # Clamp to [0, 1]

        # Build evidence with raw metrics and score components
        evidence = [
            {
                "raw_metrics": raw_metrics,
                "score_components": {
                    "base_score": round(base_score, 3),
                    "confidence_penalty": round(confidence_penalty, 3),
                    "ontology_bonus": round(concept_bonus, 3),
                    "final_score": round(score, 3),
                },
                "assessment": (
                    "missing"
                    if not raw_metrics["has_description"]
                    else "fully_documented"
                    if raw_metrics["has_business_name"] and raw_metrics["has_entity_type"]
                    else "partial"
                    if not raw_metrics["has_business_name"] and not raw_metrics["has_entity_type"]
                    else "documented"
                ),
            }
        ]

        # Resolution options based on missing data (not semantic judgment)
        resolution_options: list[ResolutionOption] = []

        if not raw_metrics["has_description"]:
            resolution_options.append(
                ResolutionOption(
                    action="document_description",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    effort="low",
                    description="Add a business description for this column",
                )
            )

        if not raw_metrics["has_business_name"]:
            resolution_options.append(
                ResolutionOption(
                    action="document_business_name",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    effort="low",
                    description="Add a human-readable business name",
                )
            )

        if not raw_metrics["has_entity_type"]:
            resolution_options.append(
                ResolutionOption(
                    action="document_entity_type",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    effort="low",
                    description="Classify the entity type for this column",
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
