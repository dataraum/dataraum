"""Business meaning entropy detector.

Measures uncertainty in business meaning/description.
Columns without clear business descriptions are harder to interpret correctly.
"""

from dataraum_context.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum_context.entropy.models import EntropyObject, ResolutionOption


class BusinessMeaningDetector(EntropyDetector):
    """Detector for business meaning clarity.

    Uses business_description from semantic annotation to measure
    how well the column's purpose is documented.

    Source: semantic/SemanticAnnotation.business_description
    Formula:
        - 1.0 if empty/missing
        - 0.7 if brief (< 20 chars)
        - 0.2 if substantial (>= 20 chars)
    """

    detector_id = "business_meaning"
    layer = "semantic"
    dimension = "business_meaning"
    sub_dimension = "naming_clarity"
    required_analyses = ["semantic"]
    description = "Measures clarity of business meaning and description"

    # Thresholds for description quality
    BRIEF_THRESHOLD = 20
    SUBSTANTIAL_THRESHOLD = 50

    async def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect business meaning entropy.

        Args:
            context: Detector context with semantic analysis results

        Returns:
            List with single EntropyObject for business meaning
        """
        semantic = context.get_analysis("semantic", {})

        # Extract business description and related fields
        if hasattr(semantic, "business_description"):
            description = semantic.business_description or ""
            business_name = getattr(semantic, "business_name", None)
            entity_type = getattr(semantic, "entity_type", None)
            semantic_role = getattr(semantic, "semantic_role", None)
            confidence = getattr(semantic, "confidence", 1.0)
        else:
            description = semantic.get("business_description", "") or ""
            business_name = semantic.get("business_name")
            entity_type = semantic.get("entity_type")
            semantic_role = semantic.get("semantic_role")
            confidence = semantic.get("confidence", 1.0)

        # Calculate entropy based on description quality
        desc_length = len(description.strip())

        if desc_length == 0:
            score = 1.0
            clarity = "missing"
        elif desc_length < self.BRIEF_THRESHOLD:
            score = 0.7
            clarity = "brief"
        elif desc_length < self.SUBSTANTIAL_THRESHOLD:
            score = 0.4
            clarity = "moderate"
        else:
            score = 0.2
            clarity = "substantial"

        # Adjust score based on other semantic fields
        # Having a business name reduces entropy slightly
        if business_name:
            score = max(0.0, score - 0.1)

        # Having an entity type reduces entropy slightly
        if entity_type:
            score = max(0.0, score - 0.05)

        # Low confidence in semantic analysis increases entropy
        if confidence < 0.7:
            score = min(1.0, score + 0.2)

        # Build evidence
        evidence = [
            {
                "description_length": desc_length,
                "clarity": clarity,
                "has_business_name": bool(business_name),
                "has_entity_type": bool(entity_type),
                "semantic_role": str(semantic_role) if semantic_role else None,
                "semantic_confidence": confidence,
            }
        ]

        # Build resolution options
        resolution_options: list[ResolutionOption] = []

        if score > 0.5:
            # Missing or very brief description
            resolution_options.append(
                ResolutionOption(
                    action="add_description",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    expected_entropy_reduction=score * 0.8,
                    effort="low",
                    description="Add a business description for this column",
                    cascade_dimensions=["computational.aggregations"],
                )
            )

        if not business_name and score > 0.3:
            resolution_options.append(
                ResolutionOption(
                    action="add_business_name",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    expected_entropy_reduction=0.2,
                    effort="low",
                    description="Add a human-readable business name",
                )
            )

        if not entity_type and score > 0.4:
            resolution_options.append(
                ResolutionOption(
                    action="add_entity_type",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    expected_entropy_reduction=0.15,
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
