"""Business meaning entropy detector.

Measures uncertainty in business meaning/description.
Columns without clear business descriptions are harder to interpret correctly.

NOTE: This detector collects raw metrics for LLM interpretation.
The score calculation is provisional - Phase 2.5 will use LLM to
evaluate semantic quality rather than character counting.
"""

from dataraum_context.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum_context.entropy.models import EntropyObject, ResolutionOption


class BusinessMeaningDetector(EntropyDetector):
    """Detector for business meaning clarity.

    Collects raw metrics about business description and semantic annotation.
    Score is provisional - will be refined by LLM in Phase 2.5.

    Source: semantic/SemanticAnnotation.business_description
    """

    detector_id = "business_meaning"
    layer = "semantic"
    dimension = "business_meaning"
    sub_dimension = "naming_clarity"
    required_analyses = ["semantic"]
    description = "Measures clarity of business meaning and description"

    async def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect business meaning entropy.

        Collects raw metrics about semantic annotations.
        Score is binary (has description vs. doesn't) - semantic quality
        evaluation will be done by LLM in Phase 2.5.

        Args:
            context: Detector context with semantic analysis results

        Returns:
            List with single EntropyObject for business meaning
        """
        semantic = context.get_analysis("semantic", {})

        # Extract raw metrics from semantic annotation
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
        }

        # Provisional score: binary based on presence of description
        # TODO (Phase 2.5): LLM will evaluate semantic quality
        if not raw_metrics["has_description"]:
            score = 1.0  # No description = high entropy
        elif not raw_metrics["has_business_name"] and not raw_metrics["has_entity_type"]:
            score = 0.6  # Description but no other context
        else:
            score = 0.2  # Has description and additional context

        # Build evidence with raw metrics for LLM interpretation
        evidence = [
            {
                "raw_metrics": raw_metrics,
                # Provisional classification (will be LLM-determined)
                "provisional_assessment": (
                    "missing"
                    if not raw_metrics["has_description"]
                    else "partial"
                    if score > 0.3
                    else "documented"
                ),
            }
        ]

        # Resolution options based on missing data (not semantic judgment)
        resolution_options: list[ResolutionOption] = []

        if not raw_metrics["has_description"]:
            resolution_options.append(
                ResolutionOption(
                    action="add_description",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    expected_entropy_reduction=0.8,  # Provisional estimate
                    effort="low",
                    description="Add a business description for this column",
                    cascade_dimensions=["computational.aggregations"],
                )
            )

        if not raw_metrics["has_business_name"]:
            resolution_options.append(
                ResolutionOption(
                    action="add_business_name",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    expected_entropy_reduction=0.2,  # Provisional estimate
                    effort="low",
                    description="Add a human-readable business name",
                )
            )

        if not raw_metrics["has_entity_type"]:
            resolution_options.append(
                ResolutionOption(
                    action="add_entity_type",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    expected_entropy_reduction=0.15,  # Provisional estimate
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
