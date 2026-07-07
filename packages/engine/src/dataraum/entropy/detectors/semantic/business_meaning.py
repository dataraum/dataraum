"""Business meaning entropy detector.

The measurement is the LLM's NAMING CONFIDENCE alone: score = 1 - confidence
(``stats.confidence_entropy``). The LLM is instructed to lower confidence when a
column name is meaningless/random even if it can guess meaning from the data — that
confidence is the naming entropy, and a teach (name the column) closes it.

docs/architecture/entropy.md hard rule: NO deterministic semantic override. The old additive formula
(``base_score`` from description/metadata presence + ``confidence_weight·(1-conf)`` −
``ontology_bonus``) is GONE (DAT-442 two-table): documentation presence and ontology
alignment are CONTEXT carried in evidence, never the score — a confident annotation of
a well-named column is low entropy whether or not someone wrote a description for it.
"""

from dataraum.entropy import stats
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class BusinessMeaningDetector(EntropyDetector):
    """Detector for business meaning clarity.

    The score is the LLM's naming confidence alone (score = 1 - confidence). No
    deterministic metadata override (docs/architecture/entropy.md): description / business_name /
    entity_type / business_concept are evidence CONTEXT, not score.

    Source: semantic/SemanticAnnotation
    """

    detector_id = "business_meaning"
    layer = Layer.SEMANTIC
    dimension = Dimension.BUSINESS_MEANING
    sub_dimension = SubDimension.NAMING_CLARITY
    required_analyses = [AnalysisKey.SEMANTIC]
    description = "Measures clarity of business meaning and description"

    def load_data(self, context: DetectorContext) -> None:
        """Load semantic annotation for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_semantic

        result = load_semantic(
            context.session, context.column_id, context.run_id, base_runs=context.base_runs
        )
        if result is not None:
            context.analysis_results["semantic"] = result

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect business meaning entropy.

        score = 1 - confidence (``stats.confidence_entropy``) — the LLM's naming
        confidence alone. Documentation / ontology presence are evidence context, not
        score (docs/architecture/entropy.md hard rule, no deterministic semantic override).

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

        # The measurement is the LLM's naming confidence ALONE (docs/architecture/entropy.md hard rule:
        # no deterministic semantic override). score = 1 - confidence. Documentation
        # presence + ontology alignment in raw_metrics below are CONTEXT, not score.
        score = stats.confidence_entropy(confidence)

        # Build evidence with raw metrics and score components
        evidence = [
            {
                "raw_metrics": raw_metrics,
                "score_components": {
                    "naming_confidence": round(confidence, 3),
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

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
            )
        ]
