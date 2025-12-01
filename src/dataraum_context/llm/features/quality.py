"""Quality rule generation feature - LLM-powered quality rules."""

import json
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import DecisionSource, QualitySeverity, Result
from dataraum_context.enrichment.models import SemanticEnrichmentResult
from dataraum_context.llm.features._base import LLMFeature
from dataraum_context.quality.models import QualityRule


class QualityRulesFeature(LLMFeature):
    """LLM-powered quality rule generation.

    Generates domain-appropriate quality rules based on:
    - Semantic understanding of columns
    - Ontology constraints
    - Statistical profiles
    """

    async def generate_rules(
        self,
        session: AsyncSession,
        semantic_result: SemanticEnrichmentResult,
        ontology: str = "general",
        ontology_rules: dict[str, Any] | None = None,
    ) -> Result[list[QualityRule]]:
        """Generate quality rules based on semantic analysis.

        Args:
            session: Database session
            semantic_result: Semantic enrichment result
            ontology: Ontology name
            ontology_rules: Domain-specific rules from ontology (optional)

        Returns:
            Result containing list of quality rules
        """
        # Check if feature is enabled
        feature_config = self.config.features.quality_rule_generation
        if not feature_config.enabled:
            return Result.fail("Quality rule generation is disabled in config")

        # Build schema JSON from semantic result
        schema_json = self._build_schema_json(semantic_result)

        # Format ontology rules
        rules_text = self._format_ontology_rules(ontology_rules or {})

        context = {
            "schema_json": json.dumps(schema_json, indent=2),
            "ontology_name": ontology,
            "ontology_rules": rules_text,
        }

        # Render prompt
        try:
            prompt, temperature = self.renderer.render("quality_rules", context)
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Call LLM
        response_result = await self._call_llm(
            session=session,
            feature_name="quality_rule_generation",
            prompt=prompt,
            temperature=temperature,
            model_tier=feature_config.model_tier,
            ontology=ontology,
        )

        if not response_result.success or not response_result.value:
            return Result.fail(response_result.error if response_result.error else "Unknown Error")

        response = response_result.value

        # Parse response
        try:
            parsed = json.loads(response.content)
            return self._parse_rules_response(parsed, response.model)
        except json.JSONDecodeError as e:
            return Result.fail(f"Failed to parse LLM response as JSON: {e}")
        except Exception as e:
            return Result.fail(f"Failed to parse quality rules: {e}")

    def _build_schema_json(self, semantic_result: SemanticEnrichmentResult) -> dict[str, Any]:
        """Build schema JSON from semantic analysis result.

        Args:
            semantic_result: Semantic enrichment result

        Returns:
            Schema dict for JSON serialization
        """
        # Group annotations by table
        tables_data: dict[str, dict[str, Any]] = {}

        for annotation in semantic_result.annotations:
            table_name = annotation.column_ref.table_name

            if table_name not in tables_data:
                # Find entity detection for this table
                entity = next(
                    (e for e in semantic_result.entity_detections if e.table_name == table_name),
                    None,
                )

                tables_data[table_name] = {
                    "table_name": table_name,
                    "entity_type": entity.entity_type if entity else "unknown",
                    "description": entity.description if entity else None,
                    "columns": [],
                }

            col_data = {
                "column_name": annotation.column_ref.column_name,
                "semantic_role": annotation.semantic_role.value,
                "entity_type": annotation.entity_type,
                "business_name": annotation.business_name,
                "description": annotation.business_description,
            }

            tables_data[table_name]["columns"].append(col_data)

        return {"tables": list(tables_data.values())}

    def _format_ontology_rules(self, ontology_rules: dict[str, Any]) -> str:
        """Format ontology rules for prompt.

        Args:
            ontology_rules: Ontology rules dict

        Returns:
            Formatted string
        """
        if not ontology_rules:
            return "No specific ontology rules defined"

        lines = []
        for rule_name, rule_spec in ontology_rules.items():
            if isinstance(rule_spec, str):
                lines.append(f"- {rule_name}: {rule_spec}")
            elif isinstance(rule_spec, dict):
                desc = rule_spec.get("description", rule_name)
                lines.append(f"- {desc}")

        return "\n".join(lines) if lines else "No specific ontology rules defined"

    def _parse_rules_response(
        self, parsed: dict[str, Any], model_name: str
    ) -> Result[list[QualityRule]]:
        """Parse LLM response into quality rules.

        Args:
            parsed: Parsed JSON response
            model_name: Model that generated the response

        Returns:
            Result containing list of quality rules
        """
        try:
            rules = []

            for rule_data in parsed.get("rules", []):
                # Parse severity
                severity_str = rule_data.get("severity", "warning")
                try:
                    severity = QualitySeverity(severity_str)
                except ValueError:
                    severity = QualitySeverity.WARNING

                rule = QualityRule(
                    rule_id=str(uuid4()),
                    table_name=rule_data["table_name"],
                    column_name=rule_data.get("column_name"),
                    rule_name=rule_data["rule_name"],
                    rule_type=rule_data["rule_type"],
                    rule_expression=rule_data["expression"],
                    parameters={},
                    severity=severity,
                    source=DecisionSource.LLM,
                    description=rule_data.get("description"),
                )
                rules.append(rule)

            return Result.ok(rules)

        except Exception as e:
            return Result.fail(f"Failed to parse quality rules: {e}")
