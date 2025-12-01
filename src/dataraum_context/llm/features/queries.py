"""Suggested queries feature - LLM-powered query generation."""

import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.context.models import SuggestedQuery
from dataraum_context.core.models.base import Result
from dataraum_context.enrichment.models import SemanticEnrichmentResult
from dataraum_context.llm.features._base import LLMFeature


class SuggestedQueriesFeature(LLMFeature):
    """LLM-powered suggested query generation.

    Generates useful SQL queries for data exploration based on:
    - Table structure and semantics
    - Available metrics from ontology
    - Temporal columns and relationships
    """

    async def generate_queries(
        self,
        session: AsyncSession,
        semantic_result: SemanticEnrichmentResult,
        ontology: str = "general",
        ontology_metrics: list[dict[str, Any]] | None = None,
    ) -> Result[list[SuggestedQuery]]:
        """Generate suggested SQL queries.

        Args:
            session: Database session
            semantic_result: Semantic enrichment result
            ontology: Ontology name
            ontology_metrics: Available metrics from ontology

        Returns:
            Result containing list of suggested queries
        """
        # Check if feature is enabled
        feature_config = self.config.features.suggested_queries
        if not feature_config.enabled:
            return Result.fail("Suggested queries feature is disabled in config")

        # Build schema JSON from semantic result
        schema_json = self._build_schema_json(semantic_result)

        # Format ontology metrics
        metrics_text = self._format_ontology_metrics(ontology_metrics or [])

        context = {
            "schema_json": json.dumps(schema_json, indent=2),
            "ontology_name": ontology,
            "ontology_metrics": metrics_text,
        }

        # Render prompt
        try:
            prompt, temperature = self.renderer.render("suggested_queries", context)
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Call LLM
        response_result = await self._call_llm(
            session=session,
            feature_name="suggested_queries",
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
            return self._parse_queries_response(parsed)
        except json.JSONDecodeError as e:
            return Result.fail(f"Failed to parse LLM response as JSON: {e}")
        except Exception as e:
            return Result.fail(f"Failed to parse suggested queries: {e}")

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
                    "time_column": entity.time_column if entity else None,
                    "columns": [],
                }

            col_data = {
                "column_name": annotation.column_ref.column_name,
                "semantic_role": annotation.semantic_role.value,
                "business_name": annotation.business_name,
            }

            tables_data[table_name]["columns"].append(col_data)

        # Add relationships
        return {
            "tables": list(tables_data.values()),
            "relationships": [
                {
                    "from_table": r.from_table,
                    "from_column": r.from_column,
                    "to_table": r.to_table,
                    "to_column": r.to_column,
                }
                for r in semantic_result.relationships
            ],
        }

    def _format_ontology_metrics(self, ontology_metrics: list[dict[str, Any]]) -> str:
        """Format ontology metrics for prompt.

        Args:
            ontology_metrics: List of metric definitions

        Returns:
            Formatted string
        """
        if not ontology_metrics:
            return "No specific metrics defined"

        lines = []
        for metric in ontology_metrics:
            name = metric.get("name", "Unknown")
            formula = metric.get("formula", "")
            desc = metric.get("description", "")
            lines.append(f"- {name}: {formula} - {desc}")

        return "\n".join(lines) if lines else "No specific metrics defined"

    def _parse_queries_response(self, parsed: dict[str, Any]) -> Result[list[SuggestedQuery]]:
        """Parse LLM response into suggested queries.

        Args:
            parsed: Parsed JSON response

        Returns:
            Result containing list of suggested queries
        """
        try:
            queries = []

            for query_data in parsed.get("queries", []):
                query = SuggestedQuery(
                    name=query_data["name"],
                    description=query_data["description"],
                    category=query_data.get("category", "overview"),
                    sql=query_data["sql"],
                    complexity=query_data.get("complexity", "simple"),
                )
                queries.append(query)

            return Result.ok(queries)

        except Exception as e:
            return Result.fail(f"Failed to parse suggested queries: {e}")
