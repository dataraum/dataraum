"""Context summary feature - LLM-powered natural language summaries."""

import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.context.models import ContextSummary, QualitySummary
from dataraum_context.core.models.base import Result
from dataraum_context.enrichment.models import SemanticEnrichmentResult
from dataraum_context.llm.features._base import LLMFeature


class ContextSummaryFeature(LLMFeature):
    """LLM-powered context summary generation.

    Generates natural language summaries of the dataset including:
    - Overview of data structure and purpose
    - Key facts and statistics
    - Important warnings and caveats
    """

    async def generate_summary(
        self,
        session: AsyncSession,
        semantic_result: SemanticEnrichmentResult,
        quality_summary: QualitySummary | None = None,
    ) -> Result[ContextSummary]:
        """Generate natural language context summary.

        Args:
            session: Database session
            semantic_result: Semantic enrichment result
            quality_summary: Quality assessment summary (optional)

        Returns:
            Result containing ContextSummary
        """
        # Check if feature is enabled
        feature_config = self.config.features.context_summary
        if not feature_config.enabled:
            return Result.fail("Context summary feature is disabled in config")

        # Build schema JSON from semantic result
        schema_json = self._build_schema_json(semantic_result)

        # Build quality JSON
        quality_json = self._build_quality_json(quality_summary)

        context = {
            "schema_json": json.dumps(schema_json, indent=2),
            "quality_summary": json.dumps(quality_json, indent=2),
        }

        # Render prompt
        try:
            prompt, temperature = self.renderer.render("context_summary", context)
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Call LLM
        response_result = await self._call_llm(
            session=session,
            feature_name="context_summary",
            prompt=prompt,
            temperature=temperature,
            model_tier=feature_config.model_tier,
        )

        if not response_result.success or not response_result.value:
            return Result.fail(response_result.error if response_result.error else "Unknown Error")

        response = response_result.value

        # Parse response
        try:
            parsed = json.loads(response.content)
            return self._parse_summary_response(parsed)
        except json.JSONDecodeError as e:
            return Result.fail(f"Failed to parse LLM response as JSON: {e}")
        except Exception as e:
            return Result.fail(f"Failed to parse context summary: {e}")

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
                    "column_count": 0,
                }

            tables_data[table_name]["column_count"] += 1

        return {
            "tables": list(tables_data.values()),
            "relationship_count": len(semantic_result.relationships),
        }

    def _build_quality_json(self, quality_summary: QualitySummary | None) -> dict[str, Any]:
        """Build quality JSON.

        Args:
            quality_summary: Quality summary or None

        Returns:
            Quality dict for JSON serialization
        """
        if quality_summary is None:
            return {"available": False}

        return {
            "available": True,
            "overall_score": quality_summary.overall_score,
            "tables_assessed": quality_summary.tables_assessed,
            "issues_found": quality_summary.issues_found,
            "critical_issues": quality_summary.critical_issues,
        }

    def _parse_summary_response(self, parsed: dict[str, Any]) -> Result[ContextSummary]:
        """Parse LLM response into context summary.

        Args:
            parsed: Parsed JSON response

        Returns:
            Result containing ContextSummary
        """
        try:
            summary = ContextSummary(
                summary=parsed.get("summary", ""),
                key_facts=parsed.get("key_facts", []),
                warnings=parsed.get("warnings", []),
            )

            return Result.ok(summary)

        except Exception as e:
            return Result.fail(f"Failed to parse context summary: {e}")
