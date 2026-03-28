"""Enrichment Agent - LLM-powered analysis to identify valuable dimension joins.

This agent analyzes tables, semantic annotations, and confirmed relationships
to recommend dimension joins that add analytical value to main datasets.
Uses tool-based output for structured responses.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from dataraum.analysis.views.builder import DimensionJoin
from dataraum.analysis.views.enrichment_models import (
    EnrichmentAnalysisOutput,
    EnrichmentAnalysisResult,
    EnrichmentRecommendation,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)

if TYPE_CHECKING:
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


class EnrichmentAgent(LLMFeature):
    """LLM-powered enrichment analysis agent.

    Analyzes tables and confirmed relationships to identify valuable dimension
    joins for enriching main datasets with geographic, categorical,
    and reference data.

    Uses inputs from:
    - Table entity classifications (fact/dimension)
    - Semantic annotations (roles, entity types)
    - LLM-confirmed relationships (from semantic phase)
    - Existing enriched views (to avoid duplication)
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
    ) -> None:
        """Initialize enrichment agent.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
        """
        super().__init__(config, provider, prompt_renderer)

    def analyze(
        self,
        session: Session,
        context_data: dict[str, Any],
    ) -> Result[EnrichmentAnalysisResult]:
        """Analyze tables to recommend valuable enrichment joins.

        Args:
            session: Database session
            context_data: Pre-loaded context containing:
                - tables: Table metadata with entity classifications
                - annotations: Semantic annotations per column
                - confirmed_relationships: LLM-confirmed relationships
                - existing_views: Already-created enriched views

        Returns:
            Result containing EnrichmentAnalysisResult or error
        """
        # Check if feature is enabled
        feature_config = self.config.features.enrichment_analysis
        if not feature_config or not feature_config.enabled:
            return Result.fail("Enrichment analysis is disabled in config")

        # Build context for prompt
        context = {
            "tables_json": json.dumps(context_data.get("tables", []), indent=2),
            "annotations_json": json.dumps(context_data.get("annotations", []), indent=2),
            "confirmed_relationships": self._format_relationships(
                context_data.get("confirmed_relationships", [])
            ),
            "existing_views": self._format_existing_views(context_data.get("existing_views", [])),
        }

        # Render prompt with system/user split
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "enrichment_analysis", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Define tool for structured output
        tool = ToolDefinition(
            name="analyze_enrichment",
            description="Provide structured enrichment recommendations for main datasets",
            input_schema=EnrichmentAnalysisOutput.model_json_schema(),
        )

        # Get model for tier
        model = self.provider.get_model_for_tier(feature_config.model_tier)

        # Call LLM with tool use
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "analyze_enrichment"},
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        result = self.provider.converse(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "LLM call failed")

        response = result.value

        # Extract tool call result
        if not response.tool_calls:
            # LLM didn't use the tool - try to parse text as fallback
            if response.content:
                try:
                    response_data = json.loads(response.content)
                    output = EnrichmentAnalysisOutput.model_validate(response_data)
                except Exception:
                    return Result.fail(f"LLM did not use tool. Response: {response.content[:200]}")
            else:
                return Result.fail("LLM did not use the analyze_enrichment tool")
        else:
            # Parse tool response using Pydantic model
            tool_call = response.tool_calls[0]
            if tool_call.name != "analyze_enrichment":
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

            try:
                output = EnrichmentAnalysisOutput.model_validate(tool_call.input)
            except Exception as e:
                return Result.fail(f"Failed to validate tool response: {e}")

        # Convert Pydantic output to EnrichmentAnalysisResult
        return self._convert_output_to_result(output, context_data, response.model)

    def _format_relationships(self, relationships: list[dict[str, Any]]) -> str:
        """Format confirmed relationships for prompt.

        Args:
            relationships: List of confirmed relationship dicts

        Returns:
            Formatted string for the prompt
        """
        if not relationships:
            return "No confirmed relationships available."

        lines = []
        for rel in relationships:
            from_table = rel.get("from_table", "?")
            to_table = rel.get("to_table", "?")
            from_col = rel.get("from_column", "?")
            to_col = rel.get("to_column", "?")
            cardinality = rel.get("cardinality", "unknown")
            confidence = rel.get("confidence", 0.0)

            # Only show grain-safe cardinalities prominently
            grain_safe = cardinality in ("many-to-one", "one-to-one")
            grain_marker = " [GRAIN-SAFE]" if grain_safe else " [AVOID - may duplicate rows]"

            lines.append(
                f"- {from_table}.{from_col} -> {to_table}.{to_col} "
                f"({cardinality}, confidence={confidence:.2f}){grain_marker}"
            )

        return "\n".join(lines)

    def _format_existing_views(self, views: list[dict[str, Any]]) -> str:
        """Format existing enriched views for prompt.

        Args:
            views: List of existing view dicts

        Returns:
            Formatted string for the prompt
        """
        if not views:
            return "No enriched views exist yet."

        lines = []
        for view in views:
            view_name = view.get("view_name", "?")
            fact_table = view.get("fact_table", "?")
            dimensions = view.get("dimension_columns", [])
            dim_str = ", ".join(dimensions[:5])
            if len(dimensions) > 5:
                dim_str += f" ... (+{len(dimensions) - 5} more)"

            lines.append(f"- {view_name}: {fact_table} enriched with [{dim_str}]")

        return "\n".join(lines)

    def _convert_output_to_result(
        self,
        output: EnrichmentAnalysisOutput,
        context_data: dict[str, Any],
        model_name: str,
    ) -> Result[EnrichmentAnalysisResult]:
        """Convert Pydantic tool output to EnrichmentAnalysisResult.

        Args:
            output: Validated Pydantic output from LLM
            context_data: Original context data for lookups
            model_name: Model that generated the response

        Returns:
            Result containing EnrichmentAnalysisResult
        """
        recommendations: list[EnrichmentRecommendation] = []

        # Build lookup maps
        table_map = {t["table_name"]: t for t in context_data.get("tables", [])}

        # Build column lookup: (table_name, column_name) -> column_info
        column_map: dict[tuple[str, str], dict[str, Any]] = {}
        for table in context_data.get("tables", []):
            for col in table.get("columns", []):
                key = (table["table_name"], col["column_name"])
                column_map[key] = col

        # Convert recommendations from Pydantic output
        for dataset in output.main_datasets:
            fact_table_name = dataset.table_name
            fact_table_info = table_map.get(fact_table_name, {})
            fact_table_id = fact_table_info.get("table_id", "")

            if not fact_table_id:
                logger.warning(
                    "fact_table_not_found",
                    table_name=fact_table_name,
                )
                continue

            # When the same dimension table is joined multiple times (e.g. via
            # kontonummer_des_kontos and kontonummer_des_gegenkontos), the LLM
            # may rate columns inconsistently across joins. Unify: take the
            # union of all high/medium columns per dimension table, so every
            # join of that table gets the same column set.
            unified_dim_columns: dict[str, set[str]] = {}
            for enrichment in dataset.recommended_enrichments:
                dim_name = enrichment.dimension_table
                for col in enrichment.enrichment_columns:
                    if col.enrichment_value in ("high", "medium"):
                        unified_dim_columns.setdefault(dim_name, set()).add(col.column_name)

            for enrichment in dataset.recommended_enrichments:
                dim_table_name = enrichment.dimension_table
                dim_table_info = table_map.get(dim_table_name, {})
                dim_table_id = dim_table_info.get("table_id", "")
                dim_duckdb_path = dim_table_info.get("duckdb_path", "")

                if not dim_table_id or not dim_duckdb_path:
                    logger.warning(
                        "dimension_table_not_found",
                        table_name=dim_table_name,
                    )
                    continue

                # Use unified column set for this dimension table
                include_columns = sorted(unified_dim_columns.get(dim_table_name, set()))

                if not include_columns:
                    logger.info(
                        "no_valuable_columns",
                        dimension_table=dim_table_name,
                    )
                    continue

                # Create DimensionJoin
                dimension_join = DimensionJoin(
                    dim_table_name=dim_table_name,
                    dim_duckdb_path=dim_duckdb_path,
                    fact_fk_column=enrichment.join_fact_column,
                    dim_pk_column=enrichment.join_dimension_column,
                    include_columns=include_columns,
                    relationship_id="",  # Will be filled by caller if needed
                )

                # Create recommendation
                enrichment_columns = [
                    f"{col.column_name}:{col.enrichment_value}"
                    for col in enrichment.enrichment_columns
                ]

                recommendation = EnrichmentRecommendation(
                    fact_table_id=fact_table_id,
                    fact_table_name=fact_table_name,
                    dimension_joins=[dimension_join],
                    dimension_type=enrichment.dimension_type,
                    confidence=enrichment.confidence,
                    reasoning=enrichment.reasoning,
                    enrichment_columns=enrichment_columns,
                )
                recommendations.append(recommendation)

        result = EnrichmentAnalysisResult(
            recommendations=recommendations,
            summary=output.summary,
            model_name=model_name,
        )

        logger.info(
            "enrichment_analysis_complete",
            recommendations=len(recommendations),
            model=model_name,
        )

        return Result.ok(result)


__all__ = ["EnrichmentAgent"]
