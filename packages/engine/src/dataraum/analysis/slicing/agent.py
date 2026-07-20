"""Slicing Agent - LLM-powered analysis to identify optimal data slices.

This agent analyzes table statistics, semantic annotations, and correlations
to recommend the best categorical dimensions for slicing data into subsets.
Uses Anthropic structured outputs for a typed response.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from sqlalchemy.orm import Session

from dataraum.analysis.slicing.models import (
    SliceRecommendation,
    SlicingAnalysisOutput,
    SlicingAnalysisResult,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import DecisionSource, Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
)

if TYPE_CHECKING:
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


class SlicingAgent(LLMFeature):
    """LLM-powered slicing analysis agent.

    Analyzes tables to identify the best categorical dimensions for
    creating data subsets (slices). Each unique value in a slice
    dimension creates a separate subset.

    Uses Anthropic structured outputs for a typed response.

    Uses inputs from:
    - Statistical profiles (distinct counts, value distributions)
    - Semantic annotations (business meaning, roles)
    - Correlation analysis (relationships between columns)
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
    ) -> None:
        """Initialize slicing agent.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
        """
        super().__init__(config, provider, prompt_renderer)

    def analyze(
        self,
        session: Session,
        table_ids: list[str],
        context_data: dict[str, Any],
    ) -> Result[SlicingAnalysisResult]:
        """Analyze tables to recommend optimal slicing dimensions.

        Args:
            session: Database session
            table_ids: List of table IDs to analyze
            context_data: Pre-loaded context containing:
                - tables: Table metadata with columns
                - statistics: Statistical profiles per column
                - semantic: Semantic annotations per column
                - correlations: Correlation analysis results

        Returns:
            Result containing SlicingAnalysisResult or error
        """
        # Check if feature is enabled
        feature_config = self.config.features.slicing_analysis
        if not feature_config or not feature_config.enabled:
            return Result.fail("Slicing analysis is disabled in config")

        # Build context for prompt
        constraints = context_data.get("constraints", {})
        tables = context_data.get("tables", [])
        context = {
            "tables_json": json.dumps(tables, indent=2),
            "num_tables": len(tables),
            "table_names": ", ".join(t["table_name"] for t in tables),
            "max_recommendations": constraints.get("max_recommendations", 6),
        }

        # Render prompt with system/user split
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "slicing_analysis", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Call LLM — structured output (DAT-807): constrained decoding against
        # SlicingAnalysisOutput's schema; the answer is JSON message content.
        # ``model`` is passed explicitly like every other agent — omitting it fell
        # back to the provider's default_model, so this call site silently ignored
        # its configured tier (DAT-807 fix).
        model = self.provider.get_model_for_tier(feature_config.model_tier)
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            output_schema=SlicingAnalysisOutput.model_json_schema(),
            label="slicing_analysis",
            effort=feature_config.effort,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        # converse raises a typed ProviderError on an API failure (DAT-503) —
        # retryability rides the exception to the worker's durable boundary, so
        # we don't re-wrap it. A returned Result is always a success.
        response = self.provider.converse(request).unwrap()

        try:
            output = SlicingAnalysisOutput.model_validate_json(response.content)
        except ValidationError as e:
            return Result.fail(f"Failed to validate slicing response: {e}")

        # Convert Pydantic output to SlicingAnalysisResult
        return self._convert_output_to_result(output, context_data)

    def _convert_output_to_result(
        self,
        output: SlicingAnalysisOutput,
        context_data: dict[str, Any],
    ) -> Result[SlicingAnalysisResult]:
        """Convert the validated LLM output to SlicingAnalysisResult.

        Args:
            output: Validated Pydantic output from the LLM
            context_data: Original context data for lookups

        Returns:
            Result containing SlicingAnalysisResult
        """
        recommendations: list[SliceRecommendation] = []

        # Build lookup maps
        table_map = {t["table_name"]: t for t in context_data.get("tables", [])}
        column_map = {}
        for table in context_data.get("tables", []):
            for col in table.get("columns", []):
                key = (table["table_name"], col["column_name"])
                column_map[key] = col

        # Convert recommendations from Pydantic output
        for rec in output.recommendations:
            table_name = rec.table_name
            column_name = rec.column_name
            table_info = table_map.get(table_name, {})
            col_key = (table_name, column_name)
            col_info = column_map.get(col_key, {})

            # Ground every recommendation against the REAL column universe before
            # building it — the same discipline the time-axis already applies
            # (slicing_phase validates the time column against the universe). A
            # recommendation the LLM emits for a column that is not in this run's
            # context (a hallucination, or a cross-run enriched-view shape change —
            # e.g. a fact's dimension join drops to a passthrough view on a re-run,
            # so its ``fk__dim`` columns vanish) has no resolvable column_id. Stored,
            # that empty id is a guaranteed FK violation on ``slice_definitions`` that
            # crashes the whole begin_session. Drop it (loudly) instead.
            if not col_info or not col_info.get("column_id") or not table_info.get("table_id"):
                logger.warning(
                    "slice_recommendation_ungrounded",
                    table=table_name,
                    column=column_name,
                    reason="column not in this run's context (hallucinated or cross-run drift)",
                )
                continue

            # Get distinct values from output or column dict top_values
            distinct_values = rec.distinct_values
            if not distinct_values:
                top_values = col_info.get("top_values", [])
                distinct_values = [v.get("value", "") for v in top_values]

            recommendation = SliceRecommendation(
                table_id=table_info.get("table_id", ""),
                table_name=table_name,
                column_id=col_info.get("column_id", ""),
                column_name=column_name,
                slice_priority=rec.priority,
                distinct_values=distinct_values,
                value_count=len(distinct_values),
                reasoning=rec.reasoning,
                business_context=rec.business_context,
                confidence=rec.confidence,
            )
            recommendations.append(recommendation)

        result = SlicingAnalysisResult(
            recommendations=recommendations,
            # The per-table time-axis judgments (DAT-491); only tables the agent
            # actually analyzed count — hallucinated names resolve to nothing.
            time_columns={
                tc.table_name: tc.column_name
                for tc in output.time_columns
                if tc.table_name in table_map
            },
            source=DecisionSource.LLM,
            tables_analyzed=len(table_map),
            columns_considered=len(column_map),
        )

        return Result.ok(result)


__all__ = ["SlicingAgent"]
