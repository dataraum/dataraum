"""LLM-powered dimensional entropy summary agent.

Generates executive summaries, refined quality concerns, and
recommendations from dimensional entropy analysis results.

Extracted from dimensional_entropy.py for separation of concerns:
- The detector (dimensional_entropy.py) handles pattern detection
- This agent handles LLM-powered summarization
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from dataraum.core.models.base import Result

if TYPE_CHECKING:
    from dataraum.entropy.detectors.semantic.dimensional_entropy import (
        DatasetDimensionalSummary,
    )
    from dataraum.llm.cache import LLMCache
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider


class DimensionalSummaryOutput(BaseModel):
    """Pydantic model for LLM tool output - dimensional entropy executive summary.

    Used as a tool definition for structured LLM output via tool use API.
    """

    executive_summary: str = Field(
        description="Concise 3-5 sentence executive summary of dimensional entropy findings"
    )
    data_quality_concerns: list[str] = Field(
        default_factory=list,
        description="Refined data quality concerns with business impact context",
    )
    recommendations: list[str] = Field(
        default_factory=list,
        description="Prioritized actionable recommendations to reduce dimensional entropy",
    )


class DimensionalSummaryAgent:
    """LLM-powered dimensional entropy summary agent.

    Generates executive summaries, refined quality concerns, and
    recommendations from dimensional entropy analysis results.

    Note: Follows the EntropyInterpreter pattern - does not inherit
    from LLMFeature to avoid circular imports with entropy module.
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        cache: LLMCache,
    ) -> None:
        """Initialize dimensional summary agent.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
            cache: Response cache
        """
        self.config = config
        self.provider = provider
        self.renderer = prompt_renderer
        self.cache = cache

    def summarize(
        self,
        summary: DatasetDimensionalSummary,
    ) -> Result[DimensionalSummaryOutput]:
        """Generate LLM-powered executive summary.

        Args:
            summary: The computed DatasetDimensionalSummary with all
                     structured fields already populated.

        Returns:
            Result containing DimensionalSummaryOutput with
            executive_summary, quality_concerns, recommendations.
        """
        from dataraum.llm.providers.base import (
            ConversationRequest,
            Message,
            ToolDefinition,
        )

        # Check if feature is enabled
        feature_config = self.config.features.dimensional_summary
        if not feature_config or not feature_config.enabled:
            return Result.fail("Dimensional summary LLM feature is disabled")

        # Build context from the summary dataclass
        context = {
            "table_name": summary.table_name,
            "slice_column": summary.slice_column or "none",
            "total_columns": str(summary.total_columns),
            "interesting_categorical_columns": str(summary.interesting_categorical_columns),
            "interesting_temporal_columns": str(summary.interesting_temporal_columns),
            "stable_columns": str(summary.stable_columns),
            "empty_columns": str(summary.empty_columns),
            "constant_columns": str(summary.constant_columns),
            "dimensional_entropy_score": f"{summary.dimensional_entropy_score:.2f}",
            "categorical_entropy": f"{summary.categorical_entropy:.2f}",
            "temporal_entropy": f"{summary.temporal_entropy:.2f}",
            "uncertainty_bits": f"{summary.uncertainty_bits:.1f}",
            "complexity_level": summary.complexity_level,
            "mutual_exclusivity_patterns": str(summary.mutual_exclusivity_patterns),
            "conditional_dependency_patterns": str(summary.conditional_dependency_patterns),
            "correlated_variance_patterns": str(summary.correlated_variance_patterns),
            "temporal_correlation_patterns": str(summary.temporal_correlation_patterns),
            "temporal_drift_patterns": str(summary.temporal_drift_patterns),
            "interesting_columns_json": json.dumps(
                [
                    {
                        "column_name": col.column_name,
                        "classification": col.classification,
                        "source": col.source,
                        "reasons": col.reasons,
                        "metrics": col.metrics,
                    }
                    for col in summary.interesting_columns
                ],
                indent=2,
            ),
            "business_rules_json": json.dumps(
                [
                    {
                        "rule_type": rule.rule_type,
                        "columns": rule.columns,
                        "confidence": rule.confidence,
                        "description": rule.description,
                        "hypothesis": rule.hypothesis,
                    }
                    for rule in summary.business_rules
                ],
                indent=2,
            ),
            "data_quality_concerns_json": json.dumps(summary.data_quality_concerns),
            "current_recommendations_json": json.dumps(summary.recommendations),
        }

        # Render prompt with system/user split
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "dimensional_summary", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Define tool for structured output
        tool = ToolDefinition(
            name="summarize_dimensional_entropy",
            description="Provide structured dimensional entropy executive summary",
            input_schema=DimensionalSummaryOutput.model_json_schema(),
        )

        # Call LLM with tool use
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
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
                    output = DimensionalSummaryOutput.model_validate(response_data)
                    return Result.ok(output)
                except Exception:
                    return Result.fail(f"LLM did not use tool. Response: {response.content[:200]}")
            return Result.fail("LLM did not use the summarize_dimensional_entropy tool")

        # Parse tool response using Pydantic model
        tool_call = response.tool_calls[0]
        if tool_call.name != "summarize_dimensional_entropy":
            return Result.fail(f"Unexpected tool call: {tool_call.name}")

        try:
            output = DimensionalSummaryOutput.model_validate(tool_call.input)
        except Exception as e:
            return Result.fail(f"Failed to validate tool response: {e}")

        return Result.ok(output)
