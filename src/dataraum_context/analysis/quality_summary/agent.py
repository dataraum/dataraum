"""Quality Summary Agent - LLM-powered quality summarization.

This agent analyzes aggregated quality metrics across slices
and generates human-readable quality summaries per column.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.quality_summary.models import (
    AggregatedColumnData,
    ColumnQualitySummary,
    QualityIssue,
    SliceComparison,
    SliceMetrics,
)
from dataraum_context.core.models.base import Result
from dataraum_context.llm.features._base import LLMFeature

if TYPE_CHECKING:
    from dataraum_context.llm.cache import LLMCache
    from dataraum_context.llm.config import LLMConfig
    from dataraum_context.llm.prompts import PromptRenderer
    from dataraum_context.llm.providers.base import LLMProvider


class QualitySummaryAgent(LLMFeature):
    """LLM-powered quality summary agent.

    Analyzes aggregated column data across slices to generate
    quality summaries with findings, issues, and recommendations.
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        cache: LLMCache,
    ) -> None:
        """Initialize quality summary agent.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
            cache: Response cache
        """
        super().__init__(config, provider, prompt_renderer, cache)

    async def summarize_column(
        self,
        session: AsyncSession,
        column_data: AggregatedColumnData,
    ) -> Result[ColumnQualitySummary]:
        """Generate quality summary for a column across slices.

        Args:
            session: Database session
            column_data: Aggregated data for the column

        Returns:
            Result containing ColumnQualitySummary
        """
        # Check if feature is enabled
        feature_config = self.config.features.quality_summary
        if not feature_config or not feature_config.enabled:
            return Result.fail("Quality summary is disabled in config")

        # Build context for prompt
        context = {
            "column_name": column_data.column_name,
            "source_table_name": column_data.source_table_name,
            "slice_column_name": column_data.slice_column_name,
            "resolved_type": column_data.resolved_type or "unknown",
            "total_slices": len(column_data.slice_data),
            "total_rows": column_data.total_rows,
            "slice_data_json": json.dumps(column_data.slice_data, indent=2),
            "semantic_role": column_data.semantic_role or "unknown",
            "business_name": column_data.business_name or column_data.column_name,
            "business_description": column_data.business_description or "",
            "benford_violations": column_data.benford_violation_count,
            "outlier_slices": column_data.outlier_slice_count,
        }

        # Render prompt
        try:
            prompt, temperature = self.renderer.render("quality_summary", context)
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Call LLM
        response_result = await self._call_llm(
            session=session,
            feature_name="quality_summary",
            prompt=prompt,
            temperature=temperature,
            model_tier=feature_config.model_tier,
        )

        if not response_result.success:
            return Result.fail(response_result.error or "LLM call failed")

        response = response_result.unwrap()

        # Parse response
        return self._parse_response(column_data, response.content)

    def _parse_response(
        self,
        column_data: AggregatedColumnData,
        response_content: str,
    ) -> Result[ColumnQualitySummary]:
        """Parse LLM response into ColumnQualitySummary.

        Args:
            column_data: Original column data
            response_content: Raw LLM response

        Returns:
            Result containing parsed ColumnQualitySummary
        """
        try:
            # Extract JSON from response
            json_match = re.search(r"\{[\s\S]*\}", response_content)
            if not json_match:
                return Result.fail("No JSON found in response")

            parsed = json.loads(json_match.group())

            # Build slice metrics from original data
            slice_metrics = []
            for sd in column_data.slice_data:
                slice_metrics.append(
                    SliceMetrics(
                        slice_name=sd.get("slice_name", ""),
                        slice_value=sd.get("slice_value", ""),
                        row_count=sd.get("row_count", 0),
                        null_count=sd.get("null_count"),
                        null_ratio=sd.get("null_ratio"),
                        distinct_count=sd.get("distinct_count"),
                        cardinality_ratio=sd.get("cardinality_ratio"),
                        min_value=sd.get("min_value"),
                        max_value=sd.get("max_value"),
                        mean_value=sd.get("mean_value"),
                        stddev=sd.get("stddev"),
                        benford_compliant=sd.get("benford_compliant"),
                        benford_p_value=sd.get("benford_p_value"),
                        has_outliers=sd.get("has_outliers"),
                        outlier_ratio=sd.get("outlier_ratio"),
                    )
                )

            # Parse quality issues
            quality_issues = []
            for issue in parsed.get("quality_issues", []):
                quality_issues.append(
                    QualityIssue(
                        issue_type=issue.get("issue_type", "unknown"),
                        severity=issue.get("severity", "medium"),
                        description=issue.get("description", ""),
                        affected_slices=issue.get("affected_slices", []),
                        sample_values=issue.get("sample_values"),
                        investigation_sql=issue.get("investigation_sql"),
                    )
                )

            # Parse slice comparisons
            slice_comparisons = []
            for comp in parsed.get("slice_comparisons", []):
                slice_comparisons.append(
                    SliceComparison(
                        metric_name=comp.get("metric_name", ""),
                        description=comp.get("description", ""),
                        min_value=comp.get("min_value"),
                        max_value=comp.get("max_value"),
                        mean_value=comp.get("mean_value"),
                        variance=comp.get("variance"),
                        outlier_slices=comp.get("outlier_slices", []),
                        notes=comp.get("notes"),
                    )
                )

            summary = ColumnQualitySummary(
                column_name=column_data.column_name,
                source_table_name=column_data.source_table_name,
                slice_column_name=column_data.slice_column_name,
                total_slices=len(column_data.slice_data),
                overall_quality_score=parsed.get("overall_quality_score", 0.5),
                quality_grade=parsed.get("quality_grade", "C"),
                summary=parsed.get("summary", "No summary available"),
                key_findings=parsed.get("key_findings", []),
                quality_issues=quality_issues,
                slice_comparisons=slice_comparisons,
                recommendations=parsed.get("recommendations", []),
                investigation_views=parsed.get("investigation_views", []),
                slice_metrics=slice_metrics,
            )

            return Result.ok(summary)

        except json.JSONDecodeError as e:
            return Result.fail(f"Failed to parse JSON: {e}")
        except Exception as e:
            return Result.fail(f"Failed to parse response: {e}")


__all__ = ["QualitySummaryAgent"]
