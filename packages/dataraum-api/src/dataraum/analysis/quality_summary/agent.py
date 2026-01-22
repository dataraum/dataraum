"""Quality Summary Agent - LLM-powered quality summarization.

This agent analyzes aggregated quality metrics across slices
and generates human-readable quality summaries per column.

Supports both single-column and batch processing modes.
Uses tool-based output for structured responses.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from dataraum.analysis.quality_summary.models import (
    AggregatedColumnData,
    ColumnQualitySummary,
    QualityIssue,
    QualitySummaryBatchOutput,
    QualitySummaryOutput,
    SliceComparison,
    SliceMetrics,
)
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature

if TYPE_CHECKING:
    from dataraum.llm.cache import LLMCache
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

# Maximum columns per batch (to avoid token limits)
MAX_BATCH_SIZE = 10


class QualitySummaryAgent(LLMFeature):
    """LLM-powered quality summary agent.

    Analyzes aggregated column data across slices to generate
    quality summaries with findings, issues, and recommendations.
    Uses tool-based output for structured responses.
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

    def summarize_column(
        self,
        session: Session,
        column_data: AggregatedColumnData,
    ) -> Result[ColumnQualitySummary]:
        """Generate quality summary for a column across slices.

        Args:
            session: Database session
            column_data: Aggregated data for the column

        Returns:
            Result containing ColumnQualitySummary
        """
        from dataraum.llm.providers.base import (
            ConversationRequest,
            Message,
            ToolDefinition,
        )

        # Check if feature is enabled
        feature_config = self.config.features.quality_summary
        if not feature_config or not feature_config.enabled:
            return Result.fail("Quality summary is disabled in config")

        # Build context for prompt
        # Note: Optional fields like incomplete_periods, volume_anomalies, etc.
        # have defaults defined in the prompt YAML and are handled by the renderer
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

        # Render prompt with system/user split
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "quality_summary", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Define tool for structured output
        tool = ToolDefinition(
            name="summarize_quality",
            description="Provide structured quality summary for the column",
            input_schema=QualitySummaryOutput.model_json_schema(),
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
                    output = QualitySummaryOutput.model_validate(response_data)
                except Exception:
                    return Result.fail(f"LLM did not use tool. Response: {response.content[:200]}")
            else:
                return Result.fail("LLM did not use the summarize_quality tool")
        else:
            # Parse tool response using Pydantic model
            tool_call = response.tool_calls[0]
            if tool_call.name != "summarize_quality":
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

            try:
                output = QualitySummaryOutput.model_validate(tool_call.input)
            except Exception as e:
                return Result.fail(f"Failed to validate tool response: {e}")

        # Convert Pydantic output to ColumnQualitySummary
        return self._convert_output_to_summary(column_data, output)

    def summarize_columns_batch(
        self,
        session: Session,
        columns_data: list[AggregatedColumnData],
        source_table_name: str,
        slice_column_name: str,
        total_slices: int,
    ) -> Result[list[ColumnQualitySummary]]:
        """Generate quality summaries for multiple columns in one LLM call.

        Args:
            session: Database session
            columns_data: List of aggregated column data
            source_table_name: Name of the source table
            slice_column_name: Name of the slice column
            total_slices: Number of slices

        Returns:
            Result containing list of ColumnQualitySummary
        """
        from dataraum.llm.providers.base import (
            ConversationRequest,
            Message,
            ToolDefinition,
        )

        if not columns_data:
            return Result.ok([])

        # Check if feature is enabled
        feature_config = self.config.features.quality_summary
        if not feature_config or not feature_config.enabled:
            return Result.fail("Quality summary is disabled in config")

        # Build compact column data for prompt
        columns_for_prompt = []
        for col_data in columns_data:
            col_info = {
                "column_name": col_data.column_name,
                "data_type": col_data.resolved_type or "unknown",
                "total_rows": col_data.total_rows,
                "semantic_role": col_data.semantic_role or "unknown",
                "business_name": col_data.business_name or col_data.column_name,
                "benford_violations": col_data.benford_violation_count,
                "outlier_slices": col_data.outlier_slice_count,
                "slice_stats": [
                    {
                        "slice": sd.get("slice_value", ""),
                        "rows": sd.get("row_count", 0),
                        "null_ratio": sd.get("null_ratio"),
                        "distinct": sd.get("distinct_count"),
                        "benford_ok": sd.get("benford_compliant"),
                        "has_outliers": sd.get("has_outliers"),
                    }
                    for sd in col_data.slice_data
                ],
            }
            columns_for_prompt.append(col_info)

        # Build context for prompt
        context = {
            "source_table_name": source_table_name,
            "slice_column_name": slice_column_name,
            "total_slices": total_slices,
            "columns_json": json.dumps(columns_for_prompt, indent=2),
        }

        # Render batch prompt with system/user split
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "quality_summary_batch", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render batch prompt: {e}")

        # Define tool for structured output
        tool = ToolDefinition(
            name="summarize_quality_batch",
            description="Provide structured quality summaries for multiple columns",
            input_schema=QualitySummaryBatchOutput.model_json_schema(),
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
                    output = QualitySummaryBatchOutput.model_validate(response_data)
                except Exception:
                    return Result.fail(f"LLM did not use tool. Response: {response.content[:200]}")
            else:
                return Result.fail("LLM did not use the summarize_quality_batch tool")
        else:
            # Parse tool response using Pydantic model
            tool_call = response.tool_calls[0]
            if tool_call.name != "summarize_quality_batch":
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

            try:
                output = QualitySummaryBatchOutput.model_validate(tool_call.input)
            except Exception as e:
                return Result.fail(f"Failed to validate tool response: {e}")

        # Convert Pydantic output to list of ColumnQualitySummary
        return self._convert_batch_output_to_summaries(columns_data, output)

    def _convert_batch_output_to_summaries(
        self,
        columns_data: list[AggregatedColumnData],
        output: QualitySummaryBatchOutput,
    ) -> Result[list[ColumnQualitySummary]]:
        """Convert batch Pydantic tool output to list of ColumnQualitySummary.

        Args:
            columns_data: Original column data list
            output: Validated Pydantic output from LLM

        Returns:
            Result containing list of ColumnQualitySummary
        """
        # Build lookup for original data
        col_data_map = {cd.column_name: cd for cd in columns_data}

        summaries = []
        for col_output in output.columns:
            col_name = col_output.column_name
            col_data = col_data_map.get(col_name)

            if not col_data:
                continue

            # Build slice metrics from original data
            slice_metrics = self._build_slice_metrics(col_data)

            # Convert quality issues from Pydantic output
            quality_issues = [
                QualityIssue(
                    issue_type=issue.issue_type,
                    severity=issue.severity,
                    description=issue.description,
                    affected_slices=issue.affected_slices,
                    investigation_sql=issue.investigation_sql,
                )
                for issue in col_output.quality_issues
            ]

            # Convert slice comparisons from Pydantic output
            slice_comparisons = [
                SliceComparison(
                    metric_name=comp.metric_name,
                    description=comp.description,
                    min_value=comp.min_value,
                    max_value=comp.max_value,
                    outlier_slices=comp.outlier_slices,
                )
                for comp in col_output.slice_comparisons
            ]

            summary = ColumnQualitySummary(
                column_name=col_name,
                source_table_name=col_data.source_table_name,
                slice_column_name=col_data.slice_column_name,
                total_slices=len(col_data.slice_data),
                overall_quality_score=col_output.overall_quality_score,
                quality_grade=col_output.quality_grade,
                summary=col_output.summary,
                key_findings=col_output.key_findings,
                quality_issues=quality_issues,
                slice_comparisons=slice_comparisons,
                recommendations=col_output.recommendations,
                investigation_views=[],  # Skip in batch mode
                slice_metrics=slice_metrics,
            )
            summaries.append(summary)

        return Result.ok(summaries)

    def _build_slice_metrics(self, column_data: AggregatedColumnData) -> list[SliceMetrics]:
        """Build slice metrics from column data."""
        return [
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
            for sd in column_data.slice_data
        ]

    def _convert_output_to_summary(
        self,
        column_data: AggregatedColumnData,
        output: QualitySummaryOutput,
    ) -> Result[ColumnQualitySummary]:
        """Convert Pydantic tool output to ColumnQualitySummary.

        Args:
            column_data: Original column data
            output: Validated Pydantic output from LLM

        Returns:
            Result containing ColumnQualitySummary
        """
        # Build slice metrics from original data
        slice_metrics = self._build_slice_metrics(column_data)

        # Convert quality issues from Pydantic output
        quality_issues = [
            QualityIssue(
                issue_type=issue.issue_type,
                severity=issue.severity,
                description=issue.description,
                affected_slices=issue.affected_slices,
                investigation_sql=issue.investigation_sql,
            )
            for issue in output.quality_issues
        ]

        # Convert slice comparisons from Pydantic output
        slice_comparisons = [
            SliceComparison(
                metric_name=comp.metric_name,
                description=comp.description,
                min_value=comp.min_value,
                max_value=comp.max_value,
                outlier_slices=comp.outlier_slices,
            )
            for comp in output.slice_comparisons
        ]

        # Convert investigation views from Pydantic output
        investigation_views = [
            {
                "name": view.name,
                "description": view.description,
                "sql": view.sql,
            }
            for view in output.investigation_views
        ]

        summary = ColumnQualitySummary(
            column_name=column_data.column_name,
            source_table_name=column_data.source_table_name,
            slice_column_name=column_data.slice_column_name,
            total_slices=len(column_data.slice_data),
            overall_quality_score=output.overall_quality_score,
            quality_grade=output.quality_grade,
            summary=output.summary,
            key_findings=output.key_findings,
            quality_issues=quality_issues,
            slice_comparisons=slice_comparisons,
            recommendations=output.recommendations,
            investigation_views=investigation_views,
            slice_metrics=slice_metrics,
        )

        return Result.ok(summary)


__all__ = ["QualitySummaryAgent"]
