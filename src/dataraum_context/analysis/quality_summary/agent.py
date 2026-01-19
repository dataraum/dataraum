"""Quality Summary Agent - LLM-powered quality summarization.

This agent analyzes aggregated quality metrics across slices
and generates human-readable quality summaries per column.

Supports both single-column and batch processing modes.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

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

# Maximum columns per batch (to avoid token limits)
MAX_BATCH_SIZE = 10


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
        response_result = self._call_llm(
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

        # Render batch prompt
        try:
            prompt, temperature = self.renderer.render("quality_summary_batch", context)
        except Exception as e:
            return Result.fail(f"Failed to render batch prompt: {e}")

        # Call LLM
        response_result = self._call_llm(
            session=session,
            feature_name="quality_summary",
            prompt=prompt,
            temperature=temperature,
            model_tier=feature_config.model_tier,
        )

        if not response_result.success:
            return Result.fail(response_result.error or "LLM call failed")

        response = response_result.unwrap()

        # Parse batch response
        return self._parse_batch_response(columns_data, response.content)

    def _parse_batch_response(
        self,
        columns_data: list[AggregatedColumnData],
        response_content: str,
    ) -> Result[list[ColumnQualitySummary]]:
        """Parse batch LLM response into list of ColumnQualitySummary.

        Args:
            columns_data: Original column data list
            response_content: Raw LLM response

        Returns:
            Result containing list of parsed ColumnQualitySummary
        """
        try:
            # Extract JSON from response
            json_match = re.search(r"\{[\s\S]*\}", response_content)
            if not json_match:
                return Result.fail("No JSON found in batch response")

            parsed = json.loads(json_match.group())
            columns_response = parsed.get("columns", [])

            # Build lookup for original data
            col_data_map = {cd.column_name: cd for cd in columns_data}

            summaries = []
            for col_resp in columns_response:
                col_name = col_resp.get("column_name", "")
                col_data = col_data_map.get(col_name)

                if not col_data:
                    continue

                # Build slice metrics from original data
                slice_metrics = self._build_slice_metrics(col_data)

                # Parse quality issues
                quality_issues = [
                    QualityIssue(
                        issue_type=issue.get("issue_type", "unknown"),
                        severity=issue.get("severity", "medium"),
                        description=issue.get("description", ""),
                        affected_slices=issue.get("affected_slices", []),
                    )
                    for issue in col_resp.get("quality_issues", [])
                ]

                # Parse slice comparisons
                slice_comparisons = [
                    SliceComparison(
                        metric_name=comp.get("metric_name", ""),
                        description=comp.get("description", ""),
                        min_value=comp.get("min_value"),
                        max_value=comp.get("max_value"),
                        outlier_slices=comp.get("outlier_slices", []),
                    )
                    for comp in col_resp.get("slice_comparisons", [])
                ]

                summary = ColumnQualitySummary(
                    column_name=col_name,
                    source_table_name=col_data.source_table_name,
                    slice_column_name=col_data.slice_column_name,
                    total_slices=len(col_data.slice_data),
                    overall_quality_score=col_resp.get("overall_quality_score", 0.5),
                    quality_grade=col_resp.get("quality_grade", "C"),
                    summary=col_resp.get("summary", "No summary available"),
                    key_findings=col_resp.get("key_findings", []),
                    quality_issues=quality_issues,
                    slice_comparisons=slice_comparisons,
                    recommendations=col_resp.get("recommendations", []),
                    investigation_views=[],  # Skip in batch mode
                    slice_metrics=slice_metrics,
                )
                summaries.append(summary)

            return Result.ok(summaries)

        except json.JSONDecodeError as e:
            return Result.fail(f"Failed to parse batch JSON: {e}")
        except Exception as e:
            return Result.fail(f"Failed to parse batch response: {e}")

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
