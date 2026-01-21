"""Slicing Agent - LLM-powered analysis to identify optimal data slices.

This agent analyzes table statistics, semantic annotations, and correlations
to recommend the best categorical dimensions for slicing data into subsets.
Uses tool-based output for structured responses.
"""

import json
import re
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from dataraum_context.analysis.slicing.models import (
    SliceRecommendation,
    SliceSQL,
    SlicingAnalysisOutput,
    SlicingAnalysisResult,
)
from dataraum_context.core.models.base import DecisionSource, Result
from dataraum_context.llm.features._base import LLMFeature

if TYPE_CHECKING:
    from dataraum_context.llm.cache import LLMCache
    from dataraum_context.llm.config import LLMConfig
    from dataraum_context.llm.prompts import PromptRenderer
    from dataraum_context.llm.providers.base import LLMProvider


class SlicingAgent(LLMFeature):
    """LLM-powered slicing analysis agent.

    Analyzes tables to identify the best categorical dimensions for
    creating data subsets (slices). Each unique value in a slice
    dimension creates a separate subset.

    Uses tool-based output for structured responses.

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
        cache: LLMCache,
    ) -> None:
        """Initialize slicing agent.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
            cache: Response cache
        """
        super().__init__(config, provider, prompt_renderer, cache)

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
        from dataraum_context.llm.providers.base import (
            ConversationRequest,
            Message,
            ToolDefinition,
        )

        # Check if feature is enabled
        feature_config = self.config.features.slicing_analysis
        if not feature_config or not feature_config.enabled:
            return Result.fail("Slicing analysis is disabled in config")

        # Build context for prompt
        context = {
            "tables_json": json.dumps(context_data.get("tables", []), indent=2),
            "statistics_json": json.dumps(context_data.get("statistics", []), indent=2),
            "semantic_json": json.dumps(context_data.get("semantic", []), indent=2),
            "correlations_json": json.dumps(context_data.get("correlations", []), indent=2),
            "quality_json": json.dumps(context_data.get("quality", []), indent=2),
        }

        # Render prompt with system/user split
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "slicing_analysis", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Define tool for structured output
        tool = ToolDefinition(
            name="analyze_slicing",
            description="Provide structured slicing dimension recommendations",
            input_schema=SlicingAnalysisOutput.model_json_schema(),
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
                    output = SlicingAnalysisOutput.model_validate(response_data)
                except Exception:
                    return Result.fail(f"LLM did not use tool. Response: {response.content[:200]}")
            else:
                return Result.fail("LLM did not use the analyze_slicing tool")
        else:
            # Parse tool response using Pydantic model
            tool_call = response.tool_calls[0]
            if tool_call.name != "analyze_slicing":
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

            try:
                output = SlicingAnalysisOutput.model_validate(tool_call.input)
            except Exception as e:
                return Result.fail(f"Failed to validate tool response: {e}")

        # Convert Pydantic output to SlicingAnalysisResult
        return self._convert_output_to_result(output, context_data)

    def _convert_output_to_result(
        self,
        output: SlicingAnalysisOutput,
        context_data: dict[str, Any],
    ) -> Result[SlicingAnalysisResult]:
        """Convert Pydantic tool output to SlicingAnalysisResult.

        Args:
            output: Validated Pydantic output from LLM
            context_data: Original context data for lookups

        Returns:
            Result containing SlicingAnalysisResult
        """
        recommendations: list[SliceRecommendation] = []
        slice_queries: list[SliceSQL] = []

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

            # Get distinct values from output or statistics
            distinct_values = rec.distinct_values
            if not distinct_values:
                # Try to get from statistics
                for stat in context_data.get("statistics", []):
                    if (
                        stat.get("table_name") == table_name
                        and stat.get("column_name") == column_name
                    ):
                        top_values = stat.get("top_values", [])
                        distinct_values = [v.get("value", "") for v in top_values]
                        break

            # Build SQL template
            duckdb_table = table_info.get("duckdb_path", f"typed_{table_name}")
            sql_template = self._build_sql_template(duckdb_table, column_name, distinct_values)

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
                sql_template=sql_template,
            )
            recommendations.append(recommendation)

            # Generate individual slice SQL queries
            for value in distinct_values:
                safe_value = self._sanitize_for_table_name(str(value))
                safe_column = self._sanitize_for_table_name(column_name)
                slice_table_name = f"slice_{safe_column}_{safe_value}"

                sql_query = self._build_slice_sql(
                    duckdb_table, column_name, value, slice_table_name
                )

                slice_queries.append(
                    SliceSQL(
                        slice_name=f"{column_name}={value}",
                        slice_value=str(value),
                        table_name=slice_table_name,
                        sql_query=sql_query,
                    )
                )

        result = SlicingAnalysisResult(
            recommendations=recommendations,
            slice_queries=slice_queries,
            source=DecisionSource.LLM,
            tables_analyzed=len(table_map),
            columns_considered=len(column_map),
        )

        return Result.ok(result)

    def _build_sql_template(
        self,
        table_name: str,
        column_name: str,
        distinct_values: list[str],
    ) -> str:
        """Build SQL template for creating all slices.

        Args:
            table_name: Source table name
            column_name: Column to slice on
            distinct_values: List of values to create slices for

        Returns:
            SQL template string
        """
        lines = [f"-- Slicing on {column_name} ({len(distinct_values)} slices)"]
        lines.append(f"-- Source table: {table_name}")
        lines.append("")

        for value in distinct_values:
            safe_value = self._sanitize_for_table_name(str(value))
            safe_column = self._sanitize_for_table_name(column_name)
            slice_table = f"slice_{safe_column}_{safe_value}"

            # Use proper quoting for column names with spaces
            quoted_column = f'"{column_name}"'

            lines.append(f"-- Slice: {column_name} = '{value}'")
            lines.append(f"CREATE TABLE {slice_table} AS")
            lines.append(f"SELECT * FROM {table_name}")
            lines.append(f"WHERE {quoted_column} = '{value}';")
            lines.append("")

        return "\n".join(lines)

    def _build_slice_sql(
        self,
        source_table: str,
        column_name: str,
        value: str,
        target_table: str,
    ) -> str:
        """Build SQL for a single slice.

        Args:
            source_table: Source table name
            column_name: Column to filter on
            value: Value to filter for
            target_table: Name for the slice table

        Returns:
            DuckDB SQL query
        """
        # Quote column name to handle spaces and special chars
        quoted_column = f'"{column_name}"'

        # Escape single quotes in value
        escaped_value = str(value).replace("'", "''")

        return f"""CREATE TABLE {target_table} AS
SELECT * FROM {source_table}
WHERE {quoted_column} = '{escaped_value}';"""

    def _sanitize_for_table_name(self, value: str) -> str:
        """Sanitize a value for use in a table name.

        Args:
            value: Raw value

        Returns:
            Sanitized string safe for table names
        """
        # Replace spaces and special chars with underscores
        sanitized = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
        # Remove consecutive underscores
        sanitized = re.sub(r"_+", "_", sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip("_")
        # Lowercase
        sanitized = sanitized.lower()
        # Ensure not empty
        if not sanitized:
            sanitized = "unknown"
        return sanitized


__all__ = ["SlicingAgent"]
