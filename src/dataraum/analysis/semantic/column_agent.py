"""Column Annotation Agent - Fast tier 1 LLM annotation.

Annotates columns with semantic roles, entity types, business terms,
and ontology concept mappings. Uses a fast/cheap model (e.g. Haiku)
since this is pattern recognition work that doesn't need reasoning.

The output feeds into the tier 2 SemanticAgent as additional context,
allowing the capable model to focus on relationships and table analysis.
"""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
)
from dataraum.analysis.semantic.ontology import OntologyLoader
from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.core.logging import get_logger
from dataraum.core.models.base import (
    Result,
)
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.privacy import DataSampler
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


class ColumnAnnotationAgent(LLMFeature):
    """Fast column annotation agent (tier 1).

    Annotates columns with semantic metadata using a fast model.
    Does NOT handle relationships or table-level entity classification.

    Output is used as input context for the tier 2 SemanticAgent.
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        verticals_dir: Path | None = None,
    ) -> None:
        super().__init__(config, provider, prompt_renderer)
        self._ontology_loader = OntologyLoader(verticals_dir)

    def annotate(
        self,
        session: Session,
        table_ids: list[str],
        ontology: str = "general",
        profiles: list[ColumnProfile] | None = None,
    ) -> Result[ColumnAnnotationOutput]:
        """Annotate columns with semantic metadata.

        Args:
            session: Database session
            table_ids: List of table IDs to annotate
            ontology: Ontology name for concept mapping
            profiles: Pre-loaded column profiles (avoids re-loading)

        Returns:
            Result containing ColumnAnnotationOutput
        """
        feature_config = self.config.features.column_annotation
        if not feature_config or not feature_config.enabled:
            return Result.fail("Column annotation is disabled in config")

        # Load profiles if not provided
        if profiles is None:
            from dataraum.analysis.semantic.agent import SemanticAgent

            temp_agent = SemanticAgent.__new__(SemanticAgent)
            profiles_result = SemanticAgent._load_profiles(temp_agent, session, table_ids)
            if not profiles_result.success or not profiles_result.value:
                return Result.fail(
                    profiles_result.error if profiles_result.error else "Failed to load profiles"
                )
            profiles = profiles_result.value

        # Prepare samples
        sampler = DataSampler(self.config.privacy)
        samples = sampler.prepare_samples(profiles)

        # Build tables JSON (reuse SemanticAgent's method)
        tables_json = self._build_tables_json(profiles, samples)

        # Load ontology
        ontology_def = self._ontology_loader.load(ontology)
        if ontology_def is None:
            available = self._ontology_loader.list_verticals()
            return Result.fail(f"Vertical '{ontology}' not found. Available: {available}")

        context = {
            "tables_json": json.dumps(tables_json),
            "ontology_name": ontology,
            "ontology_concepts": self._ontology_loader.format_concepts_for_prompt(ontology_def),
        }

        # Render prompt
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "column_annotation", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render column_annotation prompt: {e}")

        # Create tool definition
        schema = ColumnAnnotationOutput.model_json_schema()
        tool = ToolDefinition(
            name="annotate_columns",
            description=(
                "Provide semantic annotations for each column in the database schema. "
                "Classify columns by role, entity type, and business concept."
            ),
            input_schema=schema,
        )

        # Call LLM
        model = self.provider.get_model_for_tier(feature_config.model_tier)
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "annotate_columns"},
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        response_result = self.provider.converse(request)
        if not response_result.success or not response_result.value:
            return Result.fail(response_result.error or "Column annotation LLM call failed")

        response = response_result.value

        # Extract tool output
        if not response.tool_calls:
            if response.content:
                try:
                    parsed = json.loads(response.content)
                    output = ColumnAnnotationOutput.model_validate(parsed)
                    return Result.ok(output)
                except (json.JSONDecodeError, Exception):
                    pass
            return Result.fail("LLM did not use the annotate_columns tool")

        tool_call = response.tool_calls[0]
        if tool_call.name != "annotate_columns":
            return Result.fail(f"Unexpected tool call: {tool_call.name}")

        try:
            output = ColumnAnnotationOutput.model_validate(tool_call.input)
            logger.debug(
                "column_annotation_complete",
                tables=len(output.tables),
                columns=sum(len(t.columns) for t in output.tables),
                model=response.model,
            )
            return Result.ok(output)
        except Exception as e:
            return Result.fail(f"Failed to parse column annotation output: {e}")

    @staticmethod
    def _truncate_sample(value: Any, max_length: int = 100) -> Any:
        if isinstance(value, str) and len(value) > max_length:
            return value[:max_length] + "..."
        return value

    def _build_tables_json(
        self, profiles: list[ColumnProfile], samples: dict[tuple[str, str], list[Any]]
    ) -> list[dict[str, Any]]:
        """Build JSON representation of tables for prompt."""
        tables_data: dict[str, dict[str, Any]] = {}

        for profile in profiles:
            table_name = profile.column_ref.table_name
            column_name = profile.column_ref.column_name

            if table_name not in tables_data:
                tables_data[table_name] = {
                    "table_name": table_name,
                    "row_count": profile.total_count,
                    "columns": [],
                }

            col_data: dict[str, Any] = {
                "column_name": column_name,
                "distinct_count": profile.distinct_count,
                "cardinality_ratio": round(profile.cardinality_ratio, 4),
                "sample_values": [
                    self._truncate_sample(v) for v in samples.get((table_name, column_name), [])
                ],
            }

            # Include original column name when it differs from normalized name
            if profile.original_name and profile.original_name != column_name:
                col_data["original_name"] = profile.original_name

            null_ratio = round(profile.null_ratio, 4)
            if null_ratio > 0.0:
                col_data["null_ratio"] = null_ratio

            if profile.numeric_stats:
                col_data["min"] = profile.numeric_stats.min_value
                col_data["max"] = profile.numeric_stats.max_value
                col_data["mean"] = round(profile.numeric_stats.mean, 4)

            if profile.string_stats:
                col_data["avg_length"] = round(profile.string_stats.avg_length, 1)

            tables_data[table_name]["columns"].append(col_data)

        return list(tables_data.values())
