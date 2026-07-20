"""Column Annotation Agent — authoritative per-column LLM annotation.

Annotates columns with semantic roles, entity types, business terms, ontology
concept mappings, and unit sources. Post-DAT-362 this is the per-column phase's
authoritative agent, run on the capable (balanced) model — not a throwaway fast
pre-pass. Its output is persisted as ``SemanticAnnotation`` rows and later read
by ``semantic_per_table`` as read-only context.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.concept_store import load_workspace_concepts
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
)

if TYPE_CHECKING:
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


class ColumnAnnotationAgent(LLMFeature):
    """Authoritative per-column annotation agent (DAT-362 semantic_per_column).

    Annotates columns with semantic metadata on the configured model tier
    (balanced post-split). Does NOT handle relationships or table-level entity
    classification — that is ``semantic_per_table``'s job. Output is persisted
    as ``SemanticAnnotation`` rows.
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
        required_standard_fields: list[str] | None = None,
    ) -> Result[ColumnAnnotationOutput]:
        """Annotate columns with semantic metadata.

        Args:
            session: Database session
            table_ids: List of table IDs to annotate
            ontology: Ontology name for concept mapping
            profiles: Pre-loaded column profiles (avoids re-loading)
            required_standard_fields: Standard-field concepts required by active
                metric graphs. When provided, the prompt prioritizes mapping
                these concepts to actual dataset columns (DAT-362: this used to
                live in the tier-2 SemanticAgent; concept mapping is now owned by
                the per-column phase).

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

        # Concepts from the typed vocabulary table (DAT-728, config→DB); the
        # loader below is retained only as the prompt formatter.
        ontology_def = load_workspace_concepts(session, ontology)
        if not ontology_def.concepts:
            return Result.fail(f"Vertical '{ontology}' has no concepts to ground against.")

        context = {
            "tables_json": json.dumps(tables_json),
            "ontology_name": ontology,
            "ontology_concepts": self._ontology_loader.format_concepts_for_prompt(ontology_def),
            "required_standard_fields": self._format_required_fields(required_standard_fields),
        }

        # Render prompt
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "column_annotation", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render column_annotation prompt: {e}")

        # Call LLM — structured output (DAT-807): the API constrains decoding to
        # the schema, so the answer is JSON message content, not tool arguments.
        model = self.provider.get_model_for_tier(feature_config.model_tier)
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            output_schema=ColumnAnnotationOutput.model_json_schema(),
            label="column_annotation",
            effort=feature_config.effort,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        # converse raises a typed ProviderError on an API failure (DAT-503) —
        # transient/permanent retryability rides the exception to the worker's
        # durable boundary, so we don't re-wrap it as a Result here. A returned
        # Result is always a success.
        response = self.provider.converse(request).unwrap()

        try:
            output = ColumnAnnotationOutput.model_validate_json(response.content)
        except ValidationError as e:
            return Result.fail(f"Failed to parse column annotation output: {e}")

        logger.debug(
            "column_annotation_complete",
            tables=len(output.tables),
            columns=sum(len(t.columns) for t in output.tables),
            model=response.model,
        )
        return Result.ok(output)

    @staticmethod
    def _format_required_fields(fields: list[str] | None) -> str:
        """Format required standard fields for the prompt."""
        if not fields:
            return "No specific standard fields required by metrics."
        lines = ["The following standard_field concepts are used by active metrics:"]
        lines.extend(f"  - {f}" for f in fields)
        lines.append("")
        # DAT-769: vocabulary context only — concept binding was retired with the
        # catalogue-grain meaning redesign; nothing asks the per-column agent to
        # map columns onto concept names.
        lines.append("Use these as vocabulary context when describing columns.")
        return "\n".join(lines)

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
