"""Validation Induction Agent — cold-start validation proposal.

Proposes data validation checks from semantic annotations, relationships,
and data profiles when no validation specs exist (cold start). The
induced specs are written to _adhoc/validations/ and executed by the
ValidationAgent on the same pipeline run.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)

logger = get_logger(__name__)


# Output schema for the LLM tool call
_VALIDATIONS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["validations"],
    "properties": {
        "validations": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "validation_id",
                    "name",
                    "description",
                    "category",
                    "severity",
                    "check_type",
                    "sql_hints",
                    "expected_outcome",
                ],
                "properties": {
                    "validation_id": {"type": "string"},
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "category": {
                        "type": "string",
                        "enum": [
                            "financial",
                            "data_quality",
                            "business_rule",
                            "referential_integrity",
                        ],
                    },
                    "severity": {
                        "type": "string",
                        "enum": ["critical", "error", "warning", "info"],
                    },
                    "check_type": {
                        "type": "string",
                        "enum": ["balance", "comparison", "constraint", "aggregate", "referential"],
                    },
                    "parameters": {"type": "object"},
                    "sql_hints": {"type": "string"},
                    "expected_outcome": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "relevant_cycles": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    },
}


class ValidationInductionAgent(LLMFeature):
    """Propose validation checks from data schema and annotations.

    Used on cold start to bootstrap validation with domain-appropriate
    checks. The induced specs are SQL-based validation rules that the
    ValidationAgent can execute.
    """

    def induce(
        self,
        session: Session,
        table_ids: list[str],
    ) -> Result[list[dict[str, Any]]]:
        """Induce validation specs from semantic annotations and relationships.

        Args:
            session: SQLAlchemy session with semantic annotations.
            table_ids: Tables to analyze.

        Returns:
            Result containing list of validation spec dicts.
        """
        # Reuse the shared context builder from cycles.induction
        from dataraum.analysis.cycles.induction import _build_induction_context

        tables_json, annotations_summary, relationships_summary = _build_induction_context(
            session, table_ids
        )

        if not tables_json:
            return Result.fail("No tables found for validation induction")

        context = {
            "tables_json": json.dumps(tables_json, indent=2),
            "annotations_summary": annotations_summary,
            "relationships_summary": relationships_summary,
        }

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "validation_induction", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render validation_induction prompt: {e}")

        tool = ToolDefinition(
            name="induce_validations",
            description="Propose validation checks for the given data schema.",
            input_schema=_VALIDATIONS_SCHEMA,
        )

        model = self.provider.get_model_for_tier("balanced")
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "induce_validations"},
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        response_result = self.provider.converse(request)
        if not response_result.success or not response_result.value:
            return Result.fail(f"Validation induction LLM call failed: {response_result.error}")

        response = response_result.value
        if not response.tool_calls:
            return Result.fail("LLM did not use the induce_validations tool")

        output = response.tool_calls[0].input
        validations = output.get("validations", [])
        logger.info("validation_induction_complete", validations=len(validations))
        return Result.ok(validations)


def save_validation_specs(vertical: str, specs: list[dict[str, Any]]) -> None:
    """Write validation specs to the vertical's validations directory."""
    from dataraum.core.vertical import VerticalConfig

    validations_dir: Path = VerticalConfig(vertical).validations_dir
    validations_dir.mkdir(parents=True, exist_ok=True)
    for spec in specs:
        vid = spec.get("validation_id", "unknown")
        spec_path = validations_dir / f"{vid}.yaml"
        # Add version field expected by ValidationSpec model
        spec.setdefault("version", "1.0")
        with open(spec_path, "w") as f:
            yaml.dump(spec, f, default_flow_style=False, sort_keys=False)
