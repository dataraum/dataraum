"""Cycle Induction Agent — cold-start business cycle discovery.

Generates business cycle vocabulary from semantic annotations and
relationships when no cycle vocabulary exists (cold start). The
induced vocabulary is written to _adhoc/cycles.yaml and used by the
BusinessCycleAgent for cycle detection on the same pipeline run.
"""

from __future__ import annotations

import json
from typing import Any

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)
from dataraum.storage import Column, Table

logger = get_logger(__name__)


# Output schema for the LLM tool call
_CYCLES_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["cycle_types"],
    "properties": {
        "cycle_types": {
            "type": "object",
            "description": "Mapping of cycle_name to cycle definition",
            "additionalProperties": {
                "type": "object",
                "required": [
                    "description",
                    "business_value",
                    "typical_stages",
                    "participating_entities",
                ],
                "properties": {
                    "description": {"type": "string"},
                    "business_value": {"type": "string", "enum": ["high", "medium", "low"]},
                    "aliases": {"type": "array", "items": {"type": "string"}},
                    "typical_stages": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name", "order", "indicators"],
                            "properties": {
                                "name": {"type": "string"},
                                "order": {"type": "integer"},
                                "indicators": {"type": "array", "items": {"type": "string"}},
                            },
                        },
                    },
                    "participating_entities": {"type": "array", "items": {"type": "string"}},
                    "completion_indicators": {"type": "array", "items": {"type": "string"}},
                    "feeds_into": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    },
}


class CycleInductionAgent(LLMFeature):
    """Discover business cycle vocabulary from data schema.

    Used on cold start to bootstrap cycle detection with a domain-appropriate
    vocabulary. The induced vocabulary provides stage names and indicators
    that the BusinessCycleAgent uses for detection.
    """

    def induce(
        self,
        session: Session,
        table_ids: list[str],
    ) -> Result[dict[str, Any]]:
        """Induce cycle vocabulary from semantic annotations and relationships.

        Args:
            session: SQLAlchemy session with semantic annotations.
            table_ids: Tables to analyze.

        Returns:
            Result containing cycles config dict (same shape as cycles.yaml).
        """
        # Build context from annotations and relationships
        tables_json, annotations_summary, relationships_summary = _build_induction_context(
            session, table_ids
        )

        if not tables_json:
            return Result.fail("No tables found for cycle induction")

        context = {
            "tables_json": json.dumps(tables_json, indent=2),
            "annotations_summary": annotations_summary,
            "relationships_summary": relationships_summary,
        }

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "cycle_induction", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render cycle_induction prompt: {e}")

        tool = ToolDefinition(
            name="induce_cycles",
            description="Propose business cycle vocabulary for the given data schema.",
            input_schema=_CYCLES_SCHEMA,
        )

        model = self.provider.get_model_for_tier("balanced")
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "induce_cycles"},
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        response_result = self.provider.converse(request)
        if not response_result.success or not response_result.value:
            return Result.fail(f"Cycle induction LLM call failed: {response_result.error}")

        response = response_result.value
        if not response.tool_calls:
            return Result.fail("LLM did not use the induce_cycles tool")

        cycles_config = response.tool_calls[0].input
        cycle_count = len(cycles_config.get("cycle_types", {}))
        logger.info("cycle_induction_complete", cycles=cycle_count)
        return Result.ok(cycles_config)


def save_cycles_config(vertical: str, config: dict[str, Any]) -> None:
    """Write cycles config to the vertical's cycles.yaml."""
    from dataraum.core.vertical import VerticalConfig

    cycles_path = VerticalConfig(vertical).cycles_path
    with open(cycles_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def _build_induction_context(
    session: Session,
    table_ids: list[str],
) -> tuple[list[dict[str, Any]], str, str]:
    """Build context for cycle/validation induction from annotations and relationships.

    Returns:
        Tuple of (tables_json, annotations_summary, relationships_summary)
    """
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import SemanticAnnotation

    # Load tables with annotations
    tables_stmt = (
        select(Table)
        .where(Table.table_id.in_(table_ids))
        .options(selectinload(Table.columns).selectinload(Column.semantic_annotation))
    )
    tables = session.execute(tables_stmt).scalars().all()

    # Build tables JSON (lightweight — names, roles, concepts)
    tables_json: list[dict[str, Any]] = []
    annotation_lines: list[str] = []

    for table in tables:
        cols = []
        for col in table.columns:
            col_info: dict[str, Any] = {"column_name": col.column_name}
            ann: SemanticAnnotation | None = col.semantic_annotation
            if ann:
                if ann.semantic_role:
                    col_info["role"] = ann.semantic_role
                if ann.business_concept:
                    col_info["concept"] = ann.business_concept
                if ann.entity_type:
                    col_info["entity_type"] = ann.entity_type
                if ann.temporal_behavior:
                    col_info["temporal_behavior"] = ann.temporal_behavior
                annotation_lines.append(
                    f"  {table.table_name}.{col.column_name}: "
                    f"role={ann.semantic_role or '?'}, "
                    f"concept={ann.business_concept or '?'}, "
                    f"entity={ann.entity_type or '?'}"
                )
            cols.append(col_info)
        tables_json.append(
            {
                "table_name": table.table_name,
                "row_count": table.row_count,
                "columns": cols,
            }
        )

    annotations_summary = (
        "\n".join(annotation_lines) if annotation_lines else "No annotations available."
    )

    # Load LLM-confirmed relationships (not raw statistical candidates)
    rel_stmt = (
        select(Relationship)
        .options(
            selectinload(Relationship.from_column).selectinload(Column.table),
            selectinload(Relationship.to_column).selectinload(Column.table),
        )
        .where(
            Relationship.from_table_id.in_(table_ids),
            Relationship.detection_method != "candidate",
        )
    )
    relationships = session.execute(rel_stmt).scalars().all()

    rel_lines: list[str] = []
    for rel in relationships:
        from_col = rel.from_column
        to_col = rel.to_column
        if from_col and to_col and from_col.table and to_col.table:
            rel_lines.append(
                f"  {from_col.table.table_name}.{from_col.column_name} → "
                f"{to_col.table.table_name}.{to_col.column_name} "
                f"({rel.relationship_type}, {rel.cardinality or '?'})"
            )

    relationships_summary = "\n".join(rel_lines) if rel_lines else "No relationships detected."

    return tables_json, annotations_summary, relationships_summary
