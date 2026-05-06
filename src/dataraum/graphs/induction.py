"""Metric Induction Agent — cold-start metric discovery.

Generates business metric definitions from semantic annotations and
relationships when no metric configs exist (cold start). The induced
metrics are written to _adhoc/metrics/ and executed by GraphExecutionPhase
on the same pipeline run.
"""

from __future__ import annotations

import json
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


# Output schema for the LLM tool call — mirrors the full metric YAML format
_METRICS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["metrics"],
    "properties": {
        "metrics": {
            "type": "array",
            "description": "List of metric definitions to create",
            "items": {
                "type": "object",
                "required": ["graph_id", "metadata", "output", "dependencies"],
                "properties": {
                    "graph_id": {
                        "type": "string",
                        "description": "Unique metric identifier (e.g. 'dso', 'gross_margin')",
                    },
                    "metadata": {
                        "type": "object",
                        "required": ["name", "description", "category"],
                        "properties": {
                            "name": {"type": "string"},
                            "description": {"type": "string"},
                            "category": {"type": "string"},
                            "tags": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                    "output": {
                        "type": "object",
                        "required": ["type", "metric_id", "unit"],
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["scalar", "series"],
                            },
                            "metric_id": {"type": "string"},
                            "unit": {"type": "string"},
                            "decimal_places": {"type": "integer"},
                        },
                    },
                    "parameters": {
                        "type": "object",
                        "description": "Named parameters with type, default, description",
                        "additionalProperties": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string"},
                                "default": {},
                                "description": {"type": "string"},
                                "options": {"type": "array"},
                            },
                        },
                    },
                    "dependencies": {
                        "type": "object",
                        "description": "Named steps: extract, constant, or formula",
                        "additionalProperties": {
                            "type": "object",
                            "required": ["level", "type"],
                            "properties": {
                                "level": {"type": "integer"},
                                "type": {
                                    "type": "string",
                                    "enum": ["extract", "constant", "formula"],
                                },
                                "source": {
                                    "type": "object",
                                    "properties": {
                                        "standard_field": {"type": "string"},
                                        "statement": {"type": "string"},
                                    },
                                },
                                "aggregation": {"type": "string"},
                                "parameter": {"type": "string"},
                                "default": {},
                                "expression": {"type": "string"},
                                "depends_on": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "output_step": {"type": "boolean"},
                                "validation": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "condition": {"type": "string"},
                                            "severity": {"type": "string"},
                                            "message": {"type": "string"},
                                        },
                                    },
                                },
                            },
                        },
                    },
                    "interpretation": {
                        "type": "object",
                        "properties": {
                            "ranges": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "min": {"type": "number"},
                                        "max": {"type": "number"},
                                        "label": {"type": "string"},
                                        "description": {"type": "string"},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    },
}


class MetricInductionAgent(LLMFeature):
    """Discover computable metrics from data schema.

    Used on cold start to bootstrap metric computation with
    domain-appropriate definitions. The induced metrics provide
    the graph agent with specifications for SQL generation.
    """

    def induce(
        self,
        session: Session,
        table_ids: list[str],
    ) -> Result[list[dict[str, Any]]]:
        """Induce metric definitions from semantic annotations and relationships.

        Args:
            session: SQLAlchemy session with semantic annotations.
            table_ids: Tables to analyze.

        Returns:
            Result containing list of metric config dicts (one per metric YAML file).
        """
        from dataraum.analysis.cycles.induction import _build_induction_context

        tables_json, annotations_summary, relationships_summary = _build_induction_context(
            session, table_ids
        )

        if not tables_json:
            return Result.fail("No tables found for metric induction")

        context = {
            "tables_json": json.dumps(tables_json, indent=2),
            "annotations_summary": annotations_summary,
            "relationships_summary": relationships_summary,
        }

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "metric_induction", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render metric_induction prompt: {e}")

        tool = ToolDefinition(
            name="induce_metrics",
            description="Propose business metrics computable from the given data schema.",
            input_schema=_METRICS_SCHEMA,
        )

        model = self.provider.get_model_for_tier("balanced")
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "induce_metrics"},
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        response_result = self.provider.converse(request)
        if not response_result.success or not response_result.value:
            return Result.fail(f"Metric induction LLM call failed: {response_result.error}")

        response = response_result.value
        if not response.tool_calls:
            return Result.fail("LLM did not use the induce_metrics tool")

        raw = response.tool_calls[0].input
        metrics = raw.get("metrics", [])
        if not isinstance(metrics, list):
            return Result.fail(f"Expected metrics list, got {type(metrics).__name__}")
        logger.info("metric_induction_complete", metrics=len(metrics))
        return Result.ok(metrics)


def save_metrics_config(vertical: str, metrics: list[dict[str, Any]]) -> None:
    """Write metric config files to the vertical's metrics directory.

    Each metric becomes its own YAML file at:
    {vertical}/metrics/{category}/{graph_id}.yaml
    """
    from dataraum.core.vertical import VerticalConfig

    metrics_dir = VerticalConfig(vertical).metrics_dir

    for metric in metrics:
        graph_id = metric.get("graph_id", "unknown")
        category = metric.get("metadata", {}).get("category", "general")

        # Create category subdirectory
        category_dir = metrics_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)

        # Build the YAML content — full format matching the loader's expectations
        yaml_content: dict[str, Any] = {
            "graph_id": graph_id,
            "version": "1.0",
        }

        if "metadata" in metric:
            meta = dict(metric["metadata"])
            meta["source"] = "induced"
            yaml_content["metadata"] = meta

        if "output" in metric:
            yaml_content["output"] = metric["output"]

        if "parameters" in metric:
            yaml_content["parameters"] = metric["parameters"]

        if "dependencies" in metric:
            yaml_content["dependencies"] = metric["dependencies"]

        if "interpretation" in metric:
            yaml_content["interpretation"] = metric["interpretation"]

        # Write
        file_path = category_dir / f"{graph_id}.yaml"
        with open(file_path, "w") as f:
            yaml.dump(yaml_content, f, default_flow_style=False, sort_keys=False)

        logger.info("metric_saved", graph_id=graph_id, path=str(file_path))
