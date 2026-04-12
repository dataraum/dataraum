"""Evidence synthesis agent — translates entropy scores into explanations.

Assembles evidence from entropy objects, BBN network inference, ontology
concepts, and existing teachings, then calls an LLM to synthesize a
domain explanation with actionable teach suggestions.

Three target levels:
- column: why(target="table.column") — single column focus
- table:  why(target="table") — aggregated across columns
- dataset: why() — top entropy drivers across the dataset
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.views.network_context import (
    ColumnNetworkResult,
    EntropyForNetwork,
)
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)
from dataraum.mcp.teach import PARAM_MODELS, VALID_TEACH_TYPES
from dataraum.pipeline.fixes.models import DataFix

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Output models
# ---------------------------------------------------------------------------


class EvidenceItem(BaseModel):
    """A single piece of evidence from a detector."""

    detector: str = Field(description="Detector that produced this observation")
    dimension: str = Field(description="Dimension path (layer.dimension.sub_dimension)")
    score: float = Field(description="Entropy score (0=clean, 1=maximum uncertainty)")
    state: str = Field(default="", description="Discretized state: low/medium/high")
    impact_delta: float = Field(
        default=0.0, description="Causal impact: how much fixing this reduces P(high)"
    )
    detail: str = Field(default="", description="Human-readable observation summary")


class TeachSuggestion(BaseModel):
    """An executable teach call that could address an entropy issue."""

    teach_type: str = Field(description="One of the registered teach types")
    target: str | None = Field(default=None, description="Target (e.g. 'table.column')")
    params: dict[str, Any] = Field(description="Parameters passable to teach()")
    description: str = Field(description="What this teach would accomplish")
    expected_impact: str = Field(default="", description="Which dimensions should improve")
    valid: bool = Field(default=True, description="Whether params validated against schema")
    validation_warning: str | None = Field(
        default=None, description="Validation warning if params didn't match schema"
    )


class WhyResponse(BaseModel):
    """Structured response from the why agent."""

    target: str = Field(description="What was analyzed")
    readiness: str = Field(description="Overall readiness: ready/investigate/blocked")
    analysis: str = Field(description="Natural language explanation grounded in evidence")
    evidence: list[EvidenceItem] = Field(default_factory=list)
    resolution_options: list[TeachSuggestion] = Field(default_factory=list)
    intents: dict[str, Any] = Field(
        default_factory=dict, description="BBN intent readiness (query/aggregation/reporting)"
    )


# ---------------------------------------------------------------------------
# LLM output schema (for tool_choice)
# ---------------------------------------------------------------------------

_WHY_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["analysis", "evidence", "resolution_options"],
    "properties": {
        "analysis": {
            "type": "string",
            "description": "Natural language explanation of why entropy is elevated. "
            "Ground every claim in the evidence. No generic filler.",
        },
        "evidence": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["detector", "dimension", "score", "detail"],
                "properties": {
                    "detector": {"type": "string"},
                    "dimension": {"type": "string"},
                    "score": {"type": "number"},
                    "state": {"type": "string", "enum": ["low", "medium", "high"]},
                    "impact_delta": {"type": "number"},
                    "detail": {"type": "string"},
                },
            },
        },
        "resolution_options": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["teach_type", "params", "description"],
                "properties": {
                    "teach_type": {
                        "type": "string",
                        "enum": sorted(VALID_TEACH_TYPES),
                    },
                    "target": {"type": "string"},
                    "params": {"type": "object"},
                    "description": {"type": "string"},
                    "expected_impact": {"type": "string"},
                },
            },
        },
    },
}


# ---------------------------------------------------------------------------
# Evidence assembly — builds context dicts for the LLM prompt
# ---------------------------------------------------------------------------


def build_column_evidence(
    col_key: str,
    col_result: ColumnNetworkResult,
    session: Session,
    *,
    dimension_filter: str | None = None,
) -> dict[str, Any]:
    """Build evidence context for a single column.

    Args:
        col_key: Column target key (e.g. "column:orders.amount").
        col_result: Per-column BBN inference result.
        session: SQLAlchemy session for metadata queries.
        dimension_filter: Optional dimension prefix to filter evidence.

    Returns:
        Dict with column-level evidence suitable for prompt context.
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.storage import Column, Table

    # Parse table.column from target key
    col_ref = col_key.removeprefix("column:")
    table_name, _, column_name = col_ref.partition(".")

    # Network evidence (BBN nodes)
    nodes = []
    for ne in col_result.node_evidence:
        if dimension_filter and not ne.dimension_path.startswith(dimension_filter):
            continue
        node_info: dict[str, Any] = {
            "node": ne.node_name,
            "dimension_path": ne.dimension_path,
            "state": ne.state,
            "score": round(ne.score, 3),
            "impact_delta": round(ne.impact_delta, 3),
            "detector": ne.detector_id,
        }
        if ne.evidence:
            node_info["observations"] = ne.evidence
        nodes.append(node_info)

    # Sort by impact_delta descending (most impactful first)
    nodes.sort(key=lambda n: n["impact_delta"], reverse=True)

    # Intent readiness
    intents = {}
    for intent in col_result.intents:
        intents[intent.intent_name] = {
            "p_high": round(intent.p_high, 3),
            "readiness": intent.readiness,
            "posterior": {k: round(v, 3) for k, v in intent.posterior.items()},
        }

    # Semantic metadata
    semantic: dict[str, Any] = {}
    col_row = (
        session.execute(
            select(Column)
            .join(Table, Column.table_id == Table.table_id)
            .where(
                Table.table_name == table_name,
                Table.layer == "typed",
                Column.column_name == column_name,
            )
        )
        .scalars()
        .first()
    )
    if col_row:
        ann = session.execute(
            select(SemanticAnnotation).where(SemanticAnnotation.column_id == col_row.column_id)
        ).scalar_one_or_none()
        if ann:
            for field in (
                "semantic_role",
                "business_name",
                "business_concept",
                "entity_type",
                "temporal_behavior",
            ):
                val = getattr(ann, field, None)
                if val:
                    semantic[field] = val

    return {
        "target": col_ref,
        "table_name": table_name,
        "column_name": column_name,
        "readiness": col_result.readiness,
        "top_priority_node": col_result.top_priority_node,
        "top_priority_impact": round(col_result.top_priority_impact, 3),
        "nodes_observed": col_result.nodes_observed,
        "nodes_high": col_result.nodes_high,
        "intents": intents,
        "network_evidence": nodes,
        "semantic": semantic,
    }


def build_table_evidence(
    table_name: str,
    network_ctx: EntropyForNetwork,
    session: Session,
    *,
    dimension_filter: str | None = None,
) -> dict[str, Any]:
    """Build evidence context for a table (aggregated across columns).

    Args:
        table_name: Table name to filter columns.
        network_ctx: Full network context with all columns.
        session: SQLAlchemy session.
        dimension_filter: Optional dimension prefix to filter evidence.

    Returns:
        Dict with table-level evidence suitable for prompt context.
    """
    # Filter to columns belonging to this table
    table_columns: dict[str, ColumnNetworkResult] = {}
    for key, col in network_ctx.columns.items():
        col_ref = key.removeprefix("column:")
        tname, _, _ = col_ref.partition(".")
        if tname == table_name:
            table_columns[key] = col

    # Build per-column summaries
    column_summaries = []
    for col_key, col_result in table_columns.items():
        col_ref = col_key.removeprefix("column:")
        _, _, cname = col_ref.partition(".")

        # Top issues for this column
        top_nodes = []
        for ne in col_result.node_evidence:
            if ne.state == "low":
                continue
            if dimension_filter and not ne.dimension_path.startswith(dimension_filter):
                continue
            top_nodes.append(
                {
                    "node": ne.node_name,
                    "state": ne.state,
                    "impact_delta": round(ne.impact_delta, 3),
                }
            )
        top_nodes.sort(key=lambda n: n.get("impact_delta", 0), reverse=True)  # type: ignore[arg-type, return-value]

        column_summaries.append(
            {
                "column": cname,
                "readiness": col_result.readiness,
                "worst_intent_p_high": round(col_result.worst_intent_p_high, 3),
                "top_priority_node": col_result.top_priority_node,
                "top_issues": top_nodes[:5],
            }
        )

    # Sort by worst readiness
    rank: dict[str, int] = {"blocked": 2, "investigate": 1, "ready": 0}
    column_summaries.sort(key=lambda c: rank.get(str(c.get("readiness", "")), 0), reverse=True)  # type: ignore[return-value]

    # Aggregate readiness
    blocked = sum(1 for c in column_summaries if c["readiness"] == "blocked")
    investigate = sum(1 for c in column_summaries if c["readiness"] == "investigate")

    return {
        "target": table_name,
        "total_columns": len(column_summaries),
        "columns_blocked": blocked,
        "columns_investigate": investigate,
        "columns_ready": len(column_summaries) - blocked - investigate,
        "columns": column_summaries,
    }


def build_dataset_evidence(
    network_ctx: EntropyForNetwork,
    *,
    dimension_filter: str | None = None,
) -> dict[str, Any]:
    """Build evidence context for dataset-level analysis.

    Args:
        network_ctx: Full network context.
        dimension_filter: Optional dimension prefix to filter evidence.

    Returns:
        Dict with dataset-level evidence suitable for prompt context.
    """
    # Top entropy drivers — columns sorted by worst_intent_p_high
    at_risk = [(key, col) for key, col in network_ctx.columns.items() if col.readiness != "ready"]
    at_risk.sort(key=lambda x: x[1].worst_intent_p_high, reverse=True)

    top_drivers = []
    for col_key, col in at_risk[:15]:
        top_node = col.top_priority_node
        high_nodes = [
            {"node": ne.node_name, "state": ne.state, "impact_delta": round(ne.impact_delta, 3)}
            for ne in col.node_evidence
            if ne.state != "low"
            and (not dimension_filter or ne.dimension_path.startswith(dimension_filter))
        ]
        high_nodes.sort(key=lambda n: n.get("impact_delta", 0), reverse=True)  # type: ignore[arg-type, return-value]

        top_drivers.append(
            {
                "target": col_key.removeprefix("column:"),
                "readiness": col.readiness,
                "worst_p_high": round(col.worst_intent_p_high, 3),
                "top_priority_node": top_node,
                "high_nodes": high_nodes[:3],
            }
        )

    # Cross-column fix
    top_fix = None
    if network_ctx.top_fix:
        top_fix = {
            "node": network_ctx.top_fix.node_name,
            "dimension_path": network_ctx.top_fix.dimension_path,
            "columns_affected": network_ctx.top_fix.columns_affected,
            "total_delta": round(network_ctx.top_fix.total_intent_delta, 3),
            "example_columns": network_ctx.top_fix.example_columns,
        }

    # Aggregate intents
    intents = {}
    for agg in network_ctx.intents:
        intents[agg.intent_name] = {
            "worst_p_high": round(agg.worst_p_high, 3),
            "mean_p_high": round(agg.mean_p_high, 3),
            "columns_blocked": agg.columns_blocked,
            "columns_investigate": agg.columns_investigate,
            "overall_readiness": agg.overall_readiness,
        }

    return {
        "target": "dataset",
        "overall_readiness": network_ctx.overall_readiness,
        "total_columns": network_ctx.total_columns,
        "columns_blocked": network_ctx.columns_blocked,
        "columns_investigate": network_ctx.columns_investigate,
        "columns_ready": network_ctx.columns_ready,
        "avg_entropy_score": round(network_ctx.avg_entropy_score, 3),
        "intents": intents,
        "top_fix": top_fix,
        "top_drivers": top_drivers,
    }


def get_existing_teachings(
    session: Session,
    source_id: str,
    *,
    table_name: str | None = None,
    column_name: str | None = None,
) -> list[dict[str, Any]]:
    """Query existing DataFix (teach) records for context.

    Returns a lightweight summary of what has already been taught.
    """
    stmt = select(DataFix).where(DataFix.source_id == source_id)
    if table_name:
        stmt = stmt.where(DataFix.table_name == table_name)
    if column_name:
        stmt = stmt.where(DataFix.column_name == column_name)
    stmt = stmt.order_by(DataFix.ordinal)

    fixes = session.execute(stmt).scalars().all()
    return [
        {
            "action": f.action,
            "target": f"{f.table_name}.{f.column_name}" if f.column_name else f.table_name,
            "dimension": f.dimension,
            "description": f.description,
            "status": f.status,
        }
        for f in fixes
    ]


def get_teach_type_schemas() -> dict[str, Any]:
    """Build a summary of available teach types with their parameter schemas."""
    schemas: dict[str, Any] = {}
    for teach_type, model_cls in PARAM_MODELS.items():
        schema = model_cls.model_json_schema()
        # Extract just the properties and required fields for the prompt
        schemas[teach_type] = {
            "required": schema.get("required", []),
            "properties": {
                name: {
                    "type": prop.get("type", "string"),
                    "description": prop.get("description", ""),
                }
                for name, prop in schema.get("properties", {}).items()
            },
        }
    return schemas


def validate_resolution_option(option: dict[str, Any]) -> TeachSuggestion:
    """Validate a resolution option from LLM output against teach schemas.

    Returns the option with valid=True if params match the schema,
    or valid=False with a validation_warning if they don't.
    """
    teach_type = option.get("teach_type", "")
    params = option.get("params", {})

    result = TeachSuggestion(
        teach_type=teach_type,
        target=option.get("target"),
        params=params,
        description=option.get("description", ""),
        expected_impact=option.get("expected_impact", ""),
    )

    if teach_type not in VALID_TEACH_TYPES:
        result.valid = False
        result.validation_warning = f"Unknown teach type: {teach_type!r}"
        return result

    model_cls = PARAM_MODELS.get(teach_type)
    if model_cls:
        try:
            model_cls.model_validate(params)
        except Exception as e:
            result.valid = False
            result.validation_warning = str(e)

    return result


# ---------------------------------------------------------------------------
# WhyAgent — LLM-backed evidence synthesis
# ---------------------------------------------------------------------------


class WhyAgent(LLMFeature):
    """Synthesize entropy evidence into domain explanations with teach suggestions."""

    def analyze(
        self,
        evidence_context: dict[str, Any],
        teach_schemas: dict[str, Any],
        existing_teachings: list[dict[str, Any]],
    ) -> WhyResponse:
        """Run the why analysis LLM call.

        Args:
            evidence_context: Pre-assembled evidence dict (from build_*_evidence).
            teach_schemas: Available teach type schemas.
            existing_teachings: Already-applied teachings for context.

        Returns:
            WhyResponse with analysis, evidence, and resolution options.
        """
        target = evidence_context.get("target", "unknown")
        readiness = evidence_context.get("readiness") or evidence_context.get(
            "overall_readiness", "unknown"
        )

        context = {
            "evidence_json": json.dumps(evidence_context, indent=2),
            "teach_schemas_json": json.dumps(teach_schemas, indent=2),
            "existing_teachings_json": json.dumps(existing_teachings, indent=2)
            if existing_teachings
            else "[]",
            "target": target,
        }

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "why_analysis", context
            )
        except Exception as e:
            logger.error("why_prompt_render_failed", error=str(e))
            return WhyResponse(
                target=target,
                readiness=readiness,
                analysis=f"Failed to render prompt: {e}",
            )

        tool = ToolDefinition(
            name="explain_entropy",
            description="Provide structured explanation of entropy observations with teach suggestions.",
            input_schema=_WHY_OUTPUT_SCHEMA,
        )

        model = self.provider.get_model_for_tier("balanced")
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "explain_entropy"},
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        response_result = self.provider.converse(request)
        if not response_result.success or not response_result.value:
            error = response_result.error or "LLM call failed"
            logger.error("why_llm_failed", error=error)
            return WhyResponse(
                target=target,
                readiness=readiness,
                analysis=f"LLM analysis failed: {error}",
            )

        response = response_result.value
        if not response.tool_calls:
            return WhyResponse(
                target=target,
                readiness=readiness,
                analysis="LLM did not produce structured output.",
            )

        raw = response.tool_calls[0].input

        # Parse evidence items
        evidence_items = [
            EvidenceItem(
                detector=e.get("detector", ""),
                dimension=e.get("dimension", ""),
                score=e.get("score", 0.0),
                state=e.get("state", ""),
                impact_delta=e.get("impact_delta", 0.0),
                detail=e.get("detail", ""),
            )
            for e in raw.get("evidence", [])
        ]

        # Parse and validate resolution options
        resolution_options = [
            validate_resolution_option(opt) for opt in raw.get("resolution_options", [])
        ]

        # Build intents from evidence context
        intents = evidence_context.get("intents", {})

        return WhyResponse(
            target=target,
            readiness=readiness,
            analysis=raw.get("analysis", ""),
            evidence=evidence_items,
            resolution_options=resolution_options,
            intents=intents,
        )
