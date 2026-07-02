"""Business Cycle Detection Agent — the lifecycle family's grounding step (DAT-455).

Single-call LLM agent that synthesizes pre-computed pipeline metadata into
business cycle analysis. No exploration tools — the context is rich enough
(slice definitions, statistical profiles, temporal profiles, enriched views,
quality signals) for direct synthesis.

The cycle lifecycle family grounds + measures in ONE synthesis call (unlike
validation, where each artifact's bind and execute are two distinct SQL
operations — see the DAT-455 substrate-generality note). :meth:`ground_cycles`
runs that call and returns the detected cycles keyed by canonical type; the
phase reconciles each against its declared artifact (grounded vs ungroundable,
measured vs not). Persistence lives in the phase (mirrors validation's
``_persist_results``), not here — the agent is source-free and stateless.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dataraum.analysis.cycles.config import map_to_canonical_type
from dataraum.analysis.cycles.context import (
    build_cycle_detection_context,
    format_context_for_prompt,
)
from dataraum.analysis.cycles.models import (
    BusinessCycleAnalysis,
    BusinessCycleAnalysisOutput,
    CycleStage,
    DetectedCycle,
    EntityFlow,
)
from dataraum.analysis.cycles.verify import verify_cycles
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.lifecycle import BaseRunMap

logger = get_logger(__name__)

# Prompt template name (loaded from config/llm/prompts/business_cycles.yaml)
CYCLE_DETECTION_TEMPLATE_NAME = "business_cycles"


class BusinessCycleAgent(LLMFeature):
    """Expert LLM agent for business cycle detection.

    Uses rich pre-computed pipeline metadata for single-call synthesis.
    No exploration tools — the context contains slice definitions,
    statistical profiles, temporal patterns, enriched views, and
    quality signals.
    """

    def ground_cycles(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        *,
        vertical: str,
        base_runs: BaseRunMap,
    ) -> Result[BusinessCycleAnalysis]:
        """Ground the declared cycle vocabulary against the workspace (DAT-455).

        The single synthesis call: assemble the workspace context (every
        run-versioned read pinned to ``base_runs``, ADR-0008 in-run mode),
        let the LLM detect which declared cycle types ground to real
        columns/flows, and return them with completion measurements. The
        phase then reconciles each detected cycle against its declared
        artifact — no per-cycle bind/execute calls (the substrate-generality
        difference from validation; see module docstring).

        Source-free: scopes purely to ``table_ids`` (the session's typed
        selection), never a ``source_id``.

        Returns:
            ``Result.ok(BusinessCycleAnalysis)`` with the detected cycles, or
            ``Result.fail(reason)`` when the synthesis call fails (no tool
            call, LLM error) — a hard failure the phase surfaces, distinct from
            a declared cycle that simply did not ground.
        """
        start_time = time.time()

        # Get feature config
        feature_config = self.config.features.business_cycles
        if not feature_config or not feature_config.enabled:
            return Result.fail("Business cycles feature is disabled in config")

        # 1. Build rich context from all pipeline metadata, run-pinned (ADR-0008).
        context = build_cycle_detection_context(
            session,
            duckdb_conn,
            table_ids,
            vertical=vertical,
            base_runs=base_runs,
        )
        context_str = format_context_for_prompt(context)

        # 2. Render prompt from template
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                CYCLE_DETECTION_TEMPLATE_NAME, {"context": context_str}
            )
        except Exception as e:
            return Result.fail(f"Failed to render business cycles prompt: {e}")

        # 3. Single LLM call with structured output
        tool = ToolDefinition(
            name="submit_analysis",
            description=(
                "Submit your final business cycle analysis. "
                "Call this tool with your structured findings."
            ),
            input_schema=BusinessCycleAnalysisOutput.model_json_schema(),
        )

        model = self.provider.get_model_for_tier(feature_config.model_tier)

        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "submit_analysis"},
            label="business_cycles",
            effort=feature_config.effort,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        # converse raises a typed ProviderError on an API failure (DAT-503) —
        # retryability rides the exception to the worker's durable boundary, so
        # we don't re-wrap it. A returned Result is always a success.
        response = self.provider.converse(request).unwrap()

        # 4. Parse structured output. No tool call = degraded generation — a
        # hard failure for the synthesis (DAT-439 standard: no silent rescue).
        if not response.tool_calls:
            return Result.fail(
                "LLM did not call submit_analysis tool. No structured output received."
            )

        tool_call = response.tool_calls[0]
        if tool_call.name != "submit_analysis":
            return Result.fail(f"Unexpected tool call: {tool_call.name}")

        analysis = self._parse_output(
            tool_call.input,
            context,
            start_time,
            model=model,
            vertical=vertical,
        )

        return Result.ok(analysis)

    def _parse_output(
        self,
        tool_input: dict[str, Any],
        context: dict[str, Any],
        start_time: float,
        *,
        model: str | None = None,
        vertical: str,
    ) -> BusinessCycleAnalysis:
        """Parse submit_analysis tool input into structured analysis.

        The LLM output uses a flat schema (stages and entity_flows are
        top-level lists referencing cycles by name). This method groups
        them back into nested DetectedCycle objects.
        """
        # Validate against Pydantic model
        try:
            output = BusinessCycleAnalysisOutput.model_validate(tool_input)
        except Exception as e:
            logger.warning("tool_output_validation_failed", error=str(e))
            output = None

        # Group flat stages by cycle_name
        stages_by_cycle: dict[str, list[dict[str, Any]]] = {}
        raw_stages = (
            [s.model_dump() for s in output.stages] if output else tool_input.get("stages", [])
        )
        for s in raw_stages:
            if isinstance(s, dict):
                stages_by_cycle.setdefault(s.get("cycle_name", ""), []).append(s)

        # Group flat entity flows by cycle_name
        flows_by_cycle: dict[str, list[dict[str, Any]]] = {}
        raw_flows = (
            [ef.model_dump() for ef in output.entity_flows]
            if output
            else tool_input.get("entity_flows", [])
        )
        for ef in raw_flows:
            if isinstance(ef, dict):
                flows_by_cycle.setdefault(ef.get("cycle_name", ""), []).append(ef)

        # Build cycles from flat summaries + grouped stages/flows
        cycles = []
        cycle_data_list = (
            [c.model_dump() for c in output.cycles] if output else tool_input.get("cycles", [])
        )

        for cd in cycle_data_list:
            if not isinstance(cd, dict):
                continue

            cname = cd.get("cycle_name", "Unknown Cycle")

            # Reconstruct nested stages — flat schema uses indicator_value (singular)
            entity_flows = [
                EntityFlow(
                    entity_type=ef.get("entity_type", "unknown"),
                    entity_column=ef.get("entity_column", ""),
                    entity_table=ef.get("entity_table", ""),
                    fact_table=ef.get("fact_table"),
                    fact_column=ef.get("fact_column"),
                )
                for ef in flows_by_cycle.get(cname, [])
            ]

            # Group stage entries by (stage_name, stage_order) to collect indicator_values
            stage_map: dict[tuple[str, int], dict[str, Any]] = {}
            for s in stages_by_cycle.get(cname, []):
                key = (s.get("stage_name", ""), s.get("stage_order", 0))
                if key not in stage_map:
                    stage_map[key] = {
                        "stage_name": s.get("stage_name", ""),
                        "stage_order": s.get("stage_order", 0),
                        "indicator_column": s.get("indicator_column"),
                        "indicator_values": [],
                    }
                val = s.get("indicator_value")
                if val and val not in stage_map[key]["indicator_values"]:
                    stage_map[key]["indicator_values"].append(val)

            stages = [
                CycleStage(
                    stage_name=sm["stage_name"],
                    stage_order=sm["stage_order"],
                    indicator_column=sm["indicator_column"],
                    indicator_values=sm["indicator_values"],
                )
                for sm in stage_map.values()
            ]

            raw_cycle_type = cd.get("cycle_type", "unknown")
            canonical_type, is_known_type = map_to_canonical_type(raw_cycle_type, vertical)

            cycle = DetectedCycle(
                cycle_id=str(uuid4()),
                cycle_name=cname,
                cycle_type=raw_cycle_type,
                canonical_type=canonical_type,
                is_known_type=is_known_type,
                description=cd.get("description", ""),
                business_value=cd.get("business_value", "medium"),
                stages=stages,
                entity_flows=entity_flows,
                tables_involved=cd.get("tables_involved", []),
                status_column=cd.get("status_column"),
                status_table=cd.get("status_table"),
                completion_value=cd.get("completion_value"),
                total_records=cd.get("total_records"),
                completed_cycles=cd.get("completed_cycles"),
                completion_rate=cd.get("completion_rate"),
                confidence=cd.get("confidence", 0.5),
                evidence=cd.get("evidence", []),
            )
            cycles.append(cycle)

        # Membership floor (DAT-630): drop any cycle citing a column/value the
        # context never served — an improvised reference is a hallucination, and
        # an honest "not detected" beats a fabricated cycle. Loud, never silent.
        cycles, rejections = verify_cycles(cycles, context)
        for reason in rejections:
            logger.warning("cycle_rejected_improvised_reference", reason=reason)

        # Build analysis
        if output:
            business_summary = output.business_summary
            detected_processes = output.detected_processes
            data_quality_obs = output.data_quality_observations
            recommendations = output.recommendations
        else:
            business_summary = tool_input.get("business_summary", "")
            detected_processes = tool_input.get("detected_processes", [])
            data_quality_obs = tool_input.get("data_quality_observations", [])
            recommendations = tool_input.get("recommendations", [])

        analysis = BusinessCycleAnalysis(
            analysis_id=str(uuid4()),
            tables_analyzed=[t["table_name"] for t in context["tables"]],
            total_columns=context["summary"]["total_columns"],
            total_relationships=context["summary"]["total_relationships"],
            cycles=cycles,
            total_cycles_detected=len(cycles),
            high_value_cycles=sum(1 for c in cycles if c.business_value == "high"),
            business_summary=business_summary,
            detected_processes=detected_processes,
            data_quality_observations=data_quality_obs,
            recommendations=recommendations,
            llm_model=model,
            analysis_duration_seconds=time.time() - start_time,
            context_provided={"summary": context["summary"]},
        )

        if cycles:
            completion_rates = [c.completion_rate for c in cycles if c.completion_rate is not None]
            if completion_rates:
                analysis.overall_cycle_health = sum(completion_rates) / len(completion_rates)

        return analysis


__all__ = ["BusinessCycleAgent"]
