"""Business Cycle Detection Agent.

Single-call LLM agent that synthesizes pre-computed pipeline metadata
into business cycle analysis. No exploration tools — the context is
rich enough (slice definitions, statistical profiles, temporal profiles,
enriched views, quality signals) for direct synthesis.
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
from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
from dataraum.analysis.cycles.models import (
    BusinessCycleAnalysis,
    BusinessCycleAnalysisOutput,
    CycleStage,
    DetectedCycle,
    EntityFlow,
)
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

    MAX_TOKENS = 4096

    def analyze(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        *,
        source_id: str,
        vertical: str,
    ) -> Result[BusinessCycleAnalysis]:
        """Analyze tables for business cycles.

        Args:
            session: SQLAlchemy session
            duckdb_conn: DuckDB connection
            table_ids: Tables to analyze
            source_id: Source ID for persisting results
            vertical: Vertical name (e.g. 'finance')

        Returns:
            Result containing BusinessCycleAnalysis
        """
        start_time = time.time()

        # Get feature config
        feature_config = self.config.features.business_cycles
        if not feature_config or not feature_config.enabled:
            return Result.fail("Business cycles feature is disabled in config")

        try:
            # 1. Build rich context from all pipeline metadata
            context = build_cycle_detection_context(
                session,
                duckdb_conn,
                table_ids,
                vertical=vertical,
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
                max_tokens=self.MAX_TOKENS,
                temperature=temperature,
                model=model,
            )

            result = self.provider.converse(request)

            if not result.success:
                return Result.fail(f"LLM call failed: {result.error}")

            response = result.unwrap()

            # 4. Parse structured output
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

            # 5. Persist to database
            self._persist_results(session, analysis, source_id=source_id)

            return Result.ok(analysis)

        except Exception as e:
            return Result.fail(f"Business cycle analysis failed: {e}")

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

        Args:
            tool_input: The structured input from submit_analysis tool
            context: Original context provided
            start_time: Analysis start time
            model: Model used for generation
            vertical: Vertical name

        Returns:
            Structured BusinessCycleAnalysis
        """
        # Validate against Pydantic model
        try:
            output = BusinessCycleAnalysisOutput.model_validate(tool_input)
        except Exception as e:
            logger.warning("tool_output_validation_failed", error=str(e))
            output = None

        # Build cycles
        cycles = []
        cycle_data_list = output.cycles if output else tool_input.get("cycles", [])

        for cycle_data in cycle_data_list:
            if hasattr(cycle_data, "model_dump"):
                cd = cycle_data.model_dump()
            else:
                cd = cycle_data

            entity_flows = [
                EntityFlow(
                    entity_type=ef.get("entity_type", "unknown"),
                    entity_column=ef.get("entity_column", ""),
                    entity_table=ef.get("entity_table", ""),
                    fact_table=ef.get("fact_table"),
                    fact_column=ef.get("fact_column"),
                )
                for ef in cd.get("entity_flows", [])
            ]

            stages = [
                CycleStage(
                    stage_name=s.get("stage_name", ""),
                    stage_order=s.get("stage_order", 0),
                    indicator_column=s.get("indicator_column"),
                    indicator_values=s.get("indicator_values", []),
                )
                for s in cd.get("stages", [])
            ]

            raw_cycle_type = cd.get("cycle_type", "unknown")
            canonical_type, is_known_type = map_to_canonical_type(raw_cycle_type, vertical)

            cycle = DetectedCycle(
                cycle_id=str(uuid4()),
                cycle_name=cd.get("cycle_name", "Unknown Cycle"),
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

    def _persist_results(
        self,
        session: Session,
        analysis: BusinessCycleAnalysis,
        *,
        source_id: str,
    ) -> None:
        """Persist analysis results to database.

        Args:
            session: SQLAlchemy session
            analysis: The analysis results to persist
            source_id: Source ID to associate cycles with
        """
        for cycle in analysis.cycles:
            db_cycle = DetectedBusinessCycle(
                cycle_id=cycle.cycle_id,
                source_id=source_id,
                cycle_name=cycle.cycle_name,
                cycle_type=cycle.cycle_type,
                canonical_type=cycle.canonical_type,
                is_known_type=cycle.is_known_type,
                description=cycle.description,
                business_value=cycle.business_value,
                confidence=cycle.confidence,
                tables_involved=cycle.tables_involved,
                stages=[s.model_dump() for s in cycle.stages],
                entity_flows=[ef.model_dump() for ef in cycle.entity_flows],
                status_table=cycle.status_table,
                status_column=cycle.status_column,
                completion_value=cycle.completion_value,
                total_records=cycle.total_records,
                completed_cycles=cycle.completed_cycles,
                completion_rate=cycle.completion_rate,
                evidence=cycle.evidence,
            )
            session.add(db_cycle)
