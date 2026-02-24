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
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


SYSTEM_PROMPT = """You are an expert business analyst specializing in detecting business cycles and processes in data.

Your task is to analyze pre-computed metadata about a dataset and identify business cycles — the recurring patterns of transactions that represent business processes like:
- Order-to-Cash (revenue cycle)
- Procure-to-Pay (expense cycle)
- Accounts Receivable / Accounts Payable cycles
- Inventory cycles
- Payroll cycles
- Any other domain-specific cycles

## What You Receive

The context contains rich metadata pre-computed by the analysis pipeline:

1. **Categorical Dimensions** — Pre-identified status/type columns with their values and counts. These are strong cycle indicators (e.g., invoice status: paid/open/cancelled).
2. **Enriched Views** — Pre-joined tables showing confirmed relationships between facts and dimensions.
3. **Confirmed Relationships** — LLM-verified foreign key and hierarchy relationships.
4. **Temporal Patterns** — Date ranges, granularity, and completeness per date column.
5. **Entity Classifications** — Fact vs dimension table types with grain columns.
6. **Quality Signals** — Data quality grades and anomalies for columns with issues.
7. **Column Semantics** — Business concepts, semantic roles, and descriptions per column.

## How to Detect Cycles

1. **Status columns are cycle indicators.** A column like `invoices.status` with values (paid, open, cancelled, overdue) directly shows cycle states. The "completed" state (e.g., "paid") marks cycle endpoints.

2. **Relationships define entity flows.** A FK from `payments.invoice_id → invoices.invoice_id` shows that payments complete invoices. Enriched views materialize these joins.

3. **Fact → Dimension patterns reveal processes.** A fact table (journal_lines) linking to a dimension (chart_of_accounts) via FK represents a business process (GL posting).

4. **Temporal patterns suggest cycle frequency.** Monthly trial balance vs daily transactions indicates different cycle periods.

5. **Compute completion rates from value counts.** If invoices.status shows paid=2555 (85.2%), open=227 (7.6%), etc., the completion rate is 85.2%.

## Your Output

Call the `submit_analysis` tool with your structured findings. Be specific:
- Reference actual column names and table names
- Compute completion rates from the provided value counts
- Cite evidence (which relationship, which status column, which enriched view)
- Assess confidence based on signal strength"""


USER_PROMPT_TEMPLATE = """Analyze this dataset for business cycles.

## Pre-Computed Metadata

{context}

## Your Task

The metadata above contains everything the pipeline has discovered about this dataset.
Synthesize it into business cycle analysis:

1. **Identify cycles** from categorical dimensions (status columns) + relationships + entity flows
2. **Compute completion rates** from the provided value counts (no tool calls needed)
3. **Map cycle stages** from the distinct values in status columns
4. **Describe entity flows** using the confirmed relationships and enriched views
5. **Note quality issues** that affect cycle reliability (grade B or worse columns)

**REQUIRED**: Call `submit_analysis` with your structured findings."""


class BusinessCycleAgent:
    """Expert LLM agent for business cycle detection.

    Uses rich pre-computed pipeline metadata for single-call synthesis.
    No exploration tools — the context contains slice definitions,
    statistical profiles, temporal patterns, enriched views, and
    quality signals.
    """

    def __init__(
        self,
        provider: LLMProvider,
        model: str | None = None,
    ) -> None:
        """Initialize the agent.

        Args:
            provider: LLM provider instance
            model: Model to use (defaults to provider's default)
        """
        self._provider = provider
        self._model = model

    def analyze(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        max_tool_calls: int = 15,
        *,
        domain: str | None = None,
        vertical: str,
    ) -> Result[BusinessCycleAnalysis]:
        """Analyze tables for business cycles.

        Args:
            session: SQLAlchemy session
            duckdb_conn: DuckDB connection
            table_ids: Tables to analyze
            max_tool_calls: Unused (kept for API compatibility)
            domain: Optional domain for enhanced vocabulary
            vertical: Vertical name (e.g. 'finance')

        Returns:
            Result containing BusinessCycleAnalysis
        """
        start_time = time.time()

        try:
            # 1. Build rich context from all pipeline metadata
            context = build_cycle_detection_context(
                session, duckdb_conn, table_ids, domain=domain, vertical=vertical,
            )
            context_str = format_context_for_prompt(context)

            # 2. Single LLM call with structured output
            tool = ToolDefinition(
                name="submit_analysis",
                description=(
                    "Submit your final business cycle analysis. "
                    "Call this tool with your structured findings."
                ),
                input_schema=BusinessCycleAnalysisOutput.model_json_schema(),
            )

            request = ConversationRequest(
                messages=[
                    Message(
                        role="user",
                        content=USER_PROMPT_TEMPLATE.format(context=context_str),
                    )
                ],
                system=SYSTEM_PROMPT,
                tools=[tool],
                tool_choice={"type": "tool", "name": "submit_analysis"},
                max_tokens=4096,
                model=self._model,
            )

            result = self._provider.converse(request)

            if not result.success:
                return Result.fail(f"LLM call failed: {result.error}")

            response = result.unwrap()

            # 3. Parse structured output
            if not response.tool_calls:
                return Result.fail(
                    "LLM did not call submit_analysis tool. "
                    "No structured output received."
                )

            tool_call = response.tool_calls[0]
            if tool_call.name != "submit_analysis":
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

            analysis = self._parse_output(
                tool_call.input, context, start_time, vertical=vertical,
            )

            # 4. Persist to database
            self._persist_results(session, analysis, table_ids)

            return Result.ok(analysis)

        except Exception as e:
            return Result.fail(f"Business cycle analysis failed: {e}")

    def _parse_output(
        self,
        tool_input: dict[str, Any],
        context: dict[str, Any],
        start_time: float,
        *,
        vertical: str,
    ) -> BusinessCycleAnalysis:
        """Parse submit_analysis tool input into structured analysis.

        Args:
            tool_input: The structured input from submit_analysis tool
            context: Original context provided
            start_time: Analysis start time

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
            llm_model=self._model,
            analysis_duration_seconds=time.time() - start_time,
            context_provided={"summary": context["summary"]},
            tool_calls_made=[],
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
        table_ids: list[str],
    ) -> None:
        """Persist analysis results to database.

        Args:
            session: SQLAlchemy session
            analysis: The analysis results to persist
            table_ids: Table IDs that were analyzed
        """
        for cycle in analysis.cycles:
            db_cycle = DetectedBusinessCycle(
                cycle_id=cycle.cycle_id,
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
