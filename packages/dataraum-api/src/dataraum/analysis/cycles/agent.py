"""Business Cycle Detection Agent.

An expert LLM agent that detects business cycles using semantic metadata.
No hardcoded pattern matching - the agent analyzes the data structure
and identifies cycles based on:
- Entity flows (dimension → fact relationships)
- Status/state columns (cycle completion indicators)
- Transaction type columns (cycle stages)
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dataraum.analysis.cycles.config import map_to_canonical_type
from dataraum.analysis.cycles.context import (
    build_cycle_detection_context,
    format_context_for_prompt,
)
from dataraum.analysis.cycles.db_models import (
    BusinessCycleAnalysisRun,
    DetectedBusinessCycle,
)
from dataraum.analysis.cycles.models import (
    BusinessCycleAnalysis,
    CycleStage,
    DetectedCycle,
    EntityFlow,
)
from dataraum.analysis.cycles.tools import CycleDetectionTools, get_tool_definitions
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
    ToolResult,
)

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


SYSTEM_PROMPT = """You are an expert business analyst specializing in detecting business cycles and processes in data.

Your task is to analyze a dataset's metadata and identify business cycles - the recurring patterns of transactions that represent business processes like:
- Order-to-Cash (revenue cycle)
- Procure-to-Pay (expense cycle)
- Accounts Receivable cycles
- Accounts Payable cycles
- Inventory cycles
- Payroll cycles
- Any other domain-specific cycles

## How to Detect Cycles

1. **Entity Flows**: Look for dimension tables (customer, vendor, product) that connect to fact tables. Each dimension → fact relationship suggests an entity participating in business processes.

2. **Status Columns**: Status/state columns indicate cycle completion. Values like "Paid", "Completed", "Closed" mark cycle endpoints.

3. **Transaction Types**: Transaction type columns show different stages within a cycle. The distinct values represent cycle stages.

4. **Relationships**: Follow relationships from dimensions through facts to understand how entities flow through the system.

## Your Approach

1. First, analyze the provided context to understand the data structure
2. Form hypotheses about potential cycles based on entity flows and status columns
3. Use exploration tools (3-5 calls) to validate hypotheses and gather key metrics
4. Call `submit_analysis` with your structured findings

## IMPORTANT: Output via submit_analysis Tool

You MUST call the `submit_analysis` tool to provide your final analysis. Do NOT output raw JSON text.

The `submit_analysis` tool accepts your structured findings including:
- cycles: List of detected business cycles with details
- business_summary: Overall interpretation of the business model
- detected_processes: List of business processes found
- data_quality_observations: Any data quality issues noticed
- recommendations: Suggestions for improving data completeness

Be specific and evidence-based. Reference actual column names and values from the data."""


USER_PROMPT_TEMPLATE = """Analyze this dataset for business cycles.

## Dataset Context

{context}

## Your Task

1. Review the provided context - it already contains rich metadata including:
   - Table classifications (fact vs dimension)
   - Status/state columns with their distinct values
   - Semantic annotations with business descriptions
   - Relationships between tables

2. Form hypotheses about business cycles based on:
   - Status columns (A/R paid, A/P paid, Cleared, etc.) → cycle completion indicators
   - Entity columns (Customer name, Vendor name) → cycle participants
   - Transaction type columns → cycle stages

3. Use exploration tools to validate and gather metrics:
   - `get_cycle_completion_metrics`: Get completion rates for each potential cycle
   - `get_column_value_distribution`: Check status values, transaction types
   - `get_entity_transaction_flow`: Understand how entities progress through cycles
   - `get_functional_dependencies`: Understand column relationships

4. **REQUIRED**: Call `submit_analysis` with your structured findings when done.

## Guidelines

- The context provides rich metadata - use tools to validate hypotheses and gather metrics
- For each potential cycle, call `get_cycle_completion_metrics` to get actual completion rates
- Explore thoroughly - the tools are fast database queries
- Focus on cycles you can identify with confidence and evidence
- If no clear cycles exist, submit an analysis with an empty cycles list and explain why
- You MUST call `submit_analysis` to complete the task"""


class BusinessCycleAgent:
    """Expert LLM agent for business cycle detection.

    Uses semantic metadata as context and provides tools for
    on-demand data exploration to detect business cycles.
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
    ) -> Result[BusinessCycleAnalysis]:
        """Analyze tables for business cycles.

        Args:
            session: SQLAlchemy session
            duckdb_conn: DuckDB connection
            table_ids: Tables to analyze
            max_tool_calls: Maximum tool calls allowed
            domain: Optional domain for enhanced vocabulary
                    (e.g., "financial", "retail", "manufacturing")

        Returns:
            Result containing BusinessCycleAnalysis
        """
        start_time = time.time()
        tool_calls_made: list[dict[str, Any]] = []

        try:
            # 1. Build context from metadata (with optional domain vocabulary)
            context = build_cycle_detection_context(session, duckdb_conn, table_ids, domain=domain)
            context_str = format_context_for_prompt(context)

            # 2. Initialize tools
            table_map = {
                t["table_id"]: t["table_name"] for t in context["dataset_overview"]["tables"]
            }
            tools = CycleDetectionTools(session, duckdb_conn, table_map)

            # 3. Convert tool definitions to ToolDefinition objects
            tool_defs = [
                ToolDefinition(
                    name=td["name"],
                    description=td["description"],
                    input_schema=td["input_schema"],
                )
                for td in get_tool_definitions()
            ]

            # 4. Start conversation
            messages: list[Message] = [
                Message(role="user", content=USER_PROMPT_TEMPLATE.format(context=context_str))
            ]

            final_response = None

            # 5. Run agent loop with tool use
            for iteration in range(max_tool_calls + 1):
                # Make request
                request = ConversationRequest(
                    messages=messages,
                    system=SYSTEM_PROMPT,
                    tools=tool_defs,
                    max_tokens=4096,
                    model=self._model,
                )

                result = self._provider.converse(request)

                if not result.success:
                    return Result.fail(f"LLM call failed: {result.error}")

                response = result.unwrap()

                # Debug output
                logger.debug(
                    "agent_iteration",
                    iteration=iteration,
                    stop_reason=response.stop_reason,
                    tool_count=len(response.tool_calls),
                    content_len=len(response.content),
                )

                # Check if there are tool calls
                if response.tool_calls:
                    # Check for submit_analysis - this is the terminal tool
                    submit_call = next(
                        (tc for tc in response.tool_calls if tc.name == "submit_analysis"),
                        None,
                    )

                    if submit_call:
                        # Agent is submitting final analysis via tool
                        logger.debug("tool_call", tool="submit_analysis", final=True)
                        tool_calls_made.append(
                            {
                                "tool": "submit_analysis",
                                "input": submit_call.input,
                                "output": {"status": "accepted"},
                            }
                        )

                        # Parse the structured tool output
                        analysis = self._parse_tool_output(
                            submit_call.input,
                            context,
                            tool_calls_made,
                            start_time,
                        )

                        # Persist and return
                        self._persist_results(session, analysis, table_ids)
                        return Result.ok(analysis)

                    # Execute exploration tools and collect results
                    tool_results: list[ToolResult] = []

                    for tool_call in response.tool_calls:
                        tool_name = tool_call.name
                        tool_input = tool_call.input

                        # Log the tool call
                        logger.debug("tool_call", tool=tool_name, input=tool_input)

                        # Execute the tool
                        tool_output = self._execute_tool(tools, tool_name, tool_input)

                        tool_calls_made.append(
                            {
                                "tool": tool_name,
                                "input": tool_input,
                                "output": tool_output,
                            }
                        )

                        tool_results.append(
                            ToolResult(
                                tool_use_id=tool_call.id,
                                content=json.dumps(tool_output),
                                is_error="error" in tool_output,
                            )
                        )

                    # Add assistant message with tool calls
                    messages.append(
                        Message(
                            role="assistant",
                            content=response.content,
                            tool_calls=response.tool_calls,
                        )
                    )

                    # Add tool results as user message
                    messages.append(Message(role="user", content=tool_results))

                else:
                    # No tool calls - try to parse as JSON (fallback)
                    final_response = response.content
                    break

            if not final_response:
                return Result.fail(
                    "Agent did not call submit_analysis tool. "
                    "Ensure the agent uses the submit_analysis tool to provide final output."
                )

            # 6. Parse response into structured output (fallback for text response)
            analysis = self._parse_response(
                final_response,
                context,
                tool_calls_made,
                start_time,
            )

            # 7. Persist to database
            self._persist_results(session, analysis, table_ids)

            return Result.ok(analysis)

        except Exception as e:
            return Result.fail(f"Business cycle analysis failed: {e}")

    def _execute_tool(
        self,
        tools: CycleDetectionTools,
        tool_name: str,
        tool_input: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a tool call.

        Args:
            tools: Tools instance
            tool_name: Name of tool to call
            tool_input: Tool input parameters

        Returns:
            Tool result
        """
        if tool_name == "get_column_value_distribution":
            return tools.get_column_value_distribution(
                table_name=tool_input["table_name"],
                column_name=tool_input["column_name"],
                limit=tool_input.get("limit", 20),
            )
        elif tool_name == "get_cycle_completion_metrics":
            return tools.get_cycle_completion_metrics(
                table_name=tool_input["table_name"],
                entity_column=tool_input["entity_column"],
                status_column=tool_input["status_column"],
                completion_value=tool_input["completion_value"],
            )
        elif tool_name == "get_entity_transaction_flow":
            return tools.get_entity_transaction_flow(
                table_name=tool_input["table_name"],
                entity_column=tool_input["entity_column"],
                type_column=tool_input["type_column"],
                date_column=tool_input.get("date_column"),
                sample_size=tool_input.get("sample_size", 5),
            )
        elif tool_name == "get_functional_dependencies":
            return tools.get_functional_dependencies(
                table_name=tool_input["table_name"],
            )
        else:
            return {"error": f"Unknown tool: {tool_name}"}

    def _parse_tool_output(
        self,
        tool_input: dict[str, Any],
        context: dict[str, Any],
        tool_calls: list[dict[str, Any]],
        start_time: float,
    ) -> BusinessCycleAnalysis:
        """Parse submit_analysis tool input into structured analysis.

        This is the preferred method - the LLM provides structured output
        via the submit_analysis tool, which we validate and convert.

        Args:
            tool_input: The structured input from submit_analysis tool
            context: Original context provided
            tool_calls: Tool calls made during analysis
            start_time: Analysis start time

        Returns:
            Structured BusinessCycleAnalysis
        """
        from dataraum.analysis.cycles.models import BusinessCycleAnalysisOutput

        # Validate the input against our Pydantic model
        try:
            output = BusinessCycleAnalysisOutput.model_validate(tool_input)
        except Exception as e:
            # If validation fails, fall back to dict parsing
            logger.warning("tool_output_validation_failed", error=str(e))
            output = None

        # Build cycles from the output
        cycles = []
        cycle_data_list = output.cycles if output else tool_input.get("cycles", [])

        for cycle_data in cycle_data_list:
            # Handle both Pydantic model and dict
            if hasattr(cycle_data, "model_dump"):
                cd = cycle_data.model_dump()
            else:
                cd = cycle_data

            # Parse entity flows
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

            # Parse stages
            stages = [
                CycleStage(
                    stage_name=s.get("stage_name", ""),
                    stage_order=s.get("stage_order", 0),
                    indicator_column=s.get("indicator_column"),
                    indicator_values=s.get("indicator_values", []),
                )
                for s in cd.get("stages", [])
            ]

            # Map cycle_type to canonical vocabulary
            raw_cycle_type = cd.get("cycle_type", "unknown")
            canonical_type, is_known_type = map_to_canonical_type(raw_cycle_type)

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

        # Get summary fields
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
            tables_analyzed=[t["table_name"] for t in context["dataset_overview"]["tables"]],
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
            tool_calls_made=tool_calls,
        )

        # Calculate overall health
        if cycles:
            completion_rates = [c.completion_rate for c in cycles if c.completion_rate is not None]
            if completion_rates:
                analysis.overall_cycle_health = sum(completion_rates) / len(completion_rates)

        return analysis

    def _parse_response(
        self,
        response: str,
        context: dict[str, Any],
        tool_calls: list[dict[str, Any]],
        start_time: float,
    ) -> BusinessCycleAnalysis:
        """Parse LLM response into structured analysis.

        Args:
            response: Raw LLM response
            context: Original context provided
            tool_calls: Tool calls made during analysis
            start_time: Analysis start time

        Returns:
            Structured BusinessCycleAnalysis
        """
        # Try to extract JSON from response
        try:
            # Find JSON in response
            json_start = response.find("{")
            json_end = response.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                data = json.loads(json_str)
            else:
                data = {}
        except json.JSONDecodeError:
            data = {}

        # Build analysis from parsed data
        cycles = []
        for cycle_data in data.get("cycles", []):
            # Parse entity flows
            entity_flows = [
                EntityFlow(
                    entity_type=ef.get("entity_type", "unknown"),
                    entity_column=ef.get("entity_column", ""),
                    entity_table=ef.get("entity_table", ""),
                    fact_table=ef.get("fact_table"),
                    fact_column=ef.get("fact_column"),
                )
                for ef in cycle_data.get("entity_flows", [])
            ]

            # Parse stages
            stages = [
                CycleStage(
                    stage_name=s.get("stage_name", ""),
                    stage_order=s.get("stage_order", 0),
                    indicator_column=s.get("indicator_column"),
                    indicator_values=s.get("indicator_values", []),
                )
                for s in cycle_data.get("stages", [])
            ]

            # Map cycle_type to canonical vocabulary
            raw_cycle_type = cycle_data.get("cycle_type", "unknown")
            canonical_type, is_known_type = map_to_canonical_type(raw_cycle_type)

            cycle = DetectedCycle(
                cycle_id=str(uuid4()),
                cycle_name=cycle_data.get("cycle_name", "Unknown Cycle"),
                cycle_type=raw_cycle_type,
                canonical_type=canonical_type,
                is_known_type=is_known_type,
                description=cycle_data.get("description", ""),
                business_value=cycle_data.get("business_value", "medium"),
                stages=stages,
                entity_flows=entity_flows,
                tables_involved=cycle_data.get("tables_involved", []),
                status_column=cycle_data.get("status_column"),
                status_table=cycle_data.get("status_table"),
                completion_value=cycle_data.get("completion_value"),
                total_records=cycle_data.get("total_records"),
                completed_cycles=cycle_data.get("completed_cycles"),
                completion_rate=cycle_data.get("completion_rate"),
                confidence=cycle_data.get("confidence", 0.0),
                evidence=cycle_data.get("evidence", []),
            )
            cycles.append(cycle)

        analysis = BusinessCycleAnalysis(
            analysis_id=str(uuid4()),
            tables_analyzed=[t["table_name"] for t in context["dataset_overview"]["tables"]],
            total_columns=context["summary"]["total_columns"],
            total_relationships=context["summary"]["total_relationships"],
            cycles=cycles,
            total_cycles_detected=len(cycles),
            high_value_cycles=sum(1 for c in cycles if c.business_value == "high"),
            business_summary=data.get("business_summary", ""),
            detected_processes=data.get("detected_processes", []),
            data_quality_observations=data.get("data_quality_observations", []),
            recommendations=data.get("recommendations", []),
            llm_model=self._model,
            analysis_duration_seconds=time.time() - start_time,
            context_provided={"summary": context["summary"]},
            tool_calls_made=tool_calls,
        )

        # Calculate overall health
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
        from datetime import UTC, datetime

        # Create analysis run record
        run = BusinessCycleAnalysisRun(
            analysis_id=analysis.analysis_id,
            table_ids=table_ids,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            duration_seconds=analysis.analysis_duration_seconds,
            total_cycles_detected=analysis.total_cycles_detected,
            high_value_cycles=analysis.high_value_cycles,
            overall_cycle_health=analysis.overall_cycle_health,
            llm_model=analysis.llm_model,
            tool_calls_count=len(analysis.tool_calls_made),
            business_summary=analysis.business_summary,
            detected_processes=analysis.detected_processes,
            data_quality_observations=analysis.data_quality_observations,
            recommendations=analysis.recommendations,
            context_summary=analysis.context_provided,
        )
        session.add(run)

        # Create detected cycle records
        for cycle in analysis.cycles:
            db_cycle = DetectedBusinessCycle(
                cycle_id=cycle.cycle_id,
                analysis_id=analysis.analysis_id,
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
