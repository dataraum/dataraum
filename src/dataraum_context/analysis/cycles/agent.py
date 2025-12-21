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

from dataraum_context.analysis.cycles.context import (
    build_cycle_detection_context,
    format_context_for_prompt,
)
from dataraum_context.analysis.cycles.db_models import (
    BusinessCycleAnalysisRun,
    DetectedBusinessCycle,
)
from dataraum_context.analysis.cycles.models import (
    BusinessCycleAnalysis,
    CycleStage,
    DetectedCycle,
    EntityFlow,
)
from dataraum_context.analysis.cycles.tools import CycleDetectionTools, get_tool_definitions
from dataraum_context.core.models.base import Result
from dataraum_context.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
    ToolResult,
)

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.ext.asyncio import AsyncSession

    from dataraum_context.llm.providers.base import LLMProvider


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
2. Identify potential cycles based on entity flows and status columns
3. Use the tools to validate your hypotheses and gather metrics
4. Return structured findings about detected cycles

## Output Format

When you have gathered enough information, provide your analysis in a structured JSON format with:
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

1. Based on the semantic annotations and relationships, identify potential business cycles
2. Use the tools to validate your hypotheses:
   - get_column_value_distribution: Check status column values, transaction types
   - get_cycle_completion_metrics: Measure cycle completion rates
   - get_entity_transaction_flow: See how transactions flow for entities
   - get_functional_dependencies: Understand data structure

3. Return your findings as JSON with this structure:
```json
{{
  "cycles": [
    {{
      "cycle_name": "string - descriptive name",
      "cycle_type": "string - ar_cycle, ap_cycle, revenue_cycle, etc.",
      "description": "string - what this cycle represents",
      "business_value": "high|medium|low",
      "entity_flows": [
        {{
          "entity_type": "string",
          "entity_column": "string",
          "entity_table": "string",
          "fact_table": "string",
          "fact_column": "string"
        }}
      ],
      "status_column": "string - column tracking completion",
      "status_table": "string",
      "completion_value": "string - value indicating completion",
      "stages": [
        {{
          "stage_name": "string",
          "stage_order": 1,
          "indicator_column": "string",
          "indicator_values": ["string"]
        }}
      ],
      "tables_involved": ["string"],
      "total_records": 0,
      "completed_cycles": 0,
      "completion_rate": 0.0,
      "confidence": 0.0,
      "evidence": ["string - what evidence supports this cycle"]
    }}
  ],
  "business_summary": "string - overall business model interpretation",
  "detected_processes": ["string"],
  "data_quality_observations": ["string"],
  "recommendations": ["string"]
}}
```

Start by examining the status columns and entity relationships, then use tools to validate."""


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

    async def analyze(
        self,
        session: AsyncSession,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        max_tool_calls: int = 10,
        *,
        domain: str | None = None,
    ) -> Result[BusinessCycleAnalysis]:
        """Analyze tables for business cycles.

        Args:
            session: SQLAlchemy async session
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
            context = await build_cycle_detection_context(
                session, duckdb_conn, table_ids, domain=domain
            )
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

                result = await self._provider.converse(request)

                if not result.success:
                    return Result.fail(f"LLM call failed: {result.error}")

                response = result.unwrap()

                # Debug output
                print(
                    f"   [Iteration {iteration}] stop={response.stop_reason}, tools={len(response.tool_calls)}, content_len={len(response.content)}"
                )

                # Check if there are tool calls
                if response.tool_calls:
                    # Execute tools and collect results
                    tool_results: list[ToolResult] = []

                    for tool_call in response.tool_calls:
                        tool_name = tool_call.name
                        tool_input = tool_call.input

                        # Log the tool call
                        print(f"     → {tool_name}({tool_input})")

                        # Execute the tool
                        tool_output = await self._execute_tool(tools, tool_name, tool_input)

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
                    # No tool calls - this is the final response
                    final_response = response.content
                    break

            if not final_response:
                return Result.fail("Agent did not produce a final response")

            # 6. Parse response into structured output
            analysis = self._parse_response(
                final_response,
                context,
                tool_calls_made,
                start_time,
            )

            # 7. Persist to database
            await self._persist_results(session, analysis, table_ids)

            return Result.ok(analysis)

        except Exception as e:
            return Result.fail(f"Business cycle analysis failed: {e}")

    async def _execute_tool(
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
            return await tools.get_column_value_distribution(
                table_name=tool_input["table_name"],
                column_name=tool_input["column_name"],
                limit=tool_input.get("limit", 20),
            )
        elif tool_name == "get_cycle_completion_metrics":
            return await tools.get_cycle_completion_metrics(
                table_name=tool_input["table_name"],
                entity_column=tool_input["entity_column"],
                status_column=tool_input["status_column"],
                completion_value=tool_input["completion_value"],
            )
        elif tool_name == "get_entity_transaction_flow":
            return await tools.get_entity_transaction_flow(
                table_name=tool_input["table_name"],
                entity_column=tool_input["entity_column"],
                type_column=tool_input["type_column"],
                date_column=tool_input.get("date_column"),
                sample_size=tool_input.get("sample_size", 5),
            )
        elif tool_name == "get_functional_dependencies":
            return await tools.get_functional_dependencies(
                table_name=tool_input["table_name"],
            )
        else:
            return {"error": f"Unknown tool: {tool_name}"}

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

            cycle = DetectedCycle(
                cycle_id=str(uuid4()),
                cycle_name=cycle_data.get("cycle_name", "Unknown Cycle"),
                cycle_type=cycle_data.get("cycle_type", "unknown"),
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

    async def _persist_results(
        self,
        session: AsyncSession,
        analysis: BusinessCycleAnalysis,
        table_ids: list[str],
    ) -> None:
        """Persist analysis results to database.

        Args:
            session: SQLAlchemy async session
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

        await session.commit()
