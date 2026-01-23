"""Query Agent for natural language to SQL conversion.

This agent converts natural language questions into executable SQL
with entropy awareness and assumption tracking.

Usage:
    agent = QueryAgent(config, provider, renderer, cache)
    result = agent.analyze(
        session=session,
        duckdb_conn=conn,
        question="What was total revenue last month?",
        table_ids=["t1", "t2"],
    )
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.entropy.context import build_entropy_context
from dataraum.entropy.contracts import (
    ConfidenceLevel,
    ContractEvaluation,
    evaluate_contract,
    find_best_contract,
    get_contract,
)
from dataraum.graphs.context import build_execution_context, format_context_for_prompt
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import ConversationRequest, Message, ToolDefinition

from .models import (
    QueryAnalysisOutput,
    QueryResult,
    assumption_output_to_query_assumption,
)

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.entropy.models import EntropyContext
    from dataraum.graphs.context import GraphExecutionContext
    from dataraum.graphs.models import QueryAssumption

logger = get_logger(__name__)


@dataclass
class QueryContext:
    """Context for query execution."""

    session: Session
    duckdb_conn: duckdb.DuckDBPyConnection
    table_ids: list[str]
    source_id: str | None = None

    # Rich metadata context
    execution_context: GraphExecutionContext | None = None
    entropy_context: EntropyContext | None = None

    # Contract settings
    contract_name: str | None = None
    auto_contract: bool = False


class QueryAgent(LLMFeature):
    """Agent for converting natural language questions to SQL.

    Extends LLMFeature to provide:
    - Natural language understanding
    - Schema-aware SQL generation
    - Entropy-aware assumption tracking
    - Contract-based confidence levels
    """

    def analyze(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        question: str,
        table_ids: list[str],
        *,
        contract: str | None = None,
        auto_contract: bool = False,
        source_id: str | None = None,
    ) -> Result[QueryResult]:
        """Analyze a natural language question and generate SQL.

        Args:
            session: SQLAlchemy session for metadata access
            duckdb_conn: DuckDB connection for schema introspection
            question: Natural language question
            table_ids: List of table IDs to query against
            contract: Explicit contract name for evaluation
            auto_contract: If True, find the strictest passing contract
            source_id: Optional source ID for context

        Returns:
            Result containing QueryResult with SQL, data, and confidence level
        """
        execution_id = str(uuid4())

        # Build rich context
        try:
            execution_context = build_execution_context(
                session=session,
                table_ids=table_ids,
                duckdb_conn=duckdb_conn,
            )
        except Exception as e:
            logger.error(f"Failed to build execution context: {e}")
            return Result.fail(f"Failed to build context: {e}")

        # Build entropy context
        try:
            entropy_context = build_entropy_context(
                session=session,
                table_ids=table_ids,
            )
        except Exception as e:
            logger.warning(f"Failed to build entropy context: {e}")
            entropy_context = None

        # Determine contract and evaluate
        contract_evaluation: ContractEvaluation | None = None
        confidence_level = ConfidenceLevel.GREEN

        if entropy_context:
            if auto_contract:
                # Find strictest passing contract
                best_name, best_eval = find_best_contract(entropy_context)
                if best_name and best_eval:
                    contract = best_name
                    contract_evaluation = best_eval
                    confidence_level = best_eval.confidence_level
                else:
                    # No contracts pass - use default with RED
                    contract = "exploratory_analysis"
                    confidence_level = ConfidenceLevel.RED
            elif contract:
                # Evaluate explicit contract
                if get_contract(contract) is None:
                    return Result.fail(f"Contract not found: {contract}")
                contract_evaluation = evaluate_contract(entropy_context, contract)
                confidence_level = contract_evaluation.confidence_level
            else:
                # Default contract
                contract = "exploratory_analysis"
                contract_evaluation = evaluate_contract(entropy_context, contract)
                confidence_level = contract_evaluation.confidence_level

        # Check if query is blocked
        if confidence_level == ConfidenceLevel.RED and contract_evaluation:
            return Result.ok(
                QueryResult(
                    execution_id=execution_id,
                    question=question,
                    success=False,
                    confidence_level=ConfidenceLevel.RED,
                    contract=contract,
                    contract_evaluation=contract_evaluation,
                    answer=self._format_blocked_response(contract_evaluation),
                    error="Query blocked due to data quality issues",
                )
            )

        # Generate SQL using LLM
        gen_result = self._generate_query(
            question=question,
            execution_id=execution_id,
            execution_context=execution_context,
            entropy_context=entropy_context,
        )

        if not gen_result.success or not gen_result.value:
            return Result.ok(
                QueryResult(
                    execution_id=execution_id,
                    question=question,
                    success=False,
                    confidence_level=confidence_level,
                    contract=contract,
                    contract_evaluation=contract_evaluation,
                    error=gen_result.error or "Query generation failed",
                )
            )

        analysis_output = gen_result.value

        # Execute the generated SQL
        exec_result = self._execute_query(
            sql=analysis_output.final_sql,
            duckdb_conn=duckdb_conn,
        )

        data: list[dict[str, Any]] | None = None
        columns: list[str] | None = None
        exec_error: str | None = None

        if exec_result.success and exec_result.value:
            columns, data = exec_result.value
        else:
            exec_error = exec_result.error

        # Convert assumptions
        assumptions: list[QueryAssumption] = [
            assumption_output_to_query_assumption(a, execution_id)
            for a in analysis_output.assumptions
        ]

        # Calculate overall entropy score
        entropy_score = 0.0
        if entropy_context:
            scores = [p.composite_score for p in entropy_context.column_profiles.values()]
            if scores:
                entropy_score = sum(scores) / len(scores)

        # Format answer
        answer = self._format_answer(
            question=question,
            data=data,
            columns=columns,
            metric_type=analysis_output.metric_type,
            assumptions=assumptions,
            confidence_level=confidence_level,
        )

        return Result.ok(
            QueryResult(
                execution_id=execution_id,
                question=question,
                executed_at=datetime.now(UTC),
                answer=answer,
                sql=analysis_output.final_sql,
                data=data,
                columns=columns,
                confidence_level=confidence_level,
                entropy_score=entropy_score,
                assumptions=assumptions,
                contract=contract,
                contract_evaluation=contract_evaluation,
                interpreted_question=analysis_output.interpreted_question,
                metric_type=analysis_output.metric_type,
                column_mappings=analysis_output.column_mappings,
                validation_notes=analysis_output.validation_notes,
                success=exec_error is None,
                error=exec_error,
            )
        )

    def _generate_query(
        self,
        question: str,
        execution_id: str,
        execution_context: GraphExecutionContext,
        entropy_context: EntropyContext | None,
    ) -> Result[QueryAnalysisOutput]:
        """Use LLM to analyze question and generate SQL."""
        # Build schema information
        schema_info = self._build_schema_info(execution_context)

        # Format context for prompt
        context_str = format_context_for_prompt(execution_context)

        # Format entropy warnings
        entropy_warnings = ""
        if entropy_context:
            from dataraum.graphs.context import format_entropy_for_prompt

            entropy_warnings = format_entropy_for_prompt(execution_context)

        # Build prompt context
        prompt_context = {
            "question": question,
            "schema_info": json.dumps(schema_info, indent=2),
            "dataset_context": context_str,
            "entropy_warnings": entropy_warnings or "No data quality warnings.",
        }

        # Render prompt
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "query_analysis", prompt_context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Compute prompt hash
        prompt_hash = hashlib.sha256(user_prompt.encode()).hexdigest()[:16]
        logger.debug(f"Query prompt hash: {prompt_hash}")

        # Define tool for structured output
        tool = ToolDefinition(
            name="analyze_query",
            description="Analyze the question and provide SQL to answer it",
            input_schema=QueryAnalysisOutput.model_json_schema(),
        )

        # Call LLM
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
        )

        result = self.provider.converse(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "LLM call failed")

        response = result.value

        # Extract tool call result
        if not response.tool_calls:
            if response.content:
                try:
                    response_data = json.loads(response.content)
                    output = QueryAnalysisOutput.model_validate(response_data)
                except Exception:
                    return Result.fail(f"LLM did not use tool. Response: {response.content[:200]}")
            else:
                return Result.fail("LLM did not use the analyze_query tool")
        else:
            tool_call = response.tool_calls[0]
            if tool_call.name != "analyze_query":
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

            try:
                output = QueryAnalysisOutput.model_validate(tool_call.input)
            except Exception as e:
                return Result.fail(f"Failed to validate tool response: {e}")

        return Result.ok(output)

    def _execute_query(
        self,
        sql: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> Result[tuple[list[str], list[dict[str, Any]]]]:
        """Execute generated SQL and return results."""
        try:
            # Validate SQL is read-only
            sql_upper = sql.upper().strip()
            dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
            for keyword in dangerous:
                if keyword in sql_upper and not sql_upper.startswith("SELECT"):
                    return Result.fail(f"Query contains dangerous keyword: {keyword}")

            result = duckdb_conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            data = [dict(zip(columns, row, strict=True)) for row in rows]

            return Result.ok((columns, data))
        except Exception as e:
            return Result.fail(f"SQL execution failed: {e}")

    def _build_schema_info(
        self,
        execution_context: GraphExecutionContext,
    ) -> dict[str, Any]:
        """Build schema information for prompt."""
        tables = []
        for table_ctx in execution_context.tables:
            columns = []
            for col_ctx in table_ctx.columns:
                col_info: dict[str, Any] = {
                    "name": col_ctx.column_name,
                    "data_type": col_ctx.data_type,
                }
                if col_ctx.semantic_role:
                    col_info["semantic_role"] = col_ctx.semantic_role
                if col_ctx.business_concept:
                    col_info["business_concept"] = col_ctx.business_concept
                columns.append(col_info)

            tables.append(
                {
                    "name": table_ctx.table_name,
                    "row_count": table_ctx.row_count,
                    "columns": columns,
                }
            )

        return {"tables": tables}

    def _format_answer(
        self,
        question: str,
        data: list[dict[str, Any]] | None,
        columns: list[str] | None,
        metric_type: str,
        assumptions: list[QueryAssumption],
        confidence_level: ConfidenceLevel,
    ) -> str:
        """Format the answer for display."""
        if data is None:
            return "Unable to retrieve data."

        if not data:
            return "No data found matching your query."

        # Simple formatting based on metric type
        if metric_type == "scalar" and len(data) == 1 and columns:
            # Single value answer
            row = data[0]
            col = columns[0]
            value = row.get(col, "N/A")
            return f"The answer is: {value}"

        # Default: describe the data
        row_count = len(data)
        col_count = len(columns) if columns else 0

        return f"Found {row_count} result(s) with {col_count} column(s)."

    def _format_blocked_response(self, evaluation: ContractEvaluation) -> str:
        """Format response when query is blocked."""
        lines = [
            f"Cannot provide answer for {evaluation.contract_display_name} because:",
            "",
        ]

        for v in evaluation.violations:
            if v.dimension:
                lines.append(f"❌ {v.dimension}: {v.actual:.2f} (max allowed: {v.max_allowed:.2f})")
            elif v.condition:
                lines.append(f"❌ {v.condition}: {v.details}")

        lines.append("")
        lines.append("Options:")
        lines.append("1. Use a less strict contract (e.g., exploratory_analysis)")
        lines.append("2. Resolve the data quality issues first")

        return "\n".join(lines)
