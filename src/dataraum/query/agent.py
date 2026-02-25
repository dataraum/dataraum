"""Query Agent for natural language to SQL conversion.

This agent converts natural language questions into executable SQL
with entropy awareness, assumption tracking, and snippet knowledge base.

The agent uses the SQL Knowledge Base:
1. Discover validated SQL snippets from previous graph/query executions
2. Inject snippets into LLM prompt as validated building blocks
3. Generate SQL using LLM (with snippet hints)
4. Track snippet usage for stabilization metrics
5. Save novel query patterns as snippets for future reuse

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

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.entropy.contracts import (
    ConfidenceLevel,
    ContractEvaluation,
)
from dataraum.graphs.context import build_execution_context, format_context_for_prompt
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import ConversationRequest, Message, ToolDefinition
from dataraum.storage import Table

from .models import (
    QueryAnalysisOutput,
    QueryResult,
    assumption_output_to_query_assumption,
)

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager
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
        manager: ConnectionManager,
        ephemeral: bool = False,
    ) -> Result[QueryResult]:
        """Analyze a natural language question and generate SQL.

        Uses snippet knowledge base for validated SQL patterns.

        Args:
            session: SQLAlchemy session for metadata access
            duckdb_conn: DuckDB connection for schema introspection
            question: Natural language question
            table_ids: List of table IDs to query against
            contract: Explicit contract name for evaluation
            auto_contract: If True, find the strictest passing contract
            source_id: Optional source ID for context
            manager: ConnectionManager for snippet library and embeddings
            ephemeral: If True, don't save novel snippets

        Returns:
            Result containing QueryResult with SQL, data, and confidence level
        """
        execution_id = str(uuid4())

        # Filter to only typed tables (these are the tables the LLM should query)
        typed_table_ids = [
            t.table_id
            for t in session.execute(
                select(Table).where(
                    Table.table_id.in_(table_ids),
                    Table.layer == "typed",
                )
            )
            .scalars()
            .all()
        ]

        if not typed_table_ids:
            return Result.fail("No typed tables found. Run the typing phase first.")

        # Build rich context (using only typed tables)
        try:
            execution_context = build_execution_context(
                session=session,
                table_ids=typed_table_ids,
                duckdb_conn=duckdb_conn,
            )
        except Exception as e:
            logger.error(f"Failed to build execution context: {e}")
            return Result.fail(f"Failed to build context: {e}")

        # Evaluate contract using column_summaries from execution_context
        contract_evaluation: ContractEvaluation | None = None
        confidence_level = ConfidenceLevel.YELLOW  # Default when unknown

        if execution_context.column_summaries:
            from dataraum.entropy.contracts import (
                evaluate_contract,
                find_best_contract,
                get_contract,
            )

            if contract and not auto_contract:
                # Explicit contract — validate it exists
                if get_contract(contract) is None:
                    return Result.fail(f"Contract not found: {contract}")
                contract_evaluation = evaluate_contract(
                    execution_context.column_summaries, contract
                )
                confidence_level = contract_evaluation.confidence_level
            elif auto_contract:
                best_name, best_eval = find_best_contract(
                    execution_context.column_summaries
                )
                if best_name and best_eval:
                    contract = best_name
                    contract_evaluation = best_eval
                    confidence_level = best_eval.confidence_level
                else:
                    contract = "exploratory_analysis"
                    confidence_level = ConfidenceLevel.RED
            else:
                # Default: evaluate exploratory_analysis
                contract = "exploratory_analysis"
                try:
                    contract_evaluation = evaluate_contract(
                        execution_context.column_summaries, "exploratory_analysis"
                    )
                    confidence_level = contract_evaluation.confidence_level
                except ValueError:
                    readiness = (execution_context.entropy_summary or {}).get(
                        "overall_readiness", "investigate"
                    )
                    if readiness == "blocked":
                        confidence_level = ConfidenceLevel.RED
                    elif readiness == "investigate":
                        confidence_level = ConfidenceLevel.YELLOW
                    else:
                        confidence_level = ConfidenceLevel.GREEN
        elif execution_context.entropy_summary:
            # No column summaries but have entropy summary — use heuristic
            readiness = execution_context.entropy_summary.get("overall_readiness", "investigate")
            if readiness == "blocked":
                confidence_level = ConfidenceLevel.RED
            elif readiness == "investigate":
                confidence_level = ConfidenceLevel.YELLOW
            else:
                confidence_level = ConfidenceLevel.GREEN

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

        # Discover validated SQL snippets from the knowledge base
        # Track snippets provided to LLM for post-execution comparison
        provided_snippets: dict[str, dict[str, str]] = {}

        discovered_snippets = self._discover_snippets(
            session=session,
            question=question,
            schema_mapping_id=source_id or "default",
            manager=manager,
        )
        if discovered_snippets:
            provided_snippets = {
                s["step_id"]: s for s in discovered_snippets
            }

        gen_result = self._generate_query(
            question=question,
            execution_id=execution_id,
            execution_context=execution_context,
            discovered_snippets=discovered_snippets if discovered_snippets else None,
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

        # Execute with step-by-step model (if steps exist) or simple execution
        if analysis_output.steps:
            exec_result = self._execute_with_steps(
                analysis_output=analysis_output,
                duckdb_conn=duckdb_conn,
                execution_context=execution_context,
            )
        else:
            # Fallback for queries without steps
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

        # Overall entropy score from execution context
        entropy_score = execution_context.overall_entropy_score

        # Format answer
        answer = self._format_answer(
            question=question,
            data=data,
            columns=columns,
            metric_type=analysis_output.metric_type,
            assumptions=assumptions,
            confidence_level=confidence_level,
        )

        # Track snippet usage (compare generated steps to provided snippets)
        if exec_error is None:
            self._track_snippet_usage(
                session=session,
                execution_id=execution_id,
                analysis_output=analysis_output,
                provided_snippets=provided_snippets,
                manager=manager,
            )

        # Save novel snippets for future reuse (skip if ephemeral or failed)
        if exec_error is None and not ephemeral and source_id:
            self._save_novel_snippets(
                session=session,
                execution_id=execution_id,
                analysis_output=analysis_output,
                schema_mapping_id=source_id,
                provided_snippets=provided_snippets,
                manager=manager,
            )

        # Record execution
        if source_id:
            self._record_execution(
                session=session,
                source_id=source_id,
                question=question,
                sql=analysis_output.final_sql,
                success=exec_error is None,
                row_count=len(data) if data else None,
                error_message=exec_error,
                confidence_level=confidence_level,
                contract=contract,
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

    def _discover_snippets(
        self,
        session: Session,
        question: str,
        schema_mapping_id: str,
        manager: ConnectionManager,
    ) -> list[dict[str, Any]]:
        """Search the SQL Knowledge Base for relevant snippets via similarity.

        Uses semantic similarity search to find validated SQL patterns
        relevant to the user's question. Returns empty list when nothing
        is similar enough — the LLM then generates fresh SQL.

        Args:
            session: SQLAlchemy session
            question: Natural language question
            schema_mapping_id: Schema mapping identifier
            manager: ConnectionManager for embeddings

        Returns:
            List of snippet dicts with step_id, sql, description, snippet_id
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session, manager)

        matches = library.find_by_similarity(
            text=question,
            schema_mapping_id=schema_mapping_id,
            min_similarity=0.3,
            limit=20,
        )
        if not matches:
            return []

        results = []
        for m in matches:
            step_id = m.snippet.standard_field or m.snippet.snippet_id[:8]
            results.append({
                "step_id": step_id,
                "sql": m.snippet.sql,
                "description": m.snippet.description,
                "snippet_id": m.snippet.snippet_id,
                "snippet_type": m.snippet.snippet_type,
                "source": m.snippet.source,
                "similarity": m.match_confidence,
            })

        logger.info(f"Discovered {len(results)} snippets via similarity search")
        return results

    def _track_snippet_usage(
        self,
        session: Session,
        execution_id: str,
        analysis_output: QueryAnalysisOutput,
        provided_snippets: dict[str, dict[str, str]],
        manager: ConnectionManager,
    ) -> None:
        """Compare generated SQL steps to provided snippets and record usage."""
        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_utils import track_snippet_usage

        library = SnippetLibrary(session, manager)

        # Normalize Pydantic steps to list[dict] for the shared function
        generated_steps = [
            {"step_id": s.step_id, "sql": s.sql, "description": s.description or ""}
            for s in analysis_output.steps
        ]

        track_snippet_usage(
            library=library,
            execution_id=execution_id,
            execution_type="query",
            provided_snippets=provided_snippets,
            generated_steps=generated_steps,
        )

    def _save_novel_snippets(
        self,
        session: Session,
        execution_id: str,
        analysis_output: QueryAnalysisOutput,
        schema_mapping_id: str,
        provided_snippets: dict[str, dict[str, str]],
        manager: ConnectionManager,
    ) -> None:
        """Save novel query steps as snippets for future reuse.

        After a successful query execution, saves any freshly generated steps
        (not reused from existing snippets) as "query" type snippets.

        Args:
            session: SQLAlchemy session
            execution_id: Query execution ID
            analysis_output: Generated query analysis with steps
            schema_mapping_id: Schema mapping identifier (source_id)
            provided_snippets: Dict of step_id -> snippet info that were provided
            manager: ConnectionManager for embeddings
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session, manager)

        saved_count = 0
        for step in analysis_output.steps:
            # Skip steps that came from existing snippets
            if step.step_id in provided_snippets:
                continue

            # Save as a query-type snippet
            library.save_snippet(
                snippet_type="query",
                sql=step.sql,
                description=step.description or f"Query step: {step.step_id}",
                schema_mapping_id=schema_mapping_id,
                source=f"query:{execution_id}",
                standard_field=step.step_id,
                column_mappings=analysis_output.column_mappings,
            )
            saved_count += 1

        if saved_count:
            logger.info(f"Saved {saved_count} novel query snippet(s) for reuse")

    def _generate_query(
        self,
        question: str,
        execution_id: str,
        execution_context: GraphExecutionContext,
        discovered_snippets: list[dict[str, Any]] | None = None,
    ) -> Result[QueryAnalysisOutput]:
        """Use LLM to analyze question and generate SQL."""
        # Build schema information
        schema_info = self._build_schema_info(execution_context)

        # Format context for prompt
        context_str = format_context_for_prompt(execution_context)

        # Format entropy warnings from execution_context
        entropy_warnings = ""
        if execution_context.entropy_summary:
            from dataraum.graphs.context import format_entropy_for_prompt

            entropy_warnings = format_entropy_for_prompt(execution_context)

        # Format discovered snippets for injection
        snippets_str = ""
        if discovered_snippets:
            snippets_str = json.dumps(
                [
                    {
                        "step_id": s["step_id"],
                        "sql": s["sql"],
                        "description": s["description"],
                        "type": s.get("snippet_type", "unknown"),
                        "source": s.get("source", "unknown"),
                        "similarity": round(s.get("similarity", 0.0), 2),
                    }
                    for s in discovered_snippets
                ],
                indent=2,
            )

        # Build field mappings string
        field_mappings_str = ""
        if execution_context.field_mappings:
            from dataraum.graphs.field_mapping import format_mappings_for_prompt

            field_mappings_str = format_mappings_for_prompt(execution_context.field_mappings)

        # Build prompt context
        prompt_context = {
            "question": question,
            "schema_info": json.dumps(schema_info, indent=2),
            "dataset_context": context_str,
            "field_mappings": field_mappings_str,
            "entropy_warnings": entropy_warnings
            or "Data quality assessment not available - proceed with caution.",
            "validated_snippets": snippets_str,
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
            tool_choice={"type": "tool", "name": "analyze_query"},
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
        """Build schema information for prompt.

        Note: Table names are prefixed with 'typed_' to match the actual
        DuckDB table names. The context stores base names but DuckDB has
        layer-prefixed tables (typed_*, raw_*, quarantine_*).
        """
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

            # Use the actual DuckDB table name (typed_ prefix)
            duckdb_table_name = f"typed_{table_ctx.table_name}"

            tables.append(
                {
                    "name": duckdb_table_name,
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


    def _record_execution(
        self,
        session: Session,
        source_id: str,
        question: str,
        sql: str,
        success: bool,
        row_count: int | None,
        error_message: str | None,
        confidence_level: ConfidenceLevel,
        contract: str | None,
        entropy_action: str | None = None,
    ) -> None:
        """Record a query execution for audit trail.

        Args:
            session: SQLAlchemy session
            source_id: Source ID
            question: Question asked
            sql: SQL executed
            success: Whether execution succeeded
            row_count: Number of rows returned
            error_message: Error if failed
            confidence_level: Confidence level
            contract: Contract name
            entropy_action: Entropy action determined at query time
        """
        from dataraum.query.db_models import QueryExecutionRecord

        record = QueryExecutionRecord(
            execution_id=str(uuid4()),
            source_id=source_id,
            question=question,
            sql_executed=sql,
            executed_at=datetime.now(UTC),
            success=success,
            row_count=row_count,
            error_message=error_message,
            confidence_level=confidence_level.value,
            contract_name=contract,
            entropy_action=entropy_action,
        )
        session.add(record)

    def _execute_with_steps(
        self,
        analysis_output: QueryAnalysisOutput,
        duckdb_conn: duckdb.DuckDBPyConnection,
        execution_context: GraphExecutionContext,
    ) -> Result[tuple[list[str], list[dict[str, Any]]]]:
        """Execute query with step-by-step temp view creation.

        Delegates to shared execute_sql_steps() for the common pattern of
        creating temp views per step, executing final SQL, and cleanup.

        Args:
            analysis_output: Generated query analysis with steps
            duckdb_conn: DuckDB connection
            execution_context: Context for schema info (used in repair)

        Returns:
            Result with (columns, data) on success
        """
        from dataraum.query.execution import SQLStep, execute_sql_steps

        # Convert analysis steps to shared format
        steps = [
            SQLStep(step_id=s.step_id, sql=s.sql, description=s.description)
            for s in analysis_output.steps
        ]

        # Create repair function that captures execution_context
        def repair_fn(failed_sql: str, error_msg: str, description: str) -> Result[str]:
            return self._repair_sql(
                failed_sql=failed_sql,
                error_message=error_msg,
                step_description=description,
                execution_context=execution_context,
            )

        result = execute_sql_steps(
            steps=steps,
            final_sql=analysis_output.final_sql,
            duckdb_conn=duckdb_conn,
            max_repair_attempts=2,
            repair_fn=repair_fn,
            return_table=True,
        )

        if not result.success or not result.value:
            return Result.fail(result.error or "Execution failed")

        exec_result = result.value
        columns = exec_result.columns or []
        rows = exec_result.rows or []
        data = [dict(zip(columns, row, strict=True)) for row in rows]
        return Result.ok((columns, data))

    def _repair_sql(
        self,
        failed_sql: str,
        error_message: str,
        step_description: str,
        execution_context: GraphExecutionContext,
    ) -> Result[str]:
        """Attempt to repair failed SQL using LLM.

        Args:
            failed_sql: The SQL that failed
            error_message: Error message from DuckDB
            step_description: What the SQL should do
            execution_context: Context for schema info

        Returns:
            Result with repaired SQL on success
        """
        schema_info = self._build_schema_info(execution_context)

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "sql_repair",
                {
                    "error_message": error_message,
                    "failed_sql": failed_sql,
                    "table_schema": json.dumps(schema_info, indent=2),
                    "step_description": step_description,
                },
            )
        except Exception as e:
            return Result.fail(f"Failed to render repair prompt: {e}")

        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
        )

        result = self.provider.converse(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "Repair LLM call failed")

        repaired_sql = result.value.content.strip()

        # Strip markdown code blocks if present
        if repaired_sql.startswith("```"):
            lines = repaired_sql.split("\n")
            if lines[-1].strip() == "```":
                repaired_sql = "\n".join(lines[1:-1])
            else:
                repaired_sql = "\n".join(lines[1:])

        return Result.ok(repaired_sql)
