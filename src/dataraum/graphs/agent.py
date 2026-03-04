"""Unified Graph Agent for SQL generation and execution.

This agent handles ALL graph types (filters and metrics) through a unified approach:
1. Load graph specification (YAML with accounting context)
2. Analyze actual data schema (columns, types, samples)
3. Look up cached SQL snippets from the knowledge base
4. Use LLM to generate executable SQL (with snippet hints)
5. Cache generated SQL in-memory + save as snippets for cross-agent reuse
6. Execute SQL and capture results

See docs/CALCULATION_ENGINE_DESIGN.md for full architecture.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import duckdb
import yaml
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.config import LLMConfig
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.prompts import PromptRenderer
from dataraum.llm.providers.base import LLMProvider

from .models import (
    AssumptionBasis,
    DatasetSchemaMapping,
    GraphExecution,
    GraphSQLGenerationOutput,
    QueryAssumption,
    StepResult,
    StepType,
    TransformationGraph,
)

logger = get_logger(__name__)


@dataclass
class GeneratedCode:
    """LLM-generated SQL for a specific graph + schema combination."""

    code_id: str
    graph_id: str
    graph_version: str
    schema_mapping_id: str

    # Generated SQL
    summary: str  # Plain English description of what the query calculates
    steps: list[dict[str, str]]  # List of {step_id, sql, description}
    final_sql: str
    column_mappings: dict[str, str]  # abstract_field -> concrete_column

    # Generation metadata
    llm_model: str
    prompt_hash: str
    generated_at: datetime

    # Validation
    is_validated: bool = False
    validation_errors: list[str] = field(default_factory=list)


@dataclass
class ExecutionContext:
    """Context for graph execution."""

    duckdb_conn: duckdb.DuckDBPyConnection
    table_name: str = ""  # Deprecated: use rich_context.tables for multi-table access
    schema_mapping: DatasetSchemaMapping | None = None
    schema_mapping_id: str | None = None
    period: str | None = None
    is_period_final: bool = False
    filter_execution_id: str | None = None

    # Rich metadata context (optional)
    # When provided, gives the LLM additional information about:
    # - Column semantics (roles, entity types)
    # - Statistical profiles (null ratios, outliers)
    # - Table relationships and topology
    # - Quality flags
    # - Entropy scores and data readiness
    rich_context: Any | None = None  # GraphExecutionContext from graphs.context

    @classmethod
    def with_rich_context(
        cls,
        session: Any,  # Session
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        **kwargs: Any,
    ) -> ExecutionContext:
        """Create ExecutionContext with rich metadata loaded from analysis modules.

        This is the recommended way to create an ExecutionContext when you want
        the LLM to have access to semantic, statistical, and relational metadata.

        Args:
            session: SQLAlchemy session
            duckdb_conn: DuckDB connection for queries
            table_ids: List of table IDs to include in context
            **kwargs: Additional ExecutionContext arguments

        Returns:
            ExecutionContext with rich_context populated
        """
        from dataraum.graphs.context import build_execution_context

        # Pop table_name if passed (backward compat), but it's no longer used
        kwargs.pop("table_name", None)

        rich_context = build_execution_context(
            session=session,
            table_ids=table_ids,
            duckdb_conn=duckdb_conn,
            slice_column=kwargs.pop("slice_column", None),
            slice_value=kwargs.pop("slice_value", None),
        )

        return cls(
            duckdb_conn=duckdb_conn,
            rich_context=rich_context,
            **kwargs,
        )


class GraphAgent(LLMFeature):
    """Unified agent for executing any graph type.

    The agent:
    1. Takes a graph specification and data schema
    2. Uses LLM to generate executable SQL
    3. Caches generated SQL for reuse
    4. Executes SQL deterministically
    5. Returns traced results
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
    ):
        """Initialize the graph agent."""
        super().__init__(config, provider, prompt_renderer)
        self._code_cache: dict[str, GeneratedCode] = {}  # In-memory cache

    def execute(
        self,
        session: Session,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any] | None = None,
        force_regenerate: bool = False,
    ) -> Result[GraphExecution]:
        """Execute a graph by generating and running SQL.

        Args:
            session: Database session for LLM cache
            graph: The graph specification to execute
            context: Execution context with data connection
            parameters: Parameter values for the graph
            force_regenerate: If True, regenerate SQL even if cached

        Returns:
            Result containing GraphExecution with results
        """
        parameters = parameters or {}

        # Resolve parameters with defaults
        resolved_params = self._resolve_parameters(graph, parameters)

        # Generate cache key
        schema_mapping_id = context.schema_mapping_id or "default"
        cache_key = self._cache_key(graph, schema_mapping_id)

        # Check in-memory cache for generated code
        generated_code: GeneratedCode | None = None

        if not force_regenerate:
            # Check in-memory cache
            generated_code = self._code_cache.get(cache_key)

        if generated_code is None:
            # Check snippet library for cached individual steps
            cached_snippets = self._lookup_snippets(
                session,
                graph,
                schema_mapping_id,
                resolved_params,
            )

            # If ALL steps have cached snippets, assemble without LLM
            if cached_snippets and len(cached_snippets) == len(graph.steps):
                generated_code = self._assemble_from_snippets(
                    graph, context, cached_snippets, resolved_params
                )
                if generated_code:
                    logger.debug(
                        "assembled_from_snippets",
                        graph_id=graph.graph_id,
                        snippet_count=len(cached_snippets),
                    )
                    # Track usage: all steps were exact reuses
                    self._track_snippet_usage(
                        session=session,
                        execution_id=generated_code.code_id,
                        cached_snippets=cached_snippets,
                        generated_steps=generated_code.steps,
                    )
            else:
                # Generate SQL using LLM (with cached snippet hints)
                gen_result = self._generate_sql(
                    session,
                    graph,
                    context,
                    resolved_params,
                    cached_snippets=cached_snippets if cached_snippets else None,
                )
                if not gen_result.success or not gen_result.value:
                    return Result.fail(gen_result.error or "SQL generation failed")

                generated_code = gen_result.value

                # Track usage: compare generated steps against provided snippets
                self._track_snippet_usage(
                    session=session,
                    execution_id=generated_code.code_id,
                    cached_snippets=cached_snippets or {},
                    generated_steps=generated_code.steps,
                )

            if generated_code is None:
                return Result.fail("Failed to generate or assemble SQL code")

            self._code_cache[cache_key] = generated_code

            # Save generated steps as snippets for cross-graph reuse
            self._save_snippets(
                session=session,
                graph=graph,
                generated_code=generated_code,
                schema_mapping_id=schema_mapping_id,
            )

        # Execute the generated SQL
        exec_result = self._execute_sql(generated_code, context, graph, resolved_params)
        if not exec_result.success or not exec_result.value:
            return Result.fail(exec_result.error or "SQL execution failed")

        execution = exec_result.value

        return Result.ok(execution)

    def _assemble_from_snippets(
        self,
        graph: TransformationGraph,
        context: ExecutionContext,
        cached_snippets: dict[str, dict[str, Any]],
        parameters: dict[str, Any],
    ) -> GeneratedCode | None:
        """Assemble GeneratedCode from cached snippets without LLM call.

        When ALL graph steps have cached SQL snippets, we can skip the LLM
        entirely and assemble the generated code from the cache.

        Args:
            graph: Graph specification
            context: Execution context
            cached_snippets: Dict of step_id -> {sql, description, snippet_id}
            parameters: Resolved parameter values

        Returns:
            GeneratedCode if assembly succeeds, None if not possible
        """
        steps = []
        merged_column_mappings: dict[str, str] = {}
        for step_id in graph.steps:
            snippet = cached_snippets.get(step_id)
            if not snippet:
                return None  # Missing a step, can't assemble
            steps.append(
                {
                    "step_id": step_id,
                    "sql": snippet["sql"],
                    "description": snippet["description"],
                }
            )
            # Merge column_mappings from each snippet
            snippet_mappings = snippet.get("column_mappings")
            if isinstance(snippet_mappings, dict):
                merged_column_mappings.update(snippet_mappings)

        # Build final_sql by referencing the output step
        output_step = graph.get_output_step()
        if output_step:
            final_sql = f"SELECT * FROM {output_step.step_id}"
        else:
            # Fallback: select from last step
            final_sql = f"SELECT * FROM {steps[-1]['step_id']}"

        return GeneratedCode(
            code_id=str(uuid4()),
            graph_id=graph.graph_id,
            graph_version=graph.version,
            schema_mapping_id=context.schema_mapping_id or "default",
            summary=f"Assembled from {len(steps)} cached snippets",
            steps=steps,
            final_sql=final_sql,
            column_mappings=merged_column_mappings,
            llm_model="cached",
            prompt_hash="snippets",
            generated_at=datetime.now(UTC),
            is_validated=True,  # Snippets are pre-validated
        )

    def _generate_sql(
        self,
        session: Session,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any],
        cached_snippets: dict[str, dict[str, Any]] | None = None,
    ) -> Result[GeneratedCode]:
        """Use LLM to generate SQL from graph specification using tool-based output."""
        from dataraum.llm.providers.base import (
            ConversationRequest,
            Message,
            ToolDefinition,
        )

        # Build multi-table schema from rich context
        schema_info = self._build_schema_info(context)

        # Serialize graph to YAML for LLM context
        graph_yaml = self._graph_to_yaml(graph)

        # Build prompt context
        prompt_context = {
            "graph_yaml": graph_yaml,
            "graph_type": graph.graph_type.value,
            "table_schema": json.dumps(schema_info, indent=2),
            "parameters": json.dumps(parameters, indent=2),
        }

        # Inject cached snippets into prompt context
        if cached_snippets:
            prompt_context["cached_steps"] = json.dumps(
                {
                    step_id: {"sql": info["sql"], "description": info["description"]}
                    for step_id, info in cached_snippets.items()
                },
                indent=2,
            )
        else:
            prompt_context["cached_steps"] = ""

        # Add rich context if available (provides semantic, statistical, relational metadata)
        if context.rich_context is not None:
            from dataraum.graphs.context import (
                format_context_for_prompt,
                format_entropy_for_prompt,
            )

            prompt_context["rich_context"] = format_context_for_prompt(context.rich_context)

            # Add entropy warnings if entropy data is available
            entropy_section = format_entropy_for_prompt(context.rich_context)
            if entropy_section:
                prompt_context["entropy_warnings"] = entropy_section
            else:
                prompt_context["entropy_warnings"] = ""

            # Add field mappings if available (for resolving standard_field references)
            if (
                hasattr(context.rich_context, "field_mappings")
                and context.rich_context.field_mappings
            ):
                from dataraum.graphs.field_mapping import format_mappings_for_prompt

                prompt_context["field_mappings"] = format_mappings_for_prompt(
                    context.rich_context.field_mappings
                )
            else:
                prompt_context["field_mappings"] = ""
        else:
            prompt_context["rich_context"] = ""
            prompt_context["field_mappings"] = ""
            prompt_context["entropy_warnings"] = ""

        # Render prompt with system/user split
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "graph_sql_generation", prompt_context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Compute prompt hash for reproducibility
        prompt_hash = hashlib.sha256(user_prompt.encode()).hexdigest()[:16]

        # Define tool for structured output
        tool = ToolDefinition(
            name="generate_sql",
            description="Provide generated SQL for the graph specification",
            input_schema=GraphSQLGenerationOutput.model_json_schema(),
        )

        # Get model for this feature (graph_sql_generation uses balanced tier)
        model = self.provider.get_model_for_tier("balanced")

        # Call LLM with tool use
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "generate_sql"},
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        result = self.provider.converse(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "LLM call failed")

        response = result.value

        # Extract tool call result
        if not response.tool_calls:
            # LLM didn't use the tool - try to parse text as fallback
            if response.content:
                try:
                    response_data = json.loads(response.content)
                    output = GraphSQLGenerationOutput.model_validate(response_data)
                except Exception:
                    return Result.fail(f"LLM did not use tool. Response: {response.content[:200]}")
            else:
                return Result.fail("LLM did not use the generate_sql tool")
        else:
            # Parse tool response using Pydantic model
            tool_call = response.tool_calls[0]
            if tool_call.name != "generate_sql":
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

            try:
                output = GraphSQLGenerationOutput.model_validate(tool_call.input)
            except Exception as e:
                return Result.fail(f"Failed to validate tool response: {e}")

        # Create GeneratedCode from Pydantic output
        generated_code = GeneratedCode(
            code_id=str(uuid4()),
            graph_id=graph.graph_id,
            graph_version=graph.version,
            schema_mapping_id=context.schema_mapping_id or "default",
            summary=output.summary,
            steps=[
                {
                    "step_id": step.step_id,
                    "sql": step.sql,
                    "description": step.description,
                }
                for step in output.steps
            ],
            final_sql=output.final_sql,
            column_mappings=output.column_mappings,
            llm_model=model,
            prompt_hash=prompt_hash,
            generated_at=datetime.now(UTC),
        )

        # Validate generated SQL
        validation_result = self._validate_sql(generated_code, context)
        generated_code.is_validated = validation_result.success
        if not validation_result.success:
            generated_code.validation_errors = [validation_result.error or "Unknown"]
            logger.warning(
                "sql_validation_failed",
                graph_id=graph.graph_id,
                error=validation_result.error,
            )

        return Result.ok(generated_code)

    def _execute_sql(
        self,
        generated_code: GeneratedCode,
        context: ExecutionContext,
        graph: TransformationGraph,
        parameters: dict[str, Any],
    ) -> Result[GraphExecution]:
        """Execute generated SQL and capture results.

        Delegates step execution to shared execute_sql_steps(), then enriches
        the GraphExecution with entropy info, assumptions, and interpretation.
        """
        from dataraum.query.execution import SQLStep, execute_sql_steps

        # Create execution record
        execution = GraphExecution.create(graph, parameters, context.period)
        execution.is_period_final = context.is_period_final

        if context.filter_execution_id:
            execution.depends_on_executions.append(context.filter_execution_id)

        # Extract entropy information from rich context
        entropy_info = self._extract_entropy_info(context)
        execution.max_entropy_score = entropy_info.get("max_entropy", 0.0)
        execution.entropy_warnings = entropy_info.get("warnings", [])

        # Create assumptions from high-entropy columns
        execution.assumptions = self._create_assumptions_from_entropy(
            execution.execution_id, entropy_info, context
        )

        # Get max repair attempts from config (default 2)
        feature_config = getattr(self.config.features, "sql_repair", None)
        max_repair_attempts = (
            getattr(feature_config, "max_repair_attempts", 2) if feature_config else 2
        )

        # Convert generated code steps to shared format
        steps = [
            SQLStep(
                step_id=s.get("step_id", "unknown"),
                sql=s.get("sql", ""),
                description=s.get("description", ""),
            )
            for s in generated_code.steps
        ]

        # Create repair function that captures context
        def repair_fn(failed_sql: str, error_msg: str, description: str) -> Result[str]:
            return self._repair_sql(
                failed_sql=failed_sql,
                error_message=error_msg,
                context=context,
                step_description=description,
            )

        # Execute using shared function
        exec_result = execute_sql_steps(
            steps=steps,
            final_sql=generated_code.final_sql,
            duckdb_conn=context.duckdb_conn,
            max_repair_attempts=max_repair_attempts,
            repair_fn=repair_fn,
            return_table=False,
        )

        if not exec_result.success or not exec_result.value:
            return Result.fail(exec_result.error or "SQL execution failed")

        result = exec_result.value

        # Build StepResult objects from shared execution results
        for sr in result.step_results:
            step_result = StepResult(
                step_id=sr.step_id,
                level=1,
                step_type=StepType.FORMULA,
                source_query=sr.sql_executed,
                inputs_used={
                    "sql": sr.sql_executed,
                    "view_name": sr.step_id,
                    "repair_attempts": sr.repair_attempts,
                },
            )
            value = sr.value
            if isinstance(value, (int, float)):
                step_result.value_scalar = float(value)
            elif isinstance(value, bool):
                step_result.value_boolean = value
            elif isinstance(value, str):
                step_result.value_string = value

            execution.step_results.append(step_result)

        execution.output_value = result.final_value

        # Add interpretation if available
        if graph.interpretation and execution.output_value is not None:
            execution.output_interpretation = self._interpret_value(execution.output_value, graph)

        # Generate execution hash
        execution.execution_hash = self._generate_execution_hash(
            graph, parameters, context, generated_code.code_id
        )

        return Result.ok(execution)

    def _extract_entropy_info(self, context: ExecutionContext) -> dict[str, Any]:
        """Extract entropy information from the execution context.

        Returns dict with:
            - max_entropy: Highest entropy score encountered
            - warnings: List of warning messages
            - high_entropy_columns: List of columns with entropy > 0.6
            - readiness_blockers: List of blocked column descriptions
        """
        result: dict[str, Any] = {
            "max_entropy": 0.0,
            "warnings": [],
            "high_entropy_columns": [],
            "readiness_blockers": [],
        }

        if context.rich_context is None:
            return result

        # Check for entropy_summary
        entropy_summary = getattr(context.rich_context, "entropy_summary", None)
        if not entropy_summary:
            return result

        # Extract entropy counts (network-derived)
        high_count = entropy_summary.get("high_entropy_count", 0)
        critical_count = entropy_summary.get("critical_entropy_count", 0)
        blockers = entropy_summary.get("readiness_blockers", [])

        # Build warnings
        if critical_count > 0:
            result["warnings"].append(f"{critical_count} columns have critical entropy")
        if high_count > 0:
            result["warnings"].append(f"{high_count} columns have high uncertainty")

        # Find max entropy from tables
        tables = getattr(context.rich_context, "tables", [])
        for table in tables:
            for col in getattr(table, "columns", []):
                entropy_scores = getattr(col, "entropy_scores", None)
                if entropy_scores:
                    score = entropy_scores.get("worst_intent_p_high", 0.0)
                    if score > result["max_entropy"]:
                        result["max_entropy"] = score
                    readiness = entropy_scores.get("readiness", "ready")
                    if readiness != "ready" or score > 0.3:
                        result["high_entropy_columns"].append(
                            {
                                "table": getattr(table, "table_name", "unknown"),
                                "column": getattr(col, "column_name", "unknown"),
                                "score": score,
                                "dimensions": entropy_scores.get("high_entropy_dimensions", []),
                            }
                        )

        # Add blockers
        result["readiness_blockers"] = blockers

        return result

    def _create_assumptions_from_entropy(
        self,
        execution_id: str,
        entropy_info: dict[str, Any],
        context: ExecutionContext,
    ) -> list[QueryAssumption]:
        """Create QueryAssumption objects from entropy interpretations.

        Extracts LLM-generated assumptions from the entropy context and
        converts them to QueryAssumption objects for execution tracking.

        Args:
            execution_id: ID of the current execution
            entropy_info: Extracted entropy info with high_entropy_columns
            context: Execution context with rich_context

        Returns:
            List of QueryAssumption objects
        """
        assumptions: list[QueryAssumption] = []

        if context.rich_context is None:
            return assumptions

        # Get tables from rich context
        tables = getattr(context.rich_context, "tables", [])

        for table in tables:
            for col in getattr(table, "columns", []):
                entropy_scores = getattr(col, "entropy_scores", None)
                if not entropy_scores:
                    continue

                # Check if this column has interpretation data
                interpretation = entropy_scores.get("interpretation")
                if not interpretation:
                    continue

                # Only include assumptions for columns with meaningful entropy
                p_high = entropy_scores.get("worst_intent_p_high", 0.0)
                if p_high < 0.3:
                    continue

                table_name = getattr(table, "table_name", "unknown")
                column_name = getattr(col, "column_name", "unknown")
                target = f"column:{table_name}.{column_name}"

                # Convert interpretation assumptions to QueryAssumptions
                for assumption_data in interpretation.get("assumptions", []):
                    # Map basis string to AssumptionBasis enum
                    basis_str = assumption_data.get("basis", "inferred")
                    if basis_str == "default":
                        basis = AssumptionBasis.SYSTEM_DEFAULT
                    elif basis_str == "user_specified":
                        basis = AssumptionBasis.USER_SPECIFIED
                    else:
                        basis = AssumptionBasis.INFERRED

                    # Map confidence string to float
                    confidence_str = assumption_data.get("confidence", "medium")
                    confidence_map = {"high": 0.9, "medium": 0.6, "low": 0.3}
                    confidence = confidence_map.get(confidence_str, 0.6)

                    assumptions.append(
                        QueryAssumption.create(
                            execution_id=execution_id,
                            dimension=assumption_data.get("dimension", "unknown"),
                            target=target,
                            assumption=assumption_data.get("assumption_text", ""),
                            basis=basis,
                            confidence=confidence,
                        )
                    )

        return assumptions

    def _build_schema_info(
        self,
        context: ExecutionContext,
    ) -> dict[str, Any]:
        """Build multi-table schema information from rich context and DuckDB.

        When enriched views exist, only includes those (they are pre-joined
        supersets of typed tables). Falls back to typed tables otherwise.

        Returns:
            Dict with 'tables' list, each containing name, columns (with
            sample_values), and row_count.
        """
        tables: list[dict[str, Any]] = []

        if context.rich_context is not None:
            if context.rich_context.enriched_views:
                # Prefer enriched views — pre-joined with dimension columns
                for ev in context.rich_context.enriched_views:
                    table_info = self._describe_table(context.duckdb_conn, ev.view_name)
                    if table_info:
                        tables.append(table_info)
            else:
                # Fallback: typed tables when no enriched views exist
                for table_ctx in context.rich_context.tables:
                    duckdb_name = table_ctx.duckdb_name or table_ctx.table_name
                    table_info = self._describe_table(context.duckdb_conn, duckdb_name)
                    if table_info:
                        tables.append(table_info)

        return {"tables": tables}

    @staticmethod
    def _describe_table(
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_name: str,
    ) -> dict[str, Any] | None:
        """DESCRIBE a single DuckDB table and return schema with sample values."""
        try:
            columns_result = duckdb_conn.execute(f'DESCRIBE "{table_name}"').fetchall()

            columns = []
            for col in columns_result:
                col_name = col[0]
                col_type = col[1]

                sample_result = duckdb_conn.execute(
                    f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
                    f'WHERE "{col_name}" IS NOT NULL LIMIT 5'
                ).fetchall()
                samples = [str(r[0]) for r in sample_result]

                columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "sample_values": samples,
                    }
                )

            count_result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
            row_count = count_result[0] if count_result else 0

            return {
                "table_name": table_name,
                "columns": columns,
                "row_count": row_count,
            }
        except Exception:
            logger.warning("describe_table_failed", table=table_name)
            return None

    def _graph_to_yaml(self, graph: TransformationGraph) -> str:
        """Serialize graph to YAML for LLM context."""
        # Convert graph to dict for YAML serialization
        graph_dict: dict[str, Any] = {
            "graph_id": graph.graph_id,
            "graph_type": graph.graph_type.value,
            "version": graph.version,
            "metadata": {
                "name": graph.metadata.name,
                "description": graph.metadata.description,
                "category": graph.metadata.category,
            },
            "output": {
                "type": graph.output.output_type.value if graph.output else None,
                "metric_id": graph.output.metric_id if graph.output else None,
                "unit": graph.output.unit if graph.output else None,
            },
            "parameters": [
                {
                    "name": p.name,
                    "type": p.param_type,
                    "default": p.default,
                    "description": p.description,
                }
                for p in graph.parameters
            ],
            "dependencies": {
                step_id: {
                    "level": step.level,
                    "type": step.step_type.value,
                    "source": {
                        "standard_field": step.source.standard_field if step.source else None,
                        "statement": step.source.statement if step.source else None,
                    }
                    if step.source
                    else None,
                    "expression": step.expression,
                    "condition": step.condition,
                    "aggregation": step.aggregation,
                    "depends_on": step.depends_on,
                }
                for step_id, step in graph.steps.items()
            },
        }

        if graph.interpretation:
            graph_dict["interpretation"] = {
                "ranges": [
                    {
                        "min": r.min_value,
                        "max": r.max_value,
                        "label": r.label,
                        "description": r.description,
                    }
                    for r in graph.interpretation.ranges
                ]
            }

        return yaml.dump(graph_dict, default_flow_style=False, allow_unicode=True)

    def _validate_sql(
        self, generated_code: GeneratedCode, context: ExecutionContext
    ) -> Result[bool]:
        """Validate generated SQL syntax and column references."""
        # Try to explain the SQL (validates syntax without executing)
        try:
            context.duckdb_conn.execute(f"EXPLAIN {generated_code.final_sql}")
            return Result.ok(True)
        except Exception as e:
            return Result.fail(f"SQL validation failed: {e}")

    def _repair_sql(
        self,
        failed_sql: str,
        error_message: str,
        context: ExecutionContext,
        step_description: str = "",
    ) -> Result[str]:
        """Use LLM to repair SQL that failed validation or execution.

        Uses the sql_repair feature from llm.yaml with proper caching
        and model tier configuration.

        Args:
            failed_sql: The SQL that failed
            error_message: Error message from DuckDB
            context: Execution context with table schema
            step_description: What the SQL should accomplish

        Returns:
            Result containing repaired SQL or error
        """
        from dataraum.llm.providers.base import ConversationRequest, Message

        # Check if sql_repair feature is enabled
        feature_config = getattr(self.config.features, "sql_repair", None)
        if not feature_config or not getattr(feature_config, "enabled", True):
            return Result.fail("SQL repair feature is disabled")

        # Get model tier from config (default to fast)
        model_tier = getattr(feature_config, "model_tier", "fast")

        # Build multi-table schema for context
        schema_info = self._build_schema_info(context)

        # Build prompt context
        prompt_context = {
            "error_message": error_message,
            "failed_sql": failed_sql,
            "table_schema": json.dumps(schema_info, indent=2),
            "step_description": step_description or "Execute the query",
        }

        # Render repair prompt
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "sql_repair", prompt_context
            )
        except Exception as e:
            return Result.fail(f"Failed to render repair prompt: {e}")

        # Call LLM with configured model tier
        # Note: SQL repairs are not cached since errors are typically unique situations
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=self.provider.get_model_for_tier(model_tier),
        )

        result = self.provider.converse(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "LLM repair call failed")

        response = result.value
        if not response.content:
            return Result.fail("LLM returned empty response")

        # Extract SQL from response (strip markdown code blocks if present)
        repaired_sql = response.content.strip()
        if repaired_sql.startswith("```sql"):
            repaired_sql = repaired_sql[6:]
        if repaired_sql.startswith("```"):
            repaired_sql = repaired_sql[3:]
        if repaired_sql.endswith("```"):
            repaired_sql = repaired_sql[:-3]
        repaired_sql = repaired_sql.strip()

        return Result.ok(repaired_sql)

    def _resolve_parameters(
        self, graph: TransformationGraph, provided: dict[str, Any]
    ) -> dict[str, Any]:
        """Merge provided parameters with graph defaults."""
        resolved = {}
        for param in graph.parameters:
            if param.name in provided:
                resolved[param.name] = provided[param.name]
            elif param.default is not None:
                resolved[param.name] = param.default
        return resolved

    def _interpret_value(self, value: Any, graph: TransformationGraph) -> str | None:
        """Interpret a metric value based on defined ranges."""
        if not graph.interpretation or not graph.interpretation.ranges:
            return None

        if not isinstance(value, (int, float)):
            return None

        for range_def in graph.interpretation.ranges:
            if range_def.min_value <= value <= range_def.max_value:
                return range_def.label

        return None

    def _cache_key(self, graph: TransformationGraph, schema_mapping_id: str) -> str:
        """Generate cache key for generated code."""
        return f"{graph.graph_id}:{graph.version}:{schema_mapping_id}"

    def _generate_execution_hash(
        self,
        graph: TransformationGraph,
        parameters: dict[str, Any],
        context: ExecutionContext,
        code_id: str,
    ) -> str:
        """Generate hash for reproducibility verification."""
        hash_input = {
            "graph": f"{graph.graph_id}@{graph.version}",
            "params": sorted(parameters.items()),
            "schema_mapping_id": context.schema_mapping_id,
            "filter_execution_id": context.filter_execution_id,
            "period": context.period,
            "code_id": code_id,
        }

        return hashlib.sha256(
            json.dumps(hash_input, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]

    def _track_snippet_usage(
        self,
        session: Session,
        execution_id: str,
        cached_snippets: dict[str, dict[str, Any]],
        generated_steps: list[dict[str, str]],
    ) -> None:
        """Track how cached snippets were used in graph execution."""
        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_utils import determine_usage_type

        library = SnippetLibrary(session)
        used_snippet_ids: set[str] = set()

        for gen_step in generated_steps:
            step_id = gen_step.get("step_id", "")
            provided = cached_snippets.get(step_id)

            if provided is None:
                library.record_usage(
                    execution_id=execution_id,
                    execution_type="graph",
                    usage_type="newly_generated",
                    step_id=step_id,
                )
            else:
                snippet_id = provided.get("snippet_id")
                usage_type = determine_usage_type(
                    gen_step.get("sql", ""),
                    provided.get("sql", ""),
                )
                is_exact = usage_type == "exact_reuse"
                library.record_usage(
                    execution_id=execution_id,
                    execution_type="graph",
                    usage_type=usage_type,
                    snippet_id=snippet_id,
                    match_confidence=1.0,
                    sql_match_ratio=1.0 if is_exact else 0.0,
                    step_id=step_id,
                )
                if snippet_id:
                    used_snippet_ids.add(snippet_id)

        # Provided snippets not used by any generated step
        generated_step_ids = {s.get("step_id", "") for s in generated_steps}
        for step_id, provided in cached_snippets.items():
            if step_id not in generated_step_ids:
                snippet_id = provided.get("snippet_id")
                if snippet_id and snippet_id not in used_snippet_ids:
                    library.record_usage(
                        execution_id=execution_id,
                        execution_type="graph",
                        usage_type="provided_not_used",
                        snippet_id=snippet_id,
                        step_id=step_id,
                    )

    def _save_snippets(
        self,
        session: Session,
        graph: TransformationGraph,
        generated_code: GeneratedCode,
        schema_mapping_id: str,
    ) -> None:
        """Save generated SQL steps as snippets for cross-graph reuse.

        Decomposes the generated code into individual snippets based on the
        graph step definitions. Each graph step maps to a snippet type:
        - extract steps -> extract snippets (keyed by standard_field + statement + aggregation)
        - constant steps -> constant snippets (keyed by parameter + value)
        - formula steps -> formula template snippets (keyed by normalized expression)

        Args:
            session: SQLAlchemy session
            graph: Graph specification (defines step types and metadata)
            generated_code: LLM-generated SQL code
            schema_mapping_id: Schema mapping identifier
        """
        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_utils import normalize_expression

        library = SnippetLibrary(session)

        source = f"graph:{graph.graph_id}"

        # Build a map of generated step_id -> {sql, description}
        generated_steps: dict[str, dict[str, str]] = {}
        for step_dict in generated_code.steps:
            step_id = step_dict.get("step_id", "")
            if step_id:
                generated_steps[step_id] = step_dict

        # Map graph steps to snippets
        for step_id, graph_step in graph.steps.items():
            gen_step = generated_steps.get(step_id)
            if not gen_step:
                continue

            sql = gen_step.get("sql", "")
            description = gen_step.get("description", "")

            if graph_step.step_type == StepType.EXTRACT and graph_step.source:
                # Extract snippet: keyed by standard_field + statement + aggregation
                library.save_snippet(
                    snippet_type="extract",
                    sql=sql,
                    description=description,
                    schema_mapping_id=schema_mapping_id,
                    source=source,
                    standard_field=graph_step.source.standard_field,
                    statement=graph_step.source.statement,
                    aggregation=graph_step.aggregation,
                    column_mappings=generated_code.column_mappings,
                    llm_model=generated_code.llm_model,
                )

            elif graph_step.step_type == StepType.CONSTANT:
                # Constant snippet: keyed by parameter name + resolved value
                param_value = None
                if graph_step.parameter:
                    # Look up the resolved parameter value from the graph defaults
                    for param in graph.parameters:
                        if param.name == graph_step.parameter:
                            param_value = str(param.default) if param.default is not None else None
                            break

                library.save_snippet(
                    snippet_type="constant",
                    sql=sql,
                    description=description,
                    schema_mapping_id=schema_mapping_id,
                    source=source,
                    standard_field=graph_step.parameter or step_id,
                    parameter_value=param_value,
                    llm_model=generated_code.llm_model,
                )

            elif graph_step.step_type == StepType.FORMULA and graph_step.expression:
                # Formula template snippet: keyed by normalized expression
                normalized, sorted_fields, bindings = normalize_expression(graph_step.expression)

                library.save_snippet(
                    snippet_type="formula",
                    sql=sql,
                    description=description,
                    schema_mapping_id=schema_mapping_id,
                    source=source,
                    normalized_expression=normalized,
                    input_fields=sorted_fields,
                    llm_model=generated_code.llm_model,
                )

        logger.debug("saved_snippets", graph_id=graph.graph_id)

    def _lookup_snippets(
        self,
        session: Session,
        graph: TransformationGraph,
        schema_mapping_id: str,
        parameters: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """Look up cached snippets for graph steps before LLM generation.

        For each graph step, check the snippet library for a matching cached SQL.
        Returns a dict of step_id -> {sql, description, snippet_id, column_mappings}
        for steps that have cached SQL.

        Args:
            session: SQLAlchemy session
            graph: Graph specification
            schema_mapping_id: Schema mapping identifier
            parameters: Resolved parameter values

        Returns:
            Dict mapping step_id to cached snippet info for found snippets
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session)

        cached_steps: dict[str, dict[str, Any]] = {}

        for step_id, graph_step in graph.steps.items():
            match = None

            if graph_step.step_type == StepType.EXTRACT and graph_step.source:
                match = library.find_by_key(
                    snippet_type="extract",
                    schema_mapping_id=schema_mapping_id,
                    standard_field=graph_step.source.standard_field,
                    statement=graph_step.source.statement,
                    aggregation=graph_step.aggregation,
                )

            elif graph_step.step_type == StepType.CONSTANT:
                param_value = None
                if graph_step.parameter and graph_step.parameter in parameters:
                    param_value = str(parameters[graph_step.parameter])

                match = library.find_by_key(
                    snippet_type="constant",
                    schema_mapping_id=schema_mapping_id,
                    standard_field=graph_step.parameter or step_id,
                    parameter_value=param_value,
                )

            elif graph_step.step_type == StepType.FORMULA and graph_step.expression:
                match = library.find_by_expression(
                    expression=graph_step.expression,
                    schema_mapping_id=schema_mapping_id,
                )

            if match:
                cached_steps[step_id] = {
                    "sql": match.snippet.sql,
                    "description": match.snippet.description,
                    "snippet_id": match.snippet.snippet_id,
                    "column_mappings": match.snippet.column_mappings or {},
                }

        if cached_steps:
            logger.debug(
                "found_cached_snippets",
                cached=len(cached_steps),
                total=len(graph.steps),
                graph_id=graph.graph_id,
            )

        return cached_steps
