"""Unified Graph Agent for SQL generation and execution.

This agent handles ALL graph types (filters and metrics) through a unified approach:
1. Load graph specification (YAML with accounting context)
2. Analyze actual data schema (columns, types, samples)
3. Use LLM to generate executable SQL
4. Cache generated SQL for deterministic re-execution (in-memory + DB)
5. Execute SQL and capture results

See docs/CALCULATION_ENGINE_DESIGN.md for full architecture.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb
import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.graphs.db_models import GeneratedCodeRecord
from dataraum.llm.cache import LLMCache
from dataraum.llm.config import LLMConfig
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.prompts import PromptRenderer
from dataraum.llm.providers.base import LLMProvider

from .entropy_behavior import (
    EntropyBehaviorConfig,
    format_entropy_sql_comments,
    get_default_config,
)
from .models import (
    AssumptionBasis,
    DatasetSchemaMapping,
    GraphExecution,
    GraphSQLGenerationOutput,
    QueryAssumption,
    SQLStepOutput,
    StepResult,
    StepType,
    TransformationGraph,
)

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager

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
    table_name: str
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

    # Entropy behavior configuration (controls how agent responds to uncertainty)
    entropy_behavior: EntropyBehaviorConfig | None = None

    @classmethod
    def with_rich_context(
        cls,
        session: Any,  # Session
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_name: str,
        table_ids: list[str],
        entropy_behavior_mode: str = "balanced",
        **kwargs: Any,
    ) -> ExecutionContext:
        """Create ExecutionContext with rich metadata loaded from analysis modules.

        This is the recommended way to create an ExecutionContext when you want
        the LLM to have access to semantic, statistical, and relational metadata.

        Args:
            session: SQLAlchemy session
            duckdb_conn: DuckDB connection for queries
            table_name: Primary table name for execution
            table_ids: List of table IDs to include in context
            entropy_behavior_mode: One of "strict", "balanced", "lenient"
            **kwargs: Additional ExecutionContext arguments

        Returns:
            ExecutionContext with rich_context and entropy_behavior populated
        """
        from dataraum.graphs.context import build_execution_context

        rich_context = build_execution_context(
            session=session,
            table_ids=table_ids,
            duckdb_conn=duckdb_conn,
            slice_column=kwargs.pop("slice_column", None),
            slice_value=kwargs.pop("slice_value", None),
        )

        entropy_behavior = get_default_config(entropy_behavior_mode)

        return cls(
            duckdb_conn=duckdb_conn,
            table_name=table_name,
            rich_context=rich_context,
            entropy_behavior=entropy_behavior,
            **kwargs,
        )


@dataclass
class TableSchema:
    """Schema information for a table."""

    table_name: str
    columns: list[dict[str, Any]]  # name, type, sample_values
    row_count: int


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
        cache: LLMCache,
    ):
        """Initialize the graph agent."""
        super().__init__(config, provider, prompt_renderer, cache)
        self._code_cache: dict[str, GeneratedCode] = {}  # In-memory cache

    def execute(
        self,
        session: Session,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any] | None = None,
        force_regenerate: bool = False,
        *,
        manager: ConnectionManager | None = None,
        source_id: str | None = None,
    ) -> Result[GraphExecution]:
        """Execute a graph by generating and running SQL.

        Args:
            session: Database session for LLM cache
            graph: The graph specification to execute
            context: Execution context with data connection
            parameters: Parameter values for the graph
            force_regenerate: If True, regenerate SQL even if cached
            manager: ConnectionManager for library access (enables cross-agent reuse)
            source_id: Source ID for library storage

        Returns:
            Result containing GraphExecution with results
        """
        parameters = parameters or {}

        # Resolve parameters with defaults
        resolved_params = self._resolve_parameters(graph, parameters)

        # Generate cache key
        schema_mapping_id = context.schema_mapping_id or "default"
        cache_key = self._cache_key(graph, schema_mapping_id)

        # Check caches for generated code (in-memory first, then DB)
        generated_code: GeneratedCode | None = None

        if not force_regenerate:
            # Check in-memory cache
            generated_code = self._code_cache.get(cache_key)

            # Check database cache if not in memory
            if generated_code is None:
                db_code = self._load_from_db(
                    session, graph.graph_id, graph.version, schema_mapping_id
                )
                if db_code:
                    generated_code = db_code
                    self._code_cache[cache_key] = generated_code

        if generated_code is None:
            # Generate SQL using LLM
            gen_result = self._generate_sql(session, graph, context, resolved_params)
            if not gen_result.success or not gen_result.value:
                return Result.fail(gen_result.error or "SQL generation failed")

            generated_code = gen_result.value
            self._code_cache[cache_key] = generated_code

            # Persist to database
            self._save_to_db(session, generated_code)

        # Execute the generated SQL
        exec_result = self._execute_sql(generated_code, context, graph, resolved_params)
        if not exec_result.success or not exec_result.value:
            return Result.fail(exec_result.error or "SQL execution failed")

        execution = exec_result.value

        # Save to QueryLibrary for cross-agent reuse
        if manager and source_id:
            self._save_to_library(
                session=session,
                manager=manager,
                source_id=source_id,
                graph=graph,
                generated_code=generated_code,
                execution=execution,
            )

        return exec_result

    def _generate_sql(
        self,
        session: Session,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any],
    ) -> Result[GeneratedCode]:
        """Use LLM to generate SQL from graph specification using tool-based output."""
        from dataraum.llm.providers.base import (
            ConversationRequest,
            Message,
            ToolDefinition,
        )

        # Get table schema
        schema_result = self._get_table_schema(context)
        if not schema_result.success or not schema_result.value:
            return Result.fail(schema_result.error or "Failed to get schema")

        table_schema = schema_result.value

        # Serialize graph to YAML for LLM context
        graph_yaml = self._graph_to_yaml(graph)

        # Build prompt context
        prompt_context = {
            "graph_yaml": graph_yaml,
            "graph_type": graph.graph_type.value,
            "table_schema": json.dumps(
                {
                    "table_name": table_schema.table_name,
                    "columns": table_schema.columns,
                    "row_count": table_schema.row_count,
                },
                indent=2,
            ),
            "parameters": json.dumps(parameters, indent=2),
        }

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
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
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

        return Result.ok(generated_code)

    def _execute_sql(
        self,
        generated_code: GeneratedCode,
        context: ExecutionContext,
        graph: TransformationGraph,
        parameters: dict[str, Any],
    ) -> Result[GraphExecution]:
        """Execute generated SQL and capture results."""
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

        # Add entropy comments to SQL
        entropy_comments = format_entropy_sql_comments(
            execution.max_entropy_score,
            assumptions=[
                {"dimension": a.dimension, "assumption": a.assumption}
                for a in execution.assumptions
            ],
            warnings=execution.entropy_warnings,
        )

        # Execute each step, creating temp views for intermediate results
        # Note: duckdb_conn is actually a cursor (from ConnectionManager.duckdb_cursor())
        # Temp views on a cursor are isolated to that cursor, so no naming conflicts
        # with concurrent executions. LLM-generated SQL can reference steps directly
        # by their step_id (e.g., "SELECT * FROM step_1").
        step_values: dict[str, Any] = {}
        created_views: list[str] = []

        # Get max repair attempts from config (default 2)
        feature_config = getattr(self.config.features, "sql_repair", None)
        max_repair_attempts = (
            getattr(feature_config, "max_repair_attempts", 2) if feature_config else 2
        )

        try:
            for step_info in generated_code.steps:
                step_id = step_info.get("step_id", "unknown")
                sql = step_info.get("sql", "")
                step_description = step_info.get("description", "")

                # Prepend entropy comments to first step only
                if step_info == generated_code.steps[0] and entropy_comments:
                    sql_with_comments = entropy_comments + sql
                else:
                    sql_with_comments = sql

                # Try to execute, with repair attempts on failure
                current_sql = sql
                last_error = None

                for attempt in range(max_repair_attempts + 1):
                    try:
                        # Create a temp view using step_id as the view name
                        view_sql = f"CREATE OR REPLACE TEMP VIEW {step_id} AS {current_sql}"
                        context.duckdb_conn.execute(view_sql)
                        created_views.append(step_id)

                        # Execute the view to get the result
                        result = context.duckdb_conn.execute(f"SELECT * FROM {step_id}").fetchone()
                        value = result[0] if result else None
                        step_values[step_id] = value

                        # Create step result
                        step_result = StepResult(
                            step_id=step_id,
                            level=1,
                            step_type=StepType.FORMULA,
                            source_query=sql_with_comments if attempt == 0 else current_sql,
                            inputs_used={
                                "sql": current_sql,
                                "view_name": step_id,
                                "repair_attempts": attempt,
                            },
                        )

                        if isinstance(value, (int, float)):
                            step_result.value_scalar = float(value)
                        elif isinstance(value, bool):
                            step_result.value_boolean = value
                        elif isinstance(value, str):
                            step_result.value_string = value

                        execution.step_results.append(step_result)
                        break  # Success, exit retry loop

                    except Exception as e:
                        last_error = str(e)
                        if attempt < max_repair_attempts:
                            # Try to repair the SQL
                            repair_result = self._repair_sql(
                                failed_sql=current_sql,
                                error_message=last_error,
                                context=context,
                                step_description=step_description,
                            )
                            if repair_result.success and repair_result.value:
                                current_sql = repair_result.value
                            else:
                                # Repair failed, no point retrying
                                break
                        else:
                            # All attempts exhausted
                            return Result.fail(
                                f"Step {step_id} failed after {attempt + 1} attempts: {last_error}"
                            )
                else:
                    # Loop completed without break = all repairs failed
                    return Result.fail(f"Step {step_id} failed: {last_error}")

            # Execute final SQL with repair attempts
            final_sql = generated_code.final_sql
            last_error = None

            for attempt in range(max_repair_attempts + 1):
                try:
                    final_result = context.duckdb_conn.execute(final_sql).fetchone()
                    execution.output_value = final_result[0] if final_result else None
                    break  # Success

                except Exception as e:
                    last_error = str(e)
                    if attempt < max_repair_attempts:
                        repair_result = self._repair_sql(
                            failed_sql=final_sql,
                            error_message=last_error,
                            context=context,
                            step_description=f"Final calculation for {graph.metadata.name}",
                        )
                        if repair_result.success and repair_result.value:
                            final_sql = repair_result.value
                        else:
                            break
                    else:
                        return Result.fail(
                            f"Final SQL failed after {attempt + 1} attempts: {last_error}"
                        )
            else:
                return Result.fail(f"Final SQL failed: {last_error}")

        finally:
            # Clean up temporary views
            for view_name in created_views:
                try:
                    context.duckdb_conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                except Exception:
                    pass  # Ignore cleanup errors

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
            - compound_risks: List of compound risk descriptions
        """
        result: dict[str, Any] = {
            "max_entropy": 0.0,
            "warnings": [],
            "high_entropy_columns": [],
            "compound_risks": [],
        }

        if context.rich_context is None:
            return result

        # Check for entropy_summary
        entropy_summary = getattr(context.rich_context, "entropy_summary", None)
        if not entropy_summary:
            return result

        # Extract high entropy count
        high_count = entropy_summary.get("high_entropy_count", 0)
        critical_count = entropy_summary.get("critical_entropy_count", 0)
        compound_count = entropy_summary.get("compound_risk_count", 0)
        blockers = entropy_summary.get("readiness_blockers", [])

        # Build warnings
        if critical_count > 0:
            result["warnings"].append(f"{critical_count} columns have critical entropy")
        if high_count > 0:
            result["warnings"].append(f"{high_count} columns have high uncertainty")
        if compound_count > 0:
            result["warnings"].append(f"{compound_count} dangerous column combinations")

        # Find max entropy from tables
        tables = getattr(context.rich_context, "tables", [])
        for table in tables:
            for col in getattr(table, "columns", []):
                entropy_scores = getattr(col, "entropy_scores", None)
                if entropy_scores:
                    score = entropy_scores.get("composite_score", 0.0)
                    if score > result["max_entropy"]:
                        result["max_entropy"] = score
                    if score >= 0.6:
                        result["high_entropy_columns"].append(
                            {
                                "table": getattr(table, "table_name", "unknown"),
                                "column": getattr(col, "column_name", "unknown"),
                                "score": score,
                                "dimensions": entropy_scores.get("high_entropy_dimensions", []),
                            }
                        )

        # Add blockers as compound risks
        result["compound_risks"] = blockers

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
                composite_score = entropy_scores.get("composite_score", 0.0)
                if composite_score < 0.3:
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

    def _get_table_schema(self, context: ExecutionContext) -> Result[TableSchema]:
        """Get schema information from the actual table."""
        try:
            # Get column info
            columns_result = context.duckdb_conn.execute(
                f'DESCRIBE "{context.table_name}"'
            ).fetchall()

            columns = []
            for col in columns_result:
                col_name = col[0]
                col_type = col[1]

                # Get sample values (quote column name for spaces/special chars)
                sample_result = context.duckdb_conn.execute(
                    f'SELECT DISTINCT "{col_name}" FROM "{context.table_name}" LIMIT 5'
                ).fetchall()
                samples = [str(r[0]) for r in sample_result if r[0] is not None]

                columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "sample_values": samples,
                    }
                )

            # Get row count
            count_result = context.duckdb_conn.execute(
                f'SELECT COUNT(*) FROM "{context.table_name}"'
            ).fetchone()
            row_count = count_result[0] if count_result else 0

            return Result.ok(
                TableSchema(
                    table_name=context.table_name,
                    columns=columns,
                    row_count=row_count,
                )
            )

        except Exception as e:
            return Result.fail(f"Failed to get table schema: {e}")

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

        # Get table schema for context
        schema_result = self._get_table_schema(context)
        if not schema_result.success or not schema_result.value:
            return Result.fail("Cannot repair: failed to get schema")

        table_schema = schema_result.value

        # Build prompt context
        prompt_context = {
            "error_message": error_message,
            "failed_sql": failed_sql,
            "table_schema": json.dumps(
                {
                    "table_name": table_schema.table_name,
                    "columns": table_schema.columns,
                },
                indent=2,
            ),
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

    def _load_from_db(
        self,
        session: Session,
        graph_id: str,
        graph_version: str,
        schema_mapping_id: str,
    ) -> GeneratedCode | None:
        """Load generated code from database cache."""
        stmt = select(GeneratedCodeRecord).where(
            GeneratedCodeRecord.graph_id == graph_id,
            GeneratedCodeRecord.graph_version == graph_version,
            GeneratedCodeRecord.schema_mapping_id == schema_mapping_id,
        )
        result = session.execute(stmt)
        record = result.scalar_one_or_none()

        if record is None:
            return None

        return GeneratedCode(
            code_id=record.code_id,
            graph_id=record.graph_id,
            graph_version=record.graph_version,
            schema_mapping_id=record.schema_mapping_id,
            summary=record.summary or "",
            steps=record.steps_json,
            final_sql=record.final_sql,
            column_mappings=record.column_mappings,
            llm_model=record.llm_model,
            prompt_hash=record.prompt_hash,
            generated_at=record.generated_at,
            is_validated=record.is_validated,
            validation_errors=record.validation_errors,
        )

    def _save_to_db(
        self,
        session: Session,
        generated_code: GeneratedCode,
    ) -> None:
        """Save generated code to database cache."""
        record = GeneratedCodeRecord(
            code_id=generated_code.code_id,
            graph_id=generated_code.graph_id,
            graph_version=generated_code.graph_version,
            schema_mapping_id=generated_code.schema_mapping_id,
            summary=generated_code.summary,
            steps_json=generated_code.steps,
            final_sql=generated_code.final_sql,
            column_mappings=generated_code.column_mappings,
            llm_model=generated_code.llm_model,
            prompt_hash=generated_code.prompt_hash,
            generated_at=generated_code.generated_at,
            is_validated=generated_code.is_validated,
            validation_errors=generated_code.validation_errors,
        )
        session.add(record)
        # No flush needed - code_id is client-generated UUID, commit happens at session_scope() end

    def _save_to_library(
        self,
        session: Session,
        manager: ConnectionManager,
        source_id: str,
        graph: TransformationGraph,
        generated_code: GeneratedCode,
        execution: GraphExecution,
    ) -> None:
        """Save graph execution to QueryLibrary for cross-agent reuse.

        This enables Query Agent to find and reuse graph-generated SQL
        via semantic similarity search.

        Args:
            session: SQLAlchemy session
            manager: ConnectionManager with vectors
            source_id: Source ID for the library
            graph: The graph specification
            generated_code: Generated SQL code
            execution: Execution result with assumptions
        """
        from dataraum.query.document import QueryDocument
        from dataraum.query.library import QueryLibrary, QueryLibraryError

        # Build assumptions from execution
        assumptions = [
            {
                "dimension": a.dimension,
                "target": a.target,
                "assumption": a.assumption,
                "basis": a.basis.value,
                "confidence": a.confidence,
            }
            for a in execution.assumptions
        ]

        # Convert to GraphSQLGenerationOutput format for QueryDocument
        output = GraphSQLGenerationOutput(
            summary=generated_code.summary or f"Calculates {graph.metadata.name}",
            steps=[
                SQLStepOutput(
                    step_id=s["step_id"],
                    sql=s["sql"],
                    description=s["description"],
                )
                for s in generated_code.steps
            ],
            final_sql=generated_code.final_sql,
            column_mappings=generated_code.column_mappings,
        )

        document = QueryDocument.from_graph_output(output, assumptions)

        try:
            library = QueryLibrary(session, manager)
            library.save(
                source_id=source_id,
                document=document,
                graph_id=graph.graph_id,
                name=graph.metadata.name,
                description=graph.metadata.description,
                confidence_level="GREEN",  # Graphs are pre-validated
            )
            logger.info(f"Saved graph '{graph.graph_id}' to query library")
        except QueryLibraryError as e:
            # Library not available (vectors disabled) - not an error
            logger.debug(f"Skipping library save (vectors not enabled): {e}")
        except Exception as e:
            logger.warning(f"Failed to save graph to library: {e}")
