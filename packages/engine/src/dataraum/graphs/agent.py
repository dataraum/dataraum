"""Graph agent: generates and executes SQL for a metric graph spec.

Pipeline per graph:
1. Load graph specification (YAML with accounting context)
2. Analyze actual data schema (columns, types)
3. Look up cached SQL snippets from the knowledge base
4. Author the output node by type (DAT-643): a FORMULA/CONSTANT is composed
   DETERMINISTICALLY over already-grounded deps (no LLM); an EXTRACT is the sole
   LLM authoring surface (or a cache-assemble when already minted on a prior run)
5. Save as snippets for cross-agent reuse
6. Execute SQL and capture results
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
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
    GraphAssumptionOutput,
    GraphExecution,
    GraphProvenanceOutput,
    GraphSQLGenerationOutput,
    GraphStep,
    QueryAssumption,
    StepResult,
    StepType,
    TransformationGraph,
)
from .verifier import verify_execution

if TYPE_CHECKING:
    from dataraum.graphs.node_warming import NodeDecision, NodeKey

logger = get_logger(__name__)


def _ordered_dep_steps(graph: TransformationGraph, output_step: GraphStep) -> list[str]:
    """``output_step``'s transitive dependency subgraph, deps-before-dependents.

    Depth-first post-order over ``depends_on`` (DAT-645): a step is appended only
    after all its own deps, so the result is a valid CTE materialization order for a
    NESTED formula (an inner formula's snippet references its own extract deps, which
    must be defined as earlier CTEs). Visits only the transitive deps of
    ``output_step`` — the output step itself is never in the list. A dep that is not a
    step in ``graph`` (a leaf snippet supplied only via the cache, e.g. in a unit
    fixture) is treated as a leaf with no further deps.

    Raises:
        ValueError: a ``depends_on`` cycle. The warm DAG already rejects cycles
            (``node_warming.build_warm_dag``), so this only fires on a malformed graph
            reaching the composer off that path — born-loud, not a silently-wrong order.
    """
    order: list[str] = []
    done: set[str] = set()
    on_path: set[str] = set()

    def visit(step_id: str) -> None:
        if step_id in done:
            return
        if step_id in on_path:
            raise ValueError(
                f"formula '{output_step.step_id}' has a dependency cycle at '{step_id}'"
            )
        on_path.add(step_id)
        step = graph.steps.get(step_id)
        if step is not None:
            for dep in step.depends_on:
                visit(dep)
        on_path.discard(step_id)
        done.add(step_id)
        order.append(step_id)

    for dep in output_step.depends_on:
        visit(dep)
    return order


@dataclass
class GeneratedCode:
    """LLM-generated SQL for a specific graph + schema combination."""

    code_id: str
    graph_id: str

    # Generated SQL
    summary: str  # Plain English description of what the query calculates
    steps: list[dict[str, str]]  # List of {step_id, sql, description}
    final_sql: str
    column_mappings: dict[str, str]  # abstract_field -> concrete_column

    # Generation metadata
    llm_model: str
    prompt_hash: str
    generated_at: datetime

    # Provenance and assumptions (from LLM output, optional)
    provenance: GraphProvenanceOutput | None = None
    assumptions: list[GraphAssumptionOutput] = field(default_factory=list)


@dataclass
class ExecutionContext:
    """Context for graph execution."""

    duckdb_conn: duckdb.DuckDBPyConnection
    schema_mapping_id: str | None = None

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
        *,
        slice_column: str | None = None,
        slice_value: str | None = None,
        vertical: str | None = None,
        om_run_id: str | None = None,
        catalogue_run_id: str | None = None,
        **kwargs: Any,
    ) -> ExecutionContext:
        """Create ExecutionContext with rich metadata loaded from analysis modules.

        This is the recommended way to create an ExecutionContext when you want
        the LLM to have access to semantic, statistical, and relational metadata.

        Args:
            session: SQLAlchemy session
            duckdb_conn: DuckDB connection for queries
            table_ids: List of table IDs to include in context
            slice_column: Optional column to slice the context by.
            slice_value: Optional value for the slice column.
            vertical: Runtime vertical for the cycle-health computation.
            om_run_id: Explicit operating_model run to read cycles/validation/cycle
                health at — passed by the in-run metrics phase so the graph context
                sees THIS run's evidence (written by the earlier validation +
                business_cycles activities). Omitted ⇒ the promoted operating_model
                head (the post-promote current-state read; the query agent's path).
            **kwargs: Additional ExecutionContext dataclass fields.

        Returns:
            ExecutionContext with rich_context populated
        """
        from dataraum.graphs.context import build_execution_context

        rich_context = build_execution_context(
            session=session,
            table_ids=table_ids,
            duckdb_conn=duckdb_conn,
            slice_column=slice_column,
            slice_value=slice_value,
            vertical=vertical,
            om_run_id=om_run_id,
            catalogue_run_id=catalogue_run_id,
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

    def execute(
        self,
        session: Session,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any] | None = None,
        inspiration_sql: str | None = None,
        *,
        workspace_id: str,
    ) -> Result[GraphExecution]:
        """Execute a graph by generating and running SQL.

        Args:
            session: Database session for LLM cache
            graph: The graph specification to execute
            context: Execution context with data connection
            parameters: Parameter values for the graph
            inspiration_sql: SQL hint from a promoted snippet (injected as cached_step)
            workspace_id: Workspace id for the snippet library (per-row population +
                write-path guard) — DAT-506, replaces the former session_id.

        Returns:
            Result containing GraphExecution with results
        """
        parameters = parameters or {}

        # Resolve parameters with defaults
        resolved_params = self._resolve_parameters(graph, parameters)

        schema_mapping_id = context.schema_mapping_id or "default"

        # Check the snippet library for cached individual steps. The DB-backed
        # snippet base IS the cache (cross-run, shared with the query agent) —
        # there is no in-memory assembled-code cache: assembly without the LLM is
        # cheap, and a per-instance cache would mask the snippet self-heal on a
        # stale-snippet retry (and never hit anyway — one agent, one run, one
        # execution per graph_id).
        cached_snippets = self._lookup_snippets(
            session,
            graph,
            schema_mapping_id,
            resolved_params,
        )

        # Inject inspiration SQL as a hint (from snippet promotion path)
        if inspiration_sql and not cached_snippets:
            cached_snippets["_inspiration"] = {
                "sql": inspiration_sql,
                "description": "SQL hint from promoted ad-hoc query",
                "snippet_id": None,
            }

        generated_code: GeneratedCode | None
        # The authored node's type fixes the path, with NO fallback between them
        # (DAT-643): a FORMULA/CONSTANT is grounding-free arithmetic over already-decided
        # nodes, so it is composed DETERMINISTICALLY and can never reach the LLM — a
        # formula whose deps did not ground honest-fails born-loud (the warm DAG authors
        # deps first, so a missing dep is a real keying/contract bug, not a cue to
        # LLM-re-derive a shared extract). An EXTRACT is the SOLE LLM authoring surface
        # (or a cache-assemble when already minted on a prior run).
        output_step = graph.get_output_step()
        if output_step is not None and output_step.step_type in (
            StepType.FORMULA,
            StepType.CONSTANT,
        ):
            try:
                generated_code = self._compose_grounding_free(
                    output_step, graph, cached_snippets, resolved_params
                )
            except ValueError as exc:
                # Deps ungroundable / constant unresolved / malformed expression —
                # born-loud, never handed to the LLM.
                return Result.fail(str(exc))
            self._track_snippet_usage(
                session=session,
                execution_id=generated_code.code_id,
                cached_snippets=cached_snippets or {},
                generated_steps=generated_code.steps,
                workspace_id=workspace_id,
            )
        # EXTRACT already minted on a prior run → assemble from cache without the LLM.
        elif cached_snippets and len(cached_snippets) == len(graph.steps):
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
                    workspace_id=workspace_id,
                )
        else:
            # EXTRACT grounding — the only LLM call in the authoring path.
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
                workspace_id=workspace_id,
            )

        if generated_code is None:
            return Result.fail("Failed to generate or assemble SQL code")

        # Execute the generated SQL
        exec_result = self._execute_sql(generated_code, context, graph)
        if not exec_result.success or not exec_result.value:
            # This node is ungroundable — the caller records it in the run's binding
            # map. Do NOT record_failure the cached DEP snippets: they were
            # decided-once and grounded by their OWN authoring, so blaming them for
            # THIS node's failure poisons shared extracts (a broken formula would mark
            # `revenue` failed → every metric using it can no longer find it, and
            # honest metrics like dso silently break). DAT-636 (Bug B).
            return Result.fail(exec_result.error or "SQL execution failed")

        execution = exec_result.value

        # Verifier gate (DAT-616): execution-pass is NOT validation. A node whose SQL
        # ran cleanly is still inconclusive if it had no support (empty filter -> NULL),
        # the value is degenerate (NULL), or a catalogue-declared condition is violated.
        # Such a node stays ungroundable with the reason — never executed-green — and its
        # SQL is NOT cached (we return before _save_snippets). As above, the cached deps
        # are NOT blamed for this node's verdict.
        verdict = verify_execution(graph, execution)
        if not verdict.success:
            return Result.fail(verdict.error or "metric verification failed")

        # Save snippets AFTER successful execution AND verification — includes
        # repair info and only saves SQL that actually works AND is trustworthy.
        self._save_snippets(
            session=session,
            graph=graph,
            generated_code=generated_code,
            schema_mapping_id=schema_mapping_id,
            step_results=execution.step_results,
            resolved_params=resolved_params,
            workspace_id=workspace_id,
        )

        return Result.ok(execution)

    def assemble(
        self,
        session: Session,
        graph: TransformationGraph,
        context: ExecutionContext,
        bindings: dict[NodeKey, NodeDecision],
        parameters: dict[str, Any] | None = None,
        *,
        workspace_id: str,
    ) -> Result[GraphExecution]:
        """Assemble a metric from already-decided node bindings — NO LLM (DAT-636).

        The authoring pass (``metrics_phase._warm_shared_nodes``) decided every
        unique node ONCE and recorded it in ``bindings``. This composes the metric
        from those decisions: if any of the metric's nodes is ungroundable, the
        metric honest-fails immediately with the failing dependency named — it is
        NEVER re-authored. Otherwise it assembles ``final_sql`` from the snippets
        the pass minted, then executes and verifies. The per-metric path is a dumb
        assembler; the LLM is only ever called in the authoring pass — so the same
        concept can no longer ground three different ways across dependent metrics.
        """
        from dataraum.graphs.node_warming import node_key

        resolved_params = self._resolve_parameters(graph, parameters or {})
        schema_mapping_id = context.schema_mapping_id or "default"

        # Honest-fail on the first ungroundable dependency — born-loud, no LLM. A
        # keyable step absent from the map is a contract violation (the authoring
        # pass authors every keyable node), so fail loud here, not silently later.
        for step_id, step in graph.steps.items():
            key = node_key(step, graph)
            if key is None:
                continue  # non-keyable step (rare) — caught by the cache check below
            decision = bindings.get(key)
            if decision is None or not decision.grounded:
                reason = (
                    decision.reason
                    if decision is not None
                    else "not authored (absent from binding map)"
                )
                return Result.fail(f"dependency '{step_id}' is ungroundable: {reason}")

        # Every dependency grounded → assemble from the snippets the pass minted.
        cached_snippets = self._lookup_snippets(session, graph, schema_mapping_id, resolved_params)
        if not cached_snippets or len(cached_snippets) != len(graph.steps):
            missing = len(graph.steps) - len(cached_snippets or {})
            return Result.fail(
                f"metric '{graph.graph_id}': {missing} step(s) grounded per the binding "
                "map but absent from the snippet cache"
            )
        generated_code = self._assemble_from_snippets(
            graph, context, cached_snippets, resolved_params
        )
        if generated_code is None:
            return Result.fail(f"metric '{graph.graph_id}': failed to assemble cached snippets")
        self._track_snippet_usage(
            session=session,
            execution_id=generated_code.code_id,
            cached_snippets=cached_snippets,
            generated_steps=generated_code.steps,
            workspace_id=workspace_id,
        )

        exec_result = self._execute_sql(generated_code, context, graph)
        if not exec_result.success or not exec_result.value:
            return Result.fail(exec_result.error or "metric assembly execution failed")
        execution = exec_result.value
        verdict = verify_execution(graph, execution)
        if not verdict.success:
            return Result.fail(verdict.error or "metric verification failed")
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
        # DAT-631: carry each snippet's authored grounding confidence forward.
        # Cache-assembly skips the LLM, so without this the assembled metric would
        # reach the phase with no assumptions and look confidently green — exactly
        # the silent-wrong mode for a metric resting on a 0.35-confidence proxy.
        assumptions: list[GraphAssumptionOutput] = []
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
            for a in snippet.get("assumptions") or []:
                assumptions.append(
                    GraphAssumptionOutput(
                        dimension=a.get("dimension", "grounding.cached"),
                        target=a.get("target", f"step:{step_id}"),
                        assumption=a.get("assumption", ""),
                        basis=a.get("basis", "inferred"),
                        confidence=a.get("confidence", 0.5),
                    )
                )

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
            summary=f"Assembled from {len(steps)} cached snippets",
            steps=steps,
            final_sql=final_sql,
            column_mappings=merged_column_mappings,
            llm_model="cached",
            prompt_hash="snippets",
            generated_at=datetime.now(UTC),
            assumptions=assumptions,
        )

    def _generate_sql(
        self,
        session: Session,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any],
        cached_snippets: dict[str, dict[str, Any]] | None = None,
    ) -> Result[GeneratedCode]:
        """Ground a single leaf EXTRACT to SQL via the LLM (tool-based output).

        EXTRACT is the SOLE LLM authoring surface (DAT-643): a FORMULA/CONSTANT is
        composed deterministically in ``_compose_grounding_free`` and never reaches
        here, so this path only ever grounds one leaf extract against the dataset
        context + field mappings. ``cached_snippets`` feeds the DAT-616 prior context
        (a cached extract is ASSEMBLED upstream, never re-authored), so an extract is a
        leaf with no dependency steps to carry into the prompt.
        """
        from dataraum.llm.providers.base import (
            ConversationRequest,
            Message,
            ToolDefinition,
        )

        # Serialize graph to YAML for LLM context.
        graph_yaml = self._graph_to_yaml(graph)

        prompt_name = "graph_sql_generation"
        # Tier = balanced/Sonnet. Extract grounding needs the dataset context + field
        # mappings — fail loud if the semantic phase did not produce them.
        tier = "balanced"
        if context.rich_context is None:
            return Result.fail(
                "Cannot generate SQL without dataset context. "
                "Use ExecutionContext.with_rich_context() to build context."
            )
        if (
            not context.rich_context.field_mappings
            or not context.rich_context.field_mappings.mappings
        ):
            return Result.fail(
                "Cannot generate SQL without field mappings. "
                "Run the semantic phase to map business concepts to columns."
            )
        from dataraum.graphs.context import format_metadata_document
        from dataraum.graphs.field_mapping import format_mappings_for_prompt

        prompt_context = {
            "graph_yaml": graph_yaml,
            "table_schema": json.dumps(self._build_schema_info(context), indent=2),
            "parameters": json.dumps(parameters, indent=2),
            "rich_context": format_metadata_document(context.rich_context),
            "field_mappings": format_mappings_for_prompt(context.rich_context.field_mappings),
            # DAT-616: feed back what prior runs learned for this concept — the
            # honest-fail reason + prior value→concept filter decisions.
            "prior_context": self._build_prior_context(session, graph, cached_snippets),
            # DAT-645: the vertical's conventions (e.g. the sign/natural-balance
            # rule), piped verbatim — the engine does not interpret them.
            "vertical_conventions": context.rich_context.conventions,
        }

        # Render prompt with system/user split.
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                prompt_name, prompt_context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        prompt_hash = hashlib.sha256(user_prompt.encode()).hexdigest()[:16]
        model = self.provider.get_model_for_tier(tier)

        # Dump the rendered prompt (system + user) for offline analysis — no-op unless
        # prompt_dump_dir is set. The label distinguishes formula vs grounding prompts.
        from dataraum.llm.prompt_log import dump_prompt

        dump_prompt(
            label=prompt_name,
            key=graph.graph_id,
            prompt_hash=prompt_hash,
            system=system_prompt,
            user=user_prompt,
            model=model,
        )

        # Define tool for structured output
        tool = ToolDefinition(
            name="generate_sql",
            description="Provide generated SQL for the graph specification",
            input_schema=GraphSQLGenerationOutput.model_json_schema(),
        )

        # Call LLM with tool use
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "generate_sql"},
            label=prompt_name,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        # converse raises a typed ProviderError on an API failure (DAT-503) —
        # retryability rides the exception to the worker's durable boundary, so
        # we don't re-wrap it. A returned Result is always a success.
        response = self.provider.converse(request).unwrap()

        # Extract tool call result. No tool call is a bind ERROR — never guess by
        # parsing free text as JSON (DAT-439's born-loud cut): a metric that can't
        # be composed stays grounded with the reason, it does not get a guessed SQL.
        if not response.tool_calls:
            return Result.fail("LLM did not call the generate_sql tool")

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
            provenance=output.provenance,
            assumptions=output.assumptions or [],
            llm_model=model,
            prompt_hash=prompt_hash,
            generated_at=datetime.now(UTC),
        )

        # Verification half (DAT-631): append what the agent PRODUCED to the
        # prompt dump — the SQL, per-concept grounding, and confidence — so a
        # metric that fails verification (and never persists a snippet) is still
        # inspectable offline. No-op unless prompt_dump_dir is set.
        from dataraum.llm.prompt_log import dump_response

        basis = output.provenance.column_mappings_basis if output.provenance else {}
        response_body = json.dumps(
            {
                "final_sql": output.final_sql,
                "steps": [{"step_id": s.step_id, "sql": s.sql} for s in output.steps],
                "field_resolution": output.provenance.field_resolution
                if output.provenance
                else None,
                "column_mappings_basis": basis,
                "assumptions": [
                    {"assumption": a.assumption, "basis": a.basis, "confidence": a.confidence}
                    for a in (output.assumptions or [])
                ],
            },
            indent=2,
        )
        dump_response(
            label="graph_sql_generation",
            key=graph.graph_id,
            prompt_hash=prompt_hash,
            body=response_body,
        )

        return Result.ok(generated_code)

    def _execute_sql(
        self,
        generated_code: GeneratedCode,
        context: ExecutionContext,
        graph: TransformationGraph,
    ) -> Result[GraphExecution]:
        """Execute generated SQL and capture results.

        Delegates step execution to shared execute_sql_steps(), then enriches
        the GraphExecution with assumptions and interpretation.
        """
        from dataraum.query.execution import SQLStep, execute_sql_steps

        execution = GraphExecution.create(graph)

        # Convert LLM assumptions to QueryAssumption objects
        basis_map = {
            "system_default": AssumptionBasis.SYSTEM_DEFAULT,
            "inferred": AssumptionBasis.INFERRED,
            "user_specified": AssumptionBasis.USER_SPECIFIED,
        }
        assumptions: list[QueryAssumption] = []
        for a in generated_code.assumptions or []:
            mapped_basis = basis_map.get(a.basis)
            if mapped_basis is None:
                logger.debug("unknown_assumption_basis", basis=a.basis)
                mapped_basis = AssumptionBasis.INFERRED
            assumptions.append(
                QueryAssumption.create(
                    execution_id=execution.execution_id,
                    dimension=a.dimension,
                    target=a.target,
                    assumption=a.assumption,
                    basis=mapped_basis,
                    confidence=a.confidence,
                )
            )
        execution.assumptions = assumptions

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
                source_query=sr.sql_executed,
                inputs_used={
                    "sql": sr.sql_executed,
                    "step_id": sr.step_id,
                    "repair_attempts": sr.repair_attempts,
                },
            )
            # bool BEFORE int (bool is an int subclass); Decimal IS the common
            # currency type DuckDB returns for SUM over DECIMAL columns — it must
            # land in value_scalar, else the verifier reads a real sum as NULL
            # "no support" and false-fails every real metric (DAT-616).
            value = sr.value
            if isinstance(value, bool):
                step_result.value_boolean = value
            elif isinstance(value, (int, float, Decimal)):
                step_result.value_scalar = float(value)
            elif isinstance(value, str):
                step_result.value_string = value

            execution.step_results.append(step_result)

        execution.output_value = result.final_value
        execution.composed_sql = result.composed_sql

        # Add interpretation if available
        if graph.interpretation and execution.output_value is not None:
            execution.output_interpretation = self._interpret_value(execution.output_value, graph)

        return Result.ok(execution)

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
        """DESCRIBE a single DuckDB table and return its name + column types.

        DAT-616: this no longer self-fetches `SELECT DISTINCT … LIMIT 5` per column —
        that arbitrary, count-less sample was the agent's only value view and is what it
        improvised filters from. The authoritative, complete value enumeration is now the
        per-column **Value sets** block in the rich-context metadata document
        (`format_metadata_document`); this returns physical name + type only.
        """
        try:
            columns_result = duckdb_conn.execute(f'DESCRIBE "{table_name}"').fetchall()

            columns = [{"name": col[0], "type": col[1]} for col in columns_result]

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
                    "type": step.step_type.value,
                    "source": {
                        "standard_field": step.source.standard_field if step.source else None,
                        "statement": step.source.statement if step.source else None,
                    }
                    if step.source
                    else None,
                    "expression": step.expression,
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
            label="sql_repair",
        )

        # converse raises a typed ProviderError on an API failure (DAT-503) —
        # retryability rides the exception to the worker's durable boundary, so
        # we don't re-wrap it. A returned Result is always a success.
        response = self.provider.converse(request).unwrap()
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

    def _track_snippet_usage(
        self,
        session: Session,
        execution_id: str,
        cached_snippets: dict[str, dict[str, Any]],
        generated_steps: list[dict[str, str]],
        *,
        workspace_id: str,
    ) -> None:
        """Track how cached snippets were used in graph execution."""
        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_utils import determine_usage_type

        library = SnippetLibrary(session, workspace_id=workspace_id)
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

    def _compose_grounding_free(
        self,
        output_step: GraphStep,
        graph: TransformationGraph,
        cached_snippets: dict[str, dict[str, Any]],
        resolved_params: dict[str, Any],
    ) -> GeneratedCode:
        """Deterministically compose a grounding-free node — FORMULA or CONSTANT, no LLM.

        A formula is pure arithmetic over already-decided dep steps and a constant is a
        known parameter value — neither carries judgment, so the composer authors them
        and the LLM is never involved (DAT-643 retired the comparison shadow + the
        legacy full-graph fallback). ``output_step`` is the node being authored and is
        already known to be a FORMULA or CONSTANT (``execute`` branches on type).

        Raises:
            ValueError: a constant has no resolved value, a formula depends on a node
                that did not ground (absent from ``cached_snippets``), or the expression
                is malformed — surfaced born-loud so the metric honest-fails rather than
                the LLM re-deriving a shared extract. The warm DAG authors deps before
                their dependents, so a missing dep is a real keying/contract bug.
        """
        from dataraum.graphs.formula_composer import compose_constant_sql, compose_formula_sql

        steps: list[dict[str, str]]
        if output_step.step_type == StepType.CONSTANT:
            value = resolved_params.get(output_step.parameter) if output_step.parameter else None
            if value is None:
                raise ValueError(
                    f"constant '{output_step.parameter or output_step.step_id}' has no resolved "
                    "value — cannot compose"
                )
            final_sql = compose_constant_sql(value)
            steps = []
            summary = f"Constant {output_step.parameter} = {value}"
        else:  # FORMULA
            if not output_step.expression:
                raise ValueError(f"formula '{graph.graph_id}' has no expression — cannot compose")
            # Materialize the FULL transitive dependency closure as CTE steps, ordered
            # deps-before-dependents (DAT-645). A formula may depend on another formula
            # (e.g. operating_income = gross_profit - operating_expense): the inner
            # formula's snippet is `(SELECT value FROM revenue) - ...`, so its own
            # extract deps must also be materialized as CTEs or execution fails with
            # "Table revenue does not exist". Direct deps alone are not enough.
            ordered = _ordered_dep_steps(graph, output_step)
            missing = [d for d in ordered if d not in cached_snippets]
            if missing:
                raise ValueError(
                    f"formula '{graph.graph_id}' dependency/ies {missing} not grounded — "
                    "cannot compose (the warm DAG authors deps first; a missing dep is a "
                    "keying/contract bug, never an LLM-re-derive cue)"
                )
            steps = [
                {
                    "step_id": d,
                    "sql": cached_snippets[d]["sql"],
                    "description": cached_snippets[d].get("description", ""),
                }
                for d in ordered
            ]
            # final_sql references the OUTPUT's direct deps (each a CTE in `steps`).
            final_sql = compose_formula_sql(output_step.expression, set(output_step.depends_on))
            summary = f"Composed {graph.graph_id}: {output_step.expression}"

        return GeneratedCode(
            code_id=str(uuid4()),
            graph_id=graph.graph_id,
            summary=summary,
            steps=steps,
            final_sql=final_sql,
            column_mappings={},
            llm_model="deterministic",
            prompt_hash="deterministic",
            generated_at=datetime.now(UTC),
        )

    def _save_snippets(
        self,
        session: Session,
        graph: TransformationGraph,
        generated_code: GeneratedCode,
        schema_mapping_id: str,
        step_results: list[StepResult] | None = None,
        resolved_params: dict[str, Any] | None = None,
        *,
        workspace_id: str,
    ) -> None:
        """Save generated SQL steps as snippets for cross-graph reuse.

        Called only from the authoring path (``execute`` on a single-output
        mini-graph); ``assemble`` never saves — its nodes were already minted here.
        A FORMULA mini-graph reproduces its dep extract/constant steps, so this loop
        re-encounters them — that is a harmless no-op: ``save_snippet`` is
        first-writer-wins, so an existing healthy snippet for the dep concept is KEPT
        (a hallucinated dep-SQL change can never overwrite a good extract snippet).

        Called AFTER successful execution so that:
        - Only working SQL is saved (not broken SQL that needs marking as failed)
        - Repair info from step_results can be included in provenance

        Args:
            session: SQLAlchemy session
            graph: Graph specification (defines step types and metadata)
            generated_code: LLM-generated SQL code
            schema_mapping_id: Schema mapping identifier
            step_results: Execution results for repair detection
        """
        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_utils import normalize_expression

        library = SnippetLibrary(session, workspace_id=workspace_id)

        source = f"graph:{graph.graph_id}"

        # Build a map of generated step_id -> {sql, description}
        generated_steps: dict[str, dict[str, str]] = {}
        for step_dict in generated_code.steps:
            step_id = step_dict.get("step_id", "")
            if step_id:
                generated_steps[step_id] = step_dict

        # Build repair lookup from execution results
        repair_by_step: dict[str, StepResult] = {}
        if step_results:
            for sr in step_results:
                if sr.inputs_used.get("repair_attempts", 0) > 0:
                    repair_by_step[sr.step_id] = sr

        # Build provenance dict from LLM output + repair info + assumptions
        any_repaired = bool(repair_by_step)
        provenance_dict: dict[str, Any] | None = None
        if generated_code.provenance:
            prov = generated_code.provenance
            provenance_dict = {
                "field_resolution": prov.field_resolution,
                "was_repaired": any_repaired,
                "column_mappings_basis": prov.column_mappings_basis,
                "llm_reasoning": prov.llm_reasoning,
            }
        elif any_repaired:
            provenance_dict = {"was_repaired": True}

        # Include assumptions in provenance so they're discoverable via search_snippets
        if generated_code.assumptions:
            if provenance_dict is None:
                provenance_dict = {}
            provenance_dict["assumptions"] = [
                {"assumption": a.assumption, "basis": a.basis, "confidence": a.confidence}
                for a in generated_code.assumptions
            ]

        # Map graph steps to snippets
        for step_id, graph_step in graph.steps.items():
            gen_step = generated_steps.get(step_id)

            # The authored grounding-free output's SQL lives in final_sql, not in
            # `steps`: deterministic authoring (DAT-636 step 3) emits no entry for the
            # output step, and the LLM tool schema makes `steps` OPTIONAL while
            # final_sql is REQUIRED. So a FORMULA or CONSTANT output step is persisted
            # from final_sql even when absent from `steps` — reading it from the
            # optional `steps` left the node "grounded in the binding map but absent
            # from the cache", un-assemblable per-metric (DAT-636).
            is_composed_output = graph_step.output_step and graph_step.step_type in (
                StepType.FORMULA,
                StepType.CONSTANT,
            )
            if not gen_step and not is_composed_output:
                continue

            # The composed output's SQL is ALWAYS final_sql — repair tracks per-step
            # CTE bodies (by step_id), which are not the composed final statement, so
            # a repaired formula step must not override the composition. Otherwise:
            # repaired step SQL if available, else the step's own LLM SQL.
            repaired = repair_by_step.get(step_id)
            if is_composed_output:
                sql = generated_code.final_sql
            elif repaired and repaired.source_query:
                sql = repaired.source_query
            else:
                sql = gen_step.get("sql", "") if gen_step else ""
            description = gen_step.get("description", "") if gen_step else generated_code.summary

            if graph_step.step_type == StepType.EXTRACT and graph_step.source:
                # Extract snippet: keyed by standard_field + statement + aggregation.
                # column_mappings is now PER-CONCEPT (DAT-495 closed): post-DAT-636-P1
                # _save_snippets is only ever reached via the authoring pass on a
                # single-output mini-graph, so generated_code.column_mappings describes
                # this one concept — never the sibling-leaking whole-metric dict the old
                # whole-graph authoring produced. It is a secondary, graph-level HINT for
                # the cockpit query agent (NOT a source of truth — the authoritative
                # per-concept grounding is provenance.column_mappings_basis).
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
                    provenance=provenance_dict,
                )

            elif graph_step.step_type == StepType.CONSTANT:
                # Constant snippet: keyed by parameter name + RESOLVED value. Key off
                # resolved_params (the same value the deterministic SQL and the later
                # _lookup_snippets use) — NOT param.default — so a non-default parameter
                # value can't save under one key and look up under another.
                param_value = None
                if graph_step.parameter:
                    resolved = (resolved_params or {}).get(graph_step.parameter)
                    if resolved is None:  # fall back to the graph default
                        for param in graph.parameters:
                            if param.name == graph_step.parameter:
                                resolved = param.default
                                break
                    param_value = str(resolved) if resolved is not None else None

                library.save_snippet(
                    snippet_type="constant",
                    sql=sql,
                    description=description,
                    schema_mapping_id=schema_mapping_id,
                    source=source,
                    standard_field=graph_step.parameter or step_id,
                    parameter_value=param_value,
                    llm_model=generated_code.llm_model,
                    provenance=provenance_dict,
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
                    provenance=provenance_dict,
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
                    # DAT-616: the prior value→concept FILTER decisions, fed back so
                    # grounding isn't re-invented (served, not just reused-as-SQL).
                    "column_mappings_basis": (match.snippet.provenance or {}).get(
                        "column_mappings_basis"
                    ),
                    # DAT-631: the grounding confidence the snippet was authored with.
                    # Carried so a metric ASSEMBLED from cache (no LLM call — the
                    # post-warming common path) still surfaces its weakest input's
                    # confidence to the phase gate, instead of looking confidently
                    # green because cache-assembly dropped the assumptions.
                    "assumptions": (match.snippet.provenance or {}).get("assumptions") or [],
                }

        if cached_steps:
            logger.debug(
                "found_cached_snippets",
                cached=len(cached_steps),
                total=len(graph.steps),
                graph_id=graph.graph_id,
            )

        return cached_steps

    def _build_prior_context(
        self,
        session: Session,
        graph: TransformationGraph,
        cached_snippets: dict[str, dict[str, Any]] | None,
    ) -> str:
        """Assemble what prior runs learned for this metric (DAT-616 feedback loops).

        Two signals, both written-but-never-read until now:
        - the most recent honest-fail ``state_reason`` for this metric (so the next
          attempt addresses it or abstains, instead of repeating a blind guess);
        - prior ``column_mappings_basis`` from reusable snippets (the value→concept FILTER
          decisions), so grounding isn't re-invented each run.

        Returns "" when there is nothing to feed (the prompt slot is optional). Best-effort
        — a lookup failure never blocks generation.
        """
        parts: list[str] = []

        try:
            from dataraum.lifecycle import LifecycleArtifact

            prior = (
                session.query(LifecycleArtifact)
                .filter(
                    LifecycleArtifact.artifact_type == "metric",
                    LifecycleArtifact.artifact_key == graph.graph_id,
                    LifecycleArtifact.state_reason.isnot(None),
                )
                .order_by(LifecycleArtifact.created_at.desc())
                .first()
            )
            if prior and prior.state_reason:
                parts.append(
                    f"Last run this metric was flagged: {prior.state_reason}. "
                    "Address the cause or, if it still cannot be grounded, abstain (record a "
                    "low-confidence assumption) — do not repeat a blind guess."
                )
        except Exception as e:  # pragma: no cover - feedback is best-effort
            logger.debug("prior_reason_lookup_failed", graph_id=graph.graph_id, error=str(e))

        groundings: list[str] = []
        for step_id, info in (cached_snippets or {}).items():
            basis = info.get("column_mappings_basis")
            if basis:
                groundings.append(f"  - {step_id}: {json.dumps(basis)}")
        if groundings:
            parts.append(
                "Prior value→concept groundings (reuse the same columns/filters unless wrong):\n"
                + "\n".join(groundings)
            )

        return "\n\n".join(parts)
