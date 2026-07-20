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
from pydantic import ValidationError
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.config import LLMConfig
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.prompts import PromptRenderer
from dataraum.llm.providers.base import LLMProvider

from .models import (
    AssumptionBasis,
    ExtractGroundingOutput,
    FailedSnippetProvenance,
    GraphAssumptionOutput,
    GraphExecution,
    GraphProvenanceOutput,
    GraphStep,
    HealthySnippetProvenance,
    QueryAssumption,
    SnippetAssumption,
    SnippetFailureMode,
    StepResult,
    StepType,
    TransformationGraph,
    ValueSearchInput,
)
from .verifier import verify_execution

if TYPE_CHECKING:
    from dataraum.graphs.node_warming import NodeDecision, NodeKey

logger = get_logger(__name__)

# The grounding agent's catalog-search budget (DAT-699): enough turns to
# resolve a couple of concepts' exact values on a high-cardinality
# discriminator, small enough that a lost agent fails loud instead of
# spelunking. Each search is one bounded DuckDB DISTINCT query.
_MAX_VALUE_SEARCHES = 4
_VALUE_SEARCH_LIMIT = 25


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

    # Generated SQL. A freshly-authored EXTRACT step also carries "parts" —
    # the clause-parts dict (DAT-671) its "sql" was rendered from; formula/
    # constant steps and cache-composed steps carry none here (cached parts
    # live on the snippet row).
    summary: str  # Plain English description of what the query calculates
    steps: list[dict[str, Any]]  # List of {step_id, sql, description[, parts]}
    final_sql: str

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
            vertical=vertical,
            om_run_id=om_run_id,
            catalogue_run_id=catalogue_run_id,
            # The graph traversal core (DAT-734) resolves the read schema from
            # the workspace identity — which in every engine path IS the
            # schema_mapping_id (DAT-506; ``execute`` is called with
            # ``workspace_id=schema_mapping_id`` at all call sites).
            workspace_id=kwargs.get("schema_mapping_id"),
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
        cached_snippets = self._lookup_snippets(session, graph, schema_mapping_id)

        # Inject inspiration SQL as a hint (from snippet promotion path)
        if inspiration_sql and not cached_snippets:
            cached_snippets["_inspiration"] = {
                "sql": inspiration_sql,
                "description": "SQL hint from promoted ad-hoc query",
                "snippet_id": None,
            }

        generated_code: GeneratedCode | None
        # DAT-646: the warm DAG warms only leaf EXTRACTs — a FORMULA/CONSTANT is
        # deterministic and metric-specific, composed per-metric in ``assemble`` (NOT
        # warmed, NOT cross-metric shared). So ``execute`` only ever authors a single
        # extract: a cache-assemble when it was already minted on a prior run, else the
        # one LLM grounding call (the sole LLM surface in the pipeline).
        #
        # Gate on the EXTRACT leaves specifically — not ``len(cached) == len(steps)``.
        # FORMULA/CONSTANT steps are recomposed (never cached), and a non-step hint key
        # (``_inspiration``) inflates the dict count; either makes a raw length compare
        # mis-route. "Every EXTRACT leaf is cached" is the precise compose precondition.
        extract_ids = [
            step_id for step_id, step in graph.steps.items() if step.step_type == StepType.EXTRACT
        ]
        all_extracts_cached = bool(extract_ids) and all(
            step_id in cached_snippets for step_id in extract_ids
        )
        if all_extracts_cached:
            generated_code = self._compose_metric_from_dag(graph, cached_snippets, resolved_params)
            if generated_code:
                logger.debug(
                    "assembled_from_cache",
                    graph_id=graph.graph_id,
                    snippet_count=len(cached_snippets),
                )
                # Track usage: all steps were exact reuses
                self._track_snippet_usage(
                    session=session,
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
                workspace_id=workspace_id,
            )
            if not gen_result.success or not gen_result.value:
                return Result.fail(gen_result.error or "SQL generation failed")

            generated_code = gen_result.value

            # Track usage: compare generated steps against provided snippets
            self._track_snippet_usage(
                session=session,
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
            # map. Do NOT flag the cached DEP snippets as failed: they were
            # decided-once and grounded by their OWN authoring, so blaming them for
            # THIS node's failure poisons shared extracts (a broken formula would mark
            # `revenue` failed → every metric using it can no longer find it, and
            # honest metrics like dso silently break). DAT-636 (Bug B). (The bulk
            # failure-flag path this warned against, ``record_failure()``, had zero
            # callers and was removed — DAT-781; this comment records the design
            # reason it was never wired, not merely dead code.)
            reason = exec_result.error or "SQL execution failed"
            self._save_failed_snippet(
                session,
                graph,
                generated_code,
                schema_mapping_id,
                workspace_id=workspace_id,
                mode=SnippetFailureMode.EXECUTION_FAILED,
                reason=reason,
            )
            return Result.fail(reason)

        execution = exec_result.value

        # Verifier gate (DAT-616): execution-pass is NOT validation. A node whose SQL
        # ran cleanly is still inconclusive if it had no support (empty filter -> NULL),
        # the value is degenerate (NULL), or a catalogue-declared condition is violated.
        # Such a node stays ungroundable with the reason — never executed-green. The SQL
        # is NOT reusable (excluded below), but it IS retained flagged (DAT-543): a
        # verifier_rejected extract is VALID SQL whose value the data made untrustworthy,
        # so keeping it feeds prior_context + the cockpit's ungroundable-node detail.
        verdict = verify_execution(graph, execution)
        if not verdict.success:
            reason = verdict.error or "metric verification failed"
            self._save_failed_snippet(
                session,
                graph,
                generated_code,
                schema_mapping_id,
                workspace_id=workspace_id,
                mode=SnippetFailureMode.VERIFIER_REJECTED,
                reason=reason,
            )
            return Result.fail(reason)
        execution.verification_flags = verdict.unwrap() or []

        # Save snippets AFTER successful execution AND verification — only SQL
        # that actually works AND is trustworthy, with its clause parts.
        self._save_snippets(
            session=session,
            graph=graph,
            generated_code=generated_code,
            schema_mapping_id=schema_mapping_id,
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

        # Collect EVERY ungroundable dependency — born-loud, no re-authoring. A
        # keyable step absent from the map is a contract violation (the authoring
        # pass authors every keyable node), so it fails loud here too. The metric
        # still honest-fails, but the groundable subgraph EXECUTES first
        # (DAT-699): a groundable revenue leaf can otherwise run zero times all
        # day because gross_profit aborts whole on cost_of_goods_sold, and the
        # artifact reason says nothing about what WAS measurable.
        ungroundable: dict[str, str] = {}
        for step_id, step in graph.steps.items():
            key = node_key(step, graph)
            if key is None:
                continue  # non-keyable step (rare) — caught by the cache check below
            decision = bindings.get(key)
            if decision is None or not decision.grounded:
                ungroundable[step_id] = (
                    (decision.reason or "ungroundable")
                    if decision is not None
                    else "not authored (absent from binding map)"
                )
        if ungroundable:
            cached = self._lookup_snippets(session, graph, schema_mapping_id)
            return Result.fail(
                self._partial_execution_report(
                    graph, cached, resolved_params, ungroundable, context
                )
            )

        # Every extract dep grounded → compose the metric PER-METRIC from the DAG
        # (DAT-646): extract leaves come from the warm cache, formulas/constants are
        # composed here. Only extracts are cached now — formulas/constants are not
        # warmed, so the cache holds exactly the metric's extract leaves.
        cached_snippets = self._lookup_snippets(session, graph, schema_mapping_id)
        missing = [
            step_id
            for step_id, step in graph.steps.items()
            if step.step_type == StepType.EXTRACT and step_id not in cached_snippets
        ]
        if missing:
            return Result.fail(
                f"metric '{graph.graph_id}': extract leaves {missing} grounded per the "
                "binding map but absent from the snippet cache"
            )
        generated_code = self._compose_metric_from_dag(graph, cached_snippets, resolved_params)
        if generated_code is None:
            return Result.fail(f"metric '{graph.graph_id}': failed to compose from the DAG")
        self._track_snippet_usage(
            session=session,
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
        execution.verification_flags = verdict.unwrap() or []

        # Persist THIS metric's composed FORMULA/CONSTANT snippets (DAT-646) AFTER it
        # executed AND verified — only trustworthy SQL enters the cockpit reuse KB. The
        # extract leaves were already saved by the warm pass; these have no other home.
        self._save_composed_snippets(
            session=session,
            graph=graph,
            generated_code=generated_code,
            schema_mapping_id=schema_mapping_id,
            resolved_params=resolved_params,
            workspace_id=workspace_id,
        )
        return Result.ok(execution)

    def _compose_metric_from_dag(
        self,
        graph: TransformationGraph,
        cached_snippets: dict[str, dict[str, Any]],
        resolved_params: dict[str, Any],
    ) -> GeneratedCode | None:
        """Compose a metric's SQL PER-METRIC from the DAG — no cross-metric reuse (DAT-646).

        Every step becomes a CTE, materialized in dependency order
        (:func:`_ordered_dep_steps` + the output last):

        - an **EXTRACT** leaf uses its warmed, concept-keyed cached snippet (the sole
          shared / LLM surface — authored once, reused correctly);
        - a **CONSTANT** is composed here (``compose_constant_sql`` over the resolved
          parameter value);
        - a **FORMULA** is composed here (``compose_formula_sql`` over its dep step ids,
          which are earlier CTEs).

        Formulas and constants are therefore scoped to THIS metric — two metrics that
        share an arithmetic shape can no longer alias a formula snippet (the DAT-646
        ``net_margin``/``ebitda_margin`` collision). ``final_sql`` selects the output
        CTE. Returns ``None`` when an extract leaf is absent (its dep ungroundable — the
        caller honest-fails) or a step is malformed.
        """
        output_step = graph.get_output_step()
        if output_step is None:
            return None

        # Every step, deps-before-dependents, output last.
        ordered = _ordered_dep_steps(graph, output_step) + [output_step.step_id]
        steps: list[dict[str, str]] = []
        # DAT-631: carry each EXTRACT snippet's authored grounding confidence forward, so
        # a cache-composed metric still surfaces its weakest input's confidence to the
        # phase gate instead of looking confidently green.
        assumptions: list[GraphAssumptionOutput] = []
        for step_id in ordered:
            step = graph.steps.get(step_id)
            if step is None:
                return None
            sql = self._compose_step_sql(step, step_id, cached_snippets, resolved_params)
            if sql is None:
                # Missing extract snippet / unresolvable constant / malformed
                # formula — the metric honest-fails (caller surfaces the reason).
                return None
            description = step_id
            if step.step_type == StepType.EXTRACT:
                snippet = cached_snippets.get(step_id) or {}
                description = snippet.get("description") or step_id
                for a in snippet.get("assumptions") or []:
                    # Defensive coercion at the CACHE-READ boundary: rows written
                    # before basis was enum-typed (contract v2, DAT-727) persisted
                    # the model's raw string, and this reconstruction runs on every
                    # cache-assemble — a ValidationError here would wedge a HEALTHY
                    # snippet forever (first-writer-wins never replaces it), so an
                    # off-vocabulary value degrades to INFERRED with a warning
                    # instead of crashing. New writes are enum-enforced at save.
                    raw_basis = a.get("basis", "inferred")
                    try:
                        basis = AssumptionBasis(raw_basis)
                    except ValueError:
                        logger.warning(
                            "unknown_cached_assumption_basis", basis=raw_basis, step_id=step_id
                        )
                        basis = AssumptionBasis.INFERRED
                    assumptions.append(
                        GraphAssumptionOutput(
                            dimension=a.get("dimension", "grounding.cached"),
                            target=a.get("target", f"step:{step_id}"),
                            assumption=a.get("assumption", ""),
                            basis=basis,
                            confidence=a.get("confidence", 0.5),
                        )
                    )
            steps.append({"step_id": step_id, "sql": sql, "description": description})

        return GeneratedCode(
            code_id=str(uuid4()),
            graph_id=graph.graph_id,
            summary=f"Composed {graph.graph_id} from DAG ({len(steps)} steps)",
            steps=steps,
            final_sql=f"SELECT * FROM {output_step.step_id}",
            llm_model="composed",
            prompt_hash="composed",
            generated_at=datetime.now(UTC),
            assumptions=assumptions,
        )

    @staticmethod
    def _compose_step_sql(
        step: GraphStep,
        step_key: str,
        cached_snippets: dict[str, dict[str, Any]],
        resolved_params: dict[str, Any],
    ) -> str | None:
        """One step's CTE SQL: extract = cached snippet, constant/formula = composed.

        ``step_key`` is the step's key in ``graph.steps`` — the dependency
        namespace that CTE names and the snippet cache are keyed on. It usually
        equals ``step.step_id`` but is passed explicitly because nothing
        enforces that, and a lookup on ``step.step_id`` silently misses when
        they diverge. ``None`` = not composable (missing snippet, unresolvable
        constant, malformed formula) — both the full compose and the DAT-699
        partial execution treat that as this step's honest hole.
        """
        from dataraum.graphs.formula_composer import compose_constant_sql, compose_formula_sql

        try:
            if step.step_type == StepType.EXTRACT:
                snippet = cached_snippets.get(step_key)
                return snippet["sql"] if snippet else None
            if step.step_type == StepType.CONSTANT:
                value = resolved_params.get(step.parameter) if step.parameter else None
                return compose_constant_sql(value) if value is not None else None
            if step.step_type == StepType.FORMULA:
                if not step.expression:
                    return None
                return compose_formula_sql(step.expression, set(step.depends_on))
        except ValueError:
            # Malformed expression / non-numeric constant — the composer raises;
            # the step is not composable.
            return None
        return None

    def _partial_execution_report(
        self,
        graph: TransformationGraph,
        cached_snippets: dict[str, dict[str, Any]],
        resolved_params: dict[str, Any],
        ungroundable: dict[str, str],
        context: ExecutionContext,
    ) -> str:
        """Execute the groundable subgraph and report EVERY step's outcome (DAT-699).

        A metric with an ungroundable extract used to abort whole — a
        groundable sibling (revenue) could execute zero times all day because
        every profitability metric died on cost_of_goods_sold first, and the
        artifact reason said nothing about what WAS measurable. The
        metric still honest-fails (its composed value cannot exist), but every
        step whose transitive dependencies ground executes, and the reason
        names each step's measured value, its hole, or what blocks it — the
        exact per-step story the drill-down (DAT-671) renders.
        """
        output_step = graph.get_output_step()
        ordered = (
            _ordered_dep_steps(graph, output_step) + [output_step.step_id]
            if output_step is not None
            else []
        )
        # Steps outside the output's dependency cone still get reported —
        # nothing in the graph is silently absent from the per-step story.
        ordered += sorted(step_id for step_id in graph.steps if step_id not in set(ordered))

        # The maximal executable subgraph: a step runs iff it is not a hole,
        # composes, and every dependency it names is itself executable.
        executable: list[dict[str, str]] = []
        executable_ids: set[str] = set()
        blocked: dict[str, str] = {}
        for step_id in ordered:
            step = graph.steps.get(step_id)
            if step is None or step_id in ungroundable:
                continue
            missing = [d for d in step.depends_on if d not in executable_ids]
            if missing:
                blocked[step_id] = ", ".join(missing)
                continue
            sql = self._compose_step_sql(step, step_id, cached_snippets, resolved_params)
            if sql is None:
                blocked[step_id] = "not composable"
                continue
            executable.append({"step_id": step_id, "sql": sql, "description": step_id})
            executable_ids.add(step_id)

        values: dict[str, Any] = {}
        exec_note = ""
        if executable:
            code = GeneratedCode(
                code_id=str(uuid4()),
                graph_id=graph.graph_id,
                summary=f"Partial execution of {graph.graph_id} ({len(executable)} steps)",
                steps=executable,
                final_sql=f"SELECT * FROM {executable[-1]['step_id']}",
                llm_model="composed",
                prompt_hash="composed",
                generated_at=datetime.now(UTC),
            )
            exec_result = self._execute_sql(code, context, graph)
            if exec_result.success and exec_result.value:
                values = {sr.step_id: sr.value for sr in exec_result.value.step_results}
            else:
                exec_note = f" (partial execution failed: {exec_result.error})"

        parts: list[str] = []
        for step_id in ordered:
            if step_id in ungroundable:
                parts.append(f"{step_id} ✗ {ungroundable[step_id]}")
            elif step_id in blocked:
                parts.append(f"{step_id} blocked (needs {blocked[step_id]})")
            elif step_id in values:
                value = values[step_id]
                if value is None:
                    parts.append(f"{step_id} = NULL (aggregated with no measured support)")
                elif isinstance(value, bool):
                    # bool BEFORE the numeric check (bool is an int subclass) —
                    # a boolean step must read True/False, never 1.00.
                    parts.append(f"{step_id} = {value} ✓")
                elif isinstance(value, (int, float, Decimal)):
                    parts.append(f"{step_id} = {float(value):,.2f} ✓")
                else:
                    parts.append(f"{step_id} = {value} ✓")
            elif step_id in executable_ids:
                parts.append(f"{step_id} composed but not executed")
            else:
                # A dep referenced by the graph but absent from graph.steps
                # (cache-only leaf) — nothing is silently absent from the story.
                parts.append(f"{step_id} not defined in the graph")
        names = ", ".join(f"'{s}'" for s in sorted(ungroundable))
        verb = "is" if len(ungroundable) == 1 else "are"
        return f"dependency {names} {verb} ungroundable — " + " · ".join(parts) + exec_note

    def _generate_sql(
        self,
        session: Session,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any],
        cached_snippets: dict[str, dict[str, Any]] | None = None,
        *,
        workspace_id: str,
    ) -> Result[GeneratedCode]:
        """Ground a single leaf EXTRACT to SQL via the LLM (tool-based output).

        EXTRACT is the SOLE LLM authoring surface (DAT-643): a FORMULA/CONSTANT is
        composed deterministically in ``_compose_metric_from_dag`` and never reaches
        here, so this path only ever grounds one leaf extract against the dataset
        context + field mappings. ``cached_snippets`` feeds the DAT-616 prior context
        (a cached extract is ASSEMBLED upstream, never re-authored), so an extract is a
        leaf with no dependency steps to carry into the prompt.

        The output schema is single-extract (``ExtractGroundingOutput``, DAT-603): the
        model returns one SQL statement and THIS code binds it to the graph's own leaf
        id — the model never names a step, so the DAT-664 id-paraphrase class cannot
        occur. The graph must therefore BE a single-extract mini-graph
        (``node_warming.build_mini_graph``); anything else fails loud here.
        """
        from dataraum.llm.providers.base import (
            ConversationRequest,
            Message,
            ToolDefinition,
            ToolResult,
        )

        extract_leaves = [
            step
            for step in graph.steps.values()
            if step.step_type == StepType.EXTRACT and step.source
        ]
        if len(extract_leaves) != 1 or len(graph.steps) != 1:
            return Result.fail(
                f"graph '{graph.graph_id}' is not a single-extract mini-graph "
                f"({len(graph.steps)} steps, {len(extract_leaves)} extract leaves) — "
                "authoring grounds exactly one leaf (DAT-646); metrics are assembled "
                "from the binding map, never authored whole"
            )
        leaf = extract_leaves[0]

        # Serialize graph to YAML for LLM context.
        graph_yaml = self._graph_to_yaml(graph)

        prompt_name = "graph_sql_generation"
        # Tier/effort from feature config (DAT-603) — absent entry keeps the
        # defaults: balanced tier, API-default effort. `enabled` is deliberately
        # not consulted: grounding IS the pipeline, not an optional feature.
        feature_config = self.config.features.graph_sql_generation
        tier = feature_config.model_tier if feature_config else "balanced"
        # Extract grounding needs the dataset context + field mappings — fail loud
        # if the semantic phase did not produce them.
        if context.rich_context is None:
            return Result.fail(
                "Cannot generate SQL without dataset context. "
                "Use ExecutionContext.with_rich_context() to build context."
            )
        if not context.rich_context.field_mappings:
            return Result.fail(
                "Cannot generate SQL without the column meaning feed. "
                "Run the semantic phase to author column meanings."
            )
        from dataraum.graphs.context import format_served_context
        from dataraum.graphs.field_mapping import format_meanings_for_prompt

        # Built ONCE and shared by the prompt's <data_schema> block AND the
        # contract-v2 enforcement below — "served" means the same thing in both.
        schema_info = self._build_schema_info(context)
        prompt_context = {
            "graph_yaml": graph_yaml,
            "table_schema": json.dumps(schema_info, indent=2),
            "parameters": json.dumps(parameters, indent=2),
            "rich_context": format_served_context(context.rich_context),
            "field_mappings": format_meanings_for_prompt(context.rich_context.field_mappings),
            # DAT-616: feed back what prior runs learned for this concept — the
            # honest-fail reason + prior value→concept filter decisions.
            "prior_context": self._build_prior_context(
                session, graph, cached_snippets, context.schema_mapping_id or "default"
            ),
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

        # The typed grounding is a structured OUTPUT (DAT-807), not a tool: the
        # answer arrives as JSON message content constrained to the schema. The
        # ONE tool here is a tool the model genuinely calls — the bounded catalog
        # search (DAT-699), since high-cardinality discriminators are served
        # size+sample only and their exact values live behind search_values.
        # ``strict`` is right for it: a three-string fixed-shape input, nothing
        # a strict grammar can make the model under-produce.
        search_tool = ToolDefinition(
            name="search_values",
            description=(
                "Search a column's distinct values by case-insensitive substring. "
                "Use it to resolve the EXACT values of a high-cardinality "
                "discriminator (marked 'NOT enumerated' in the Value sets) before "
                "writing an IN-list — never guess a predicate. Always finish by "
                "calling generate_sql."
            ),
            input_schema=ValueSearchInput.model_json_schema(),
            strict=True,
        )

        # Thinking (DAT-603): grounding is the pipeline's hardest reasoning task
        # and Sonnet 5-class models expose no sampling knobs — the model's own
        # reflection is the quality lever. A FORCED tool_choice silently
        # suppresses thinking on the live API (probed 2026-07-03: forced -> no
        # thinking block, auto -> thinking block), so the tools are offered on
        # AUTO. Since DAT-807 auto is also the only correct choice regardless of
        # thinking: the grounding is a structured OUTPUT, so the turn that
        # finishes calls no tool at all — "any" would force a search_values call
        # forever. disable_parallel_tool_use keeps every turn to ONE call, so the
        # search loop stays sequential and binding [0] can never ground a
        # superseded SQL.
        thinking = bool(feature_config and feature_config.thinking)
        tool_choice: dict[str, Any] = {"type": "auto", "disable_parallel_tool_use": True}
        messages = [Message(role="user", content=user_prompt)]

        def _converse() -> Any:
            # converse raises a typed ProviderError on an API failure (DAT-503) —
            # retryability rides the exception to the worker's durable boundary,
            # so we don't re-wrap it. A returned Result is always a success.
            return self.provider.converse(
                ConversationRequest(
                    messages=messages,
                    system=system_prompt,
                    tools=[search_tool],
                    tool_choice=tool_choice,
                    output_schema=ExtractGroundingOutput.model_json_schema(),
                    thinking=thinking,
                    label=prompt_name,
                    effort=feature_config.effort if feature_config else None,
                    max_tokens=self.config.limits.max_output_tokens_per_request,
                    temperature=temperature,
                    model=model,
                )
            ).unwrap()

        response = _converse()

        # Bounded exploration loop (DAT-699): answer each search_values call and
        # continue the conversation (raw_content round-trips the signed thinking
        # blocks). The budget is small — a lost agent fails loud below, never
        # spelunks. The last allowed search's result carries the budget notice.
        searches = 0
        while (
            searches < _MAX_VALUE_SEARCHES
            and len(response.tool_calls) == 1
            and response.tool_calls[0].name == "search_values"
        ):
            searches += 1
            search_call = response.tool_calls[0]
            outcome = self._run_value_search(context, search_call.input)
            if searches == _MAX_VALUE_SEARCHES:
                outcome += "\n(search budget exhausted — call generate_sql now)"
            logger.info(
                "grounding_value_search",
                graph_id=graph.graph_id,
                pattern=search_call.input.get("pattern"),
                turn=searches,
            )
            messages.append(
                Message(
                    role="assistant",
                    content=response.content,
                    tool_calls=response.tool_calls,
                    raw_content=response.raw_content,
                )
            )
            messages.append(
                Message(
                    role="user",
                    content=[ToolResult(tool_use_id=search_call.id, content=outcome)],
                )
            )
            response = _converse()

        # The grounding is the turn's structured-output CONTENT (DAT-807). Still
        # holding a search_values call here means the agent burned its budget
        # without ever grounding — a bind ERROR, never a guess (DAT-439's
        # born-loud cut): a metric that can't be composed stays grounded with the
        # reason, it does not get a guessed SQL.
        if response.tool_calls:
            return Result.fail(
                f"LLM ended on a {response.tool_calls[0].name} call instead of the "
                "grounding output"
                + (" (search budget exhausted)" if searches >= _MAX_VALUE_SEARCHES else "")
            )

        try:
            output = ExtractGroundingOutput.model_validate_json(response.content)
        except ValidationError as e:
            # Constrained decoding guarantees the shape, so this is the API
            # contract breaking, not the model being lazy — fail loud.
            return Result.fail(f"Failed to parse the extract grounding output: {e}")

        # Provenance contract v2 (DAT-727): the enumerated columns in
        # column_mappings_basis are the operating-model graph's `uses` substrate
        # (og_uses un-nests them), so they are ENFORCED here — against the SAME
        # served schema the prompt rendered — never trusted, never recovered by
        # parsing the SQL later. Membership + completeness violations get one
        # contract-repair turn (DAT-807: the ONE repair that survives —
        # constrained decoding guarantees shape, never semantics); a still-invalid output
        # falls loud into the failed-snippet path (DAT-543) so the authored SQL
        # + the exact violations feed the next run's prior_context instead of
        # vanishing.
        from dataraum.graphs.grounding_validation import (
            schema_tables_from_info,
            validate_grounding_basis,
        )
        from dataraum.llm.tool_repair import repair_tool_contract

        schema_tables = schema_tables_from_info(schema_info)
        violations = validate_grounding_basis(output, schema_tables, context.duckdb_conn)
        if violations:
            repaired = repair_tool_contract(
                self.provider,
                output.model_dump(mode="json"),
                violations,
                ExtractGroundingOutput,
                model=model,
                label=prompt_name,
                max_tokens=self.config.limits.max_output_tokens_per_request,
            )
            if not repaired.success:
                return Result.fail(repaired.error or "grounding contract repair failed")
            output = repaired.unwrap()
            violations = validate_grounding_basis(output, schema_tables, context.duckdb_conn)

        # Bind the one generated grounding to the graph's own leaf id (the model
        # never names a step — DAT-664's id-paraphrase class is gone by
        # construction). The model emits CLAUSE PARTS (DAT-671); the fused
        # statement is rendered exactly once, here, and the parts travel with
        # the step so persistence keeps them as the artifact.
        from dataraum.graphs.formula_composer import compose_extract_sql, extract_parts_dict

        # The output model states every attribute (DAT-807): the fall-loud case is
        # relation="" rather than an omitted/None key. Normalize the sentinel back to
        # None here so the PERSISTED parts keep their existing shape.
        relation = output.relation or None
        rendered_sql = compose_extract_sql(output.select_expr, relation, output.where)
        generated_code = GeneratedCode(
            code_id=str(uuid4()),
            graph_id=graph.graph_id,
            summary=output.description,
            steps=[
                {
                    "step_id": leaf.step_id,
                    "sql": rendered_sql,
                    "description": output.description,
                    "parts": extract_parts_dict(output.select_expr, relation, output.where),
                }
            ],
            final_sql=f"SELECT * FROM {leaf.step_id}",
            provenance=output.provenance,
            assumptions=output.assumptions or [],
            llm_model=model,
            prompt_hash=prompt_hash,
            generated_at=datetime.now(UTC),
        )

        if violations:
            # Contract still violated after the repair turn: retain the authored
            # SQL flagged (DAT-543) with the violations as the reason — the graph
            # cannot ground `uses` edges on an unenforced enumeration, and the
            # next authoring revises against the exact violations instead of
            # re-deriving blind.
            reason = "grounding contract violated after repair: " + "; ".join(violations)
            self._save_failed_snippet(
                session,
                graph,
                generated_code,
                context.schema_mapping_id or "default",
                workspace_id=workspace_id,
                mode=SnippetFailureMode.PROVENANCE_INVALID,
                reason=reason,
            )
            return Result.fail(reason)

        # Verification half (DAT-631): append what the agent PRODUCED to the
        # prompt dump — the SQL, per-concept grounding, and confidence — so a
        # metric that fails verification (and never persists a snippet) is still
        # inspectable offline. No-op unless prompt_dump_dir is set.
        from dataraum.llm.prompt_log import dump_response

        basis = {
            e.concept: e.basis.model_dump(mode="json")
            for e in output.provenance.column_mappings_basis
        }
        response_body = json.dumps(
            {
                "step_id": leaf.step_id,
                "grounding": output.grounding,
                "relation": relation,
                "where": output.where,
                "select_expr": output.select_expr,
                "sql": rendered_sql,
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

    def _run_value_search(self, context: ExecutionContext, raw_input: dict[str, Any]) -> str:
        """Execute one bounded catalog value search (DAT-699).

        Returns compact text for the tool_result. Errors come back as TEXT (an
        unknown table/column or a bad pattern is the agent's to correct within
        its search budget), never as an exception — a broken search must not
        kill the grounding turn. Table and column names are validated against
        our own catalog before touching SQL; only the pattern is user…model
        text, escaped for the one ILIKE literal it lands in.
        """
        try:
            params = ValueSearchInput.model_validate(raw_input)
        except ValidationError as e:
            return f"invalid search_values input: {e}"
        rich = context.rich_context
        tables = {t.table_name: t for t in getattr(rich, "tables", None) or []}
        table = tables.get(params.table)
        if table is None or not table.duckdb_name:
            known = ", ".join(sorted(tables)) or "(none)"
            return f"unknown table '{params.table}' — known tables: {known}"
        if params.column not in {c.column_name for c in table.columns}:
            known = ", ".join(sorted(c.column_name for c in table.columns))
            return f"unknown column '{params.column}' on '{params.table}' — columns: {known}"
        if context.duckdb_conn is None:
            return "no data connection available for value search"
        needle = (
            params.pattern.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
            .replace("'", "''")
        )
        try:
            rows = context.duckdb_conn.execute(
                f'SELECT CAST("{params.column}" AS VARCHAR) AS value, COUNT(*) AS n '
                f'FROM "{table.duckdb_name}" '
                f'WHERE "{params.column}" IS NOT NULL '
                f"AND CAST(\"{params.column}\" AS VARCHAR) ILIKE '%{needle}%' ESCAPE '\\' "
                f"GROUP BY 1 ORDER BY n DESC, value LIMIT {_VALUE_SEARCH_LIMIT}"
            ).fetchall()
        except Exception as e:
            return f"search failed: {e}"
        if not rows:
            return f"no values matching '{params.pattern}' in {params.table}.{params.column}"
        listed = "\n".join(f"- {v} ({n} rows)" for v, n in rows)
        truncated = (
            f"\n(first {_VALUE_SEARCH_LIMIT} matches by frequency — narrow the pattern for more)"
            if len(rows) == _VALUE_SEARCH_LIMIT
            else ""
        )
        return (
            f"values matching '{params.pattern}' in "
            f"{params.table}.{params.column}:\n{listed}{truncated}"
        )

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

        # Convert LLM assumptions to QueryAssumption objects. `basis` is already
        # the AssumptionBasis enum — typed at the tool-output boundary (contract
        # v2, DAT-727), so the old string map with a silent INFERRED fallback is
        # gone: an off-vocabulary basis fails schema validation and gets the
        # repair turn instead.
        execution.assumptions = [
            QueryAssumption.create(
                execution_id=execution.execution_id,
                dimension=a.dimension,
                target=a.target,
                assumption=a.assumption,
                basis=a.basis,
                confidence=a.confidence,
            )
            for a in generated_code.assumptions or []
        ]

        # Convert generated code steps to shared format
        steps = [
            SQLStep(
                step_id=s.get("step_id", "unknown"),
                sql=s.get("sql", ""),
                description=s.get("description", ""),
            )
            for s in generated_code.steps
        ]

        # NO text repair (DAT-671; execute_sql_steps no longer carries any): a
        # repaired statement would silently diverge from the clause parts it
        # was rendered from (and, before parts, already diverged from the
        # model's committed column_mappings_basis). A failing extract
        # honest-fails into the retained-failure → prior_context → re-author
        # loop (DAT-543/616) — looping is the graph agent's healing mechanism,
        # not in-place rewrites. The retained failure rows are the measurement
        # of what this costs.
        exec_result = execute_sql_steps(
            steps=steps,
            final_sql=generated_code.final_sql,
            duckdb_conn=context.duckdb_conn,
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
        (`format_served_context`); this returns physical name + type only.
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
                    # Declared post-execution expectations (DAT-792): served to the
                    # authoring LLM so its grounding is consistent with what the
                    # catalogue declares about the value (e.g. `value > 0`). The
                    # post-hoc verifier stays the enforcement backstop (DAT-616).
                    # Unrelated to ``ContextDocument.validations`` (graphs/context.py)
                    # — those are executed data-quality rule RESULTS, not declared
                    # per-step expectations.
                    **(
                        {
                            "validations": [
                                {
                                    "condition": v.condition,
                                    "severity": v.severity,
                                    **({"message": v.message} if v.message else {}),
                                }
                                for v in step.validations
                            ]
                        }
                        if step.validations
                        else {}
                    ),
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
        cached_snippets: dict[str, dict[str, Any]],
        generated_steps: list[dict[str, str]],
        *,
        workspace_id: str,
    ) -> None:
        """Bump ``execution_count`` for cached snippets reused in this graph execution.

        Compares each generated step's SQL against the snippet offered for its
        step_id; ``record_usage`` bumps the count only for ``exact_reuse``/
        ``adapted`` matches (steps with no provided snippet, or a provided
        snippet not reflected in the output, are no-ops — DAT-781 removed the
        per-execution usage audit trail this used to also write).
        """
        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_utils import determine_usage_type

        library = SnippetLibrary(session, workspace_id=workspace_id)

        for gen_step in generated_steps:
            step_id = gen_step.get("step_id", "")
            provided = cached_snippets.get(step_id)
            if provided is None:
                continue
            snippet_id = provided.get("snippet_id")
            usage_type = determine_usage_type(
                gen_step.get("sql", ""),
                provided.get("sql", ""),
            )
            library.record_usage(snippet_id, usage_type)

    @staticmethod
    def _build_snippet_provenance(
        generated_code: GeneratedCode,
    ) -> dict[str, Any] | None:
        """The provenance blob saved alongside a snippet — typed (DAT-727).

        Carries the LLM grounding decision (column_mappings_basis) and —
        crucially for the phase confidence gate — the per-input
        ``assumptions`` (so a metric ASSEMBLED from cache still surfaces its weakest
        grounding's confidence). Composed FORMULA/CONSTANT snippets have no LLM
        provenance but DO carry forward their extract leaves' assumptions.
        (The ``was_repaired`` flag died with graph-path text repair, DAT-671 —
        a snippet's SQL can no longer diverge from its committed grounding.)

        The persisted shape is ``HealthySnippetProvenance.model_dump`` — the
        contract-v2 payload ``og_uses`` un-nests. Built through the model, never
        as a free dict, so the writer cannot drift from the graph's reader.
        """
        if not generated_code.provenance and not generated_code.assumptions:
            return None
        payload = HealthySnippetProvenance(
            # LIST of {concept, basis} on the wire (DAT-807), MAP in storage — the
            # persisted shape og_uses un-nests is unchanged.
            column_mappings_basis=(
                {e.concept: e.basis for e in generated_code.provenance.column_mappings_basis}
                if generated_code.provenance
                else {}
            ),
            assumptions=[
                SnippetAssumption(assumption=a.assumption, basis=a.basis, confidence=a.confidence)
                for a in generated_code.assumptions
            ],
        )
        return payload.model_dump(mode="json")

    def _save_snippets(
        self,
        session: Session,
        graph: TransformationGraph,
        generated_code: GeneratedCode,
        schema_mapping_id: str,
        *,
        workspace_id: str,
    ) -> None:
        """Save grounded EXTRACT leaves as snippets for cross-metric reuse.

        Called only from the authoring path (``execute`` on a single-output EXTRACT
        mini-graph — the sole LLM surface, DAT-646). ONLY EXTRACT leaves are saved
        here: they are the shared, concept-keyed cache. FORMULA/CONSTANT snippets are
        deterministic and composed per-metric, so they are persisted by
        ``_save_composed_snippets`` (from ``assemble``) sourced to their own metric —
        never shared by shape (the aliasing DAT-646 removed).

        Called AFTER successful execution AND verification, so only working,
        trustworthy SQL is saved. The step's clause ``parts`` (DAT-671) persist
        alongside the rendered ``sql`` — the parts are the artifact, the sql is
        their render, and nothing between authoring and here may rewrite either
        (graph-path text repair was removed for exactly that reason).
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session, workspace_id=workspace_id)
        source = f"graph:{graph.graph_id}"

        generated_steps: dict[str, dict[str, Any]] = {}
        for step_dict in generated_code.steps:
            step_id = step_dict.get("step_id", "")
            if step_id:
                generated_steps[step_id] = step_dict

        provenance_dict = self._build_snippet_provenance(generated_code)

        saved_count = 0
        for step_id, graph_step in graph.steps.items():
            if graph_step.step_type != StepType.EXTRACT or not graph_step.source:
                continue
            gen_step = generated_steps.get(step_id)
            if not gen_step:
                # Structurally unreachable since DAT-603: step ids are assigned by
                # THIS code (authoring binds the leaf id; compose copies graph ids) —
                # the model can no longer drift them (DAT-664). Kept LOUD as a
                # regression guard: a grounded leaf whose snippet fails to save
                # starves every metric composed from it.
                logger.warning(
                    "snippet_save_skipped",
                    graph_id=graph.graph_id,
                    step_id=step_id,
                    generated_step_ids=sorted(generated_steps),
                )
                continue

            description = gen_step.get("description", "") or generated_code.summary

            # Extract snippet: keyed by standard_field + statement + aggregation.
            # Per-concept grounding lives in provenance.column_mappings_basis.
            library.save_snippet(
                snippet_type="extract",
                sql=gen_step.get("sql", ""),
                description=description,
                schema_mapping_id=schema_mapping_id,
                source=source,
                standard_field=graph_step.source.standard_field,
                statement=graph_step.source.statement,
                aggregation=graph_step.aggregation,
                provenance=provenance_dict,
                parts=gen_step.get("parts"),
            )
            saved_count += 1

        logger.info("saved_snippets", graph_id=graph.graph_id, count=saved_count)

    def _save_failed_snippet(
        self,
        session: Session,
        graph: TransformationGraph,
        generated_code: GeneratedCode,
        schema_mapping_id: str,
        *,
        workspace_id: str,
        mode: SnippetFailureMode,
        reason: str,
    ) -> None:
        """Retain an authored-but-unusable EXTRACT SQL, flagged (DAT-543).

        A first-authoring failure is NOT dropped. Either the SQL failed to run
        (``EXECUTION_FAILED``), it ran clean and the verifier rejected the
        VALUE (``VERIFIER_REJECTED`` — e.g. negative against ``value >= 0``, or
        NULL "no support"; VALID SQL whose result the data made untrustworthy), or
        the provenance contract stayed violated after its repair turn
        (``PROVENANCE_INVALID``, DAT-727 — the SQL may be fine, but the graph
        cannot ground ``uses`` edges on an unenforced column enumeration).
        We persist THIS node's own generated extract SQL with
        ``failed=True`` (``failure_count=1`` → ``find_by_key`` keeps it OUT of reuse)
        plus ``{failure_mode, failure_reason}`` in provenance, so
        ``_build_prior_context`` can feed the exact prior SQL + reason to the next
        authoring, and the cockpit can surface it on the ungroundable node.

        This loops over EVERY extract leaf in the graph (execution/verifier failure is
        graph-level — there is no per-leaf attribution), stamping each with the same
        graph-level ``reason``. It does NOT poison a shared, already-working extract:
        ``save_snippet(failed=True)`` hits the first-writer-wins guard and leaves any
        pre-existing HEALTHY row untouched (DAT-636). The residue is only precision — a
        genuinely-fine leaf of a metric that failed elsewhere (e.g. at the formula) gets
        a retained-failure row carrying a misattributed reason, which can nudge a
        needless rewrite on the next run; it never yields a wrong value, and a later
        clean authoring heals it.
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session, workspace_id=workspace_id)
        source = f"graph:{graph.graph_id}"
        generated_steps = {
            s.get("step_id", ""): s for s in generated_code.steps if s.get("step_id")
        }
        provenance = FailedSnippetProvenance(failure_mode=mode, failure_reason=reason).model_dump(
            mode="json"
        )
        for step_id, graph_step in graph.steps.items():
            if graph_step.step_type != StepType.EXTRACT or not graph_step.source:
                continue
            gen_step = generated_steps.get(step_id)
            if not gen_step:
                # Ambiguous drift (multi-step output or multi-leaf graph) — a
                # grounded leaf whose snippet fails to save starves every metric
                # composed from it, so this must be LOUD, never silent.
                logger.warning(
                    "snippet_save_skipped",
                    graph_id=graph.graph_id,
                    step_id=step_id,
                    generated_step_ids=sorted(generated_steps),
                )
                continue
            library.save_snippet(
                snippet_type="extract",
                sql=gen_step.get("sql", ""),
                description=gen_step.get("description", "") or reason,
                schema_mapping_id=schema_mapping_id,
                source=source,
                standard_field=graph_step.source.standard_field,
                statement=graph_step.source.statement,
                aggregation=graph_step.aggregation,
                provenance=provenance,
                parts=gen_step.get("parts"),
                failed=True,
            )
        logger.debug("saved_failed_snippet", graph_id=graph.graph_id, mode=mode)

    def _save_composed_snippets(
        self,
        session: Session,
        graph: TransformationGraph,
        generated_code: GeneratedCode,
        schema_mapping_id: str,
        resolved_params: dict[str, Any],
        *,
        workspace_id: str,
    ) -> None:
        """Persist a metric's composed FORMULA/CONSTANT snippets (DAT-646, ``assemble``).

        The warm pass saves only the shared EXTRACT leaves; a metric's formula and
        constants are composed per-metric here, so they have no other home. Each is
        sourced to ``graph:{graph_id}`` so the cockpit reuse KB groups it under THIS
        metric, and FORMULA snippets are keyed PER-SOURCE (not by expression shape),
        so two same-shape margins never collapse to one row — the bug DAT-646 fixes.
        Insert-if-not-exists (``save_snippet`` is first-writer-wins) → a re-run is a
        no-op.

        Only the FORMULA OUTPUT step is persisted, and its ``sql`` is the WHOLE metric
        as one standalone statement (extract CTEs + the formula) — the cockpit answer
        agent reproduces ``snippet.sql`` independently (DAT-494), so a bare CTE body
        referencing sibling CTEs would not be reusable. Intermediate (non-output)
        FORMULA steps are intentionally NOT persisted: each is captured as a CTE inside
        the output formula's standalone SQL, so a separate fragment row would be both
        redundant and un-reproducible. CONSTANT steps are persisted standalone, keyed by
        (parameter, value) — a genuinely shared value, not aliased.

        A pure-EXTRACT-output metric (output step IS an extract) saves nothing here —
        its sole snippet is the concept-keyed EXTRACT minted by the warm pass, sourced
        to the representative metric and discoverable by concept. There is no formula to
        compose, so a per-metric row would just duplicate that shared extract.
        """
        from dataraum.query.execution import SQLStep, compose_standalone
        from dataraum.query.snippet_library import SnippetLibrary
        from dataraum.query.snippet_utils import normalize_expression

        library = SnippetLibrary(session, workspace_id=workspace_id)
        source = f"graph:{graph.graph_id}"
        provenance_dict = self._build_snippet_provenance(generated_code)
        steps_by_id = {s["step_id"]: s for s in generated_code.steps if s.get("step_id")}

        for step_id, graph_step in graph.steps.items():
            gen_step = steps_by_id.get(step_id)
            if gen_step is None:
                continue

            if graph_step.step_type == StepType.CONSTANT:
                # resolved_params is always populated by _resolve_parameters before
                # compose, and compose itself fails (returns None) on a missing value —
                # so a successful compose guarantees the value is present here.
                param_value = None
                if graph_step.parameter:
                    resolved = resolved_params.get(graph_step.parameter)
                    param_value = str(resolved) if resolved is not None else None
                library.save_snippet(
                    snippet_type="constant",
                    sql=gen_step.get("sql", ""),
                    description=gen_step.get("description", "") or generated_code.summary,
                    schema_mapping_id=schema_mapping_id,
                    source=source,
                    standard_field=graph_step.parameter or step_id,
                    parameter_value=param_value,
                    provenance=provenance_dict,
                )

            elif (
                graph_step.step_type == StepType.FORMULA
                and graph_step.output_step
                and graph_step.expression
            ):
                standalone = compose_standalone(
                    [
                        SQLStep(
                            s.get("step_id", ""),
                            s.get("sql", ""),
                            s.get("description", ""),
                        )
                        for s in generated_code.steps
                    ],
                    generated_code.final_sql,
                )
                normalized, input_fields, _ = normalize_expression(graph_step.expression)
                library.save_snippet(
                    snippet_type="formula",
                    sql=standalone,
                    description=generated_code.summary,
                    schema_mapping_id=schema_mapping_id,
                    source=source,
                    normalized_expression=normalized,
                    input_fields=input_fields,
                    provenance=provenance_dict,
                )

        logger.debug("saved_composed_snippets", graph_id=graph.graph_id)

    def _lookup_snippets(
        self,
        session: Session,
        graph: TransformationGraph,
        schema_mapping_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Look up cached snippets for graph steps before LLM generation.

        Only EXTRACT leaves are looked up (DAT-646): they are the sole shared,
        cross-metric cache surface. FORMULA/CONSTANT steps are NOT looked up — they
        are deterministic and composed per-metric in ``_compose_metric_from_dag``,
        never reused by shape (which is exactly the aliasing DAT-646 removed). The
        per-metric FORMULA/CONSTANT snippets that DO get saved (``_save_composed_
        snippets``) exist only for the cockpit reuse KB, not for engine lookup.

        Args:
            session: SQLAlchemy session
            graph: Graph specification
            schema_mapping_id: Schema mapping identifier

        Returns:
            Dict mapping EXTRACT step_id to cached snippet info for found snippets
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session)

        cached_steps: dict[str, dict[str, Any]] = {}

        for step_id, graph_step in graph.steps.items():
            if graph_step.step_type != StepType.EXTRACT or not graph_step.source:
                continue

            match = library.find_by_key(
                snippet_type="extract",
                schema_mapping_id=schema_mapping_id,
                standard_field=graph_step.source.standard_field,
                statement=graph_step.source.statement,
                aggregation=graph_step.aggregation,
            )

            if match:
                cached_steps[step_id] = {
                    "sql": match.snippet.sql,
                    "description": match.snippet.description,
                    "snippet_id": match.snippet.snippet_id,
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
        schema_mapping_id: str,
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

        # Retained failed attempts for THIS metric's own extracts (DAT-543): the exact
        # prior SQL + why it was rejected, at extract grain (the metric-level state_reason
        # above is coarser). Lets the next authoring revise the specific SQL instead of
        # re-deriving blind — the payoff of retaining, not dropping, a failed extract.
        try:
            from dataraum.query.snippet_library import SnippetLibrary

            lib = SnippetLibrary(session)
            for _step_id, gstep in graph.steps.items():
                if gstep.step_type != StepType.EXTRACT or not gstep.source:
                    continue
                rec = lib.retained_failure(
                    snippet_type="extract",
                    schema_mapping_id=schema_mapping_id,
                    standard_field=gstep.source.standard_field,
                    statement=gstep.source.statement,
                    aggregation=gstep.aggregation,
                )
                if rec and rec.sql:
                    prov = rec.provenance or {}
                    mode = prov.get("failure_mode", "failed")
                    why = prov.get("failure_reason", "(no reason recorded)")
                    parts.append(
                        f"Your prior attempt to ground this extract was {mode}: {why}\n"
                        f"Prior SQL (do NOT re-emit unchanged):\n{rec.sql}\n"
                        "Revise to address the reason, or abstain (low-confidence). If the "
                        "prior SQL aggregated to NULL, decide from the schema evidence which "
                        "case applies: the concept has no supporting rows (abstain — never "
                        "mask absence as 0), or the filter matches rows and one aggregated "
                        "operand is legitimately empty (one-sided data — combine the "
                        "operands with row-guarded NULL-safety per the empty-aggregation "
                        "rule)."
                    )
        except Exception as e:
            # Feedback is best-effort — a lookup hiccup must not fail metric authoring
            # (the metric can still ground without the hint). But log LOUD (warning, not
            # debug): a silent miss here means the retain-don't-drop loop is inert, and
            # the happy path is covered by test_retained_failure_fed_back so a real
            # wiring break fails CI rather than hiding here.
            logger.warning("prior_failed_sql_lookup_failed", graph_id=graph.graph_id, error=str(e))

        return "\n\n".join(parts)
