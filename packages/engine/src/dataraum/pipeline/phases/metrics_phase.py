"""Metrics phase — the operating_model stage's third lifecycle family (DAT-456).

Source-free and session-scoped, mirroring validation and cycles: operates on
``ctx.table_ids`` (the session's typed tables), never a ``source_id``. The
declared set is the vertical's ``metrics/`` transformation graphs ⊕ ``metric``
overlay teach rows — one ``metric`` lifecycle artifact per ``graph_id``. The
engine induces nothing (declares come from the vertical; user declares arrive
via frame-2 teach rows). Each declared metric flows through the typed artifact
lifecycle:

* **declare** — every loaded ``graph_id`` becomes a ``declared`` artifact.
* **compose** (``metric.compose``) — EVERY parseable metric is composed by the
  graph agent: it inspects the workspace (tables, columns, existing snippets) and
  materializes the metric's SQL. There is NO field-mapping pre-gate — whether an
  input like ``revenue`` is derivable from the data (e.g. from the GL via
  chart_of_accounts) is the agent's job to discover, not a heuristic dict-key
  check in front of the prompt. A definition that won't even parse stays
  ``declared`` with the parse error recorded (the one legitimate pre-gate).
* **execute** (``metric.execute``) — the agent runs the composed SQL cleanly →
  ``executed``, and the working SQL is materialized as reusable snippets (the
  durable, cross-run executable knowledge ``query`` later consumes). The snippet
  is gated on SUCCESSFUL execution — never a guess. A metric the agent cannot
  materialize into runnable SQL stays ``grounded`` with the reason: born-loud at
  the agent, not pre-empted by a gate.

A re-run supersedes: everything is re-declared and re-flowed under the fresh
``run_id`` (no skip-if-already-ran — the prior run's artifacts coexist untouched,
and the promoted head names the current run). The snippet base is NOT
run-versioned — it is the cross-run reuse cache shared with ``query``; a re-run
reuses healthy snippets and self-heals failed ones. With no vertical or no
declared metrics the phase succeeds loudly with an explicit outcome.

**Sanctioned multi-commit exception (DAT-502):** unlike every other phase
(one commit at ``session_scope`` exit), the parallel path commits once PER
METRIC (``_execute_isolated``). That is under the failure contract because
every per-metric write converges under at-least-once redelivery: snippet
state is first-writer-wins (``SnippetLibrary.save_snippet`` keeps a healthy
existing row, replaces only failed ones — the DAT-485 app-level dedup), and
the ``snippet_usage`` rows / ``execution_count`` counters are the documented
TELEMETRY exception — ``sql_snippets``/``snippet_usage`` are not run-stamped,
so a redelivery can inflate usage telemetry; nothing gates on it (write-only
since DAT-487/488).

Authoring vs assembly (DAT-636): the LLM is called ONLY in the up-front
authoring pass (``_warm_shared_nodes``), which decides every unique node once and
returns the run-scoped binding map. The per-metric fan-out is then pure ASSEMBLY
(``agent.assemble``) — no LLM — dispatched concurrently via a ``ThreadPoolExecutor``
when the phase context exposes a ConnectionManager (each parallel call gets its
own SQLAlchemy session + DuckDB cursor; ``max_workers`` is the concurrency cap).
Falls back to a serial loop in unit tests where the manager isn't wired.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from types import ModuleType
from typing import TYPE_CHECKING

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.graphs.loader import GraphLoadError
from dataraum.lifecycle import BaseRunMap, declare_artifact, transition
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases._warm_first import submit_warm_first
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

_log = get_logger(__name__)

# The journey stage this phase runs under — the lifecycle guard authorizes
# metric.declare/compose/execute for this stage only.
_STAGE = "operating_model"

# Cap concurrent metric LLM calls. Sonnet 5 tier-3+ workspaces handle
# 4000 RPM (~67 RPS) comfortably; with ~30-60s LLM latencies, 10 concurrent
# is ~10 RPS at peak — well under the limit. The warming pre-pass (DAT-629) and
# the execute wave run sequentially, each peaking at this many isolated sessions
# (+1 phase session) — comfortably under the ConnectionManager pool (15).
_MAX_CONCURRENT_METRICS = 10

# DAT-631: a metric whose SQL runs and verifies still reaches ``executed`` only
# as strongly as its WEAKEST grounded input. The graph agent already records an
# honest per-concept confidence in each snippet's assumptions (e.g. a COGS proxy
# at 0.35, a fabricated 0.0 at 0.10); below this floor the executed metric is
# FLAGGED — its ``state_reason`` names the weak grounding — so the cockpit can
# render it amber instead of plainly green. The value still shows (state stays
# ``executed``); we surface the doubt rather than hide the number. Tuned against
# eval over the iterations — a first-round floor, not a magic constant.
_LOW_CONFIDENCE_FLOOR = 0.5

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager
    from dataraum.graphs.additivity import AxisClass, MetricVerdict
    from dataraum.graphs.agent import ExecutionContext as _ExecutionContext
    from dataraum.graphs.agent import GraphAgent
    from dataraum.graphs.models import GraphExecution, TransformationGraph
    from dataraum.graphs.node_warming import NodeDecision, NodeKey, WarmNode
    from dataraum.lifecycle import LifecycleArtifact

    MetricPrep = tuple[str, TransformationGraph, str | None]
    MetricResult = tuple[str, Result[GraphExecution], str | None]


@analysis_phase
class MetricsPhase(BasePhase):
    """Compute metric graphs through the artifact lifecycle (DAT-456).

    Declares the vertical's metric graphs, composes each against the workspace
    (grounding its inputs), and executes the composed SQL — each declared metric
    moving through declare → compose → execute. Mirrors :class:`ValidationPhase`
    and :class:`BusinessCyclesPhase`.

    Requires: a begin_session workspace (typed tables, relationships, enriched
    views, slices) reachable through the run's pinned base heads.
    """

    @property
    def name(self) -> str:
        return "metrics"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.graphs import additivity_db_models
        from dataraum.lifecycle import db_models as lifecycle_db_models
        from dataraum.query import snippet_models

        return [snippet_models, lifecycle_db_models, additivity_db_models]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Declare → compose → execute every declared metric graph."""
        from dataraum.graphs.agent import ExecutionContext, GraphAgent
        from dataraum.graphs.config import get_metric_definitions
        from dataraum.graphs.loader import GraphLoader
        from dataraum.query.snippet_library import SnippetLibrary

        table_ids = ctx.table_ids
        if not table_ids:
            return PhaseResult.failed(
                "No tables in session scope — metric computation operates on the "
                "session's typed table selection (ctx.table_ids)."
            )

        # Declared set: the vertical's metric graphs ⊕ metric overlay teach rows.
        # No vertical / no declared metrics is a LOUD explicit outcome, not a
        # silent skip (the engine induces nothing now).
        vertical: str | None = ctx.config.get("vertical")
        declared_defs = get_metric_definitions(vertical) if vertical else {}
        if not vertical or not declared_defs:
            outcome = "no_vertical" if not vertical else "no_declared_metrics"
            _log.warning("metrics_nothing_declared", vertical=vertical, outcome=outcome)
            return PhaseResult.success(
                outputs={"outcome": outcome, "declared": 0, "executed": 0},
                records_processed=0,
                records_created=0,
                summary=f"0 declared metrics ({outcome}) — nothing to compose or execute",
            )

        run_id = ctx.require_run_id()
        # Pinned upstream heads (ADR-0008 in-run mode): resolved ONCE by the
        # workflow's pre-flight ``operating_model_resolve`` activity and threaded
        # here through the phase config. No per-phase head resolution — a missing
        # pin is a wiring bug, fail loud.
        raw_base_runs = ctx.config.get("base_runs")
        if raw_base_runs is None:
            return PhaseResult.failed(
                "base_runs missing from the phase config — OperatingModelWorkflow's "
                "resolve activity pins the base-run map before this phase runs "
                "(ADR-0008 in-run mode; no per-phase head resolution)."
            )
        base_runs = BaseRunMap.model_validate(raw_base_runs)

        # The snippet base is keyed by the WORKSPACE (source-free): snippets are
        # the cross-run reuse cache shared with the query agent, stable across
        # sessions/runs of one workspace's schema. Threaded into the phase config
        # by the run_metrics activity from the run identity.
        schema_mapping_id = ctx.config.get("workspace_id")
        if not schema_mapping_id:
            return PhaseResult.failed(
                "workspace_id missing from the phase config — the run_metrics "
                "activity threads it as the snippet base's schema_mapping_id "
                "(source-free, workspace-stable for cross-run reuse)."
            )

        # Initialize LLM infrastructure
        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        renderer = PromptRenderer()
        agent = GraphAgent(config=config, provider=provider, prompt_renderer=renderer)
        snippet_library = SnippetLibrary(ctx.session)

        # declare: every declared graph_id becomes a declared artifact for THIS
        # run — supersession across runs; a success-redelivery RESETS the same
        # run's row to declared (declare-or-reuse, DAT-502).
        artifacts: dict[str, LifecycleArtifact] = {}
        for graph_id, defn in declared_defs.items():
            artifacts[graph_id] = declare_artifact(
                ctx.session,
                artifact_type="metric",
                artifact_key=graph_id,
                run_id=run_id,
                stage=_STAGE,
                teaches={
                    "graph_id": graph_id,
                    "vertical": vertical,
                    "category": (defn.get("metadata") or {}).get("category"),
                },
            )
            # Persist the effective (shipped ⊕ overlay) DAG this row was assembled from
            # (DAT-591) — the cockpit reads the exact rendered structure from this one
            # Postgres source, so it never re-reads config or re-merges the overlay.
            artifacts[graph_id].graph_definition = defn

        # Parse declared definitions into graphs. A definition that won't parse
        # stays declared with the parse error recorded — visibly impossible.
        loader = GraphLoader()
        graphs: dict[str, TransformationGraph] = {}
        for graph_id, defn in declared_defs.items():
            try:
                graphs[graph_id] = loader.graphs_from_definitions({graph_id: defn})[graph_id]
            except GraphLoadError as e:
                artifacts[graph_id].state_reason = f"malformed metric definition: {e.message}"
                _log.warning("metric_definition_malformed", graph_id=graph_id, error=e.message)

        # compose: hand EVERY parseable metric to the graph agent. No
        # field-mapping pre-gate — the agent inspects the workspace (and the
        # existing snippet base) and discovers whether a required input is
        # derivable; that is the agentic job, not a dict-key check in front of the
        # prompt. Born-loud lives at execute (an agent that cannot materialize
        # runnable SQL stays grounded with the reason) and at snippet
        # materialization (gated on a clean run) — never a heuristic skip here.
        grounded_against = base_runs.model_dump(mode="json")
        prep: list[MetricPrep] = []
        for graph_id, graph in graphs.items():
            transition(
                artifacts[graph_id],
                operation="compose",
                stage=_STAGE,
                grounded_against=grounded_against,
            )

            prep.append((graph_id, graph, graph.metadata.inspiration_snippet_id))

        # Authoring pass (DAT-636): before the per-metric fan-out, decide every
        # UNIQUE cache-keyed node ONCE, in dependency order. A sub-node shared by
        # several metrics (e.g. the cost_of_goods_sold extract) is decided a single
        # time; the per-metric assembly below reads the returned binding map and
        # NEVER re-authors, so the same concept can no longer ground different ways
        # across siblings (the within-run divergence DAT-629 only half-fixed —
        # it cached successes but the per-metric path re-authored every miss).
        # The catalogue head run carries the table agent's ColumnConcept rows
        # (DAT-637) — the graph context reads concepts/field-mappings from it.
        catalogue_run_id = base_runs.relationship_run_id
        bindings = _warm_shared_nodes(
            graphs,
            ctx,
            agent,
            schema_mapping_id,
            table_ids,
            vertical,
            om_run_id=run_id,
            catalogue_run_id=catalogue_run_id,
        )
        _log.info(
            "metrics_authored",
            grounded=sum(1 for d in bindings.values() if d.grounded),
            ungroundable=sum(1 for d in bindings.values() if not d.grounded),
        )

        # assemble: compose each metric from the bindings (no LLM — the authoring
        # pass already decided every node). Parallel when the manager is wired,
        # serial fallback otherwise.
        if ctx.manager is not None:
            results = _execute_metrics_parallel(
                prep,
                ctx.manager,
                agent,
                schema_mapping_id,
                table_ids,
                vertical,
                bindings,
                om_run_id=run_id,
                catalogue_run_id=catalogue_run_id,
            )
        else:
            exec_ctx = ExecutionContext.with_rich_context(
                session=ctx.session,
                duckdb_conn=ctx.duckdb_conn,
                table_ids=table_ids,
                schema_mapping_id=schema_mapping_id,
                om_run_id=run_id,
                catalogue_run_id=catalogue_run_id,
                vertical=vertical,
            )
            results = _execute_metrics_serial(
                prep, ctx.session, exec_ctx, agent, schema_mapping_id, bindings
            )

        # A composed metric that ran cleanly AND verified reaches executed; one
        # whose SQL failed OR whose result was inconclusive (no support / a
        # declared condition violated — DAT-616 verifier) stays grounded with the
        # reason (born loud, never silently green).
        low_confidence = 0
        for graph_id, result, inspiration_id in results:
            artifact = artifacts[graph_id]
            if result.success:
                # Execute-and-flag (DAT-631 + DAT-699): a clean run reaches
                # executed, and everything the run has to say about the number
                # rides the (still-executed) artifact's state_reason — the
                # weakest input's low grounding confidence AND any declared
                # expectations the executed value violates. Never silently
                # green, never a refused number.
                confidence_reason = _low_confidence_reason(result.value)
                flags = result.value.verification_flags if result.value else []
                parts = [p for p in [confidence_reason, *flags] if p]
                reason = "; ".join(parts) or None
                transition(artifact, operation="execute", stage=_STAGE, state_reason=reason)
                if reason:
                    low_confidence += 1
                    _log.warning("metric_executed_flagged", graph_id=graph_id, reason=reason)
                else:
                    _log.info("metric_executed", graph_id=graph_id)
                # Snippet promotion: drop the ad-hoc snippet once the metric it
                # inspired executes cleanly.
                if inspiration_id:
                    ad_hoc = snippet_library.find_by_id(inspiration_id)
                    if ad_hoc:
                        ctx.session.delete(ad_hoc)
                        _log.info("snippet_promoted", graph_id=graph_id, snippet_id=inspiration_id)
            else:
                artifact.state_reason = f"composed but not executed: {result.error}"
                _log.warning("metric_not_executed", graph_id=graph_id, error=result.error)

        # Additivity verdict (DAT-716): classify how each EXECUTED metric's value
        # reconciles under aggregation (offer a time grain? does a categorical
        # breakdown sum or dash?) from the grounded snippets + catalogue
        # temporal_behavior/grain — no LLM. A metric that can't be classified
        # writes no row, never a wrong one. Read at the pinned catalogue run.
        _persist_additivity_verdicts(
            ctx.session,
            ctx.duckdb_conn,
            graphs=graphs,
            executed_keys={gid for gid, a in artifacts.items() if a.state == "executed"},
            workspace_id=schema_mapping_id,
            run_id=run_id,
            catalogue_run_id=catalogue_run_id,
        )

        executed = sum(1 for a in artifacts.values() if a.state == "executed")
        grounded_stuck = sum(1 for a in artifacts.values() if a.state == "grounded")
        declared_stuck = sum(1 for a in artifacts.values() if a.state == "declared")

        # Surface every artifact's outcome — executed, plus each stuck one WITH
        # its reason — so the cockpit shows "dso: declared — ungroundable
        # (missing: accounts_receivable)" rather than a bare count, distinguishing
        # the failure modes (ungroundable vs malformed vs composed-but-unexecutable).
        previews: list[str] = []
        for graph_id, a in artifacts.items():
            if a.state == "executed":
                # An executed artifact carries a reason ONLY when flagged —
                # low grounding confidence (DAT-631) and/or a declared
                # expectation the value violates (DAT-699). Surface it.
                if a.state_reason:
                    previews.append(f"{graph_id}: executed (flagged) — {a.state_reason}")
                else:
                    previews.append(f"{graph_id}: executed")
            else:
                previews.append(f"{graph_id}: {a.state} — {a.state_reason or 'no reason recorded'}")

        return PhaseResult.success(
            outputs={
                "declared": len(artifacts),
                "executed": executed,
                "executed_low_confidence": low_confidence,
                "stuck_grounded": grounded_stuck,
                "stuck_declared": declared_stuck,
            },
            records_processed=len(table_ids),
            records_created=len(artifacts),
            warnings=previews,
            summary=(
                f"{executed}/{len(artifacts)} metrics executed; "
                f"{declared_stuck} ungroundable, {grounded_stuck} composed but inconclusive/failed"
            ),
        )


# ---------------------------------------------------------------------------
# Grounding-confidence gate (DAT-631)
# ---------------------------------------------------------------------------


def _low_confidence_reason(execution: GraphExecution | None) -> str | None:
    """Reason string if the metric's weakest grounded input is below the floor.

    A metric is only as trustworthy as its least-confident grounding. We take
    the MIN confidence across the execution's assumptions (the graph agent's
    honest per-concept signal, carried forward even for cache-assembled metrics)
    and, when it falls below :data:`_LOW_CONFIDENCE_FLOOR`, return a short reason
    naming the floor and the weakest assumption. ``None`` when there are no
    assumptions or all clear — the metric is plainly executed.
    """
    if execution is None or not execution.assumptions:
        return None
    weakest = min(execution.assumptions, key=lambda a: a.confidence)
    if weakest.confidence >= _LOW_CONFIDENCE_FLOOR:
        return None
    return (
        f"low-confidence grounding ({weakest.confidence:.2f} < {_LOW_CONFIDENCE_FLOOR:.2f}): "
        f"{weakest.assumption}"
    )


# ---------------------------------------------------------------------------
# Additivity verdict (DAT-716)
# ---------------------------------------------------------------------------


def _persist_additivity_verdicts(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    graphs: dict[str, TransformationGraph],
    executed_keys: set[str],
    workspace_id: str,
    run_id: str,
    catalogue_run_id: str | None,
) -> None:
    """Persist additivity verdicts for the executed metrics AND their measures.

    A drill target is either a **metric** node (a formula, keyed by ``graph_id``)
    or a **measure** node (a grounded extract, keyed by ``standard_field``) — both
    are drillable, so both get a verdict. A metric's verdict rolls its extract
    classes up through the DAG; a measure's verdict IS its single extract's class
    (deduped most-restrictive across the metrics that share it). Idempotent per run
    — ``(target_kind, target_key, run_id)`` UPSERTed (ADR-0010 form-(a)). A target
    that can't be classified is skipped: no row is better than a misleading one.

    **Fault-isolated (best-effort annotation).** This runs on the shared phase
    session AFTER every metric's execute bookkeeping is already recorded there; an
    unhandled failure here would surface as a phase failure and roll that session
    back — discarding every metric's ``executed`` state and forcing a full Temporal
    retry of already-successful work. So each classification and the upsert run
    inside their own SAVEPOINT: a bug (a parse error, a bad query) rolls back only
    its own annotation, never the metric bookkeeping.
    """
    from dataraum.graphs.additivity import most_restrictive, roll_up_metric
    from dataraum.graphs.additivity_db_models import MetricAdditivity
    from dataraum.graphs.additivity_resolver import classify_metric_extracts
    from dataraum.storage.upsert import upsert

    if not catalogue_run_id:
        _log.warning("metric_additivity_skipped_no_catalogue_run")
        return

    rows: list[dict[str, object]] = []
    measure_classes: dict[str, AxisClass] = {}
    # sorted(): the measure fold below merges a shared field's class across metrics
    # `most_restrictive` (order-dependent reason), so a deterministic iteration order
    # is required — a set's is PYTHONHASHSEED-salted.
    for graph_id in sorted(executed_keys):
        graph = graphs.get(graph_id)
        if graph is None:
            continue
        # ALL fallible work (classify, roll-up, the extract→standard_field access)
        # runs inside the SAVEPOINT; only the in-memory bookkeeping happens after,
        # so a bug here rolls back its own annotation, never the phase session.
        try:
            with session.begin_nested():
                classes = classify_metric_extracts(
                    session,
                    duckdb_conn,
                    graph=graph,
                    workspace_id=workspace_id,
                    catalogue_run_id=catalogue_run_id,
                )
                if classes is None:
                    continue
                metric_verdict = roll_up_metric(graph, classes)
                # A measure node is one extract, keyed by standard_field (the drill's
                # node identity — see analyseTarget); the same field is shared across
                # metrics via the concept-keyed snippet cache (same class).
                metric_measures = [
                    (source.standard_field, cls)
                    for step_id, cls in classes.items()
                    if (source := graph.steps[step_id].source) and source.standard_field
                ]
        except Exception as exc:  # noqa: BLE001 - best-effort; never fail the phase
            _log.warning("metric_additivity_compute_error", graph_id=graph_id, error=str(exc))
            continue
        rows.append(_verdict_row(run_id, "metric", graph_id, metric_verdict))
        for field, cls in metric_measures:
            # Deduped most-restrictive: identical today (shared snippet), conservative
            # if a field is ever grounded two ways across metrics — never optimistic.
            prior = measure_classes.get(field)
            measure_classes[field] = cls if prior is None else most_restrictive(prior, cls)

    rows.extend(
        _verdict_row(run_id, "measure", field, cls) for field, cls in measure_classes.items()
    )

    if not rows:
        _log.info("metric_additivity_persisted", count=0, executed=len(executed_keys))
        return
    try:
        with session.begin_nested():
            upsert(
                session,
                MetricAdditivity,
                rows,
                index_elements=["target_kind", "target_key", "run_id"],
            )
    except Exception as exc:  # noqa: BLE001 - isolate the write from phase bookkeeping
        _log.warning("metric_additivity_upsert_error", error=str(exc), count=len(rows))
        return
    _log.info(
        "metric_additivity_persisted",
        count=len(rows),
        measures=len(measure_classes),
        executed=len(executed_keys),
    )


def _verdict_row(
    run_id: str, kind: str, key: str, v: MetricVerdict | AxisClass
) -> dict[str, object]:
    """One ``metric_additivity`` row from a verdict/AxisClass (both share the fields)."""
    return {
        "run_id": run_id,
        "target_kind": kind,
        "target_key": key,
        "categorical_additive": v.categorical_additive,
        "time_additive": v.time_additive,
        "categorical_reason": v.categorical_reason,
        "time_reason": v.time_reason,
    }


# ---------------------------------------------------------------------------
# Node warming pre-pass (DAT-629)
# ---------------------------------------------------------------------------


def _warm_shared_nodes(
    graphs: dict[str, TransformationGraph],
    ctx: PhaseContext,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    *,
    om_run_id: str,
    catalogue_run_id: str | None = None,
) -> dict[NodeKey, NodeDecision]:
    """The authoring pass: decide every unique cache-keyed node ONCE (DAT-636).

    Builds the cross-metric DAG, then authors each unique node once in dependency
    order: a generation runs concurrently (independent nodes), with a barrier
    between generations so a formula node sees its dep extracts already grounded.
    Returns the run-scoped, in-memory **binding map** ``{NodeKey: NodeDecision}`` —
    every node's decision (grounded → its concept-keyed snippet is minted;
    ungroundable → the born-loud reason). The per-metric ASSEMBLY that follows
    reads this map and never re-authors: a metric with an ungroundable dependency
    honest-fails immediately, no LLM. A cyclic metric set yields an empty map
    (every metric then honest-fails born-loud at assembly).
    """
    from dataraum.graphs.node_warming import build_warm_dag, warming_generations

    try:
        dag, nodes = build_warm_dag(graphs)
    except ValueError as e:
        _log.warning("metric_warm_dag_failed", error=str(e))
        return {}

    generations = warming_generations(dag)
    if not generations:
        return {}

    _log.info(
        "metrics_warming_start",
        nodes=sum(len(g) for g in generations),
        generations=len(generations),
    )

    if ctx.manager is not None:
        return _warm_generations_parallel(
            generations,
            nodes,
            ctx.manager,
            agent,
            schema_mapping_id,
            table_ids,
            vertical,
            om_run_id,
            catalogue_run_id,
        )
    return _warm_generations_serial(
        generations,
        nodes,
        ctx.session,
        ctx.duckdb_conn,
        agent,
        schema_mapping_id,
        table_ids,
        vertical,
        om_run_id,
        catalogue_run_id,
    )


def _warm_generations_parallel(
    generations: list[list[tuple[str | None, ...]]],
    nodes: dict[tuple[str | None, ...], WarmNode],
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    om_run_id: str,
    catalogue_run_id: str | None = None,
) -> dict[NodeKey, NodeDecision]:
    """Author generations concurrently within each wave, barrier between waves.

    One ``ThreadPoolExecutor`` (``max_workers`` IS the concurrency cap — no
    separate semaphore), the engine's standard fan-out primitive for blocking
    SQLAlchemy/DuckDB/LLM work on the sync activity worker. Draining each
    generation's futures before submitting the next is the load-bearing barrier:
    generation N+1's formula nodes must see generation N's extracts already
    grounded. Returns the run-scoped binding map; a node that raises is recorded
    ungroundable so its dependent metrics honest-fail born-loud at assembly.
    """
    from dataraum.graphs.node_warming import NodeDecision

    bindings: dict[NodeKey, NodeDecision] = {}
    with ThreadPoolExecutor(
        max_workers=_MAX_CONCURRENT_METRICS, thread_name_prefix="metric-warm"
    ) as pool:
        for generation in generations:
            # Only leaf EXTRACTs warm now (DAT-646) — they have no dependencies, so
            # there is no dep-gate: each is authored once, concept-keyed.
            # Warm-first (DAT-601): the generation's first node runs alone so its
            # completed call commits the shared prompt-cache prefix; the rest then
            # read it instead of re-writing it cap-wide.
            def _submit(key: NodeKey) -> Future[NodeDecision]:
                return pool.submit(
                    _warm_isolated,
                    nodes[key],
                    manager,
                    agent,
                    schema_mapping_id,
                    table_ids,
                    vertical,
                    om_run_id,
                    catalogue_run_id,
                )

            futures: dict[Future[NodeDecision], NodeKey] = submit_warm_first(
                _submit, list(generation)
            )
            for future in as_completed(futures):
                key = futures[future]
                try:
                    bindings[key] = future.result()
                except Exception as exc:
                    # A node that crashes warming is recorded ungroundable — the
                    # dependent metrics then honest-fail born-loud at assembly.
                    _log.warning("metric_node_warm_error", node=str(key), error=str(exc))
                    bindings[key] = NodeDecision(grounded=False, reason=f"warm error: {exc}")
    return bindings


def _warm_isolated(
    node: WarmNode,
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    om_run_id: str,
    catalogue_run_id: str | None = None,
) -> NodeDecision:
    """Author one node with an isolated session + cursor; return its decision."""
    from dataraum.graphs.agent import ExecutionContext
    from dataraum.graphs.node_warming import NodeDecision, build_mini_graph

    mini = build_mini_graph(node)
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        exec_ctx = ExecutionContext.with_rich_context(
            session=session,
            duckdb_conn=cursor,
            table_ids=table_ids,
            schema_mapping_id=schema_mapping_id,
            om_run_id=om_run_id,
            catalogue_run_id=catalogue_run_id,
            vertical=vertical,
        )
        result = agent.execute(session, mini, exec_ctx, workspace_id=schema_mapping_id)
    if result.success:
        return NodeDecision(grounded=True)
    # Ungroundable (e.g. an extract with genuinely no support): recorded, not an
    # error — the metric using it honest-fails born-loud at assembly.
    _log.info("metric_node_ungroundable", node=str(node.key), reason=result.error)
    return NodeDecision(grounded=False, reason=result.error)


def _warm_generations_serial(
    generations: list[list[tuple[str | None, ...]]],
    nodes: dict[tuple[str | None, ...], WarmNode],
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    om_run_id: str,
    catalogue_run_id: str | None = None,
) -> dict[NodeKey, NodeDecision]:
    """Serial fallback: shared session + cursor, sequential dependency order."""
    from dataraum.graphs.agent import ExecutionContext
    from dataraum.graphs.node_warming import NodeDecision, build_mini_graph

    exec_ctx = ExecutionContext.with_rich_context(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        schema_mapping_id=schema_mapping_id,
        om_run_id=om_run_id,
        catalogue_run_id=catalogue_run_id,
        vertical=vertical,
    )
    bindings: dict[NodeKey, NodeDecision] = {}
    for generation in generations:
        for key in generation:
            # Only leaf EXTRACTs warm (DAT-646) — no deps, so no dep-gate.
            try:
                result = agent.execute(
                    session, build_mini_graph(nodes[key]), exec_ctx, workspace_id=schema_mapping_id
                )
            except Exception as exc:
                _log.warning("metric_node_warm_error", node=str(key), error=str(exc))
                bindings[key] = NodeDecision(grounded=False, reason=f"warm error: {exc}")
                continue
            if result.success:
                bindings[key] = NodeDecision(grounded=True)
            else:
                _log.info("metric_node_ungroundable", node=str(key), reason=result.error)
                bindings[key] = NodeDecision(grounded=False, reason=result.error)
    return bindings


# ---------------------------------------------------------------------------
# Per-metric dispatch
# ---------------------------------------------------------------------------


def _execute_metrics_serial(
    prep: list[MetricPrep],
    session: Session,
    exec_ctx: _ExecutionContext,
    agent: GraphAgent,
    workspace_id: str,
    bindings: dict[NodeKey, NodeDecision],
) -> list[MetricResult]:
    """Fallback path: shared session + cursor, sequential dispatch.

    Pure ASSEMBLY (DAT-636): composes each metric from the authoring pass's
    bindings — no LLM. Used in unit tests where PhaseContext.manager is None.
    """
    out: list[MetricResult] = []
    for graph_id, graph, inspiration_id in prep:
        result = agent.assemble(session, graph, exec_ctx, bindings, workspace_id=workspace_id)
        out.append((graph_id, result, inspiration_id))
    return out


def _execute_metrics_parallel(
    prep: list[MetricPrep],
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    bindings: dict[NodeKey, NodeDecision],
    *,
    om_run_id: str,
    catalogue_run_id: str | None = None,
) -> list[MetricResult]:
    """Concurrent path: per-call session + cursor via a ThreadPoolExecutor.

    Pure ASSEMBLY (DAT-636): each metric composes from the authoring pass's
    bindings on a pool thread with its own SQLAlchemy session (auto-commit via
    session_scope) and its own DuckDB cursor — NO LLM in this path. ``max_workers``
    caps concurrency to _MAX_CONCURRENT_METRICS. ``om_run_id`` is this
    operating_model run — the graph context reads its cycles/validation evidence
    at this run, not the (not-yet-promoted) head.
    """
    out: list[MetricResult] = []
    with ThreadPoolExecutor(
        max_workers=_MAX_CONCURRENT_METRICS, thread_name_prefix="metric"
    ) as pool:
        futures = {
            pool.submit(
                _execute_isolated,
                graph,
                manager,
                agent,
                schema_mapping_id,
                table_ids,
                vertical,
                bindings,
                om_run_id,
                catalogue_run_id,
            ): (graph_id, inspiration_id)
            for graph_id, graph, inspiration_id in prep
        }
        for future in as_completed(futures):
            graph_id, inspiration_id = futures[future]
            # Capture unexpected exceptions as Result.fail so one worker raising
            # doesn't abort siblings.
            try:
                result = future.result()
            except Exception as exc:
                result = Result.fail(f"Unexpected error executing {graph_id}: {exc}")
            out.append((graph_id, result, inspiration_id))
    return out


def _execute_isolated(
    graph: TransformationGraph,
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    bindings: dict[NodeKey, NodeDecision],
    om_run_id: str,
    catalogue_run_id: str | None = None,
) -> Result[GraphExecution]:
    """Assemble one metric from the bindings with an isolated session + cursor.

    Wraps the call in manager.session_scope() so writes commit on success
    and roll back on exception. The DuckDB cursor is independent — the
    underlying connection is shared with other cursors safely.

    Sanctioned multi-commit shape (DAT-502): each metric commits its own
    session, so a phase that fails later does NOT roll these back. That is
    safe because every write here converges under redelivery — snippets are
    first-writer-wins (per-key app-level dedup, DAT-485) and usage counters
    are the documented telemetry exception (see the module docstring).
    """
    from dataraum.graphs.agent import ExecutionContext

    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        exec_ctx = ExecutionContext.with_rich_context(
            session=session,
            duckdb_conn=cursor,
            table_ids=table_ids,
            schema_mapping_id=schema_mapping_id,
            om_run_id=om_run_id,
            catalogue_run_id=catalogue_run_id,
            vertical=vertical,
        )
        return agent.assemble(session, graph, exec_ctx, bindings, workspace_id=schema_mapping_id)
