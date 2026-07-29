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
the ``execution_count`` counter is the documented TELEMETRY exception —
``sql_snippets`` is not run-stamped, so a redelivery can inflate the count;
nothing gates on it (write-only since DAT-487/488; the per-execution
``snippet_usage`` audit trail this once fed was itself write-only —
removed, DAT-781).

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
from dataclasses import dataclass, field
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
    from dataraum.graphs.additivity_resolver import VerdictRow
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
        from dataraum.analysis.semantic import reconciliation_db_models
        from dataraum.graphs import (
            additivity_db_models,
            metric_graph_db_models,
            unit_grain_db_models,
        )
        from dataraum.lifecycle import db_models as lifecycle_db_models
        from dataraum.query import snippet_models

        return [
            snippet_models,
            lifecycle_db_models,
            additivity_db_models,
            unit_grain_db_models,
            metric_graph_db_models,
            reconciliation_db_models,
        ]

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

        # Additivity verdicts (DAT-857/868): classify how each DECLARED metric's
        # value aggregates per axis (offer a time bucketing and how to compose it?
        # does a breakdown sum or dash?) from the grounded snippets + catalogue
        # materialization/grain — no LLM. A target that can't be classified gets a
        # typed abstention, never silence. Read at the pinned catalogue run.
        _persist_additivity_verdicts(
            ctx.session,
            ctx.duckdb_conn,
            graphs=graphs,
            declared_keys=set(declared_defs),
            workspace_id=schema_mapping_id,
            run_id=run_id,
            catalogue_run_id=catalogue_run_id,
        )

        # Unit grain (DAT-671 B1): the question a practitioner asks NEXT — which
        # vendors, which accounts — one level below the workspace scalar. Runs here,
        # immediately after the verdicts, because it is GATED by the row that call
        # just wrote for THIS run: the served per-(target × axis) verdict decides
        # whether a breakdown may be offered at all, and reading a stale run's
        # verdict would gate this run's numbers on last run's evidence.
        unit_grain = _persist_unit_grain(
            ctx.session,
            ctx.duckdb_conn,
            agent=agent,
            graphs=graphs,
            workspace_id=schema_mapping_id,
            run_id=run_id,
            catalogue_run_id=catalogue_run_id,
        )

        # reconciles_with derivation (DAT-727 part c): with this run's grounding
        # set settled, reconcile the concept-grain self-loop assertions — the
        # aggregation-lineage witness (at the pinned catalogue run) and
        # multi-grounding concepts — as source='derived' concept_edges rows
        # (insert missing, supersede vanished; seed rows untouched).
        # Fault-isolated like _persist_additivity_verdicts above, for the same
        # reason: an unhandled failure here would fail the phase and roll the
        # session back, discarding every metric's executed lifecycle state over
        # a derived-vocabulary annotation. The SAVEPOINT keeps the
        # insert+supersede pair atomic while a failure costs only this run's
        # assertions (the next run re-derives them).
        from dataraum.analysis.semantic.reconciles_with import derive_reconciles_with

        try:
            with ctx.session.begin_nested():
                derive_reconciles_with(
                    ctx.session, vertical=vertical, catalogue_run_id=catalogue_run_id
                )
        except Exception as e:
            _log.warning("reconciles_with_derivation_failed", error=str(e))

        # reconciles_with EVALUATION (DAT-739): the block above derives the
        # assertion that two computations of one quantity must tie out. Nothing
        # ever computed whether they DO — three surfaces render "must tie out"
        # as a contract, and it was never checked. This re-executes each
        # asserted concept's groundings and records the observed tie-out.
        #
        # Runs AFTER the derivation because it evaluates the edge set that call
        # just settled: an assertion whose support vanished must not be
        # evaluated, and one newly supported must. Note the edges are workspace-
        # persistent, not run-versioned, so a failed derivation above leaves the
        # PREVIOUS assertions active and evaluating them is still correct.
        #
        # SAVEPOINT-isolated like its three siblings, for the same reason: a
        # failure here costs this run's tie-out evidence and nothing else, never
        # the recorded metric execute-state. The outcome is promoted only after
        # the block RELEASES cleanly — releasing flushes, so a value assigned
        # inside could otherwise count rows its own savepoint had rolled back.
        from dataraum.analysis.semantic.reconciliation import (
            ReconciliationOutcome,
            evaluate_reconciliations,
        )

        reconciliation = ReconciliationOutcome()
        try:
            with ctx.session.begin_nested():
                produced = evaluate_reconciliations(
                    ctx.session, ctx.duckdb_conn, vertical=vertical, run_id=run_id
                )
            reconciliation = produced
        except Exception as e:
            _log.warning("concept_reconciliation_failed", error=str(e))
            # A rolled-back block leaves ZERO rows and empty channels — byte-
            # identical to the healthy "nothing was asserted" run. Absence must
            # fall loud, so the failure is disclosed on the warning channel
            # rather than living only in a log line nobody reads.
            reconciliation.failures["*"] = f"reconciliation not evaluated: {e}"

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

        # A unit-grain FAILURE is loud — it rides the same per-metric channel every
        # other unhappy outcome does. A verdict-GATED withholding is not a failure:
        # a number that provably does not partition on an axis SHOULD have no
        # breakdown, so it is disclosed as structured output instead of shouted.
        previews.extend(
            f"{graph_id}: unit grain failed — {error}"
            for graph_id, error in sorted(unit_grain.failures.items())
        )

        # A tie-out that BREACHED a declared band is a warning — someone stated
        # the bound and the data missed it. An observed delta with no declared
        # band is NOT: it is the measurement this phase now makes, and shouting
        # every one of them would invent alarm out of the absence of a
        # threshold. Those ride the structured output below instead.
        previews.extend(
            f"{concept}: reconciliation breached — {detail}"
            for concept, detail in sorted(reconciliation.breached.items())
        )
        previews.extend(
            f"{concept}: reconciliation grounding failed — {error}"
            for concept, error in sorted(reconciliation.failures.items())
        )

        return PhaseResult.success(
            outputs={
                "declared": len(artifacts),
                "executed": executed,
                "executed_low_confidence": low_confidence,
                "stuck_grounded": grounded_stuck,
                "stuck_declared": declared_stuck,
                "unit_grain_offered": unit_grain.offered,
                "unit_grain_rows": unit_grain.rows,
                # {graph_id: why} — every declared metric that got NO breakdown and
                # the served reason, so "no rows" is never an unexplained hole.
                "unit_grain_withheld": unit_grain.withheld,
                # {graph_id: what was cut} — these metrics DID get rows; the note
                # says the rows are a bounded prefix, not the whole partition.
                "unit_grain_truncated": unit_grain.truncated,
                "reconciliation_rows": reconciliation.rows,
                "reconciliation_evaluated": reconciliation.evaluated,
                # {concept: the observed delta} — an asserted tie-out that was
                # MEASURED with no declared band to grade it against. The
                # feature's normal output, not a complaint.
                "reconciliation_observed": reconciliation.observed,
                # {concept: why} — asserted but not comparable (one grounding,
                # different reporting instants, different aggregations).
                "reconciliation_withheld": reconciliation.withheld,
                "reconciliation_truncated": reconciliation.truncated,
            },
            records_processed=len(table_ids),
            records_created=len(artifacts),
            warnings=previews,
            summary=(
                f"{executed}/{len(artifacts)} metrics executed; "
                f"{declared_stuck} ungroundable, {grounded_stuck} composed but inconclusive/failed; "
                f"{unit_grain.offered} broken down per entity "
                f"({len(unit_grain.withheld)} withheld, {len(unit_grain.truncated)} truncated); "
                f"{reconciliation.evaluated} tie-outs evaluated "
                f"({len(reconciliation.withheld)} not comparable)"
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
    declared_keys: set[str],
    workspace_id: str,
    run_id: str,
    catalogue_run_id: str | None,
) -> None:
    """Persist per-(target x axis) additivity verdicts for every DECLARED metric.

    A drill target is either a **metric** node (a formula, keyed by ``graph_id``)
    or a **measure** node (a grounded extract, keyed by ``standard_field``) — both
    are drillable, so both get verdicts. A metric's verdict rolls its extract
    classes up through the DAG; a measure's verdict IS its single extract's class
    (deduped conservatively across the metrics that share it). Idempotent per run —
    ``(target_kind, target_key, axis_kind, axis_key, run_id)`` UPSERTed (ADR-0010
    form-(a)).

    **Verdict-total (DAT-868).** Every declared metric contributes rows: a
    classification when it can be classified, a typed ABSTENTION when it cannot —
    including when its DAG would not parse and when the whole run has no promoted
    catalogue head. "No row" used to mean both "not drillable" and "judged
    non-additive"; it now means neither, because there is no such thing as no row.

    **Fault-isolated (best-effort annotation).** This runs on the shared phase
    session AFTER every metric's execute bookkeeping is already recorded there; an
    unhandled failure here would surface as a phase failure and roll that session
    back — discarding every metric's ``executed`` state and forcing a full Temporal
    retry of already-successful work. So each classification and the upsert run
    inside their own SAVEPOINT: a bug (a parse error, a bad query) rolls back only
    its own annotation, never the metric bookkeeping.
    """
    from dataraum.graphs.additivity import AbstainReason, AxisKind, abstained
    from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, MetricAxisAdditivity
    from dataraum.graphs.additivity_resolver import VerdictRow, resolve_graph_verdicts
    from dataraum.server.workspace import schema_name_for
    from dataraum.storage.read_views import read_schema_name_for
    from dataraum.storage.upsert import upsert

    def _class_rows(gid: str, reason: AbstainReason) -> list[VerdictRow]:
        """Both axis-class rows for a metric we could not classify at all."""
        return [
            VerdictRow("metric", gid, kind.value, AXIS_KEY_ALL, abstained(reason))
            for kind in AxisKind
        ]

    # The universe is every DECLARED metric, not the ones that executed this run
    # (DAT-868). The canvas offers a metric for drilling as soon as it is declared
    # with a parseable DAG — it applies no state filter — so scoping the verdicts to
    # `executed` left every declared-but-not-executed target with no row at all, and
    # a missing row is indistinguishable from "we judged it non-additive". Every
    # target now gets a verdict or a TYPED ABSTENTION.
    collected: list[VerdictRow] = []
    # sorted(): the measure fold below merges a shared field across metrics with an
    # order-dependent conservative rule, so a deterministic iteration order is
    # required — a set's is PYTHONHASHSEED-salted.
    for graph_id in sorted(declared_keys):
        graph = graphs.get(graph_id)
        if graph is None:
            # Declared but not parseable (GraphLoader refused it). It is still a
            # metric the user declared, and the cockpit's own parse may well differ
            # from ours, so say so explicitly rather than leave a hole. Its measures
            # are unknowable without a DAG — metric-kind rows only.
            collected.extend(_class_rows(graph_id, AbstainReason.GRAPH_PARSE_FAILED))
            continue
        if not catalogue_run_id:
            # No promoted begin_session head: nothing can be classified this run.
            # This used to write NOTHING AT ALL — a silent, total hole. The identity
            # of each declared metric needs no catalogue, so the abstention does not
            # either.
            collected.extend(_class_rows(graph_id, AbstainReason.NO_CATALOGUE_RUN))
            continue
        # ALL fallible work runs inside the SAVEPOINT; only in-memory bookkeeping
        # happens after, so a bug here rolls back its own annotation, never the
        # phase session.
        try:
            with session.begin_nested():
                collected.extend(
                    resolve_graph_verdicts(
                        session,
                        duckdb_conn,
                        graph=graph,
                        graph_id=graph_id,
                        workspace_id=workspace_id,
                        catalogue_run_id=catalogue_run_id,
                        read_schema=read_schema_name_for(schema_name_for(workspace_id)),
                    )
                )
        except Exception as exc:  # noqa: BLE001 - best-effort; never fail the phase
            _log.warning("metric_additivity_compute_error", graph_id=graph_id, error=str(exc))
            collected.extend(_class_rows(graph_id, AbstainReason.UNRESOLVED_GROUNDING))
            continue

    rows = [_verdict_row(run_id, v) for v in _dedupe_verdicts(collected)]
    if not rows:
        _log.info("metric_additivity_persisted", count=0, declared=len(declared_keys))
        return
    try:
        with session.begin_nested():
            upsert(
                session,
                MetricAxisAdditivity,
                rows,
                index_elements=["target_kind", "target_key", "axis_kind", "axis_key", "run_id"],
            )
    except Exception as exc:  # noqa: BLE001 - isolate the write from phase bookkeeping
        _log.warning("metric_additivity_upsert_error", error=str(exc), count=len(rows))
        return
    _log.info(
        "metric_additivity_persisted",
        count=len(rows),
        abstained=sum(1 for r in rows if r["status"] == "abstained"),
        declared=len(declared_keys),
    )


#: How much a verdict CLAIMS, least first. The fold below keeps the least
#: claiming of two groundings for the same target.
_VERDICT_CLAIM_RANK: dict[str | None, int] = {
    "non_additive_recompute": 0,
    "semi_additive": 1,
    "additive": 2,
}


def _dedupe_verdicts(collected: list[VerdictRow]) -> list[VerdictRow]:
    """One row per (target, axis) — the same measure appears in several metrics.

    A ``standard_field`` is shared across the metrics that extract it (one snippet,
    one class), so this is a no-op in practice; it exists because emitting the same
    key twice would make the UPSERT fail outright ("cannot affect row a second
    time"), and because a field ever grounded two ways must resolve to the CLAIM
    ITS WEAKEST GROUNDING SUPPORTS: an abstention beats any verdict, and among
    verdicts the least-claiming wins.

    The winning row carries its own ``bucket_grain`` along as a ride-along — the
    cadence is a property of the AXIS, and the two groundings of one field resolve
    the same served relation, so the two cadences agree wherever both exist. If a
    future grounding path made them differ, the tie-break would need to widen to
    the coarsest cadence the same way ``_common_time_axes`` already does.
    """
    best: dict[tuple[str, str, str, str], VerdictRow] = {}
    for row in collected:
        key = (row.target_kind, row.target_key, row.axis_kind, row.axis_key)
        prior = best.get(key)
        if prior is None or _claims_less(row, prior):
            best[key] = row
    return [best[k] for k in sorted(best)]


def _claims_less(row: VerdictRow, prior: VerdictRow) -> bool:
    from dataraum.graphs.additivity import AdditivityStatus

    if prior.additivity.status is AdditivityStatus.ABSTAINED:
        return False
    if row.additivity.status is AdditivityStatus.ABSTAINED:
        return True
    row_verdict = row.additivity.verdict.value if row.additivity.verdict else None
    prior_verdict = prior.additivity.verdict.value if prior.additivity.verdict else None
    return _VERDICT_CLAIM_RANK.get(row_verdict, 3) < _VERDICT_CLAIM_RANK.get(prior_verdict, 3)


def _verdict_row(run_id: str, v: VerdictRow) -> dict[str, object]:
    """One ``metric_axis_additivity`` row from a resolved verdict."""
    return {
        "run_id": run_id,
        "target_kind": v.target_kind,
        "target_key": v.target_key,
        "axis_kind": v.axis_kind,
        "axis_key": v.axis_key,
        "status": v.additivity.status.value,
        "verdict": v.additivity.verdict.value if v.additivity.verdict else None,
        "reason": v.additivity.reason,
        "abstain_reason": (
            v.additivity.abstain_reason.value if v.additivity.abstain_reason else None
        ),
        "bucket_grain": v.bucket_grain,
    }


# ---------------------------------------------------------------------------
# Unit grain (DAT-671 B1)
# ---------------------------------------------------------------------------


@dataclass
class UnitGrainOutcome:
    """What the unit-grain step did — the phase's disclosure of it.

    Three channels, and a metric appears in AT MOST one of them:

    * ``withheld`` — no breakdown exists for this target, keyed by metric with the
      reason. A normal outcome (a number that does not partition on an axis should
      have no breakdown), which is why it is structured output and not a warning.
    * ``truncated`` — a breakdown DOES exist and its rows ARE persisted, but they
      are a bounded prefix of the entities rather than all of them. Separate from
      ``withheld`` because the two are different facts: "there is nothing to
      render here" versus "render these, there are more". Folding them together
      made a truncated metric count against ``(N withheld)`` and would let a
      consumer keying off ``withheld`` skip rendering rows that exist.
    * ``failures`` — the loud channel: something that should have worked did not.
    """

    offered: int = 0
    rows: int = 0
    withheld: dict[str, str] = field(default_factory=dict)
    truncated: dict[str, str] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)


def _persist_unit_grain(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    agent: GraphAgent,
    graphs: dict[str, TransformationGraph],
    workspace_id: str,
    run_id: str,
    catalogue_run_id: str | None,
) -> UnitGrainOutcome:
    """Compose and persist each declared metric's per-entity breakdown.

    Gated ENTIRELY by the served per-(target × axis) verdict this run's
    :func:`_persist_additivity_verdicts` just wrote — there is no local judgment
    of additivity here, and a missing verdict row is never permission
    (:func:`~dataraum.graphs.unit_grain.gate_unit_grain`). A withheld target
    composes NOTHING and says why; it does not fall back to a workspace scalar,
    which a consumer could not tell apart from a real one-entity breakdown.

    **Fault-isolated, like its two siblings and for the same reason.** This runs
    on the shared phase session after every metric's execute bookkeeping is
    recorded there; an unhandled failure would surface as a phase failure and roll
    that session back, discarding executed lifecycle state over an annotation. So
    each metric's composition and the final upsert run inside their own SAVEPOINT.

    The ``flush()`` first makes that isolation independent of the session's
    autoflush setting. Production runs ``autoflush=False``
    (``core.connections``), so pending bookkeeping cannot be pulled into a
    savepoint there — the flush is not fixing a live hazard. It is what lets the
    guarantee be stated without a footnote about session configuration, and it is
    why the isolation test can assert it on the harness the phase tests build.
    """
    from dataraum.graphs.agent import ExecutionContext
    from dataraum.graphs.unit_grain_db_models import MetricUnitGrain
    from dataraum.storage.upsert import upsert

    outcome = UnitGrainOutcome()
    if not graphs:
        return outcome
    if not catalogue_run_id:
        # No promoted begin_session head: the slice catalog that NAMES the entity
        # axes is read at that run. Nothing can be broken down, and every declared
        # metric says so rather than silently having no rows.
        for graph_id in sorted(graphs):
            outcome.withheld[graph_id] = (
                "no promoted catalogue head this run, so no served entity axis exists"
            )
        return outcome

    session.flush()
    context = ExecutionContext(duckdb_conn=duckdb_conn, schema_mapping_id=workspace_id)
    # Kept per metric, and counted only once its SAVEPOINT has RELEASED cleanly.
    # Releasing is itself fallible (the flush happens there), so a metric counted
    # offered inside the block could still fail on the way out — and would then be
    # both "offered" and "failed", with rows queued for a write its own savepoint
    # had just rolled back.
    per_graph: dict[str, list[dict[str, object]]] = {}
    for graph_id in sorted(graphs):
        try:
            with session.begin_nested():
                produced = _unit_grain_rows(
                    session,
                    agent=agent,
                    context=context,
                    graph=graphs[graph_id],
                    graph_id=graph_id,
                    workspace_id=workspace_id,
                    run_id=run_id,
                    catalogue_run_id=catalogue_run_id,
                    outcome=outcome,
                )
        except Exception as exc:  # noqa: BLE001 - best-effort; never fail the phase
            _log.warning("metric_unit_grain_error", graph_id=graph_id, error=str(exc))
            outcome.failures[graph_id] = str(exc)
            continue
        if produced:
            per_graph[graph_id] = produced

    rows = [row for graph_id in sorted(per_graph) for row in per_graph[graph_id]]
    outcome.offered = len(per_graph)
    if not rows:
        _log.info(
            "metric_unit_grain_persisted",
            count=0,
            offered=outcome.offered,
            # The REASONS, not their count: `warnings` reaches a human but this
            # dict is the only place a withheld breakdown explains itself, and a
            # bare length dies at the activity boundary (PhaseOutcome carries no
            # per-target detail).
            withheld=outcome.withheld,
        )
        return outcome
    try:
        with session.begin_nested():
            upsert(
                session,
                MetricUnitGrain,
                rows,
                index_elements=["target_kind", "target_key", "axis", "entity_value", "run_id"],
            )
    except Exception as exc:  # noqa: BLE001 - isolate the write from phase bookkeeping
        _log.warning("metric_unit_grain_upsert_error", error=str(exc), count=len(rows))
        # Every composed breakdown just failed to land. Without this the summary
        # renders "N broken down per entity" over an EMPTY table and the loud
        # channel stays silent — the offers were real, the rows are not.
        for graph_id in sorted(per_graph):
            outcome.failures[graph_id] = f"composed but not persisted: {exc}"
        outcome.offered = 0
        return outcome
    outcome.rows = len(rows)
    _log.info(
        "metric_unit_grain_persisted",
        count=len(rows),
        offered=outcome.offered,
        withheld=outcome.withheld,
        truncated=outcome.truncated,
    )
    return outcome


def _unit_grain_rows(
    session: Session,
    *,
    agent: GraphAgent,
    context: _ExecutionContext,
    graph: TransformationGraph,
    graph_id: str,
    workspace_id: str,
    run_id: str,
    catalogue_run_id: str,
    outcome: UnitGrainOutcome,
) -> list[dict[str, object]]:
    """One metric's persistable breakdown rows, or none with the reason recorded."""
    from dataraum.graphs.cross_fact import (
        CrossFactAbstain,
        CrossFactStatus,
        resolve_cross_fact_axis,
    )
    from dataraum.graphs.models import StepType
    from dataraum.graphs.unit_grain import (
        gate_unit_grain,
        read_categorical_verdict,
        resolve_metric_entity_axes,
    )

    # WHICH axis is legal depends on how many facts the carriers sit on, and the two
    # cases take their evidence from different places (DAT-809).
    #
    # A column-NAME intersection is sound only within ONE relation, where a name
    # unambiguously denotes a column. Across facts it is not evidence of anything:
    # two facts both carrying a `region` column are not thereby the same region, and
    # merging their numbers on that name asserts a conformance nobody established.
    # So a multi-fact metric takes its axis from the CONFIRMED conformed dimension or
    # gets no breakdown at all — and says which.
    sql_axis: str | None = None
    step_grain: dict[str, tuple[tuple[str, str], ...]] | None = None
    cross = resolve_cross_fact_axis(
        session, graph=graph, workspace_id=workspace_id, run_id=catalogue_run_id
    )
    if cross.status is CrossFactStatus.RESOLVED and cross.axis is not None:
        # Two different names for two different jobs, and they must not be swapped:
        # the IDENTITY is the merge key and the persisted axis (part of the ADR-0010
        # upsert key, so it must not drift the way a label can — DAT-800), while the
        # LABEL is what any human-facing string says. The identity embeds a table
        # uuid, so showing it would put `ref:8f0a…:account_id` in front of a reader.
        axis = cross.axis.label
        sql_axis = cross.axis.identity
        persisted_axis = cross.axis.identity
        step_grain = cross.axis.step_grain()
    elif cross.abstain_reason is not CrossFactAbstain.SINGLE_FACT:
        outcome.withheld[graph_id] = cross.reason or "withheld without a reason"
        return []
    else:
        served_axes = resolve_metric_entity_axes(
            session, graph=graph, workspace_id=workspace_id, run_id=catalogue_run_id
        )
        if not served_axes.axes:
            reason = (
                "no served categorical axis is carried by every one of its grounded "
                "carriers (an ungroundable carrier, a relation outside the analysis, or "
                "no judged categorical slice they share)"
            )
            # The catalog read's own account of what it left unsaid — today, that
            # nothing in it was ever judged. Carried through rather than dropped: a
            # never-assessed axis and an assessed-and-rejected one are different
            # facts, and only one of them is a reason to go look at the ranker.
            if served_axes.note:
                reason = f"{reason} — {served_axes.note}"
            outcome.withheld[graph_id] = reason
            return []
        # The catalog's own ranking, judgment before measurement — the workspace says
        # which axis is the interesting one, so we take its first and do not re-rank.
        # Single-fact: the axis IS a column on the one relation, so display key and
        # persisted key coincide.
        axis = served_axes.axes[0]
        persisted_axis = axis

    verdict = read_categorical_verdict(
        session, target_kind="metric", target_key=graph_id, run_id=run_id
    )
    # The carriers a recompute would be rebuilt from, keyed as the drill keys a
    # measure target: by standard_field.
    carriers = {
        step.source.standard_field: read_categorical_verdict(
            session,
            target_kind="measure",
            target_key=step.source.standard_field,
            run_id=run_id,
        )
        for step in graph.steps.values()
        if step.step_type == StepType.EXTRACT and step.source and step.source.standard_field
    }
    decision = gate_unit_grain(axis, verdict, carriers)
    if not decision.offered:
        outcome.withheld[graph_id] = decision.reason or "withheld without a reason"
        return []

    composed = agent.compose_unit_grain(
        session,
        graph,
        context,
        axis=axis,
        workspace_id=workspace_id,
        sql_axis=sql_axis,
        step_grain=step_grain,
    )
    if not composed.success or composed.value is None:
        outcome.failures[graph_id] = composed.error or "unit-grain composition failed"
        return []
    breakdown = composed.value
    if breakdown.truncated_at is not None:
        # A cut breakdown is still a breakdown: its rows land and it counts as
        # offered. It gets its OWN channel rather than riding the withheld one —
        # "rows exist, bounded" is not "no rows here", and a consumer reading
        # withheld as the second would skip rendering real rows.
        outcome.truncated[graph_id] = (
            f"breakdown per {axis!r} truncated at {breakdown.truncated_at} of "
            f"{breakdown.total_entities} entities — the persisted rows are the first "
            f"{breakdown.truncated_at} by entity value, not the whole partition"
        )
    return [
        {
            "run_id": run_id,
            "target_kind": "metric",
            "target_key": graph_id,
            "axis": persisted_axis,
            "entity_value": row.entity_value,
            "value": row.value,
            "reconciles": decision.reconciles,
            "recompute": decision.recompute,
        }
        for row in breakdown.rows
    ]


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

    The pass closes with the CROSS-CONCEPT guard (DAT-709): grounding is one call
    per concept, so this is the only scope in the run where every concept's
    decision coexists, and therefore the only place two disjoint concepts sharing
    one extract can be seen at all. Collisions are re-grounded once with the
    collision named, then abstain typed — see ``graphs.grounding_collision``.
    """
    from dataraum.graphs.grounding_collision import resolve_grounding_collisions
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

    manager = ctx.manager
    if manager is not None:
        bindings = _warm_generations_parallel(
            generations,
            nodes,
            manager,
            agent,
            schema_mapping_id,
            table_ids,
            vertical,
            om_run_id,
            catalogue_run_id,
        )
    else:
        bindings = _warm_generations_serial(
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

    def _reauthor(key: NodeKey, reason: str) -> NodeDecision:
        """Demote the collided snippet and re-author the node, in ONE unit of work.

        The demotion must be visible to the re-authoring's own snippet lookup —
        a healthy row is assembled from cache with no LLM call, which would make
        the re-ground silently inert — so both happen in the same session.
        """
        if manager is not None:
            return _warm_isolated(
                nodes[key],
                manager,
                agent,
                schema_mapping_id,
                table_ids,
                vertical,
                om_run_id,
                catalogue_run_id,
                demote_reason=reason,
            )
        return _warm_in_session(
            nodes[key],
            ctx.session,
            ctx.duckdb_conn,
            agent,
            schema_mapping_id,
            table_ids,
            vertical,
            om_run_id,
            catalogue_run_id,
            demote_reason=reason,
        )

    return resolve_grounding_collisions(
        bindings,
        nodes,
        session=ctx.session,
        workspace_id=schema_mapping_id,
        schema_mapping_id=schema_mapping_id,
        vertical=vertical,
        reauthor=_reauthor,
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
    *,
    demote_reason: str | None = None,
) -> NodeDecision:
    """Author one node with an isolated session + cursor; return its decision."""
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        return _warm_in_session(
            node,
            session,
            cursor,
            agent,
            schema_mapping_id,
            table_ids,
            vertical,
            om_run_id,
            catalogue_run_id,
            demote_reason=demote_reason,
        )


def _warm_in_session(
    node: WarmNode,
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    om_run_id: str,
    catalogue_run_id: str | None = None,
    *,
    demote_reason: str | None = None,
) -> NodeDecision:
    """Author one node against an already-open session + cursor.

    ``demote_reason`` turns this into a RE-authoring (DAT-709): the node's
    existing snippet is flagged with that reason FIRST, in this same session, so
    the lookup inside ``execute`` no longer finds a healthy row and the LLM is
    actually called — and so the reason reaches the new prompt through the
    retained-failure feedback channel.

    Deliberately NOT shared with ``_warm_generations_serial``'s inner loop, which
    keeps its own copy of these four lines. Two divergences make unification a
    net loss: that loop builds the ``ExecutionContext`` once per GENERATION, not
    per node (DAT-734 — a later generation must see the snippets earlier ones
    minted, and rebuilding the whole served context per node would pay for that
    many times over), and it catches per-node exceptions inline so one bad node
    does not abort the wave. Folding either into this helper would change the
    warm path's cost or its error boundary to save four lines.
    """
    from dataraum.graphs.agent import ExecutionContext
    from dataraum.graphs.grounding_collision import flag_collision
    from dataraum.graphs.node_warming import NodeDecision, build_mini_graph

    if demote_reason is not None:
        # workspace_id IS schema_mapping_id on the authoring path — the same value
        # ``execute`` is handed below as its snippet-library write-path guard.
        flag_collision(
            session,
            node,
            workspace_id=schema_mapping_id,
            schema_mapping_id=schema_mapping_id,
            reason=demote_reason,
        )
    exec_ctx = ExecutionContext.with_rich_context(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        schema_mapping_id=schema_mapping_id,
        om_run_id=om_run_id,
        catalogue_run_id=catalogue_run_id,
        vertical=vertical,
    )
    result = agent.execute(
        session, build_mini_graph(node), exec_ctx, workspace_id=schema_mapping_id
    )
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

    bindings: dict[NodeKey, NodeDecision] = {}
    for generation in generations:
        # Rebuild the context PER GENERATION (DAT-734): the served concept graph
        # carries prior committed groundings, and a later generation must see the
        # snippets earlier generations just minted — a once-built context would
        # serve only prior-RUN groundings here while the parallel path
        # (_warm_isolated, fresh context per node) serves same-run siblings.
        exec_ctx = ExecutionContext.with_rich_context(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=table_ids,
            schema_mapping_id=schema_mapping_id,
            om_run_id=om_run_id,
            catalogue_run_id=catalogue_run_id,
            vertical=vertical,
        )
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


def _assemble_metric(
    agent: GraphAgent,
    session: Session,
    exec_ctx: _ExecutionContext,
    graph: TransformationGraph,
    bindings: dict[NodeKey, NodeDecision],
    *,
    workspace_id: str,
) -> Result[GraphExecution]:
    """Assemble one metric, deriving ``days_in_period`` from the data (DAT-785).

    A working-capital metric's ``days_in_period`` is the window its flow (COGS,
    revenue) was measured over — the observed span of the flow fact's anchor time
    axis, not a config constant. :func:`resolve_days_in_period` reads it from the
    substrate and returns ``None`` for a metric that has no such parameter (or a
    non-Postgres bind with no read surface), in which case the graph default stands.
    A window that cannot be observed falls loud: the config default rides a visible
    verification flag on the executed artifact, never a silent 30.

    The period resolver's live window query MUST run against the same DuckDB cursor
    ``agent.assemble`` runs the flow SUM on — so it reads ``exec_ctx.duckdb_conn``,
    the one home of the cursor, rather than a separately-threaded connection.
    """
    from dataraum.graphs.period_resolver import resolve_days_in_period

    period = resolve_days_in_period(
        session, exec_ctx.duckdb_conn, graph=graph, workspace_id=workspace_id
    )
    parameters = {"days_in_period": period.days} if period is not None else None
    result = agent.assemble(
        session, graph, exec_ctx, bindings, parameters=parameters, workspace_id=workspace_id
    )
    if period is not None and result.success and result.value is not None:
        if period.flag:
            # Never a silent fallback: the flag surfaces unconditionally through the
            # artifact's state_reason (execute-and-flag), exactly like a DAT-699
            # verification flag.
            result.value.verification_flags.append(period.flag)
            _log.warning(
                "metric_period_fallback",
                graph_id=graph.graph_id,
                reason=period.evidence.get("reason"),
            )
        else:
            _log.info(
                "metric_period_derived",
                graph_id=graph.graph_id,
                days=period.days,
                anchor_time_axis=period.evidence.get("anchor_time_axis"),
            )
    return result


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
        # Guard each assembly exactly like the parallel sibling captures
        # ``future.result()``: one metric raising (e.g. a bad grounded predicate
        # reaching the period resolver's live query) must not crash the phase and
        # roll back every sibling's already-recorded execute state.
        try:
            result = _assemble_metric(
                agent,
                session,
                exec_ctx,
                graph,
                bindings,
                workspace_id=workspace_id,
            )
        except Exception as exc:
            result = Result.fail(f"Unexpected error executing {graph_id}: {exc}")
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
        return _assemble_metric(
            agent, session, exec_ctx, graph, bindings, workspace_id=schema_mapping_id
        )
