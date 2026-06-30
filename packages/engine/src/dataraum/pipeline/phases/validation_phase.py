"""Validation phase — the operating_model stage's first lifecycle family (DAT-438).

Source-free and session-scoped: operates on ``ctx.table_ids`` (the session's
typed tables), never a ``source_id``. Each declared validation (vertical YAML
⊕ ``validation`` overlay teach rows) flows through the typed artifact
lifecycle:

* **declare** — every loaded spec becomes a ``declared`` artifact for this run.
* **bind** (``validation.bind``) — the LLM grounds the spec against the
  workspace (SQL + EXPLAIN). Ungroundable specs STAY ``declared`` with the
  reason on the row — visibly impossible, never silently absent.
* **execute** (``validation.execute``) — the grounded SQL runs and is
  evaluated; the artifact reaches ``executed``. PASSED/FAILED is the
  *measurement*, not the lifecycle outcome; an execution ERROR **or an
  inconclusive evaluation** (the SQL ran but its result shape cannot be
  judged — DAT-439) keeps the artifact at ``grounded`` with the reason
  recorded. Inconclusive is never reported as FAILED.

A re-run supersedes: everything is re-declared and re-flowed under the fresh
``run_id`` (no skip-if-already-ran — the prior run's rows coexist untouched,
and the promoted head names the current run). The engine induces no
validations: with no vertical or no declared specs the phase succeeds loudly
with an explicit ``no_declared_validations`` outcome.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from types import ModuleType
from typing import TYPE_CHECKING, Any

from dataraum.analysis.semantic.ontology import OntologyLoader
from dataraum.analysis.validation import ValidationAgent
from dataraum.analysis.validation.config import load_all_validation_specs
from dataraum.analysis.validation.db_models import ValidationResultRecord
from dataraum.analysis.validation.models import (
    GeneratedSQL,
    ValidationResult,
    ValidationRunResult,
    ValidationSpec,
    ValidationStatus,
)
from dataraum.analysis.validation.resolver import get_multi_table_schema_for_llm
from dataraum.core.logging import get_logger
from dataraum.lifecycle import BaseRunMap, declare_artifact, transition
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

_log = get_logger(__name__)

# The journey stage this phase runs under — the lifecycle guard authorizes
# validation.declare/bind/execute for this stage only.
_STAGE = "operating_model"

# DAT-651: the per-validation work (one LLM bind + EXPLAIN, then a pure-DuckDB
# execute) is independent and is the phase's dominant wall-clock — N serial LLM
# round-trips. Fan it across a bounded pool; max_workers IS the concurrency cap
# (mirrors the metrics fan-out). Kept modest to avoid hammering the LLM provider.
_MAX_CONCURRENT_VALIDATIONS = 4

# (generated, bind_failure, exec_result) — the worker's outcome, keyed back to its
# validation_id by the caller. Exactly one of bind_failure / exec_result is set;
# the main thread applies the lifecycle transition from it.
_WorkOutcome = tuple[GeneratedSQL | None, ValidationResult | None, ValidationResult | None]


@analysis_phase
class ValidationPhase(BasePhase):
    """LLM-powered validation through the artifact lifecycle.

    Generates and executes SQL validation checks by passing table schemas
    to the LLM. Can generate cross-table JOINs when validations require
    data from multiple tables.

    Requires: a begin_session workspace (typed tables, relationships,
    enriched views) reachable through the run's pinned base heads.
    """

    @property
    def name(self) -> str:
        return "validation"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.validation import db_models
        from dataraum.lifecycle import db_models as lifecycle_db_models

        return [db_models, lifecycle_db_models]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Declare → bind → execute every loaded validation spec."""
        started_at = datetime.now(UTC)

        table_ids = ctx.table_ids
        if not table_ids:
            return PhaseResult.failed(
                "No tables in session scope — validation operates on the "
                "session's typed table selection (ctx.table_ids)."
            )

        # Declared set: shipped vertical YAML ⊕ validation overlay teach rows.
        # No vertical / no specs is a LOUD explicit outcome, not a silent skip:
        # the engine induces no validations (declares come from the vertical
        # now; user declares arrive via frame-2 teach rows, DAT-441).
        vertical = ctx.config.get("vertical")
        specs = load_all_validation_specs(vertical) if vertical else {}
        if not specs:
            outcome = "no_vertical" if not vertical else "no_declared_validations"
            _log.warning("validation_nothing_declared", vertical=vertical, outcome=outcome)
            return PhaseResult.success(
                outputs={"outcome": outcome, "declared": 0, "total_checks": 0},
                records_processed=0,
                records_created=0,
                summary=f"0 declared validations ({outcome}) — nothing to ground or execute",
            )

        run_id = ctx.require_run_id()
        # Pinned upstream heads (ADR-0008 in-run mode): resolved ONCE by the
        # workflow's pre-flight ``operating_model_resolve`` activity and
        # threaded here through the phase config. The phase performs NO head
        # resolution itself — a missing pin is a wiring bug, fail loud.
        raw_base_runs = ctx.config.get("base_runs")
        if raw_base_runs is None:
            return PhaseResult.failed(
                "base_runs missing from the phase config — OperatingModelWorkflow's "
                "resolve activity pins the base-run map before this phase runs "
                "(ADR-0008 in-run mode; no per-phase head resolution)."
            )
        base_runs = BaseRunMap.model_validate(raw_base_runs)
        grounded_against = base_runs.model_dump(mode="json")

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

        agent = ValidationAgent(
            config=config,
            provider=provider,
            prompt_renderer=PromptRenderer(),
        )

        # declare: every spec becomes a declared artifact for THIS run —
        # supersession across runs; a success-redelivery RESETS the same
        # run's row to declared (declare-or-reuse, DAT-502).
        artifacts = {}
        for validation_id, spec in specs.items():
            artifacts[validation_id] = declare_artifact(
                ctx.session,
                artifact_type="validation",
                artifact_key=validation_id,
                run_id=run_id,
                stage=_STAGE,
                teaches={
                    "validation_id": validation_id,
                    "vertical": vertical,
                    "version": spec.version,
                    "source": spec.source,
                },
            )

        # The workspace schema, every run-versioned read pinned to base_runs.
        schema = get_multi_table_schema_for_llm(
            ctx.session, table_ids, duckdb_conn=ctx.duckdb_conn, base_runs=base_runs
        )
        if "error" in schema:
            return PhaseResult.failed(str(schema["error"]))
        context_issues = agent.validate_context(schema)
        if context_issues:
            return PhaseResult.failed(
                f"Insufficient context for validation: {'; '.join(context_issues)}"
            )
        table_names = ", ".join(t["table_name"] for t in schema.get("tables", []))

        # DAT-645: the vertical's conventions, piped verbatim into the SQL
        # generation of the validations they target. A convention is routed
        # PER-SPEC (qualifier = the validation id), so a sign rule reaches only the
        # validations that name it (e.g. `validation:sign_conventions`) and stays
        # out of unrelated checks. Same source of truth the graph agent uses for
        # extraction. Empty when the vertical declares none.
        ontology_loader = OntologyLoader()
        ontology = ontology_loader.load(vertical) if vertical else None

        # bind → execute per declared spec, parallelized (DAT-651). The per-spec
        # work is PURE compute — one LLM bind + EXPLAIN, then a pure-DuckDB
        # execute — and touches NEITHER the SQLAlchemy session NOR the lifecycle
        # artifacts (a Session is not thread-safe). It runs on a bounded pool;
        # the session mutations (transitions + state_reason + persist) are then
        # applied serially on the main thread, in deterministic spec order.
        # Conventions are read-only ontology text, pre-computed here (off the pool).
        conventions_by_id = {
            validation_id: ontology_loader.format_conventions_for_prompt(
                ontology, "validation", qualifier=validation_id
            )
            for validation_id in specs
        }

        def _bind_and_execute(
            validation_id: str, spec: ValidationSpec, duckdb_conn: Any
        ) -> _WorkOutcome:
            """Ground + execute one spec on the given cursor — no session/artifact touch.

            An LLM ``ProviderError`` (DAT-503) is left to PROPAGATE: it rides to
            the durable boundary for retry and must never be captured as a
            per-spec ERROR result.
            """
            generated, bind_failure = agent.bind_validation(
                duckdb_conn, table_ids, spec, schema, conventions=conventions_by_id[validation_id]
            )
            if bind_failure is not None:
                return None, bind_failure, None
            assert generated is not None  # bind contract: exactly one side set
            exec_result = agent.execute_validation(duckdb_conn, table_ids, spec, schema, generated)
            return generated, None, exec_result

        # Dispatch: concurrent on per-worker lake-scoped cursors when a manager is
        # wired (``manager.duckdb_cursor()`` is DuckDB's thread-safe primitive — an
        # independent connection that re-issues ``USE lake.typed``, which a bare
        # ``.cursor()`` would NOT inherit and reusing one connection across threads
        # would serialize). No manager (unit context) → serial on the phase's
        # already-scoped connection, identical to the pre-DAT-651 path.
        collected: dict[str, _WorkOutcome] = {}
        if ctx.manager is not None:
            manager = ctx.manager

            def _isolated(validation_id: str, spec: ValidationSpec) -> _WorkOutcome:
                with manager.duckdb_cursor() as cursor:
                    return _bind_and_execute(validation_id, spec, cursor)

            with ThreadPoolExecutor(
                max_workers=_MAX_CONCURRENT_VALIDATIONS, thread_name_prefix="validation"
            ) as pool:
                futures = {
                    pool.submit(_isolated, validation_id, spec): validation_id
                    for validation_id, spec in specs.items()
                }
                for future in as_completed(futures):
                    collected[futures[future]] = future.result()
        else:
            for validation_id, spec in specs.items():
                collected[validation_id] = _bind_and_execute(validation_id, spec, ctx.duckdb_conn)

        # Apply the lifecycle transitions + collect results serially on the MAIN
        # thread (the artifacts live in ctx.session), in deterministic spec order.
        results: list[ValidationResult] = []
        for validation_id in specs:
            artifact = artifacts[validation_id]
            generated, bind_failure, exec_result = collected[validation_id]
            if bind_failure is not None:
                # Ungroundable: stays declared, reason on the row.
                artifact.state_reason = bind_failure.message
                results.append(bind_failure)
                continue
            assert generated is not None  # bind contract: exactly one side set
            assert exec_result is not None
            transition(artifact, operation="bind", stage=_STAGE, grounded_against=grounded_against)
            if exec_result.status == ValidationStatus.ERROR:
                # Execution error OR inconclusive evaluation (the SQL ran but its
                # result shape cannot be judged, DAT-439): stays grounded, with
                # the reason on the row.
                artifact.state_reason = exec_result.message
            else:
                transition(artifact, operation="execute", stage=_STAGE)
            results.append(exec_result)

        run_result = ValidationRunResult.from_results(
            run_id=run_id,
            table_ids=table_ids,
            table_name=table_names,
            started_at=started_at,
            results=results,
        )
        _persist_results(ctx.session, run_result)

        # Two distinct axes in the outputs below: the LIFECYCLE counts
        # (declared/executed/stuck_* — where each artifact landed; stuck_declared
        # covers BOTH skipped and generation-error binds) and the RESULT counts
        # (passed/failed/skipped/error — the per-check measurements). They
        # overlap by design; don't sum across axes.
        executed = sum(1 for a in artifacts.values() if a.state == "executed")
        grounded_stuck = sum(1 for a in artifacts.values() if a.state == "grounded")
        declared_stuck = sum(1 for a in artifacts.values() if a.state == "declared")

        # Surface failed validations as warnings for display
        warnings = [f"{r.validation_id}: {r.message}" for r in run_result.results if not r.passed]

        return PhaseResult.success(
            outputs={
                "declared": len(artifacts),
                "executed": executed,
                "stuck_grounded": grounded_stuck,
                "stuck_declared": declared_stuck,
                "total_checks": run_result.total_checks,
                "passed_checks": run_result.passed_checks,
                "failed_checks": run_result.failed_checks,
                "skipped_checks": run_result.skipped_checks,
                "error_checks": run_result.error_checks,
                "overall_status": run_result.overall_status.value,
                "has_critical_failures": run_result.has_critical_failures,
                "tables_validated": [t["table_name"] for t in schema.get("tables", [])],
            },
            records_processed=run_result.total_checks,
            # One ValidationResultRecord per check + one LifecycleArtifact per spec.
            records_created=run_result.total_checks + len(artifacts),
            warnings=warnings,
            summary=(
                f"{executed}/{len(artifacts)} validations executed "
                f"({run_result.passed_checks} passed, {run_result.failed_checks} failed); "
                f"{declared_stuck} ungroundable, {grounded_stuck} unresolved (execution error or inconclusive)"
            ),
        )


def _persist_results(session: Session, run_result: ValidationRunResult) -> None:
    """Persist one run-stamped ``ValidationResultRecord`` per result.

    Form-(a) upsert on ``uq_validation_result_run`` (DAT-502): a Temporal
    success-redelivery re-runs the whole phase under the same ``run_id`` and
    re-persists every check — converging in place instead of violating the
    UNIQUE. PK omitted so the model's Python-side default applies.
    """
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for result in run_result.results:
        # Serialize details to ensure JSON compatibility
        result_data = result.model_dump(mode="json")
        rows[(result.validation_id, run_result.run_id)] = {
            "run_id": run_result.run_id,
            "validation_id": result.validation_id,
            "table_ids": result.table_ids,
            "status": result.status.value,
            "severity": result.severity.value,
            "passed": result.passed,
            "message": result.message,
            "executed_at": result.executed_at,
            "sql_used": result.sql_used,
            "columns_used": result.columns_used,
            "details": result_data.get("details"),
        }
    upsert(
        session,
        ValidationResultRecord,
        list(rows.values()),
        index_elements=["validation_id", "run_id"],
    )

    _log.debug(
        "validation_results_persisted",
        run_id=run_result.run_id,
        count=len(run_result.results),
    )
