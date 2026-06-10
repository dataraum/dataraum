"""Temporal activity definitions for pipeline phases (DAT-344, per-table DAT-370).

Thin ``@activity.defn`` wrappers that translate the per-boundary contracts into
calls on the Temporal-agnostic helpers in :mod:`dataraum.worker.activity`. They
hold the worker's single :class:`ConnectionManager` (set at bootstrap) and name
each activity after its pipeline.yaml phase (plus the terminal ``detect``) — so
the workflows call them by that string, no shared catalogue.

Activities are **sync** (``def``): Temporal runs them on the worker's
``ThreadPoolExecutor``, the SDK-recommended shape for blocking SQLAlchemy/DuckDB
work. Each helper call leases a fresh Postgres session + a DuckDB **cursor** off
the worker's shared DuckLake connection. A DuckDB ``cursor()`` is an
*independent connection* to the same named in-memory lake DB: it shares the
catalog (the DuckLake ATTACH, schemas, tables) but carries its own transaction +
``USE`` state, and is DuckDB's blessed primitive for concurrent access. So
concurrent activities (parallel child workflows) run on independent channels;
DuckLake reconciles concurrent writers via MVCC + optimistic concurrency, and
the rare commit conflict raises and is absorbed by Temporal's activity retry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from temporalio import activity
from temporalio.exceptions import ApplicationError

from dataraum.llm.providers.base import is_transient_error
from dataraum.pipeline.base import PhaseStatus
from dataraum.worker.activity import (
    SESSION_DETECTOR_PHASES,
    PhaseRun,
    begin_session_select,
    check_run_column_limit,
    materialize_session_overlays,
    promote_operating_model_run,
    promote_run,
    promote_session_run,
    raw_table_ids,
    resolve_operating_model_scope,
    run_detectors,
    run_phase,
    run_session_phase,
    typed_table_id_for_raw,
    write_session_keepers,
)
from dataraum.worker.contracts import (
    ImportResult,
    OperatingModelScope,
    OperatingModelScopedInput,
    PhaseOutcome,
    ProcessTableInput,
    RunScopedInput,
    SessionIdentity,
    SessionScopedInput,
    SourceIdentity,
    TableScopedInput,
    TypingResult,
)

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager


class PhaseActivities:
    """Phase activities bound to the worker's ConnectionManager.

    Registered as bound methods (``Worker(..., activities=[acts.run_import, …])``)
    so the manager is captured by instance, not a module global — no
    import-time/runtime ordering coupling.
    """

    def __init__(self, manager: ConnectionManager) -> None:
        self._manager = manager

    @activity.defn(name="import")
    def run_import(self, identity: SourceIdentity) -> ImportResult:
        """Import activity — loads ONE source into ``lake.raw.*``, returns its raw ids.

        ``import`` is the one per-source activity (DAT-422): the parent runs it once
        per source in the run's set, each call scoped to a single ``source_id``. So a
        ``None`` source_id is a caller bug — fail loud rather than load nothing. The
        discovered raw ids are the parent workflow's fan-out source, read
        authoritatively from the substrate after the phase — correct even when import
        is skipped because the source was already imported.
        """
        if identity.source_id is None:
            raise ApplicationError(
                "import requires identity.source_id — the workflow scopes each import "
                "to one source (DAT-422).",
                type="PhaseFailed",
                non_retryable=True,
            )
        self._run_or_raise("import", identity, [])
        return ImportResult(raw_table_ids=raw_table_ids(self._manager, identity.source_id))

    @activity.defn(name="check_column_limit")
    def run_check_column_limit(self, payload: RunScopedInput) -> PhaseOutcome:
        """Run-scoped column gate — bound the run's total cost before the fan-out (DAT-430).

        Counts the columns across the UNION of the run's raw tables (the import
        loop's accumulated ids) against ``limits.max_columns``. Run-level, not
        per-source: a run is a SET of per-file content sources (DAT-422), so only
        the union bounds the pipeline/LLM cost — and because the workflow calls
        this unconditionally, it also gates runs whose imports all skipped. A
        breach raises the non-retryable ``PhaseFailed``.
        """
        run = check_run_column_limit(self._manager, payload.identity, payload.table_ids)
        return self._outcome_or_raise(run, "check_column_limit")

    @activity.defn(name="typing")
    def run_typing(self, payload: ProcessTableInput) -> TypingResult:
        """Typing activity — type-resolves one raw table, returns its typed id."""
        self._run_or_raise("typing", payload.identity, [payload.raw_table_id])
        typed_id = typed_table_id_for_raw(self._manager, payload.raw_table_id)
        if typed_id is None:
            raise ApplicationError(
                f"typing produced no typed table for raw table '{payload.raw_table_id}'",
                type="PhaseFailed",
                non_retryable=True,
            )
        return TypingResult(typed_table_id=typed_id)

    @activity.defn(name="statistics")
    def run_statistics(self, payload: TableScopedInput) -> PhaseOutcome:
        """Statistics activity — per-column statistical profiling of one typed table."""
        return self._run_or_raise("statistics", payload.identity, [payload.table_id])

    @activity.defn(name="column_eligibility")
    def run_column_eligibility(self, payload: TableScopedInput) -> PhaseOutcome:
        """Column-eligibility activity — marks which columns downstream phases analyze."""
        return self._run_or_raise("column_eligibility", payload.identity, [payload.table_id])

    @activity.defn(name="statistical_quality")
    def run_statistical_quality(self, payload: TableScopedInput) -> PhaseOutcome:
        """Statistical-quality activity — Benford + outlier detection on numeric columns."""
        return self._run_or_raise("statistical_quality", payload.identity, [payload.table_id])

    @activity.defn(name="temporal")
    def run_temporal(self, payload: TableScopedInput) -> PhaseOutcome:
        """Temporal activity — pattern/trend profiling of date/time columns."""
        return self._run_or_raise("temporal", payload.identity, [payload.table_id])

    @activity.defn(name="semantic_per_column")
    def run_semantic_per_column(self, identity: SourceIdentity) -> PhaseOutcome:
        """Semantic-per-column activity — the session-scoped LLM reduce (roles, concepts, terms).

        Runs once after the per-table fan-out over the run's SESSION tables
        (``tables_for_session``, DAT-421) — not "the whole source" — so a run whose
        tables span multiple per-object sources is grounded as one set. Grounding
        only (induction left the engine, DAT-382). Needs a working
        ``ANTHROPIC_API_KEY`` + the provider/prompt config resolvable from
        ``dataraum.core.config``; unlike the analytics phases it makes real LLM calls.
        """
        return self._run_or_raise("semantic_per_column", identity, [])

    @activity.defn(name="detect")
    def run_detect(self, identity: SourceIdentity) -> PhaseOutcome:
        """Terminal detector pass — every wired detector once, source-wide (DAT-394).

        The single stage-level detect step: after the per-table fan-out and the
        ``semantic_per_column`` reduce, run the union of all chain-declared detectors
        over the whole source. Replaces the old per-table ``detect_table`` + parent
        ``detect_source`` split — nothing consumes entropy mid-run, so one terminal
        pass is correct and simpler. (DAT-394 phase 2 persists readiness here too.)
        """
        count = run_detectors(self._manager, session_id=identity.session_id, run_id=identity.run_id)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"{count} detector records for session {identity.session_id}",
        )

    @activity.defn(name="promote_to_latest")
    def run_promote_to_latest(self, identity: SourceIdentity) -> PhaseOutcome:
        """Terminal promote step — flip the snapshot head to this run (DAT-413).

        Runs last in ``addSourceWorkflow``, after ``detect``: upserts
        :class:`MetadataSnapshotHead` for each of the run's tables × add_source
        stage so the head names this ``run_id`` as current. Behavior-preserving
        in Phase 2 — nothing reads the head yet (one run at a time), so promoting
        it cannot change downstream output; Phase 3 switches readers to it.
        """
        count = promote_run(self._manager, identity)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"promoted {count} snapshot head(s) for session {identity.session_id}",
        )

    # --- begin_session activities (DAT-401) — source-free, session-scoped ----

    @activity.defn(name="begin_session_select")
    def run_begin_session_select(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Pre-flight the selection + link it to the session (the spine's first step).

        Validates every id is a known typed table (reject unknown → non-retryable)
        and writes the ``session_tables`` links via the idempotent merge ``typing``
        uses for add_source. The session row itself is seeded by the caller.
        """
        run = begin_session_select(self._manager, payload.identity, payload.table_ids)
        return self._outcome_or_raise(run, "begin_session_select")

    @activity.defn(name="relationships")
    def run_relationships(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Relationships activity — structural cross-table candidate detection.

        Source-free: scopes to the session's selected typed tables (which may
        span sources), persisting ``detection_method='candidate'`` rows.
        """
        run = run_session_phase(self._manager, "relationships", payload.identity, payload.table_ids)
        return self._outcome_or_raise(run, "relationships")

    @activity.defn(name="semantic_per_table")
    def run_semantic_per_table(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Semantic-per-table activity — LLM table classification + relationship confirm.

        Reasons over the per-column annotations to classify tables and confirm a
        subset of the structural candidates (``detection_method='llm'``). Makes
        real Anthropic calls; needs a working ``ANTHROPIC_API_KEY`` + the session's
        ``vertical``.
        """
        run = run_session_phase(
            self._manager, "semantic_per_table", payload.identity, payload.table_ids
        )
        return self._outcome_or_raise(run, "semantic_per_table")

    @activity.defn(name="enriched_views")
    def run_enriched_views(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Enriched-views activity — grain-preserving fact×dimension views (DAT-415).

        Source-free: builds one ``CREATE OR REPLACE VIEW`` per session fact table
        over its LLM-confirmed dimension joins, versioning each view's DDL on the
        materialization-recipe substrate (run-stamped) and registering the enriched
        lake substrate latest-only. Runs after ``session_materialize_overlays`` so
        the user's durable relationship teaches are folded in. Makes real Anthropic
        calls (the enrichment agent); needs ``ANTHROPIC_API_KEY``.
        """
        run = run_session_phase(
            self._manager, "enriched_views", payload.identity, payload.table_ids
        )
        return self._outcome_or_raise(run, "enriched_views")

    # --- value layer (DAT-403) — source-free, session-scoped, after enriched_views ---

    @activity.defn(name="slicing")
    def run_slicing(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Slicing activity — LLM-recommended slice dimensions per session fact table.

        Source-free: scopes to the session's selected typed tables, persisting
        ``SliceDefinition`` rows for the fact tables that carry an enriched view.
        Makes real Anthropic calls (the slicing agent); needs ``ANTHROPIC_API_KEY``.
        """
        run = run_session_phase(self._manager, "slicing", payload.identity, payload.table_ids)
        return self._outcome_or_raise(run, "slicing")

    @activity.defn(name="slicing_view")
    def run_slicing_view(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Slicing-view activity — narrow each fact table to its slice-relevant columns.

        Projects the enriched view down to the fact columns plus the slice
        dimension columns, registering a ``slicing_view`` lake artifact per fact
        table. No LLM call.
        """
        run = run_session_phase(self._manager, "slicing_view", payload.identity, payload.table_ids)
        return self._outcome_or_raise(run, "slicing_view")

    @activity.defn(name="slice_analysis")
    def run_slice_analysis(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Slice-analysis activity — materialize the slice tables and profile them.

        Executes each slice definition's SQL to create the per-value slice tables,
        registers them, and runs statistics + quality on each. No LLM call.
        """
        run = run_session_phase(
            self._manager, "slice_analysis", payload.identity, payload.table_ids
        )
        return self._outcome_or_raise(run, "slice_analysis")

    @activity.defn(name="temporal_slice_analysis")
    def run_temporal_slice_analysis(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Temporal-slice-analysis activity — drift/period metrics on the slice tables.

        Runs JS-divergence drift detection and period completeness/anomaly analysis
        per slice, and builds the ``ColumnSliceProfile`` records the
        ``dimensional_entropy`` detector consumes. No LLM call.
        """
        run = run_session_phase(
            self._manager, "temporal_slice_analysis", payload.identity, payload.table_ids
        )
        return self._outcome_or_raise(run, "temporal_slice_analysis")

    @activity.defn(name="correlations")
    def run_correlations(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Correlations activity — detect derived columns over the enriched views.

        Finds same-table and cross-table derived columns (sums, ratios, …),
        persisting ``DerivedColumn`` formula metadata. No view, no LLM call.
        """
        run = run_session_phase(self._manager, "correlations", payload.identity, payload.table_ids)
        return self._outcome_or_raise(run, "correlations")

    @activity.defn(name="session_materialize_overlays")
    def run_session_materialize_overlays(self, identity: SessionIdentity) -> PhaseOutcome:
        """Materialize durable relationship overlays into this run (DAT-409).

        Between ``semantic_per_table`` and ``session_detect``: writes the user's
        ``add``/``keep`` relationship teaches as run-stamped ``manual``/``keeper``
        rows so the durable catalog survives every run, then detect measures it.
        """
        count = materialize_session_overlays(self._manager, identity)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"materialized {count} durable relationship(s) for session {identity.session_id}",
        )

    @activity.defn(name="session_detect")
    def run_session_detect(self, identity: SessionIdentity) -> PhaseOutcome:
        """Terminal relationship-detector pass for begin_session (DAT-408).

        Source-free analogue of ``detect``: runs the relationship detectors
        (``SESSION_DETECTOR_PHASES``) over the session's tables, persisting
        relationship-granularity entropy objects + readiness rows stamped with the
        run's ``run_id``.
        """
        count = run_detectors(
            self._manager,
            session_id=identity.session_id,
            run_id=identity.run_id,
            detector_phases=SESSION_DETECTOR_PHASES,
        )
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"{count} relationship detector records for session {identity.session_id}",
        )

    @activity.defn(name="session_write_keepers")
    def run_session_write_keepers(self, identity: SessionIdentity) -> PhaseOutcome:
        """Silent-accept writer (DAT-409 C3) — runs after detect, before promote.

        While the head still names the prior run, lift each promoted ``llm`` the
        current run didn't reproduce (and the user didn't reject) into a ``keep``
        overlay, so it re-materializes as ``keeper`` next run.
        """
        count = write_session_keepers(self._manager, identity)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"wrote {count} silent-accept keeper(s) for session {identity.session_id}",
        )

    @activity.defn(name="session_promote_to_latest")
    def run_session_promote_to_latest(self, identity: SessionIdentity) -> PhaseOutcome:
        """Terminal promote for begin_session — flip the relationship-readiness heads.

        Runs last in ``beginSessionWorkflow``, after ``session_detect``: points each
        ``(relationship:{from}::{to}, "detect")`` head at this ``run_id`` so the
        readiness reader resolves it as current (DAT-408).
        """
        count = promote_session_run(self._manager, identity)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"promoted {count} relationship head(s) for session {identity.session_id}",
        )

    @activity.defn(name="operating_model_resolve")
    def run_operating_model_resolve(self, identity: SessionIdentity) -> OperatingModelScope:
        """Pre-flight for operating_model (DAT-438) — table set + pinned base runs.

        Re-reads the session's ``session_tables`` (the session anchors its
        table set) and resolves the ADR-0008 base-run map ONCE; both travel
        with the workflow's contracts to every downstream activity. Fails loud
        (RuntimeError → activity failure) when the session is unknown or has
        no linked tables.
        """
        return resolve_operating_model_scope(self._manager, identity)

    @activity.defn(name="validation")
    def run_validation(self, payload: OperatingModelScopedInput) -> PhaseOutcome:
        """Validation activity — the lifecycle family: declare → bind → execute.

        Threads the resolved scope's base-run pin into the phase config
        (``ctx.config["base_runs"]``); the phase performs NO head resolution
        itself. Makes real Anthropic calls (SQL generation per declared spec).
        """
        run = run_session_phase(
            self._manager,
            "validation",
            payload.identity,
            payload.scope.table_ids,
            extra_config={
                "base_runs": {
                    "relationship_run_id": payload.scope.relationship_run_id,
                    "semantic_runs": payload.scope.semantic_runs,
                }
            },
        )
        return self._outcome_or_raise(run, "validation")

    @activity.defn(name="business_cycles")
    def run_business_cycles(self, payload: OperatingModelScopedInput) -> PhaseOutcome:
        """Business-cycles activity — the second lifecycle family: declare → bind → execute.

        Mirrors ``run_validation``: threads the resolved scope's base-run pin
        into the phase config (``ctx.config["base_runs"]``); the phase performs
        NO head resolution itself. Makes real Anthropic calls (one cycle
        synthesis call over the declared vocabulary). Runs after ``validation``
        so cycle health can read this run's validation results (DAT-455).
        """
        run = run_session_phase(
            self._manager,
            "business_cycles",
            payload.identity,
            payload.scope.table_ids,
            extra_config={
                "base_runs": {
                    "relationship_run_id": payload.scope.relationship_run_id,
                    "semantic_runs": payload.scope.semantic_runs,
                }
            },
        )
        return self._outcome_or_raise(run, "business_cycles")

    @activity.defn(name="metrics")
    def run_metrics(self, payload: OperatingModelScopedInput) -> PhaseOutcome:
        """Metrics activity — the third lifecycle family: declare → compose → execute.

        Mirrors ``run_validation``/``run_business_cycles``: threads the resolved
        scope's base-run pin into ``ctx.config["base_runs"]`` (the phase does NO
        head resolution itself). ALSO threads ``workspace_id`` — the metrics
        phase keys the SQL snippet base by workspace (source-free,
        workspace-stable), so the cross-run reuse cache shared with the query
        agent survives the source-free cut. Makes real Anthropic calls (per-metric
        SQL composition). Runs after ``business_cycles`` so the graph context can
        read this run's cycle + validation evidence (DAT-456).

        This is the longest-running activity on the spine — up to
        ``_MAX_CONCURRENT_METRICS`` concurrent compositions, ``ceil(N/10)`` LLM
        waves for ``N`` declared metrics. Like the other phase activities it does
        NOT heartbeat; a worker crash mid-run is recovered by the workflow's retry
        policy once the shared ``start_to_close_timeout`` fires.
        """
        run = run_session_phase(
            self._manager,
            "metrics",
            payload.identity,
            payload.scope.table_ids,
            extra_config={
                "base_runs": {
                    "relationship_run_id": payload.scope.relationship_run_id,
                    "semantic_runs": payload.scope.semantic_runs,
                },
                "workspace_id": payload.identity.workspace_id,
            },
        )
        return self._outcome_or_raise(run, "metrics")

    @activity.defn(name="operating_model_promote")
    def run_operating_model_promote(self, identity: SessionIdentity) -> PhaseOutcome:
        """Terminal promote for operating_model — flip the session's stage head.

        Points ``(session:{id}, "operating_model")`` at this ``run_id`` so the
        query tier (cockpit validation surfaces, graphs context) resolves this
        run's lifecycle artifacts + validation results as current.
        """
        count = promote_operating_model_run(self._manager, identity)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"promoted {count} operating_model head(s) for session {identity.session_id}",
        )

    def _run_or_raise(
        self,
        phase_name: str,
        identity: SourceIdentity,
        table_ids: list[str],
    ) -> PhaseOutcome:
        """Run a phase; turn a deterministic phase failure into a non-retryable error.

        A FAILED ``PhaseRun`` means the phase itself decided it cannot proceed
        (bad path, missing config) — permanent, so we raise a non-retryable
        ``ApplicationError`` rather than burning Temporal retries. Transient
        failures (e.g. a DuckLake optimistic-commit conflict) raise out of
        ``run_phase`` as ordinary exceptions and stay retryable by default.
        """
        run = run_phase(self._manager, phase_name, identity, table_ids)
        return self._outcome_or_raise(run, phase_name)

    def _outcome_or_raise(self, run: PhaseRun, phase_name: str) -> PhaseOutcome:
        """Translate a ``PhaseRun`` into a ``PhaseOutcome`` / failure to raise.

        Shared by the add_source (``run_phase``) and begin_session
        (``run_session_phase`` / ``begin_session_select``) activity paths.
        A FAILED run is normally a deterministic, permanent phase failure →
        non-retryable ``PhaseFailed``. The exception is a *transient* provider
        failure (an LLM 429 / 5xx / connection error the provider tagged via
        ``format_api_error``): that is not deterministic, so raise a retryable
        error and let Temporal re-run the whole activity with backoff (the
        ``RetryPolicy``'s durable safety net the SDK's in-process retries can't
        replace). Anything else (completed / skipped) is a normal outcome.
        """
        if run.status == PhaseStatus.FAILED.value:
            message = run.error or f"Phase '{phase_name}' failed"
            if is_transient_error(run.error):
                # Retryable: ``TransientPhaseFailure`` is absent from the
                # workflow ``RetryPolicy``'s ``non_retryable_error_types``.
                raise ApplicationError(message, type="TransientPhaseFailure")
            raise ApplicationError(message, type="PhaseFailed", non_retryable=True)
        return PhaseOutcome(status=run.status, summary=run.summary)
