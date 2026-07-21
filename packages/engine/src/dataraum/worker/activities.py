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

import contextvars
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from dataraum.llm.providers.base import ProviderError, TransientProviderError
from dataraum.pipeline.base import PhaseStatus
from dataraum.worker.activity import (
    OPERATING_MODEL_DETECTOR_PHASES,
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
    ImportInput,
    ImportResult,
    OperatingModelScope,
    OperatingModelScopedInput,
    PhaseOutcome,
    ProcessTableInput,
    RunPhaseInput,
    RunRef,
    RunScopedInput,
    SessionScopedInput,
    TableScopedInput,
    TypingResult,
)

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager


# Heartbeat cadence for the long-running ``metrics`` activity (DAT-503). Well
# under the call's ``heartbeat_timeout`` (60s in workflows.py) so a missed pulse
# is unambiguous worker death, not a slow LLM wave.
_HEARTBEAT_INTERVAL_SECONDS = 15.0


@contextmanager
def _heartbeat_pulse(interval: float = _HEARTBEAT_INTERVAL_SECONDS) -> Iterator[None]:
    """Pulse ``activity.heartbeat()`` from a daemon thread while a sync body runs.

    The phase body is a single blocking call (no per-wave hook), so the pulse
    can't ride the work itself — a background thread emits a heartbeat every
    ``interval`` seconds until the body returns or raises, then stops. Lets the
    activity declare a short ``heartbeat_timeout`` for fast worker-death
    detection without the phase having to thread a progress callback. The pulse
    is a no-op outside an activity context (unit tests), so it stays test-safe.

    The daemon thread runs inside a COPY of the caller's context
    (:func:`contextvars.copy_context`): ``activity.heartbeat()`` resolves the
    activity through a ``ContextVar`` (``temporalio.activity._current_context``),
    and a bare ``threading.Thread`` starts with an EMPTY context — so without the
    copy every ``heartbeat()`` raised ``RuntimeError("Not in activity context")``,
    the loop hit its ``except`` and exited at the first beat, and the pulse never
    fired. The activity then survived only by finishing within its
    ``heartbeat_timeout``; a longer run (e.g. the DAT-629 warming pre-pass) blew
    past it, got cancelled, and leaked the sync worker's pooled connections.
    """
    stop = threading.Event()

    def _beat() -> None:
        while not stop.wait(interval):
            try:
                activity.heartbeat()
            except RuntimeError:
                # Not inside an activity execution context (e.g. a unit test) —
                # nothing to heartbeat against; stop quietly.
                return

    # Propagate the activity context (the heartbeat ContextVar) into the daemon.
    ctx = contextvars.copy_context()
    thread = threading.Thread(target=lambda: ctx.run(_beat), name="metrics-heartbeat", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=interval)


def _provider_app_error(exc: ProviderError) -> ApplicationError:
    """Translate a typed provider failure into the right Temporal error (DAT-503).

    A :class:`TransientProviderError` (rate limit, 5xx, timeout, connection)
    becomes the retryable ``TransientPhaseFailure`` — absent from the LLM retry
    policy's ``non_retryable_error_types``, so Temporal re-runs the whole
    activity with backoff. Any other provider failure (auth, bad request,
    schema, unexpected) is permanent → the non-retryable ``PhaseFailed``. The
    message is the provider's own — preserved verbatim for the cockpit's
    failure surface.
    """
    message = str(exc)
    if isinstance(exc, TransientProviderError):
        return ApplicationError(message, type="TransientPhaseFailure")
    return ApplicationError(message, type="PhaseFailed", non_retryable=True)


def _is_transient_commit_conflict(error: str | None) -> bool:
    """True for a DuckLake optimistic-concurrency commit conflict — transient.

    add_source fans out one ``ProcessTableWorkflow`` per table concurrently; their
    per-phase commits race on the single shared DuckLake catalog, and the losing
    commit raises ``TransactionException: … Transaction conflict``. It is NOT a
    deterministic phase failure — the phases are idempotent (upsert on
    ``(column_id, run_id)``), so a retry against the now-committed rows succeeds.
    """
    if not error:
        return False
    e = error.lower()
    return "transaction conflict" in e or "failed to commit ducklake transaction" in e


class PhaseActivities:
    """Phase activities bound to the worker's ConnectionManager.

    Registered as bound methods (``Worker(..., activities=[acts.run_import, …])``)
    so the manager is captured by instance, not a module global — no
    import-time/runtime ordering coupling.
    """

    def __init__(self, manager: ConnectionManager) -> None:
        self._manager = manager

    @activity.defn(name="import")
    def run_import(self, payload: ImportInput) -> ImportResult:
        """Import activity — loads ONE source into ``lake.raw.*``, returns its raw ids.

        ``import`` is the one per-source activity (DAT-422/426): the parent runs it
        once per source in the run's set, each call carrying that source's explicit
        ``source_id`` — the ONLY source id on the wire (import runs before any
        ``Table`` row exists, so it can't resolve relationally). The discovered raw
        ids are the parent workflow's fan-out source, read authoritatively from the
        substrate after the phase — correct even when import is skipped because the
        source was already imported.
        """
        try:
            run = run_phase(
                self._manager,
                "import",
                payload.run,
                [],
                payload.vertical,
                source_id=payload.source_id,
            )
        except ProviderError as exc:
            raise _provider_app_error(exc) from exc
        self._outcome_or_raise(run, "import")
        return ImportResult(raw_table_ids=raw_table_ids(self._manager, payload.source_id))

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
        run = check_run_column_limit(self._manager, payload.run, payload.table_ids)
        return self._outcome_or_raise(run, "check_column_limit")

    @activity.defn(name="typing")
    def run_typing(self, payload: ProcessTableInput) -> TypingResult:
        """Typing activity — type-resolves one raw table, returns its typed id."""
        self._run_or_raise("typing", payload.run, [payload.raw_table_id], payload.vertical)
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
        return self._run_or_raise("statistics", payload.run, [payload.table_id], payload.vertical)

    @activity.defn(name="column_eligibility")
    def run_column_eligibility(self, payload: TableScopedInput) -> PhaseOutcome:
        """Column-eligibility activity — marks which columns downstream phases analyze."""
        return self._run_or_raise(
            "column_eligibility", payload.run, [payload.table_id], payload.vertical
        )

    @activity.defn(name="statistical_quality")
    def run_statistical_quality(self, payload: TableScopedInput) -> PhaseOutcome:
        """Statistical-quality activity — Benford + outlier detection on numeric columns."""
        return self._run_or_raise(
            "statistical_quality", payload.run, [payload.table_id], payload.vertical
        )

    @activity.defn(name="temporal")
    def run_temporal(self, payload: TableScopedInput) -> PhaseOutcome:
        """Temporal activity — pattern/trend profiling of date/time columns."""
        return self._run_or_raise("temporal", payload.run, [payload.table_id], payload.vertical)

    @activity.defn(name="semantic_per_column")
    def run_semantic_per_column(self, payload: RunPhaseInput) -> PhaseOutcome:
        """Semantic-per-column activity — the run-scoped LLM reduce (roles, concepts, terms).

        Runs once after the per-table fan-out over the run's tables
        (``tables_for_run``, DAT-506) — not "the whole source" — so a run whose
        tables span multiple per-object sources is grounded as one set. Grounding
        only (induction left the engine, DAT-382). Needs a working
        ``ANTHROPIC_API_KEY`` + the provider/prompt config resolvable from
        ``dataraum.core.config``; unlike the analytics phases it makes real LLM calls.
        """
        return self._run_or_raise("semantic_per_column", payload.run, [], payload.vertical)

    @activity.defn(name="detect")
    def run_detect(self, run: RunRef) -> PhaseOutcome:
        """Terminal detector pass — every wired detector once, run-wide (DAT-394).

        The single stage-level detect step: after the per-table fan-out and the
        ``semantic_per_column`` reduce, run the union of all chain-declared detectors
        over the run's tables. Replaces the old per-table ``detect_table`` + parent
        ``detect_source`` split — nothing consumes entropy mid-run, so one terminal
        pass is correct and simpler. (DAT-394 phase 2 persists readiness here too.)
        """
        if run.run_id is None:
            raise ApplicationError(
                "detect requires a stamped run.run_id.",
                type="PhaseFailed",
                non_retryable=True,
            )
        count = run_detectors(self._manager, run_id=run.run_id)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"{count} detector records for run {run.run_id}",
        )

    @activity.defn(name="promote_to_latest")
    def run_promote_to_latest(self, run: RunRef) -> PhaseOutcome:
        """Terminal promote step — flip the snapshot head to this run (DAT-413).

        Runs last in ``addSourceWorkflow``, after ``detect``: upserts
        :class:`MetadataSnapshotHead` for each of the run's tables × add_source
        stage so the head names this ``run_id`` as current. Behavior-preserving
        in Phase 2 — nothing reads the head yet (one run at a time), so promoting
        it cannot change downstream output; Phase 3 switches readers to it.
        """
        count = promote_run(self._manager, run)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"promoted {count} snapshot head(s) for run {run.run_id}",
        )

    # --- begin_session activities (DAT-401) — source-free, session-scoped ----

    @activity.defn(name="begin_session_select")
    def run_begin_session_select(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Pre-flight the selection + link it to the run (the spine's first step).

        Validates every id is a known typed table (reject unknown → non-retryable)
        and writes the ``run_tables`` links via the idempotent merge ``typing``
        uses for add_source. The session row itself is seeded by the caller.
        """
        run = begin_session_select(self._manager, payload.run, payload.table_ids)
        return self._outcome_or_raise(
            run, "begin_session_select"
        )  # vertical unused (no LLM config)

    @activity.defn(name="relationships")
    def run_relationships(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Relationships activity — structural cross-table candidate detection.

        Source-free: scopes to the session's selected typed tables (which may
        span sources), persisting ``detection_method='candidate'`` rows.
        """
        return self._run_session_or_raise(
            "relationships", payload.run, payload.table_ids, payload.vertical
        )

    @activity.defn(name="semantic_per_table")
    def run_semantic_per_table(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Semantic-per-table activity — LLM table classification + relationship confirm.

        Reasons over the per-column annotations to classify tables and confirm a
        subset of the structural candidates (``detection_method='llm'``). Makes
        real Anthropic calls; needs a working ``ANTHROPIC_API_KEY`` + the session's
        ``vertical``.
        """
        return self._run_session_or_raise(
            "semantic_per_table", payload.run, payload.table_ids, payload.vertical
        )

    @activity.defn(name="aggregation_lineage")
    def run_aggregation_lineage(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Aggregation-lineage activity — events→measure rollup discovery (DAT-491/536).

        Deterministic inline aggregation (one ``GROUP BY dim, period`` over each
        fact's enriched view, paired across facts by their shared catalog slice
        dimensions) — NO LLM call. Reconciled lineage persists run-versioned,
        feeding the ``structural_reconciliation`` witness at the terminal
        ``session_detect``.
        """
        return self._run_session_or_raise(
            "aggregation_lineage", payload.run, payload.table_ids, payload.vertical
        )

    @activity.defn(name="surrogate_mint")
    def run_surrogate_mint(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Surrogate-mint activity — cure confirmed composite keys at the source (DAT-277).

        Source-free: reads this run's ``surrogate_key_intents`` (written by
        ``semantic_per_table``), re-materializes both typed tables with the
        deterministic NULL-propagating hash column on the DAT-414 recipe
        substrate, and persists ONE single-column relationship per composite on
        the surrogate pair. Deterministic, no LLM call; a run with no confirmed
        composites is a fast no-op.
        """
        return self._run_session_or_raise(
            "surrogate_mint", payload.run, payload.table_ids, payload.vertical
        )

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
        return self._run_session_or_raise(
            "enriched_views", payload.run, payload.table_ids, payload.vertical
        )

    # --- value layer (DAT-403) — source-free, session-scoped, after enriched_views ---

    @activity.defn(name="slicing")
    def run_slicing(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Slicing activity — LLM-recommended slice dimensions per session fact table.

        Source-free: scopes to the session's selected typed tables, persisting
        ``SliceDefinition`` rows for the fact tables that carry an enriched view.
        Makes real Anthropic calls (the slicing agent); needs ``ANTHROPIC_API_KEY``.
        """
        return self._run_session_or_raise(
            "slicing", payload.run, payload.table_ids, payload.vertical
        )

    @activity.defn(name="dimension_hierarchies")
    def run_dimension_hierarchies(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Dimension-hierarchies activity — g3 FD / drill-down / alias discovery (DAT-537).

        Deterministic g3 functional-dependency pass over each fact's grain-verified
        enriched view across the catalog's grain-safe slice dimensions — NO LLM call.
        Drill-down chains + 1:1 aliases persist run-versioned (also folding the user's
        durable hierarchy/alias teaches into this run); consumed by the answer agent
        (DAT-538) and the driver tree (DAT-545).
        """
        return self._run_session_or_raise(
            "dimension_hierarchies", payload.run, payload.table_ids, payload.vertical
        )

    @activity.defn(name="correlations")
    def run_correlations(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Correlations activity — detect derived columns over the enriched views.

        Finds same-table and cross-table derived columns (sums, ratios, …),
        persisting ``DerivedColumn`` formula metadata. No view, no LLM call.
        """
        return self._run_session_or_raise(
            "correlations", payload.run, payload.table_ids, payload.vertical
        )

    @activity.defn(name="driver_rankings")
    def run_driver_rankings(self, payload: SessionScopedInput) -> PhaseOutcome:
        """Driver-rankings activity — persist per-measure driver discovery (DAT-546).

        Runs the validated driver-discovery engine over each measure-role fact
        column's enriched view and persists the grain-labeled ranking run-versioned
        (one row per ``(measure_column_id, run_id)``). Deterministic, NO LLM call.
        Read by the answer agent via ``look_drivers``.
        """
        return self._run_session_or_raise(
            "driver_rankings", payload.run, payload.table_ids, payload.vertical
        )

    @activity.defn(name="session_materialize_overlays")
    def run_session_materialize_overlays(self, run: RunRef) -> PhaseOutcome:
        """Materialize durable relationship overlays into this run (DAT-409).

        Between ``semantic_per_table`` and ``session_detect``: writes the user's
        ``add``/``keep`` relationship teaches as run-stamped ``manual``/``keeper``
        rows so the durable catalog survives every run, then detect measures it.
        """
        count = materialize_session_overlays(self._manager, run)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"materialized {count} durable relationship(s) for run {run.run_id}",
        )

    @activity.defn(name="session_detect")
    def run_session_detect(self, run: RunRef) -> PhaseOutcome:
        """Terminal relationship-detector pass for begin_session (DAT-408).

        Source-free analogue of ``detect``: runs the relationship detectors
        (``SESSION_DETECTOR_PHASES``) over the run's tables, persisting
        relationship-granularity entropy objects + readiness rows stamped with the
        run's ``run_id``.
        """
        if run.run_id is None:
            raise ApplicationError(
                "session_detect requires a stamped run.run_id.",
                type="PhaseFailed",
                non_retryable=True,
            )
        count = run_detectors(
            self._manager,
            run_id=run.run_id,
            detector_phases=SESSION_DETECTOR_PHASES,
        )
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"{count} relationship detector records for run {run.run_id}",
        )

    @activity.defn(name="session_write_keepers")
    def run_session_write_keepers(self, run: RunRef) -> PhaseOutcome:
        """Silent-accept writer (DAT-409) — runs after detect, before promote.

        While the head still names the prior run, lift each promoted ``llm`` the
        current run didn't reproduce (and the user didn't reject) into a ``keep``
        overlay, so it re-materializes as ``keeper`` next run.
        """
        count = write_session_keepers(self._manager, run)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"wrote {count} silent-accept keeper(s) for run {run.run_id}",
        )

    @activity.defn(name="session_promote_to_latest")
    def run_session_promote_to_latest(self, run: RunRef) -> PhaseOutcome:
        """Terminal promote for begin_session — flip the workspace catalog head.

        Runs last in ``beginSessionWorkflow``, after ``session_detect``: points the
        single ``(catalog, "catalog")`` head at this ``run_id`` so the readiness
        readers resolve this run's relationship catalog as current (DAT-506).
        """
        count = promote_session_run(self._manager, run)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"promoted {count} relationship head(s) for run {run.run_id}",
        )

    @activity.defn(name="operating_model_resolve")
    def run_operating_model_resolve(self, payload: RunPhaseInput) -> OperatingModelScope:
        """Pre-flight for operating_model (DAT-438/506) — pinned base runs.

        Validates the workspace ``vertical`` born-loud and resolves the ADR-0008
        base-run map ONCE off the catalog head's run; it travels with the
        workflow's contracts to every downstream activity. operating_model takes
        NO table set — the phases read the catalog head's ``run_tables`` directly.
        Fails loud (ApplicationError) when the vertical is unknown or no
        begin_session catalog run is promoted.
        """
        return resolve_operating_model_scope(self._manager, payload.run, payload.vertical)

    @activity.defn(name="validation")
    def run_validation(self, payload: OperatingModelScopedInput) -> PhaseOutcome:
        """Validation activity — the lifecycle family: declare → bind → execute.

        Threads the resolved scope's base-run pin into the phase config
        (``ctx.config["base_runs"]``); the phase performs NO head resolution
        itself. Makes real Anthropic calls (SQL generation per declared spec).
        """
        return self._run_session_or_raise(
            "validation",
            payload.run,
            payload.scope.table_ids,
            payload.vertical,
            extra_config={
                "base_runs": {
                    "relationship_run_id": payload.scope.relationship_run_id,
                    "semantic_runs": payload.scope.semantic_runs,
                }
            },
        )

    @activity.defn(name="business_cycles")
    def run_business_cycles(self, payload: OperatingModelScopedInput) -> PhaseOutcome:
        """Business-cycles activity — the second lifecycle family: declare → bind → execute.

        Mirrors ``run_validation``: threads the resolved scope's base-run pin
        into the phase config (``ctx.config["base_runs"]``); the phase performs
        NO head resolution itself. Makes real Anthropic calls (one cycle
        synthesis call over the declared vocabulary). Runs after ``validation``
        so cycle health can read this run's validation results (DAT-455).
        """
        return self._run_session_or_raise(
            "business_cycles",
            payload.run,
            payload.scope.table_ids,
            payload.vertical,
            extra_config={
                "base_runs": {
                    "relationship_run_id": payload.scope.relationship_run_id,
                    "semantic_runs": payload.scope.semantic_runs,
                }
            },
        )

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
        waves for ``N`` declared metrics. It HEARTBEATS (DAT-503): a background
        pulser emits ``activity.heartbeat()`` while the synchronous phase runs,
        so a worker that dies mid-run is detected at the call's
        ``heartbeat_timeout`` (seconds) instead of only at the much longer
        ``start_to_close_timeout`` — the run fails over to a retry far sooner.
        """
        with _heartbeat_pulse():
            return self._run_session_or_raise(
                "metrics",
                payload.run,
                payload.scope.table_ids,
                payload.vertical,
                extra_config={
                    "base_runs": {
                        "relationship_run_id": payload.scope.relationship_run_id,
                        "semantic_runs": payload.scope.semantic_runs,
                    },
                    "workspace_id": payload.run.workspace_id,
                },
            )

    @activity.defn(name="operating_model_detect")
    def run_operating_model_detect(self, payload: OperatingModelScopedInput) -> PhaseOutcome:
        """Terminal detector pass for operating_model (DAT-432).

        Scores this run's executed validation results — cross_table_consistency,
        declared on the ``validation`` phase — into table + column entropy
        objects and persists readiness under the OM run. Pure scoring over
        persisted rows, zero LLM calls. Runs right after ``validation`` so the
        expensive evidence is computed before the LLM-heavy families can fail —
        but like every run-stamped row, the bands become VISIBLE to
        head-resolved readers only after the terminal promote flips the
        ``operating_model`` head (failed runs never surface; review wave-1
        corrected an overclaim here).
        """
        run = payload.run
        if run.run_id is None:
            raise ApplicationError(
                "operating_model_detect requires a stamped run.run_id.",
                type="PhaseFailed",
                non_retryable=True,
            )
        # The OM run never anchors ``run_tables`` (begin_session owns them), so
        # detect must score over the table set PINNED at operating_model_resolve
        # (ADR-0008: payload.scope.table_ids) — the same pin validation / cycles /
        # metrics read. Without it, ``tables_for_run(om_run)`` is empty and the
        # whole pass no-ops (``detect_no_run_tables``), so cross_table_consistency
        # silently scores nothing.
        count = run_detectors(
            self._manager,
            run_id=run.run_id,
            detector_phases=OPERATING_MODEL_DETECTOR_PHASES,
            table_ids=payload.scope.table_ids,
        )
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"{count} validation detector records for run {run.run_id}",
        )

    @activity.defn(name="operating_model_promote")
    def run_operating_model_promote(self, run: RunRef) -> PhaseOutcome:
        """Terminal promote for operating_model — flip the catalog's stage head.

        Points ``(catalog, "operating_model")`` at this ``run_id`` so the query
        tier (cockpit validation surfaces, graphs context) resolves this run's
        lifecycle artifacts + validation results as current.
        """
        count = promote_operating_model_run(self._manager, run)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"promoted {count} operating_model head(s) for run {run.run_id}",
        )

    def _run_or_raise(
        self,
        phase_name: str,
        run_ref: RunRef,
        table_ids: list[str],
        vertical: str,
    ) -> PhaseOutcome:
        """Run an add_source phase; classify its failure for Temporal retry.

        A FAILED ``PhaseRun`` means the phase itself decided it cannot proceed
        (bad path, missing config) — deterministic, so we raise a non-retryable
        ``PhaseFailed`` rather than burning Temporal retries. A transient
        provider failure (an LLM 429 / 5xx / connection error) raises a typed
        :class:`ProviderError` out of the phase body; we translate it to the
        retryable ``TransientPhaseFailure`` here (DAT-503). Infrastructure
        failures (e.g. a DuckLake optimistic-commit conflict) raise ordinary
        exceptions and stay retryable by default.
        """
        try:
            run = run_phase(self._manager, phase_name, run_ref, table_ids, vertical)
        except ProviderError as exc:
            raise _provider_app_error(exc) from exc
        return self._outcome_or_raise(run, phase_name)

    def _run_session_or_raise(
        self,
        phase_name: str,
        run_ref: RunRef,
        table_ids: list[str],
        vertical: str,
        extra_config: dict[str, Any] | None = None,
    ) -> PhaseOutcome:
        """Run a begin_session / operating_model phase; classify failure for retry.

        Session-scoped sibling of :meth:`_run_or_raise`: a transient
        :class:`ProviderError` raised out of the phase body becomes the
        retryable ``TransientPhaseFailure``; a deterministic FAILED ``PhaseRun``
        becomes the non-retryable ``PhaseFailed`` (DAT-503).
        """
        try:
            run = run_session_phase(
                self._manager, phase_name, run_ref, table_ids, vertical, extra_config=extra_config
            )
        except ProviderError as exc:
            raise _provider_app_error(exc) from exc
        return self._outcome_or_raise(run, phase_name)

    def _outcome_or_raise(self, run: PhaseRun, phase_name: str) -> PhaseOutcome:
        """Translate a ``PhaseRun`` into a ``PhaseOutcome`` / failure to raise.

        Shared by the add_source (``run_phase``) and begin_session
        (``run_session_phase`` / ``begin_session_select``) activity paths.
        A FAILED run is a deterministic, permanent phase failure →
        non-retryable ``PhaseFailed`` (a transient provider failure never
        reaches here — it raises a typed :class:`ProviderError` out of the
        phase body, which :meth:`_run_or_raise` / :meth:`_run_session_or_raise`
        translate). Anything else (completed / skipped) is a normal outcome.
        """
        if run.status == PhaseStatus.FAILED.value:
            message = run.error or f"Phase '{phase_name}' failed"
            # A DuckLake commit CONFLICT is the one transient FAILED outcome: the
            # concurrent add_source fan-out raced on the shared catalog. The phase is
            # idempotent, so raise the retryable TransientPhaseFailure (absent from the
            # activity RetryPolicy's non_retryable_error_types) instead of the permanent
            # non-retryable PhaseFailed — Temporal re-runs it with backoff and wins.
            if _is_transient_commit_conflict(run.error):
                raise ApplicationError(message, type="TransientPhaseFailure")
            raise ApplicationError(message, type="PhaseFailed", non_retryable=True)
        return PhaseOutcome(status=run.status, summary=run.summary, declared=run.declared)
