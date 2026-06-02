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

from dataraum.pipeline.base import PhaseStatus
from dataraum.worker.activity import (
    raw_table_ids,
    run_detectors,
    run_phase,
    run_replay_cleanup,
    typed_table_id_for_raw,
)
from dataraum.worker.contracts import (
    ImportResult,
    PhaseOutcome,
    ProcessTableInput,
    ReplayCleanupInput,
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
        """Import activity — loads the source into ``lake.raw.*``, returns raw ids.

        The discovered raw ids are the parent workflow's fan-out source, so they
        are read authoritatively from the substrate after the phase — correct
        even when import is skipped because the source was already imported.
        """
        self._run_or_raise("import", identity, [])
        return ImportResult(raw_table_ids=raw_table_ids(self._manager, identity.source_id))

    @activity.defn(name="typing")
    def run_typing(self, payload: ProcessTableInput) -> TypingResult:
        """Typing activity — type-resolves one raw table, returns its typed id."""
        self._run_or_raise("typing", payload.identity, [payload.raw_table_id])
        typed_id = typed_table_id_for_raw(
            self._manager, payload.identity.source_id, payload.raw_table_id
        )
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
        """Semantic-per-column activity — the source-level LLM reduce (roles, concepts, terms).

        Runs once over the whole source after the per-table fan-out (its ontology
        induction is source-global). Needs a working ``ANTHROPIC_API_KEY`` + the
        provider/prompt config resolvable from ``dataraum.core.config``; unlike the
        analytics phases it makes real LLM calls.
        """
        return self._run_or_raise("semantic_per_column", identity, [])

    @activity.defn(name="lookup_raw_table_ids")
    def lookup_raw_table_ids(self, identity: SourceIdentity) -> ImportResult:
        """Read the source's raw table ids without re-running import (DAT-343).

        Called by ``addSourceWorkflow`` on a teach replay that starts past
        ``import`` — the parent still needs the raw ids to drive the
        fan-out, and reading them in an activity (so the result lands in
        history) keeps replay deterministic.
        """
        return ImportResult(raw_table_ids=raw_table_ids(self._manager, identity.source_id))

    @activity.defn(name="lookup_typed_table_id")
    def lookup_typed_table_id(self, payload: ProcessTableInput) -> TypingResult:
        """Resolve an existing typed table id without re-running typing (DAT-343).

        Called by ``processTableWorkflow`` on a teach replay that starts past
        ``typing``. Same shape as the ``typing`` activity's result so the
        downstream child workflow code path stays identical.
        """
        typed_id = typed_table_id_for_raw(
            self._manager, payload.identity.source_id, payload.raw_table_id
        )
        if typed_id is None:
            raise ApplicationError(
                f"No typed table for raw table '{payload.raw_table_id}' — replay "
                "started past typing but typing's output is missing. Re-run from "
                "an earlier phase.",
                type="PhaseFailed",
                non_retryable=True,
            )
        return TypingResult(typed_table_id=typed_id)

    @activity.defn(name="replay_cleanup_for_phase")
    def run_replay_cleanup_for_phase(self, payload: ReplayCleanupInput) -> PhaseOutcome:
        """Invoke a phase's ``replay_cleanup`` before the workflow re-runs it (DAT-343).

        The workflow calls this immediately before the first phase activity
        of a teach replay — that's where ``replay.from_phase`` enters the
        chain, and the cleanup wipes the phase's prior outputs so its
        existing ``should_skip`` lets the re-run proceed.
        """
        run_replay_cleanup(
            self._manager,
            payload.identity,
            payload.phase_name,
            payload.table_ids,
        )
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"cleaned up {payload.phase_name} for {len(payload.table_ids)} table(s)",
        )

    @activity.defn(name="detect")
    def run_detect(self, identity: SourceIdentity) -> PhaseOutcome:
        """Terminal detector pass — every wired detector once, source-wide (DAT-394).

        The single stage-level detect step: after the per-table fan-out and the
        ``semantic_per_column`` reduce, run the union of all chain-declared detectors
        over the whole source. Replaces the old per-table ``detect_table`` + parent
        ``detect_source`` split — nothing consumes entropy mid-run, so one terminal
        pass is correct and simpler. (DAT-394 phase 2 persists readiness here too.)
        """
        count = run_detectors(self._manager, identity)
        return PhaseOutcome(
            status=PhaseStatus.COMPLETED.value,
            summary=f"{count} detector records for source {identity.source_id}",
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
        if run.status == PhaseStatus.FAILED.value:
            raise ApplicationError(
                run.error or f"Phase '{phase_name}' failed",
                type="PhaseFailed",
                non_retryable=True,
            )
        return PhaseOutcome(status=run.status, summary=run.summary)
