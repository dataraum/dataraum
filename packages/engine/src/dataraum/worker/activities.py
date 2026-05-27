"""Temporal activity definitions for pipeline phases (DAT-344).

Thin ``@activity.defn`` wrappers over :func:`run_phase_activity`. They hold the
worker's single :class:`ConnectionManager` (set at bootstrap) and name each
activity after its pipeline.yaml phase — so the workflow calls them by that
phase-name string, no shared catalogue.

Activities are **sync** (``def``): Temporal runs them on the worker's
``ThreadPoolExecutor``, the SDK-recommended shape for blocking SQLAlchemy/DuckDB
work. Each ``run_phase_activity`` call leases a fresh Postgres session + a DuckDB
**cursor** on the worker's one shared DuckLake connection. DuckDB serializes
statements per connection, so concurrent activities serialize at the DuckDB
layer rather than running as independent transactions (see
:meth:`ConnectionManager.duckdb_cursor`) — E4a's workflow runs its two
activities sequentially, so this isn't exercised yet; when E4b fans out
concurrent source workflows, revisit (a per-activity ``connect_session`` or a
dedicated activity task queue). The rare DuckLake commit conflict raises and is
absorbed by Temporal's activity retry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from temporalio import activity
from temporalio.exceptions import ApplicationError

from dataraum.pipeline.base import PhaseStatus
from dataraum.worker.activity import (
    PhaseActivityInput,
    PhaseActivityResult,
    run_phase_activity,
)

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager


class PhaseActivities:
    """Phase activities bound to the worker's ConnectionManager.

    Registered as bound methods (``worker = Worker(..., activities=[acts.run_import,
    acts.run_typing])``) so the manager is captured by instance, not a module
    global — no import-time/runtime ordering coupling.
    """

    def __init__(self, manager: ConnectionManager) -> None:
        self._manager = manager

    @activity.defn(name="import")
    def run_import(self, payload: PhaseActivityInput) -> PhaseActivityResult:
        """Import activity — loads the bound source into ``lake.raw.*``."""
        return _run("import", self._manager, payload)

    @activity.defn(name="typing")
    def run_typing(self, payload: PhaseActivityInput) -> PhaseActivityResult:
        """Typing activity — type-resolves raw tables into ``lake.typed.*``."""
        return _run("typing", self._manager, payload)

    @activity.defn(name="statistics")
    def run_statistics(self, payload: PhaseActivityInput) -> PhaseActivityResult:
        """Statistics activity — per-column statistical profiling of typed tables."""
        return _run("statistics", self._manager, payload)

    @activity.defn(name="column_eligibility")
    def run_column_eligibility(self, payload: PhaseActivityInput) -> PhaseActivityResult:
        """Column-eligibility activity — marks which columns downstream phases analyze."""
        return _run("column_eligibility", self._manager, payload)

    @activity.defn(name="statistical_quality")
    def run_statistical_quality(self, payload: PhaseActivityInput) -> PhaseActivityResult:
        """Statistical-quality activity — Benford + outlier detection on numeric columns."""
        return _run("statistical_quality", self._manager, payload)

    @activity.defn(name="temporal")
    def run_temporal(self, payload: PhaseActivityInput) -> PhaseActivityResult:
        """Temporal activity — pattern/trend profiling of date/time columns."""
        return _run("temporal", self._manager, payload)

    @activity.defn(name="semantic_per_column")
    def run_semantic_per_column(self, payload: PhaseActivityInput) -> PhaseActivityResult:
        """Semantic-per-column activity — the first LLM phase (roles, concepts, terms).

        Needs a working ``ANTHROPIC_API_KEY`` in the worker env + the provider /
        prompt config resolvable from ``dataraum.core.config``; unlike the four
        analytics phases above it makes real LLM calls.
        """
        return _run("semantic_per_column", self._manager, payload)


def _run(
    phase_name: str,
    manager: ConnectionManager,
    payload: PhaseActivityInput,
) -> PhaseActivityResult:
    """Run a phase; turn a deterministic phase failure into a non-retryable error.

    A FAILED ``PhaseResult`` means the phase itself decided it cannot proceed
    (bad path, missing config) — permanent, so we raise a non-retryable
    ``ApplicationError`` rather than burning Temporal retries. Transient
    failures (e.g. a DuckLake optimistic-commit conflict) raise out of
    ``run_phase_activity`` as ordinary exceptions and stay retryable by default.
    """
    result = run_phase_activity(manager, phase_name, payload)
    if result.status == PhaseStatus.FAILED.value:
        raise ApplicationError(
            result.error or f"Phase '{phase_name}' failed",
            type="PhaseFailed",
            non_retryable=True,
        )
    return result
