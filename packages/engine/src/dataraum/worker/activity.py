"""Phase-runner for the Temporal activity worker (DAT-344, per-table in DAT-370).

One place wires connections to a phase. :func:`run_phase` leases a *scoped*
SQLAlchemy session + DuckDB cursor from the worker's single
:class:`~dataraum.core.connections.ConnectionManager`, builds the
``PhaseContext`` (source identity from the ``Source`` row + the phase's static
config, scoped to ``table_ids``), and runs the sync phase — without a scheduler
or ``PipelineRun``/``PhaseLog`` monitoring rows (Temporal's event history is the
execution log).

Detectors are **not** run here per phase. Per DAT-394 they run once per workflow
in a single terminal step: :func:`run_detectors` runs every wired detector over
the whole source after the fan-out + reduce. The activity wrappers
(:mod:`dataraum.worker.activities`) translate these Temporal-agnostic helpers
into the per-boundary contracts.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.core.config import load_phase_config
from dataraum.core.logging import get_logger
from dataraum.entropy.engine import run_detector_post_step
from dataraum.entropy.readiness import persist_readiness
from dataraum.investigation.queries import tables_for_session
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.pipeline.registry import get_phase_class
from dataraum.server.workspace import get_active_workspace_id
from dataraum.storage import Source, Table
from dataraum.worker.contracts import SourceIdentity

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager

logger = get_logger(__name__)

__all__ = [
    "PhaseRun",
    "declared_detector_ids",
    "raw_table_ids",
    "run_detectors",
    "run_phase",
    "run_replay_cleanup",
    "typed_table_id_for_raw",
]

# The executed-chain phases that declare entropy detectors (DAT-394). The single
# terminal ``detect`` activity runs the union of their detectors ONCE, source-wide
# — after the per-table fan-out and the ``semantic_per_column`` reduce. Detectors
# moved here from the old per-table ``detect_table`` + parent ``detect_source``
# split: nothing reads entropy mid-run, there are no live detector->detector
# ordering edges, and the split bought ~no parallelism (it ran two cheap structural
# detectors), so one terminal pass is correct and simpler. Running once,
# sequentially, makes the source-wide delete-before-insert safe (no concurrency)
# and guarantees every detector's inputs are present.
#
# Source of truth is the executed chain in ``workflows.py`` (``typing`` +
# ``_ANALYTICS_PHASES`` in the child, ``semantic_per_column`` in the parent).
# ``tests/unit/worker/test_phase_constants.py`` pins that no chain-declared
# detector is orphaned (the regression eval caught when DAT-370 first split them).
_DETECTOR_PHASES = (
    "typing",
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
    "semantic_per_column",
)


@dataclass
class PhaseRun:
    """Temporal-agnostic outcome of one phase body (no detector side-effects)."""

    status: str
    summary: str = ""
    error: str | None = None


def run_phase(
    manager: ConnectionManager,
    phase_name: str,
    identity: SourceIdentity,
    table_ids: list[str],
) -> PhaseRun:
    """Run one pipeline phase scoped to ``table_ids``, leasing connections from ``manager``.

    Args:
        manager: the worker's single workspace-level ConnectionManager
            (Postgres + workspace DuckDB already open via ``open_lake``).
        phase_name: a key in pipeline.yaml / the phase registry (e.g. "import").
        identity: the source-identity header carried by the workflow.
        table_ids: the phase's table scope. ``[]`` = source-wide (import,
            semantic_per_column); a single typed id for the analytics phases.
    """
    # Anti-footgun for the deferred multi-workspace isolation (DAT-364): the
    # worker is bound to exactly one workspace, so a payload addressed to a
    # different one must never run — it would silently write into this worker's
    # lake + ws_<id> schema. Fail loud before touching any connection (FAILED →
    # non-retryable PhaseFailed in the activity wrapper). Today workspace_id is
    # decorative; this becomes the routing key when isolation lands.
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=(
                f"Workspace mismatch: payload targets '{identity.workspace_id}' "
                f"but this worker is bound to '{active_workspace_id}'. Refusing "
                "to run to avoid a cross-workspace miswrite (DAT-364)."
            ),
        )

    phase_cls = get_phase_class(phase_name)
    if phase_cls is None:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=f"Unknown phase '{phase_name}' — not in the phase registry.",
        )
    phase = phase_cls()

    # Lease a scoped session + DuckDB cursor for the phase body. session_scope
    # commits on clean exit (so the phase's writes are visible to later
    # activities); duckdb_cursor closes the derived cursor on exit.
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        source = session.get(Source, identity.source_id)
        if source is None:
            return PhaseRun(
                status=PhaseStatus.FAILED.value,
                error=(
                    f"Source '{identity.source_id}' not found in workspace "
                    f"'{identity.workspace_id}'. The workflow caller must write the "
                    "Source row before the phase runs."
                ),
            )
        config = _build_phase_config(source, phase_name, identity)
        ctx = PhaseContext(
            session=session,
            duckdb_conn=cursor,
            source_id=identity.source_id,
            table_ids=list(table_ids),
            config=config,
            session_factory=manager.session_scope,
            manager=manager,
            session_id=identity.session_id,
        )

        skip_reason = phase.should_skip(ctx)
        if skip_reason:
            logger.info("activity.phase_skipped", phase=phase_name, reason=skip_reason)
            return PhaseRun(status=PhaseStatus.SKIPPED.value, summary=skip_reason)

        result = phase.run(ctx)

    logger.info(
        "activity.phase_done",
        phase=phase_name,
        status=result.status.value,
        duration=result.duration_seconds,
    )
    return PhaseRun(
        status=result.status.value,
        summary=result.summary,
        error=result.error,
    )


def raw_table_ids(manager: ConnectionManager, source_id: str) -> list[str]:
    """The source's raw table ids — the fan-out source the parent needs.

    Read after the ``import`` activity (run or skipped), so the parent always
    has the authoritative set regardless of whether import did fresh work.
    """
    with manager.session_scope() as session:
        rows = session.execute(
            select(Table.table_id).where(Table.source_id == source_id, Table.layer == "raw")
        )
        return [row[0] for row in rows]


def typed_table_id_for_raw(
    manager: ConnectionManager,
    source_id: str,
    raw_table_id: str,
) -> str | None:
    """Resolve the typed table id ``typing`` produced for one raw table.

    Typing creates the typed table under the same ``table_name`` as its raw
    input, so the mapping is by name. Returns the persisted id whether typing
    just minted it or it already existed (skip path) — ``None`` if neither the
    raw row nor its typed table is present.
    """
    with manager.session_scope() as session:
        raw = session.get(Table, raw_table_id)
        if raw is None:
            return None
        return session.execute(
            select(Table.table_id).where(
                Table.source_id == source_id,
                Table.table_name == raw.table_name,
                Table.layer == "typed",
            )
        ).scalar_one_or_none()


def declared_detector_ids(phase_names: Iterable[str]) -> list[str]:
    """The de-duplicated detectors the given phases declare in pipeline.yaml."""
    declarations = load_phase_declarations()
    detector_ids: list[str] = []
    for phase_name in phase_names:
        decl = declarations.get(phase_name)
        if not decl:
            continue
        for detector_id in decl.detectors:
            if detector_id not in detector_ids:
                detector_ids.append(detector_id)
    return detector_ids


def run_replay_cleanup(
    manager: ConnectionManager,
    identity: SourceIdentity,
    phase_name: str,
    table_ids: list[str],
) -> None:
    """Invoke ``phase.replay_cleanup`` for ``phase_name`` (DAT-343).

    The activity-side counterpart to ``BasePhase.replay_cleanup``: leases a
    scoped session + DuckDB cursor, builds the minimal ``PhaseContext`` the
    cleanup needs (source_id + connections; no per-phase static config —
    cleanup deletes rows, doesn't read config), and calls the phase's
    cleanup method. Commits via ``session_scope`` on clean exit.

    Workspace-mismatch guard mirrors :func:`run_phase` so a payload
    addressed to another workspace can't accidentally clean up this one.
    """
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        raise RuntimeError(
            f"Workspace mismatch: replay_cleanup payload targets "
            f"'{identity.workspace_id}' but this worker is bound to "
            f"'{active_workspace_id}'."
        )

    phase_cls = get_phase_class(phase_name)
    if phase_cls is None:
        raise RuntimeError(f"Unknown phase '{phase_name}' — not in the phase registry.")
    phase = phase_cls()

    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        ctx = PhaseContext(
            session=session,
            duckdb_conn=cursor,
            source_id=identity.source_id,
            table_ids=list(table_ids),
            config={},
            session_factory=manager.session_scope,
            manager=manager,
            session_id=identity.session_id,
        )
        phase.replay_cleanup(ctx, list(table_ids))

    logger.info(
        "activity.replay_cleanup",
        phase=phase_name,
        table_ids=table_ids,
        source_id=identity.source_id,
    )


def run_detectors(manager: ConnectionManager, identity: SourceIdentity) -> int:
    """Run every wired detector once over the run-session's tables — the terminal ``detect`` step.

    The single stage-level detector pass (DAT-394): runs the union of the detectors
    the executed chain phases declare (``_DETECTOR_PHASES``) over the tables the run's
    session composes (``session_tables``, DAT-407/410) — for ``add_source`` that is
    exactly the source's freshly-typed tables, so the scope is identical to the prior
    source-wide pass. It runs once, sequentially, after the per-table fan-out and the
    ``semantic_per_column`` reduce — so the delete-before-insert is safe (no
    concurrency) and every detector's inputs are present.
    """
    detector_ids = declared_detector_ids(_DETECTOR_PHASES)
    if not detector_ids:
        return 0

    total = 0
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        table_ids = tables_for_session(session, identity.session_id)
        if not table_ids:
            # add_source links its typed tables in ``typing`` (same transaction as
            # the Table row), so an empty set here means the session has no tables —
            # nothing to detect. Log it: a populated source with no links is a bug.
            logger.warning(
                "detect_no_session_tables",
                source_id=identity.source_id,
                session_id=identity.session_id,
            )
            return 0
        for detector_id in detector_ids:
            # Scoped to the session's tables. The single terminal pass runs once,
            # sequentially after the fan-out — no concurrent writers to collide on
            # the per-(detector, table) delete-before-insert.
            total += run_detector_post_step(
                session,
                identity.source_id,
                detector_id,
                cursor,
                session_id=identity.session_id,
                table_ids=table_ids,
            )
        # Persist readiness from the freshly-written entropy objects, in the same
        # transaction (DAT-394). flush() makes the just-added rows visible to the
        # rollup's repository select before we read them back.
        session.flush()
        readiness_rows = persist_readiness(session, identity.session_id, table_ids)
        logger.info(
            "terminal_detect_done",
            source_id=identity.source_id,
            detector_records=total,
            readiness_rows=readiness_rows,
        )
    return total


def _build_phase_config(
    source: Source,
    phase_name: str,
    identity: SourceIdentity,
) -> dict[str, Any]:
    """Reconstruct ``ctx.config`` = phase static config + source-identity runtime config.

    Mirrors the ``phase_config | runtime_config`` merge ``setup_pipeline`` did,
    minus the PipelineRun-only fields (fingerprint, source_path) the worker path
    doesn't carry. The caller (:func:`run_phase`) guarantees ``source`` exists.
    """
    runtime_config: dict[str, Any] = {
        "source_id": source.source_id,
        "source_name": source.name,
        "source_type": source.source_type,
        "source_connection_config": source.connection_config or {},
        "source_backend": source.backend,
        "vertical": identity.vertical or "_adhoc",
    }

    config: dict[str, Any] = {}
    config.update(load_phase_config(phase_name))
    config.update(runtime_config)
    return config
