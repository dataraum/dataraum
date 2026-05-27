"""Phase-runner for the Temporal activity worker (DAT-344, per-table in DAT-370).

One place wires connections to a phase. :func:`run_phase` leases a *scoped*
SQLAlchemy session + DuckDB cursor from the worker's single
:class:`~dataraum.core.connections.ConnectionManager`, builds the
``PhaseContext`` (source identity from the ``Source`` row + the phase's static
config, scoped to ``table_ids``), and runs the sync phase — without a scheduler
or ``PipelineRun``/``PhaseLog`` monitoring rows (Temporal's event history is the
execution log).

Detectors are **not** run here. Per DAT-370 they run once per workflow stage,
not once per phase: :func:`run_table_detectors` runs the table-local detectors
scoped to a single typed table at the tail of ``ProcessTableWorkflow``. The
activity wrappers (:mod:`dataraum.worker.activities`) translate these
Temporal-agnostic helpers into the per-boundary contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.core.config import load_phase_config
from dataraum.core.logging import get_logger
from dataraum.entropy.engine import run_detector_post_step
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
    "raw_table_ids",
    "run_phase",
    "run_table_detectors",
    "typed_table_id_for_raw",
]

# The table-local phases, in dependency order. ``detect_table`` runs the union
# of the detectors these phases declare in pipeline.yaml (today: type_fidelity
# from typing, null_ratio from statistics) — scoped to the one typed table, so
# parallel child workflows never touch each other's detector rows.
#
# This is ``typing`` + ``workflows._ANALYTICS_PHASES`` (typing is here for its
# type_fidelity detector but is scheduled separately as the id-minting step).
# ``tests/unit/worker/test_phase_constants.py`` pins that relationship so a new
# table-local phase can't be added to the workflow without its detectors running.
_TABLE_LOCAL_PHASES = (
    "typing",
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
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


def run_table_detectors(
    manager: ConnectionManager,
    identity: SourceIdentity,
    table_id: str,
) -> int:
    """Run the table-local detectors scoped to one typed table.

    The stage-level replacement for the per-phase detector post-steps (DAT-370):
    runs the union of the detectors the table-local phases declare in
    pipeline.yaml, each scoped to ``table_id`` via ``run_detector_post_step``'s
    ``table_ids`` filter. Scoping the delete-before-insert to the single table is
    what lets parallel child workflows run their detectors without colliding on
    the shared ``(source_id, detector_id)`` rows.
    """
    declarations = load_phase_declarations()
    detector_ids: list[str] = []
    for phase_name in _TABLE_LOCAL_PHASES:
        decl = declarations.get(phase_name)
        if not decl:
            continue
        for detector_id in decl.detectors:
            if detector_id not in detector_ids:
                detector_ids.append(detector_id)

    if not detector_ids:
        return 0

    total = 0
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        for detector_id in detector_ids:
            total += run_detector_post_step(
                session,
                identity.source_id,
                detector_id,
                cursor,
                session_id=identity.session_id,
                table_ids=[table_id],
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
