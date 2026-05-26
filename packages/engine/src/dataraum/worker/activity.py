"""Phase-runner for the Temporal activity worker (DAT-344).

One place wires connections to a phase. :func:`run_phase_activity` leases a
*scoped* SQLAlchemy session + DuckDB cursor from the worker's single
:class:`~dataraum.core.connections.ConnectionManager`, reconstructs the
``PhaseContext`` that ``setup_pipeline`` builds today (source identity from the
``Source`` row + the phase's static config), runs the sync phase, then runs its
pipeline.yaml-declared detectors as post-steps — all without the scheduler,
``PipelineRun``/``PhaseLog`` monitoring rows (Temporal's event history is the
execution log), or per-activity connection wiring.

P2 wraps this in ``@activity.defn`` activities (``run_import`` / ``run_typing``)
that read the worker-held manager; the runner itself is Temporal-agnostic so it
can be tested directly against the real substrate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataraum.core.config import load_phase_config
from dataraum.core.logging import get_logger
from dataraum.entropy.engine import run_detector_post_step
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.pipeline.registry import get_phase_class
from dataraum.storage import Source
from dataraum.worker.contracts import PhaseActivityInput, PhaseActivityResult

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager

logger = get_logger(__name__)

__all__ = ["PhaseActivityInput", "PhaseActivityResult", "run_phase_activity"]


def run_phase_activity(
    manager: ConnectionManager,
    phase_name: str,
    payload: PhaseActivityInput,
) -> PhaseActivityResult:
    """Run one pipeline phase + its detectors, leasing connections from ``manager``.

    Args:
        manager: the worker's single workspace-level ConnectionManager
            (Postgres + workspace DuckDB already open via ``open_lake``).
        phase_name: a key in pipeline.yaml / the phase registry (e.g. "import").
        payload: the activity input (IDs + optional table filter).
    """
    phase_cls = get_phase_class(phase_name)
    if phase_cls is None:
        return PhaseActivityResult(
            phase=phase_name,
            status=PhaseStatus.FAILED.value,
            error=f"Unknown phase '{phase_name}' — not in the phase registry.",
        )
    phase = phase_cls()

    # Lease a scoped session + DuckDB cursor for the phase body. session_scope
    # commits on clean exit (so the phase's writes are visible to the detector
    # scope below); duckdb_cursor closes the derived cursor on exit.
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        config = _build_phase_config(session, phase_name, payload)
        ctx = PhaseContext(
            session=session,
            duckdb_conn=cursor,
            source_id=payload.source_id,
            table_ids=list(payload.table_ids),
            config=config,
            session_factory=manager.session_scope,
            manager=manager,
            session_id=payload.session_id,
        )

        skip_reason = phase.should_skip(ctx)
        if skip_reason:
            logger.info("activity.phase_skipped", phase=phase_name, reason=skip_reason)
            return PhaseActivityResult(
                phase=phase_name,
                status=PhaseStatus.SKIPPED.value,
                summary=skip_reason,
            )

        result = phase.run(ctx)

    if result.status == PhaseStatus.COMPLETED:
        _run_detectors(manager, phase_name, payload)

    logger.info(
        "activity.phase_done",
        phase=phase_name,
        status=result.status.value,
        duration=result.duration_seconds,
    )
    return PhaseActivityResult(
        phase=phase_name,
        status=result.status.value,
        summary=result.summary,
        records_processed=result.records_processed,
        records_created=result.records_created,
        outputs=result.outputs or {},
        warnings=result.warnings,
        error=result.error,
    )


def _build_phase_config(
    session: Any,
    phase_name: str,
    payload: PhaseActivityInput,
) -> dict[str, Any]:
    """Reconstruct ``ctx.config`` = phase static config + source-identity runtime config.

    Mirrors the ``phase_config | runtime_config`` merge ``setup_pipeline`` does,
    minus the PipelineRun-only fields (fingerprint, source_path) the worker path
    doesn't carry.
    """
    source = session.get(Source, payload.source_id)
    if source is None:
        # Surface as a config gap the phase will fail on with a clear message,
        # rather than raising an opaque AttributeError here.
        runtime_config: dict[str, Any] = {}
    else:
        runtime_config = {
            "source_id": source.source_id,
            "source_name": source.name,
            "source_type": source.source_type,
            "source_connection_config": source.connection_config or {},
            "source_backend": source.backend,
            "vertical": payload.vertical or "_adhoc",
        }

    config: dict[str, Any] = {}
    config.update(load_phase_config(phase_name))
    config.update(runtime_config)
    return config


def _run_detectors(
    manager: ConnectionManager,
    phase_name: str,
    payload: PhaseActivityInput,
) -> None:
    """Run the phase's pipeline.yaml-declared detectors as post-steps.

    Each detector gets a fresh session + cursor scope (matching the scheduler),
    so it observes the committed phase output.
    """
    declarations = load_phase_declarations()
    decl = declarations.get(phase_name)
    detector_ids = decl.detectors if decl else []
    if not detector_ids:
        return

    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        for detector_id in detector_ids:
            run_detector_post_step(
                session,
                payload.source_id,
                detector_id,
                cursor,
                session_id=payload.session_id,
            )
