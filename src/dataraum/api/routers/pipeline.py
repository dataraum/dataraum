"""Pipeline control endpoints with SSE progress streaming."""

import asyncio
import threading
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import select, update

from dataraum.api.deps import SessionDep
from dataraum.api.schemas import (
    PhaseStatus,
    PipelineProgressEvent,
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineStatusResponse,
)
from dataraum.core.connections import get_connection_manager
from dataraum.pipeline.base import PIPELINE_DAG
from dataraum.pipeline.db_models import PhaseCheckpoint, PipelineRun
from dataraum.pipeline.orchestrator import PipelineConfig, run_pipeline
from dataraum.pipeline.status import get_pipeline_status
from dataraum.storage import Source

router = APIRouter()

# Shared executor for pipeline runs (single worker = one at a time)
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pipeline")

# In-memory lock for pipeline execution
_pipeline_lock = threading.Lock()
_current_run_id: str | None = None


def is_pipeline_running() -> tuple[bool, str | None]:
    """Check if a pipeline is currently running.

    Returns:
        Tuple of (is_running, run_id if running)
    """
    global _current_run_id

    # Check in-memory state first (fast path)
    if _current_run_id is not None:
        return True, _current_run_id

    # Check database for any running pipelines
    try:
        manager = get_connection_manager()
        with manager.session_scope() as session:
            stmt = select(PipelineRun).where(PipelineRun.status == "running").limit(1)
            result = session.execute(stmt)
            running = result.scalar_one_or_none()
            if running:
                return True, running.run_id
    except RuntimeError:
        pass  # Manager not initialized yet

    return False, None


def mark_interrupted_runs() -> int:
    """Mark any 'running' pipeline runs as 'interrupted'.

    Called on startup to clean up stale runs from previous process.

    Returns:
        Number of runs marked as interrupted.
    """
    try:
        manager = get_connection_manager()
        with manager.session_scope() as session:
            # Find all running pipelines
            stmt = select(PipelineRun).where(PipelineRun.status == "running")
            result = session.execute(stmt)
            running_runs = list(result.scalars().all())

            if not running_runs:
                return 0

            # Mark them as interrupted
            run_ids = [r.run_id for r in running_runs]
            update_stmt = (
                update(PipelineRun)
                .where(PipelineRun.run_id.in_(run_ids))
                .values(status="interrupted")
            )
            session.execute(update_stmt)

            return len(run_ids)
    except RuntimeError:
        return 0  # Manager not initialized


@router.get("/sources/{source_id}/status", response_model=PipelineStatusResponse)
def get_status(
    source_id: str,
    session: SessionDep,
) -> PipelineStatusResponse:
    """Get pipeline status for a source."""
    # Verify source exists
    stmt = select(Source).where(Source.source_id == source_id)
    result = session.execute(stmt)
    source = result.scalar_one_or_none()

    if source is None:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")

    # Get status
    status = get_pipeline_status(session, source_id)

    return PipelineStatusResponse(
        source_id=status.source_id,
        last_run_id=status.last_run_id,
        last_run_status=status.last_run_status,
        last_run_at=status.last_run_at,
        completed=status.completed_count,
        total=status.total_count,
        progress_percent=status.progress_percent,
        phases=[
            PhaseStatus(
                name=p.name,
                description=p.description,
                status=p.status.value,
                duration_seconds=p.duration_seconds,
                completed_at=p.completed_at,
                error=p.error,
                records_processed=p.records_processed,
                records_created=p.records_created,
            )
            for p in status.phases
        ],
    )


@router.post("/sources/{source_id}/run", response_model=PipelineRunResponse)
def trigger_pipeline(
    source_id: str,
    request: PipelineRunRequest,
    session: SessionDep,
) -> PipelineRunResponse:
    """Trigger a pipeline run for a source.

    Returns the run_id immediately. Use GET /runs/{run_id}/stream for progress.
    Only one pipeline can run at a time.
    """
    global _current_run_id

    # Verify source exists
    stmt = select(Source).where(Source.source_id == source_id)
    result = session.execute(stmt)
    source = result.scalar_one_or_none()

    if source is None:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")

    # Check if pipeline is already running
    is_running, existing_run_id = is_pipeline_running()
    if is_running:
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline already running (run_id: {existing_run_id}). "
            f"Monitor at GET /api/v1/runs/{existing_run_id}/stream",
        )

    # Acquire lock and start pipeline
    if not _pipeline_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="Pipeline is starting. Please wait.",
        )

    try:
        run_id = str(uuid4())
        _current_run_id = run_id

        manager = get_connection_manager()
        config = PipelineConfig(
            skip_llm_phases=request.skip_llm,
            skip_completed=not request.force,
        )

        # Submit to executor (will run in background)
        _executor.submit(
            _run_pipeline_with_cleanup,
            manager,
            source_id,
            request.target_phase,
            config,
            run_id,
        )

        return PipelineRunResponse(
            run_id=run_id,
            source_id=source_id,
            status="running",
            message=f"Pipeline started. Monitor at GET /api/v1/runs/{run_id}/stream",
        )
    finally:
        _pipeline_lock.release()


@router.get("/runs/{run_id}/stream")
async def stream_pipeline_progress(run_id: str) -> StreamingResponse:
    """Stream pipeline progress via Server-Sent Events.

    Connect to this endpoint to receive real-time progress updates.
    Can reconnect at any time - will resume from current state.

    Events:
    - `start`: Pipeline starting
    - `phase_complete`: A phase finished successfully
    - `phase_failed`: A phase failed
    - `complete`: Pipeline finished successfully
    - `error`: Pipeline failed
    - `not_found`: Run ID not found

    Example (JavaScript):
    ```javascript
    const es = new EventSource('/api/v1/runs/abc123/stream');
    es.onmessage = (e) => console.log(JSON.parse(e.data));
    ```
    """
    manager = get_connection_manager()

    # Verify run exists
    with manager.session_scope() as session:
        stmt = select(PipelineRun).where(PipelineRun.run_id == run_id)
        result = session.execute(stmt)
        pipeline_run = result.scalar_one_or_none()

        if pipeline_run is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

        source_id = pipeline_run.source_id
        initial_status = pipeline_run.status

    async def event_generator() -> AsyncGenerator[str]:
        """Generate SSE events for pipeline progress."""
        total_phases = len(PIPELINE_DAG)
        reported_phases: set[str] = set()
        last_status: str | None = None

        # If already completed/failed, send final event and close
        if initial_status in ("completed", "failed", "interrupted"):
            with manager.session_scope() as session:
                stmt = select(PipelineRun).where(PipelineRun.run_id == run_id)
                result = session.execute(stmt)
                final_run = result.scalar_one_or_none()

                if final_run:
                    event_type = "complete" if final_run.status == "completed" else "error"
                    yield _format_sse_event(
                        PipelineProgressEvent(
                            event=event_type,
                            run_id=run_id,
                            source_id=source_id,
                            phases_completed=final_run.phases_completed or 0,
                            phases_total=total_phases,
                            progress_percent=100.0 if final_run.status == "completed" else 0.0,
                            duration_seconds=final_run.total_duration_seconds,
                            error=final_run.error,
                            message=f"Pipeline {final_run.status}",
                        )
                    )
            return

        # Send initial event
        yield _format_sse_event(
            PipelineProgressEvent(
                event="start",
                run_id=run_id,
                source_id=source_id,
                phases_total=total_phases,
                message="Monitoring pipeline progress",
            )
        )

        # Poll for progress
        while True:
            await asyncio.sleep(0.5)

            with manager.session_scope() as session:
                # Check run status
                run_stmt = select(PipelineRun).where(PipelineRun.run_id == run_id)
                run_result = session.execute(run_stmt)
                current_run = run_result.scalar_one_or_none()

                if current_run is None:
                    yield _format_sse_event(
                        PipelineProgressEvent(
                            event="not_found",
                            run_id=run_id,
                            source_id=source_id,
                            error="Run no longer exists",
                        )
                    )
                    return

                # Get checkpoints
                cp_stmt = (
                    select(PhaseCheckpoint)
                    .where(PhaseCheckpoint.run_id == run_id)
                    .order_by(PhaseCheckpoint.completed_at.asc())
                )
                cp_result = session.execute(cp_stmt)
                checkpoints = list(cp_result.scalars().all())

                # Report new phase completions
                for cp in checkpoints:
                    if cp.phase_name not in reported_phases:
                        reported_phases.add(cp.phase_name)
                        completed = len(reported_phases)
                        progress = (completed / total_phases) * 100

                        event_type = (
                            "phase_complete" if cp.status == "completed" else "phase_failed"
                        )
                        yield _format_sse_event(
                            PipelineProgressEvent(
                                event=event_type,
                                run_id=run_id,
                                source_id=source_id,
                                phase=cp.phase_name,
                                phase_status=cp.status,
                                phases_completed=completed,
                                phases_total=total_phases,
                                progress_percent=progress,
                                duration_seconds=cp.duration_seconds,
                                error=cp.error,
                            )
                        )

                # Check if pipeline finished
                if current_run.status != last_status:
                    last_status = current_run.status

                    if current_run.status == "completed":
                        yield _format_sse_event(
                            PipelineProgressEvent(
                                event="complete",
                                run_id=run_id,
                                source_id=source_id,
                                phases_completed=current_run.phases_completed or 0,
                                phases_total=total_phases,
                                progress_percent=100.0,
                                duration_seconds=current_run.total_duration_seconds,
                                message="Pipeline completed successfully",
                            )
                        )
                        return

                    elif current_run.status in ("failed", "interrupted"):
                        yield _format_sse_event(
                            PipelineProgressEvent(
                                event="error",
                                run_id=run_id,
                                source_id=source_id,
                                phases_completed=len(reported_phases),
                                phases_total=total_phases,
                                progress_percent=(len(reported_phases) / total_phases) * 100,
                                duration_seconds=current_run.total_duration_seconds,
                                error=current_run.error,
                                message=f"Pipeline {current_run.status}",
                            )
                        )
                        return

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _run_pipeline_with_cleanup(
    manager: Any,
    source_id: str,
    target_phase: str | None,
    config: PipelineConfig,
    run_id: str,
) -> dict[str, Any]:
    """Run pipeline and clean up global state when done."""
    global _current_run_id

    try:
        return run_pipeline(
            manager=manager,
            source_id=source_id,
            target_phase=target_phase,
            config=config,
            run_id=run_id,
        )
    finally:
        _current_run_id = None


def _format_sse_event(event: PipelineProgressEvent) -> str:
    """Format a PipelineProgressEvent as an SSE message."""
    data = event.model_dump_json()
    return f"data: {data}\n\n"
