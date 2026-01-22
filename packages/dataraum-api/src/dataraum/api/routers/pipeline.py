"""Pipeline control endpoints."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from dataraum.api.deps import SessionDep
from dataraum.api.schemas import (
    PhaseStatus,
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineStatusResponse,
)
from dataraum.pipeline.status import get_pipeline_status
from dataraum.storage import Source

router = APIRouter()


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
def run_pipeline(
    source_id: str,
    request: PipelineRunRequest,
    session: SessionDep,
) -> PipelineRunResponse:
    """Trigger a pipeline run for a source.

    Note: This is a placeholder. Full implementation requires background task handling.
    """
    # Verify source exists
    stmt = select(Source).where(Source.source_id == source_id)
    result = session.execute(stmt)
    source = result.scalar_one_or_none()

    if source is None:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")

    # TODO: Implement actual pipeline execution
    # This should:
    # 1. Create a PipelineRun record
    # 2. Start background task for pipeline execution
    # 3. Return immediately with run_id

    # For now, return a placeholder response
    from uuid import uuid4

    run_id = str(uuid4())

    return PipelineRunResponse(
        run_id=run_id,
        source_id=source_id,
        status="queued",
        message="Pipeline run queued (background execution not yet implemented)",
    )
