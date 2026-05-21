"""Engine REST routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session as SASession

from dataraum.api.deps import get_workspace_session
from dataraum.api.schemas import Source, Workspace
from dataraum.api.services import get_workspace_service, list_sources_service

api_router = APIRouter(tags=["engine"])

# Annotated form (FastAPI 0.95+ pattern) avoids the B008 lint warning that
# fires on `Depends(...)` in function defaults.
WorkspaceSession = Annotated[SASession, Depends(get_workspace_session)]


@api_router.get(
    "/workspace",
    response_model=Workspace,
    summary="Return the active workspace metadata",
)
def get_workspace(session: WorkspaceSession) -> Workspace:
    """Return the active workspace metadata.

    Slice 1 has exactly one workspace bootstrapped at server startup.
    Table/source counts land in DAT-344 once those models gain a
    ``workspace_id`` FK.
    """
    return get_workspace_service(session)


@api_router.get(
    "/sources",
    response_model=list[Source],
    summary="List sources registered in the workspace",
)
def list_sources(session: WorkspaceSession) -> list[Source]:
    """Return all non-archived sources in the workspace registry.

    The shape mirrors the MCP ``list_sources`` tool's public fields. Empty
    list when no sources are registered.
    """
    return list_sources_service(session)
