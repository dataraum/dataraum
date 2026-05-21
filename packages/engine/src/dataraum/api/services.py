"""Service layer for the engine REST surface.

These are the functions the route handlers call. They are extracted from
the MCP tool handlers in ``src/dataraum/mcp/server.py`` — same engine
logic, no agent-shaped presentation. Tests live in
``tests/unit/api/test_*.py``.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session as SASession

from dataraum.api.schemas import Source, Workspace


def get_workspace_service(session: SASession) -> Workspace:
    """Return the active workspace metadata.

    Picks the lowest-``created_at`` row — the same selector
    ``bootstrap_workspace`` uses, so the route always sees the row the
    server activated at startup. Slice 1 has exactly one row.

    Raises:
        RuntimeError: If no workspace exists. The FastAPI lifespan
            calls ``bootstrap_workspace`` before serving traffic, so
            this is an unreachable-in-prod safety net.
    """
    from dataraum.storage import Workspace as WorkspaceModel

    row = session.execute(
        select(WorkspaceModel).order_by(WorkspaceModel.created_at).limit(1)
    ).scalar_one_or_none()
    if row is None:
        raise RuntimeError(
            "No workspace found. bootstrap_workspace must run at server "
            "startup before any /api/workspace request is served."
        )
    return Workspace(
        workspace_id=row.workspace_id,
        name=row.name,
        config_dir=row.config_dir,
        created_at=row.created_at,
    )


def list_sources_service(session: SASession) -> list[Source]:
    """Return all non-archived sources registered in the workspace.

    Read-only inspection of the workspace registry. The MCP equivalent is
    ``dataraum.mcp.server._list_sources``; this function performs the same
    work, returning Pydantic models instead of a dict envelope.
    """
    from dataraum.core.credentials import CredentialChain
    from dataraum.sources.manager import SourceManager

    src_mgr = SourceManager(session=session, credential_chain=CredentialChain())
    infos = src_mgr.list_sources()

    return [
        Source(
            name=info.name,
            type=info.source_type,
            status=info.status,
            path=info.path,
            backend=info.backend,
            recipe_tables=info.recipe_tables or None,
        )
        for info in infos
    ]
