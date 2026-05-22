"""Service layer for the engine REST surface.

These are the functions the route handlers call. They are extracted from
the MCP tool handlers in ``src/dataraum/mcp/server.py`` — same engine
logic, no agent-shaped presentation. Tests live in
``tests/unit/api/test_*.py``.
"""

from __future__ import annotations

from sqlalchemy.orm import Session as SASession

from dataraum.api.schemas import Source


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
