"""FastAPI dependencies for the engine REST surface."""

from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy.orm import Session as SASession

from dataraum.core.connections import ConnectionConfig, ConnectionManager

# Process-wide cached workspace manager. Lazy-initialized on first request so
# /health works without DATABASE_URL being set; engine routes that need the
# workspace registry fail loud here with the underlying ConnectionManager
# error (no DATABASE_URL → RuntimeError from ConnectionConfig.for_workspace).
_workspace_manager: ConnectionManager | None = None


def _get_workspace_manager() -> ConnectionManager:
    global _workspace_manager
    if _workspace_manager is None:
        config = ConnectionConfig.for_workspace()
        manager = ConnectionManager(config)
        manager.initialize()
        _workspace_manager = manager
    return _workspace_manager


def get_workspace_session() -> Iterator[SASession]:
    """Yield a SQLAlchemy session scoped to a single request.

    Mirrors the ``ws_mgr.session_scope()`` pattern used throughout the MCP
    server handlers. The session is committed on successful return and
    rolled back on exception by the underlying scope.
    """
    manager = _get_workspace_manager()
    with manager.session_scope() as session:
        yield session


def reset_workspace_manager_for_tests() -> None:
    """Reset the cached workspace manager between tests.

    Tests that monkeypatch ``DATABASE_URL`` or rely on a fresh manager call
    this to discard the cached instance. Not used by production code paths.
    """
    global _workspace_manager
    _workspace_manager = None
