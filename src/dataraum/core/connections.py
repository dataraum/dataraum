"""Connection management for SQLAlchemy + DuckDB.

Single-engine model post-DAT-321: every SQLAlchemy session bound to one
workspace Postgres database (workspace tables + per-session tables, the
latter scoped via ``session_id`` FK). Per-session DuckDB stays per-session;
L4 swaps that for DuckLake.

Usage:
    from dataraum.core.connections import ConnectionManager, ConnectionConfig

    config = ConnectionConfig.for_directory(Path("./output"))
    manager = ConnectionManager(config)
    manager.initialize()

    with manager.session_scope() as session:
        # Use session...

    with manager.duckdb_cursor() as cursor:
        result = cursor.execute("SELECT * FROM table").fetchdf()

    manager.close()
"""

from __future__ import annotations

import os
import threading
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from dataraum.core.logging import get_logger
from dataraum.storage import Base

logger = get_logger(__name__)


_DATABASE_URL_MISSING_MSG = (
    "DATABASE_URL is not set. The workspace SQLAlchemy engine targets Postgres; "
    "the container substrate (L1) provides this on the control-plane service "
    "(see docker-compose.yml). For local dev outside the container, export "
    "DATABASE_URL=postgresql+psycopg://<user>:<pass>@<host>:<port>/<db>."
)


def _resolve_database_url() -> str:
    """Read DATABASE_URL or fail loud with an actionable message."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(_DATABASE_URL_MISSING_MSG)
    return url


@dataclass
class ConnectionConfig:
    """Connection configuration for SQLAlchemy (Postgres) + DuckDB.

    Attributes:
        database_url: SQLAlchemy URL for the workspace Postgres engine
            (``postgresql+psycopg://...``).
        duckdb_path: Path to per-session DuckDB file. ``None`` for the
            workspace registry (no per-session data).
        pool_size: SQLAlchemy connection pool size.
        max_overflow: Maximum overflow connections beyond pool_size.
        pool_timeout: Seconds to wait for a connection from pool.
        duckdb_memory_limit: DuckDB memory limit (e.g., "2GB").
        echo_sql: Whether to echo SQL statements (for debugging).
    """

    database_url: str
    duckdb_path: Path | None = None

    # SQLAlchemy pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: float = 30.0

    # DuckDB settings
    duckdb_memory_limit: str = "2GB"

    # Debug
    echo_sql: bool = False

    @classmethod
    def for_workspace(cls, **kwargs: Any) -> ConnectionConfig:
        """Workspace registry config: Postgres-only, no DuckDB.

        Reads ``DATABASE_URL`` from the environment. Raises if unset.
        """
        return cls(database_url=_resolve_database_url(), duckdb_path=None, **kwargs)

    @classmethod
    def for_directory(cls, output_dir: Path, **kwargs: Any) -> ConnectionConfig:
        """Per-session config: workspace Postgres + per-session DuckDB.

        SQLAlchemy targets the same workspace Postgres engine as
        ``for_workspace()``; the per-session DuckDB file lives at
        ``output_dir/data.duckdb`` (L4 swaps this for DuckLake).
        """
        return cls(
            database_url=_resolve_database_url(),
            duckdb_path=output_dir / "data.duckdb",
            **kwargs,
        )


@dataclass
class ConnectionManager:
    """Thread-safe connection management for SQLAlchemy (Postgres) + DuckDB.

    Provides:
    - SQLAlchemy sync session factory bound to the workspace Postgres engine
    - DuckDB access via cursors (per-session, optional)
    - Proper cleanup on close

    The ``session_id`` field is populated by callers that open a per-session
    manager (e.g. ``mcp/server.py::_get_session_manager``). Recorders read
    it when constructing per-session rows so writes carry the FK scoping
    required by the post-L2 schema.

    Thread Safety:
    - SQLAlchemy sessions: One session per thread via ``session_scope()``
    - DuckDB: Use ``duckdb_cursor()`` which returns an independent cursor

    Usage:
        manager = ConnectionManager(config)
        manager.initialize()

        with manager.session_scope() as session:
            # SQLAlchemy operations...

        with manager.duckdb_cursor() as cursor:
            df = cursor.execute("SELECT ...").fetchdf()

        manager.close()
    """

    config: ConnectionConfig
    session_id: str | None = None
    _engine: Engine | None = field(default=None, init=False, repr=False)
    _session_factory: sessionmaker[Session] | None = field(default=None, init=False, repr=False)
    _duckdb_conn: duckdb.DuckDBPyConnection | None = field(default=None, init=False, repr=False)
    _init_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _initialized: bool = field(default=False, init=False, repr=False)

    def initialize(self) -> None:
        """Initialize connection pools and databases.

        Creates the SQLAlchemy engine + pool and the optional DuckDB
        connection. Safe to call multiple times (idempotent).

        Raises:
            RuntimeError: If initialization fails.
        """
        with self._init_lock:
            if self._initialized:
                return

            try:
                self._init_sqlalchemy()
                self._init_duckdb()
                self._initialized = True
            except Exception as e:
                self.close()
                raise RuntimeError(f"Failed to initialize connections: {e}") from e

    def _init_sqlalchemy(self) -> None:
        """Initialize the workspace Postgres SQLAlchemy engine."""
        self._engine = create_engine(
            self.config.database_url,
            echo=self.config.echo_sql,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_pre_ping=True,
        )

        # Register all models before create_all so every per-session table
        # materializes alongside the workspace tables.
        self._import_all_models()

        Base.metadata.create_all(self._engine)

        # autoflush=False keeps writes batched; commit happens at scope close.
        self._session_factory = sessionmaker(
            self._engine,
            expire_on_commit=False,
            autoflush=False,
        )

    def _init_duckdb(self) -> None:
        """Initialize the per-session DuckDB connection, if configured.

        Workspace-only configurations skip this entirely; ``duckdb_cursor()``
        will raise on attempted use. L4 swaps the file-backed DuckDB for
        DuckLake but leaves this hook intact.
        """
        if self.config.duckdb_path is None:
            return
        if self.config.duckdb_path == Path(":memory:"):
            self._duckdb_conn = duckdb.connect(":memory:")
        else:
            self.config.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            self._duckdb_conn = duckdb.connect(str(self.config.duckdb_path))

        self._duckdb_conn.execute(f"SET memory_limit='{self.config.duckdb_memory_limit}'")

    def _import_all_models(self) -> None:
        """Import all DB model modules to register them with SQLAlchemy."""
        # Core models not owned by any phase
        from dataraum.documentation import db_models as _fixes  # noqa: F401
        from dataraum.investigation import db_models as _investigation  # noqa: F401
        from dataraum.mcp import db_models as _mcp  # noqa: F401
        from dataraum.pipeline import db_models as _pipeline  # noqa: F401

        # Phase-owned models: auto-discovered from registry
        from dataraum.pipeline.registry import import_all_phase_models
        from dataraum.query import db_models as _query  # noqa: F401
        from dataraum.query import snippet_models as _snippets  # noqa: F401
        from dataraum.storage import models as _storage  # noqa: F401

        import_all_phase_models()

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError(
                "ConnectionManager not initialized. Call manager.initialize() first."
            )

    @contextmanager
    def session_scope(self) -> Generator[Session]:
        """Get a session with automatic cleanup.

        Thread-safe: each call creates a new session from the QueuePool.

        Yields:
            Session from the connection pool.

        Raises:
            RuntimeError: If manager not initialized.

        Example:
            with manager.session_scope() as session:
                result = session.execute(select(Table))
        """
        self._ensure_initialized()
        assert self._session_factory is not None

        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self) -> Session:
        """Get a new session from the pool.

        Caller is responsible for committing/closing. Prefer ``session_scope()``.

        Raises:
            RuntimeError: If manager not initialized.
        """
        self._ensure_initialized()
        assert self._session_factory is not None
        return self._session_factory()

    @contextmanager
    def duckdb_cursor(self) -> Generator[duckdb.DuckDBPyConnection]:
        """Get an independent DuckDB cursor on the shared per-session connection.

        Each call returns a fresh cursor via ``connection.cursor()``. Cursors
        have independent statement state and are safe to use from separate
        threads; the underlying connection is shared.

        Raises:
            RuntimeError: If manager not initialized or no DuckDB configured.

        Example:
            with manager.duckdb_cursor() as cursor:
                df = cursor.execute("SELECT * FROM table").fetchdf()
        """
        self._ensure_initialized()
        if self._duckdb_conn is None:
            raise RuntimeError(
                "DuckDB cursor requested on a workspace-only ConnectionManager. "
                "Workspace managers (ConnectionConfig.for_workspace) have no "
                "DuckDB; route data operations through a per-session manager."
            )

        cursor = self._duckdb_conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @property
    def engine(self) -> Engine:
        """Get the SQLAlchemy engine.

        Raises:
            RuntimeError: If manager not initialized.
        """
        self._ensure_initialized()
        assert self._engine is not None
        return self._engine

    def close(self) -> None:
        """Close all connections and dispose of pools.

        Safe to call multiple times.
        """
        if self._duckdb_conn is not None:
            try:
                self._duckdb_conn.close()
            except Exception:
                pass
            self._duckdb_conn = None

        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception:
                pass
            self._engine = None

        self._session_factory = None
        self._initialized = False

    def get_stats(self) -> dict[str, Any]:
        """Get connection pool statistics."""
        self._ensure_initialized()
        assert self._engine is not None

        pool: Any = self._engine.pool
        return {
            "pool_size": pool.size() if hasattr(pool, "size") else None,
            "pool_checked_out": pool.checkedout() if hasattr(pool, "checkedout") else None,
            "pool_overflow": pool.overflow() if hasattr(pool, "overflow") else None,
            "pool_checked_in": pool.checkedin() if hasattr(pool, "checkedin") else None,
            "duckdb_connected": self._duckdb_conn is not None,
        }


# Convenience function for simple scripts
_default_manager: ConnectionManager | None = None


def get_connection_manager(
    output_dir: Path | None = None,
    config: ConnectionConfig | None = None,
) -> ConnectionManager:
    """Get or create a default ConnectionManager.

    For simple scripts that don't need multiple managers. Creates a
    singleton manager on first call.

    Args:
        output_dir: Output directory for per-session DuckDB (if config not provided).
        config: Full configuration (takes precedence over output_dir).

    Returns:
        Initialized ConnectionManager.
    """
    global _default_manager

    if _default_manager is None:
        if config is None:
            if output_dir is None:
                output_dir = Path("./pipeline_output")
            config = ConnectionConfig.for_directory(output_dir)

        _default_manager = ConnectionManager(config)
        _default_manager.initialize()

    return _default_manager


def close_default_manager() -> None:
    """Close the default ConnectionManager if it exists."""
    global _default_manager

    if _default_manager is not None:
        _default_manager.close()
        _default_manager = None


def get_manager_for_directory(output_dir: Path) -> ConnectionManager:
    """Create and initialize a ConnectionManager for a per-session directory.

    Framework-agnostic: raises ``RuntimeError`` if ``DATABASE_URL`` is unset.

    Args:
        output_dir: Directory containing the per-session DuckDB file.

    Returns:
        Initialized ConnectionManager. Caller is responsible for closing it.
    """
    config = ConnectionConfig.for_directory(output_dir)
    manager = ConnectionManager(config)
    manager.initialize()
    return manager


__all__ = [
    "ConnectionConfig",
    "ConnectionManager",
    "get_connection_manager",
    "get_manager_for_directory",
    "close_default_manager",
]
