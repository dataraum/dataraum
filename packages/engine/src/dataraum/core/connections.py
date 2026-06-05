"""Connection management for SQLAlchemy + DuckDB.

Single-engine SQLAlchemy model post-DAT-321: every session bound to one
workspace Postgres database (workspace tables + per-session tables, the
latter scoped via ``session_id`` FK).

DuckDB-side post-DAT-341: managers obtain a fresh DuckDB connection from
the process-wide DuckLake anchor (:mod:`dataraum.server.storage`). The
anchor must be bootstrapped before any manager initializes (worker
startup, or the ``lake_anchor`` test fixture). Each manager's connection
has its own ``USE``/search_path state but shares the DuckLake catalog
(schemas, tables) with every other connection to the same named in-memory
database.

Schema layout (post-DAT-341, workspace-stable): three layer schemas
``lake.raw`` / ``lake.typed`` / ``lake.quarantine`` are created at
:func:`dataraum.server.storage.bootstrap_lake` time. The manager's
connection is ``USE``d on ``lake.typed`` — analysis modules that read
typed tables unqualified resolve there; loaders + typing path use FQN
``lake.{layer}."<source>__<table>"`` for cross-layer writes.

Usage:
    from dataraum.core.connections import ConnectionManager, ConnectionConfig

    config = ConnectionConfig.for_directory(Path("./output"))
    manager = ConnectionManager(config, session_id="abc-123")
    manager.initialize()  # opens DuckDB connection + USEs lake.typed

    with manager.session_scope() as session:
        # Use session...

    with manager.duckdb_cursor() as cursor:
        # Unqualified reads resolve against lake.typed:
        result = cursor.execute('SELECT * FROM "src__orders"').fetchdf()

    manager.close()  # closes this manager's DuckDB conn; anchor persists
"""

from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from dataraum.core.logging import get_logger
from dataraum.core.settings import get_settings
from dataraum.storage import Base

logger = get_logger(__name__)


def _resolve_database_url() -> str:
    """Return the workspace SQLAlchemy DSN from typed settings.

    The ``postgresql+psycopg://`` scheme is load-bearing (psycopg v3, not
    psycopg2); ``get_settings()`` fails loud at boot if ``DATABASE_URL`` is
    unset.
    """
    return get_settings().database_url


class _LakeScopedConnection:
    """Wrapper that scopes every derived cursor to a workspace-stable schema.

    Post-DAT-341 the default scope is ``lake.typed`` — the layer most
    consumers (analysis, query, look) read from. Loaders and the typing
    phase issue explicit ``USE lake.<layer>`` or fully-qualified SQL when
    they need to read/write ``raw`` / ``quarantine``.

    DuckDB Python's ``connection.cursor()`` opens a fresh handle whose
    connection state (``USE``/search_path) is the default — it does NOT
    inherit the parent connection's state (verified against DuckDB 1.5.2).
    The same applies to ``cursor.cursor()`` (cursor-of-cursor): the
    derived cursor lands in ``memory.main``. Without this wrapper, every
    cursor opened by pipeline phases or analysis modules would resolve
    unqualified table names against the wrong schema.

    The wrapper:

    * delegates ``execute``, ``close``, ``commit``, etc. via ``__getattr__``
      to the underlying connection (which has its own ``USE`` set in
      :meth:`ConnectionManager._init_duckdb` — so direct
      ``conn.execute(sql)`` resolves against the lake schema);
    * intercepts ``cursor()`` to issue ``USE lake.<schema>`` on the new
      cursor AND wraps the result so cursor-of-cursor chains also stay
      scoped;
    * supports ``with`` (``__enter__`` / ``__exit__``) so callers can use
      the canonical ``with conn.cursor() as sub:`` pattern — dunder
      methods bypass ``__getattr__``, so they must be explicit.

    Implementing this with composition rather than subclassing because
    ``duckdb.DuckDBPyConnection`` is a C extension type and not safely
    subclassable.

    A note on flushes: DuckLake buffers writes in memory until ``CHECKPOINT``
    runs. ``INSERT`` statements via this wrapper land in the lake's catalog
    but parquet files don't appear under ``DATA_PATH`` until checkpoint.
    Pipeline code paths that need files-on-disk semantics (export,
    hand-off) must call ``cursor.execute('CHECKPOINT')`` explicitly.
    """

    # Class-level annotations so mypy can resolve ``self._conn.cursor()``
    # without falling back to ``Any``. ``object.__setattr__`` in __init__
    # populates ``__dict__`` so attribute lookup finds the values before
    # ``__getattr__`` fires (which would otherwise recurse).
    _conn: duckdb.DuckDBPyConnection
    _qualified_schema: str

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        qualified_schema: str,
    ) -> None:
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_qualified_schema", qualified_schema)

    def cursor(self) -> _LakeScopedConnection:
        """Open a derived cursor and ``USE`` the session schema on it.

        Returns another wrapper so cursor-of-cursor chains (analysis modules
        opening a sub-cursor from a phase cursor) stay scoped to the same
        lake schema.
        """
        c = self._conn.cursor()
        c.execute(f"USE {self._qualified_schema}")
        return _LakeScopedConnection(c, self._qualified_schema)

    def __enter__(self) -> _LakeScopedConnection:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        # Close the underlying handle on context-manager exit. The wrapper
        # itself owns the lifecycle of the raw cursor returned by ``cursor()``;
        # ``ConnectionManager.close()`` owns the top-level per-session conn.
        try:
            self._conn.close()
        except Exception:
            logger.debug("Ignoring error while closing DuckDB scoped cursor", exc_info=True)

    def __getattr__(self, name: str) -> Any:
        # Falls through for everything except ``cursor`` and the two stored
        # attributes — duckdb.DuckDBPyConnection methods (execute, close,
        # commit, rollback, fetchdf, fetchall, register, sql, ...) all reach
        # the underlying connection.
        return getattr(self._conn, name)


@dataclass
class ConnectionConfig:
    """Connection configuration for SQLAlchemy (Postgres) + DuckDB.

    Attributes:
        database_url: SQLAlchemy URL for the workspace Postgres engine
            (``postgresql+psycopg://...``).
        pool_size: SQLAlchemy connection pool size.
        max_overflow: Maximum overflow connections beyond pool_size.
        pool_timeout: Seconds to wait for a connection from pool.
        duckdb_memory_limit: DuckDB memory limit (e.g., "2GB"), applied to
            each per-session DuckDB connection.
        echo_sql: Whether to echo SQL statements (for debugging).

    Post-DAT-323: ``ConnectionConfig`` no longer carries a DuckDB path.
    Workspace-vs-session is driven entirely by ``ConnectionManager.session_id``;
    ``for_workspace()`` and ``for_directory()`` produce equivalent configs.
    """

    database_url: str

    # SQLAlchemy pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: float = 30.0

    # DuckDB settings (applied per-session connection)
    duckdb_memory_limit: str = "2GB"

    # Debug
    echo_sql: bool = False

    @classmethod
    def for_workspace(cls, **kwargs: Any) -> ConnectionConfig:
        """Workspace registry config: Postgres-only.

        Reads ``DATABASE_URL`` from the environment. Raises if unset.

        Equivalent to :meth:`for_directory` post-DAT-323 — kept as the
        caller-affordance for "I am not opening a session".
        """
        return cls(database_url=_resolve_database_url(), **kwargs)

    @classmethod
    def for_directory(cls, output_dir: Path, **kwargs: Any) -> ConnectionConfig:
        """Per-session config: workspace Postgres + DuckLake-backed DuckDB.

        SQLAlchemy targets the workspace Postgres engine; the DuckDB
        connection is obtained from the DuckLake anchor at
        :meth:`ConnectionManager.initialize` time and ``USE``d on
        ``lake.typed`` — the workspace-stable typed schema (post-DAT-341).
        The manager's ``session_id`` is no longer the load-bearing scope
        for DuckDB; it stays for row provenance on per-session tables.

        ``output_dir`` is retained for caller signature compatibility but
        no longer drives any DuckDB-side state — the file-backed
        ``data.duckdb`` is gone (L4).
        """
        del output_dir  # kept for signature; lake schema is driven by session_id
        return cls(database_url=_resolve_database_url(), **kwargs)


@dataclass
class ConnectionManager:
    """Thread-safe connection management for SQLAlchemy (Postgres) + DuckDB.

    Provides:
    - SQLAlchemy sync session factory bound to the workspace Postgres engine
    - DuckDB access via cursors (per-session, optional)
    - Proper cleanup on close

    The ``session_id`` field is populated by callers that open a per-session
    manager; per-session rows carry it as FK scoping so writes land under the
    right ``InvestigationSession``.

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
    # Per-session managers store a ``_LakeScopedConnection`` here so that
    # cursors derived from it carry the lake schema's ``USE`` state. Direct
    # ``execute`` calls fall through to the underlying DuckDB connection,
    # which itself was initialized with ``USE``. Workspace managers leave
    # this as ``None``.
    _duckdb_conn: Any = field(default=None, init=False, repr=False)
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
        """Initialize the workspace Postgres SQLAlchemy engine.

        Post-DAT-339 Commit B: every connection acquired from this engine
        has ``search_path`` set to the workspace's schema (``ws_<id>``)
        so all queries without an explicit schema qualifier resolve there.
        The schema is created on first init via ``CREATE SCHEMA IF NOT
        EXISTS``. SQLite-dialect engines (unit-test fallback) skip the
        listener entirely — SQLite has no schema concept.
        """
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

        # Schema-per-workspace bootstrap. Lazy import keeps the module
        # graph clean: connections.py is a low-level utility, and
        # server/workspace.py owns the workspace_id concept.
        if self._engine.dialect.name == "postgresql":
            from dataraum.server.workspace import (
                get_active_workspace_id,
                schema_name_for,
            )

            workspace_id = get_active_workspace_id()
            schema_name = schema_name_for(workspace_id)

            # Register the per-connection search_path listener FIRST, then
            # create the schema. ``connect`` fires once per new dbapi
            # connection (not per pool checkout) — registering it before
            # any connection is established means the first connection
            # (used by ``CREATE SCHEMA`` below + create_all() after) gets
            # search_path set, and pool reuses inherit that state. Postgres
            # accepts ``SET search_path`` against a not-yet-existing schema
            # (validation is lazy until an unqualified reference resolves).
            @event.listens_for(self._engine, "connect")
            def _set_search_path(dbapi_conn: Any, _conn_record: Any) -> None:
                cursor = dbapi_conn.cursor()
                try:
                    cursor.execute(f'SET search_path TO "{schema_name}", public')
                finally:
                    cursor.close()

            with self._engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))

        Base.metadata.create_all(self._engine)

        # autoflush=False keeps writes batched; commit happens at scope close.
        self._session_factory = sessionmaker(
            self._engine,
            expire_on_commit=False,
            autoflush=False,
        )

    def _init_duckdb(self) -> None:
        """Open DuckDB at ``initialize()`` time *iff* a session is already bound.

        This is the session-gated entry point for managers constructed with
        ``session_id`` set at build time (``ConnectionManager(config,
        session_id=...)``). Workspace-only managers leave it a no-op and open
        DuckDB later via :meth:`open_lake`, where ``session_id`` is pure
        per-activity data, not a precondition.
        """
        if self.session_id is None:
            return
        self._open_lake_connection()

    def open_lake(self) -> None:
        """Open the workspace-level DuckLake DuckDB connection (no session needed).

        The long-lived Temporal activity worker (DAT-344) calls this once at
        startup. The lake ``USE`` scope (``lake.typed``) is workspace-stable
        post-DAT-341, so one connection serves every activity and ``session_id``
        stays a per-activity data tag (``PhaseContext.session_id``) rather than
        a gate on having DuckDB at all.

        DuckDB-open is opt-in (not folded into ``initialize()``) because it
        requires :func:`dataraum.server.storage.bootstrap_lake` to have run,
        and lake-less managers are legitimate (unit-test engines, the
        ``/health`` Postgres-only probe). Idempotent.
        """
        self._ensure_initialized()
        self._open_lake_connection()

    def _open_lake_connection(self) -> None:
        """Open + wrap a DuckDB connection scoped to the workspace-stable ``typed`` schema.

        The three layer schemas (``raw``, ``typed``, ``quarantine``) are created
        at :func:`dataraum.server.storage.bootstrap_lake` time, so this assumes
        they already exist — no ``CREATE SCHEMA`` here. Idempotent.
        """
        if self._duckdb_conn is not None:
            return

        # Lazy import: avoids pulling the DuckLake bootstrap surface into
        # module-load for workspace-only configurations.
        from dataraum.server.storage import (
            LAKE_CATALOG_ALIAS,
            apply_s3_secret,
            connect_session,
        )

        raw_conn = connect_session()
        raw_conn.execute(f"SET memory_limit='{self.config.duckdb_memory_limit}'")
        # The lake DATA_PATH is an ``s3://`` URI; this connection reads/writes
        # lake parquet, so it needs the object-store secret too. DuckDB's secret
        # manager is per-instance and this shares the anchor's named in-memory
        # instance, so the secret is likely already present — but we register it
        # explicitly (idempotent) rather than depend on that inheritance.
        apply_s3_secret(raw_conn)

        qualified = f"{LAKE_CATALOG_ALIAS}.typed"
        raw_conn.execute(f"USE {qualified}")

        # Wrap so derived cursors carry the same ``USE`` state — see
        # _LakeScopedConnection's docstring for the DuckDB API motivation.
        self._duckdb_conn = _LakeScopedConnection(raw_conn, qualified)

    def _import_all_models(self) -> None:
        """Import all DB model modules to register them with SQLAlchemy.

        Delegates to storage.base.load_all_models — the ONE model-registration
        list, shared with the offline DDL dump (storage.dump_ddl) so the live
        schema and the checked-in schema.sql / drizzle mirror can never see
        different model sets.
        """
        from dataraum.storage.base import load_all_models

        load_all_models()

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
        """Get a cursor on this manager's DuckDB connection.

        Each call returns a wrapped ``connection.cursor()`` that
        re-issues ``USE lake.typed`` on every derived cursor (DuckDB's
        Python API does NOT inherit USE on cursor()). See
        :class:`_LakeScopedConnection` for the API motivation. A DuckDB
        ``cursor()`` is an independent connection to the same named
        in-memory lake DB (shared catalog, own transaction + ``USE``
        state), so cursors from one manager are safe to use concurrently
        across threads — DuckDB's blessed primitive for parallel access
        (reusing a single connection across threads serializes; cursors
        do not).

        Raises:
            RuntimeError: If manager not initialized or it is workspace-only
                (no ``session_id`` set).

        Example:
            with manager.duckdb_cursor() as cursor:
                df = cursor.execute("SELECT * FROM raw_orders").fetchdf()
        """
        self._ensure_initialized()
        if self._duckdb_conn is None:
            raise RuntimeError(
                "DuckDB is not open on this ConnectionManager. Open it first: "
                "open_lake() for the worker path (no session needed), or "
                "construct the manager with a session_id so initialize() opens it."
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
_default_manager_lock = threading.Lock()


def get_connection_manager(
    output_dir: Path | None = None,
    config: ConnectionConfig | None = None,
) -> ConnectionManager:
    """Get or create a default ConnectionManager.

    For simple scripts that don't need multiple managers. Creates a
    singleton manager on first call. Thread-safe; concurrent callers
    serialize on ``_default_manager_lock`` so only one manager is created.

    Args:
        output_dir: Output directory for per-session DuckDB (if config not provided).
        config: Full configuration (takes precedence over output_dir).

    Returns:
        Initialized ConnectionManager.
    """
    global _default_manager

    with _default_manager_lock:
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

    with _default_manager_lock:
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
