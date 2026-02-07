"""Thread-safe connection management for SQLAlchemy + DuckDB.

This module provides concurrent-ready connection management:
- SQLAlchemy sync sessions (thread-safe with ThreadPoolExecutor)
- DuckDB with read cursors and serialized writes
- WAL mode for SQLite to enable concurrent reads with writes

Usage:
    from dataraum.core.connections import ConnectionManager, ConnectionConfig

    config = ConnectionConfig.for_directory(Path("./output"))
    manager = ConnectionManager(config)
    manager.initialize()

    # Get a session (thread-safe)
    with manager.session_scope() as session:
        # Use session...

    # Read from DuckDB (concurrent-safe via cursor)
    with manager.duckdb_cursor() as cursor:
        result = cursor.execute("SELECT * FROM table").fetchdf()

    # Write to DuckDB (serialized via mutex)
    with manager.duckdb_write() as conn:
        conn.execute("INSERT INTO table VALUES (...)")

    manager.close()
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
from dataraum.storage import Base

logger = get_logger(__name__)


@dataclass
class ConnectionConfig:
    """Connection configuration for SQLAlchemy and DuckDB.

    Attributes:
        sqlite_path: Path to SQLite database file
        duckdb_path: Path to DuckDB database file for data
        vectors_path: Path to DuckDB database file for embeddings
        pool_size: SQLAlchemy connection pool size
        max_overflow: Maximum overflow connections beyond pool_size
        pool_timeout: Seconds to wait for a connection from pool
        sqlite_timeout: SQLite busy timeout in seconds
        duckdb_memory_limit: DuckDB memory limit (e.g., "2GB")
        echo_sql: Whether to echo SQL statements (for debugging)
    """

    sqlite_path: Path
    duckdb_path: Path
    vectors_path: Path | None = None  # Separate DuckDB for embeddings

    # SQLAlchemy pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: float = 30.0
    sqlite_timeout: float = 120.0

    # DuckDB settings
    duckdb_memory_limit: str = "2GB"

    # Debug
    echo_sql: bool = False

    @classmethod
    def for_directory(cls, output_dir: Path, **kwargs: Any) -> ConnectionConfig:
        """Create config for a pipeline output directory.

        Args:
            output_dir: Directory for database files
            **kwargs: Override any config attributes

        Returns:
            ConnectionConfig with paths set to output_dir
        """
        return cls(
            sqlite_path=output_dir / "metadata.db",
            duckdb_path=output_dir / "data.duckdb",
            vectors_path=output_dir / "vectors.duckdb",
            **kwargs,
        )

    @classmethod
    def in_memory(cls, **kwargs: Any) -> ConnectionConfig:
        """Create config for in-memory databases (useful for testing).

        Args:
            **kwargs: Override any config attributes

        Returns:
            ConnectionConfig with in-memory paths
        """
        return cls(
            sqlite_path=Path(":memory:"),
            duckdb_path=Path(":memory:"),
            vectors_path=Path(":memory:"),
            **kwargs,
        )


@dataclass
class ConnectionManager:
    """Thread-safe connection management for SQLAlchemy + DuckDB.

    Provides:
    - SQLAlchemy sync session factory (thread-safe)
    - DuckDB read access via cursors (concurrent-safe)
    - DuckDB write access via mutex (serialized)
    - Proper cleanup on close

    Thread Safety:
    - SQLAlchemy sessions: One session per thread via session_scope()
    - DuckDB reads: Use cursor() which is thread-safe for reads
    - DuckDB writes: Serialized via _write_lock mutex

    Usage:
        manager = ConnectionManager(config)
        manager.initialize()

        with manager.session_scope() as session:
            # SQLAlchemy operations...

        with manager.duckdb_cursor() as cursor:
            df = cursor.execute("SELECT ...").fetchdf()

        with manager.duckdb_write() as conn:
            conn.execute("INSERT ...")

        manager.close()
    """

    config: ConnectionConfig
    _engine: Engine | None = field(default=None, init=False, repr=False)
    _session_factory: sessionmaker[Session] | None = field(default=None, init=False, repr=False)
    _duckdb_conn: duckdb.DuckDBPyConnection | None = field(default=None, init=False, repr=False)
    _vectors_conn: duckdb.DuckDBPyConnection | None = field(default=None, init=False, repr=False)
    _write_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _vectors_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _init_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _initialized: bool = field(default=False, init=False, repr=False)

    def initialize(self) -> None:
        """Initialize connection pools and databases.

        Creates SQLAlchemy engine with connection pool and DuckDB connection.
        Safe to call multiple times (idempotent).

        Raises:
            RuntimeError: If initialization fails
        """
        with self._init_lock:
            if self._initialized:
                return

            try:
                self._init_sqlalchemy()
                self._init_duckdb()
                self._init_vectors()
                self._initialized = True
            except Exception as e:
                self.close()
                raise RuntimeError(f"Failed to initialize connections: {e}") from e

    def _init_sqlalchemy(self) -> None:
        """Initialize SQLAlchemy sync engine with connection pool."""
        # Handle in-memory vs file-based SQLite
        if self.config.sqlite_path == Path(":memory:"):
            db_url = "sqlite:///:memory:"
        else:
            self.config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite:///{self.config.sqlite_path}"

        self._engine = create_engine(
            db_url,
            echo=self.config.echo_sql,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_pre_ping=True,
            # Explicitly allow cross-thread usage (SQLAlchemy default for file DBs)
            connect_args={"check_same_thread": False},
        )

        # Configure SQLite pragmas on each connection
        @event.listens_for(self._engine, "connect")
        def configure_sqlite(dbapi_conn: Any, connection_record: Any) -> None:
            cursor = dbapi_conn.cursor()
            # Enable foreign keys
            cursor.execute("PRAGMA foreign_keys=ON")
            # Use WAL mode for better concurrency (readers don't block writers)
            cursor.execute("PRAGMA journal_mode=WAL")
            # Set busy timeout (milliseconds)
            cursor.execute(f"PRAGMA busy_timeout={int(self.config.sqlite_timeout * 1000)}")
            # Synchronous mode - NORMAL is good balance of safety/speed
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()

        # Import all models to register them with SQLAlchemy
        self._import_all_models()

        # Create tables
        Base.metadata.create_all(self._engine)

        # Create session factory
        # autoflush=False prevents mid-query writes; we flush at commit time with a lock
        self._session_factory = sessionmaker(
            self._engine,
            expire_on_commit=False,
            autoflush=False,
        )

    def _init_duckdb(self) -> None:
        """Initialize DuckDB connection for data."""
        if self.config.duckdb_path == Path(":memory:"):
            self._duckdb_conn = duckdb.connect(":memory:")
        else:
            self.config.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            self._duckdb_conn = duckdb.connect(str(self.config.duckdb_path))

        # Configure DuckDB
        self._duckdb_conn.execute(f"SET memory_limit='{self.config.duckdb_memory_limit}'")

    def _init_vectors(self) -> None:
        """Initialize DuckDB connection for vector embeddings.

        Uses the VSS extension for similarity search.
        """
        if self.config.vectors_path is None:
            # Vectors disabled
            return

        if self.config.vectors_path == Path(":memory:"):
            self._vectors_conn = duckdb.connect(":memory:")
        else:
            self.config.vectors_path.parent.mkdir(parents=True, exist_ok=True)
            self._vectors_conn = duckdb.connect(str(self.config.vectors_path))

        # Install and load VSS extension
        self._vectors_conn.execute("INSTALL vss")
        self._vectors_conn.execute("LOAD vss")

        # Create embeddings table if not exists
        self._vectors_conn.execute("""
            CREATE TABLE IF NOT EXISTS query_embeddings (
                query_id VARCHAR PRIMARY KEY,
                embedding FLOAT[384]
            )
        """)

        # Create HNSW index if not exists (for fast similarity search)
        try:
            self._vectors_conn.execute("""
                CREATE INDEX IF NOT EXISTS embedding_idx ON query_embeddings
                USING HNSW (embedding) WITH (metric = 'cosine')
            """)
        except Exception:
            # Index may already exist or table is empty
            pass

    def _import_all_models(self) -> None:
        """Import all DB model modules to register them with SQLAlchemy."""
        # Import all modules that define SQLAlchemy models
        # This ensures Base.metadata has all tables before create_all()
        from dataraum.analysis.correlation import db_models as _correlation  # noqa: F401
        from dataraum.analysis.cycles import db_models as _cycles  # noqa: F401
        from dataraum.analysis.quality_summary import db_models as _quality  # noqa: F401
        from dataraum.analysis.relationships import db_models as _rel  # noqa: F401
        from dataraum.analysis.semantic import db_models as _semantic  # noqa: F401
        from dataraum.analysis.slicing import db_models as _slicing  # noqa: F401
        from dataraum.analysis.statistics import db_models as _statistics  # noqa: F401
        from dataraum.analysis.temporal import db_models as _temporal  # noqa: F401
        from dataraum.analysis.temporal_slicing import (
            db_models as _temp_slice,  # noqa: F401
        )
        from dataraum.analysis.typing import db_models as _typing  # noqa: F401
        from dataraum.analysis.validation import db_models as _validation  # noqa: F401
        from dataraum.entropy import db_models as _entropy  # noqa: F401
        from dataraum.graphs import db_models as _graphs  # noqa: F401
        from dataraum.llm import db_models as _llm  # noqa: F401
        from dataraum.pipeline import db_models as _pipeline  # noqa: F401
        from dataraum.query import db_models as _query  # noqa: F401

    def _ensure_initialized(self) -> None:
        """Raise if not initialized."""
        if not self._initialized:
            raise RuntimeError(
                "ConnectionManager not initialized. Call manager.initialize() first."
            )

    @contextmanager
    def session_scope(self) -> Generator[Session]:
        """Get a session with automatic cleanup.

        Thread-safe: each call creates a new session from the QueuePool.
        SQLite WAL mode + PRAGMA busy_timeout handle write contention at
        the C level — no Python-level mutex or retry needed here.

        Session is created with autoflush=False. All writes are batched and
        committed atomically at the end.

        Yields:
            Session from the connection pool

        Raises:
            RuntimeError: If manager not initialized

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

        The caller is responsible for committing/closing the session.
        Prefer session_scope() for automatic cleanup.

        Returns:
            New Session from pool

        Raises:
            RuntimeError: If manager not initialized
        """
        self._ensure_initialized()
        assert self._session_factory is not None
        return self._session_factory()

    @contextmanager
    def duckdb_cursor(self) -> Generator[duckdb.DuckDBPyConnection]:
        """Get a DuckDB cursor for read operations.

        Cursors from the same connection are thread-safe for reads.
        Use this for SELECT queries that don't modify data.

        Yields:
            DuckDB cursor for read operations

        Raises:
            RuntimeError: If manager not initialized

        Example:
            with manager.duckdb_cursor() as cursor:
                df = cursor.execute("SELECT * FROM table").fetchdf()
        """
        self._ensure_initialized()
        assert self._duckdb_conn is not None

        cursor = self._duckdb_conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @contextmanager
    def duckdb_write(self) -> Generator[duckdb.DuckDBPyConnection]:
        """Get exclusive write access to DuckDB.

        Uses mutex to serialize all write operations.
        Use this for INSERT, UPDATE, DELETE, CREATE TABLE, etc.

        Yields:
            DuckDB connection with exclusive write access

        Raises:
            RuntimeError: If manager not initialized

        Example:
            with manager.duckdb_write() as conn:
                conn.execute("INSERT INTO table VALUES (...)")
        """
        self._ensure_initialized()
        assert self._duckdb_conn is not None

        with self._write_lock:
            yield self._duckdb_conn

    @contextmanager
    def vectors_cursor(self) -> Generator[duckdb.DuckDBPyConnection]:
        """Get a cursor for reading from vectors database.

        Thread-safe for read operations.

        Yields:
            DuckDB cursor for vector queries

        Raises:
            RuntimeError: If manager not initialized or vectors disabled

        Example:
            with manager.vectors_cursor() as cursor:
                results = cursor.execute(
                    "SELECT query_id FROM query_embeddings ORDER BY ..."
                ).fetchall()
        """
        self._ensure_initialized()
        if self._vectors_conn is None:
            raise RuntimeError("Vectors database not configured")

        cursor = self._vectors_conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @contextmanager
    def vectors_write(self) -> Generator[duckdb.DuckDBPyConnection]:
        """Get exclusive write access to vectors database.

        Uses mutex to serialize all write operations.

        Yields:
            DuckDB connection with exclusive write access

        Raises:
            RuntimeError: If manager not initialized or vectors disabled

        Example:
            with manager.vectors_write() as conn:
                conn.execute("INSERT INTO query_embeddings VALUES (?, ?)", [...])
        """
        self._ensure_initialized()
        if self._vectors_conn is None:
            raise RuntimeError("Vectors database not configured")

        with self._vectors_lock:
            yield self._vectors_conn

    @property
    def vectors_enabled(self) -> bool:
        """Check if vectors database is enabled."""
        return self._vectors_conn is not None

    @property
    def engine(self) -> Engine:
        """Get the SQLAlchemy engine.

        Returns:
            The Engine instance

        Raises:
            RuntimeError: If manager not initialized
        """
        self._ensure_initialized()
        assert self._engine is not None
        return self._engine

    def close(self) -> None:
        """Close all connections and dispose of pools.

        Safe to call multiple times.
        """
        if self._vectors_conn is not None:
            try:
                self._vectors_conn.close()
            except Exception:
                pass
            self._vectors_conn = None

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

    def execute_sqlite(self, sql: str) -> Any:
        """Execute raw SQL on SQLite (for debugging/admin).

        Args:
            sql: SQL statement to execute

        Returns:
            Result of execution
        """
        self._ensure_initialized()
        assert self._engine is not None

        with self._engine.begin() as conn:
            return conn.execute(text(sql))

    def get_stats(self) -> dict[str, Any]:
        """Get connection pool statistics.

        Returns:
            Dict with pool status information
        """
        self._ensure_initialized()
        assert self._engine is not None

        pool: Any = self._engine.pool
        return {
            "sqlite_pool_size": pool.size() if hasattr(pool, "size") else None,
            "sqlite_checked_out": pool.checkedout() if hasattr(pool, "checkedout") else None,
            "sqlite_overflow": pool.overflow() if hasattr(pool, "overflow") else None,
            "sqlite_checked_in": pool.checkedin() if hasattr(pool, "checkedin") else None,
            "duckdb_connected": self._duckdb_conn is not None,
        }


# Convenience function for simple scripts
_default_manager: ConnectionManager | None = None


def get_connection_manager(
    output_dir: Path | None = None,
    config: ConnectionConfig | None = None,
) -> ConnectionManager:
    """Get or create a default ConnectionManager.

    For simple scripts that don't need multiple managers.
    Creates a singleton manager on first call.

    Args:
        output_dir: Output directory (used if config not provided)
        config: Full configuration (takes precedence over output_dir)

    Returns:
        Initialized ConnectionManager
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


__all__ = [
    "ConnectionConfig",
    "ConnectionManager",
    "get_connection_manager",
    "close_default_manager",
]
