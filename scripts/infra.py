"""Shared test infrastructure for persistent DuckDB + SQLite storage.

This module provides utilities for test scripts to:
1. Create/connect to persistent databases in data/ directory
2. Check if intermediate results exist from earlier phases
3. Load/save intermediate state between test runs

Usage:
    from infra import get_test_session, get_duckdb_conn, check_phase_complete

    async with get_test_session() as session:
        duckdb_conn = get_duckdb_conn()

        if not check_phase_complete(session, "phase1_csv"):
            # Run phase 1
            mark_phase_complete(session, "phase1_csv")

        # Continue with analysis...
"""

import asyncio
import logging
from pathlib import Path
from typing import Any

import duckdb
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dataraum_context.storage import Base

# Default paths for test databases
DEFAULT_DATA_DIR = Path(__file__).parent.parent / "data"
DEFAULT_DUCKDB_PATH = DEFAULT_DATA_DIR / "test.duckdb"
DEFAULT_SQLITE_PATH = DEFAULT_DATA_DIR / "test.sqlite"

# Global connections (reused across calls)
_duckdb_conn: duckdb.DuckDBPyConnection | None = None
_async_session: async_sessionmaker[AsyncSession] | None = None
_engine: Any = None

logger = logging.getLogger(__name__)


def ensure_data_dir() -> Path:
    """Ensure the data directory exists."""
    DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_DATA_DIR


def get_duckdb_conn(
    path: Path | str | None = None,
    reset: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Get or create a persistent DuckDB connection.

    Args:
        path: Path to DuckDB file. None = use default path.
        reset: If True, delete existing database and start fresh.

    Returns:
        DuckDB connection
    """
    global _duckdb_conn

    ensure_data_dir()
    db_path = Path(path) if path else DEFAULT_DUCKDB_PATH

    if reset and db_path.exists():
        if _duckdb_conn:
            _duckdb_conn.close()
            _duckdb_conn = None
        db_path.unlink()
        logger.info(f"Deleted existing DuckDB: {db_path}")

    if _duckdb_conn is None or reset:
        _duckdb_conn = duckdb.connect(str(db_path))
        _duckdb_conn.execute("SET memory_limit='2GB'")
        logger.info(f"Connected to DuckDB: {db_path}")

    return _duckdb_conn


async def get_test_session(
    path: Path | str | None = None,
    reset: bool = False,
) -> async_sessionmaker[AsyncSession]:
    """Get or create a persistent SQLite session factory.

    Args:
        path: Path to SQLite file. None = use default path.
        reset: If True, delete existing database and start fresh.

    Returns:
        Async session factory
    """
    global _async_session, _engine

    ensure_data_dir()
    db_path = Path(path) if path else DEFAULT_SQLITE_PATH

    if reset and db_path.exists():
        if _engine:
            await _engine.dispose()
            _engine = None
            _async_session = None
        db_path.unlink()
        logger.info(f"Deleted existing SQLite: {db_path}")

    if _async_session is None or reset:
        db_url = f"sqlite+aiosqlite:///{db_path}"
        _engine = create_async_engine(db_url, echo=False)

        @event.listens_for(_engine.sync_engine, "connect")
        def set_sqlite_pragma(dbapi_conn: Any, connection_record: Any) -> None:
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        # Import all models to register them
        _import_all_models()

        # Create tables if they don't exist
        async with _engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        _async_session = async_sessionmaker(_engine, expire_on_commit=False)
        logger.info(f"Connected to SQLite: {db_path}")

    return _async_session


def _import_all_models() -> None:
    """Import all DB model modules to register them with SQLAlchemy."""
    from dataraum_context.analysis.correlation import db_models as _correlation_models  # noqa: F401
    from dataraum_context.analysis.cycles import db_models as _cycles_models  # noqa: F401
    from dataraum_context.analysis.quality_summary import (
        db_models as _quality_summary_models,  # noqa: F401
    )
    from dataraum_context.analysis.relationships import db_models as _rel_models  # noqa: F401
    from dataraum_context.analysis.semantic import db_models as _semantic_models  # noqa: F401
    from dataraum_context.analysis.slicing import db_models as _slicing_models  # noqa: F401
    from dataraum_context.analysis.statistics import db_models as _statistics_models  # noqa: F401
    from dataraum_context.analysis.temporal import db_models as _temporal_models  # noqa: F401
    from dataraum_context.analysis.topology import db_models as _topology_models  # noqa: F401
    from dataraum_context.analysis.typing import db_models as _typing_models  # noqa: F401
    from dataraum_context.analysis.validation import db_models as _validation_models  # noqa: F401
    from dataraum_context.graphs import db_models as _graphs_models  # noqa: F401
    from dataraum_context.llm import db_models as _llm_models  # noqa: F401
    from dataraum_context.quality import db_models as _quality_models  # noqa: F401


async def check_tables_exist(
    session: AsyncSession,
    table_names: list[str],
    layer: str = "raw",
) -> dict[str, bool]:
    """Check which tables exist in the metadata database.

    Args:
        session: SQLAlchemy session
        table_names: List of expected table names (in 'tables' table)
        layer: Layer to check ('raw', 'typed', etc.)

    Returns:
        Dict mapping table_name -> exists
    """
    from dataraum_context.storage import Table

    result = {}
    for name in table_names:
        # Check for exact name or with raw_ prefix
        stmt = select(Table).where(Table.layer == layer)
        tables = (await session.execute(stmt)).scalars().all()

        # Look for matching table
        found = False
        for t in tables:
            # Match base name (e.g., "customer_table" matches "raw_customer_table")
            base_name = t.table_name.replace("raw_", "").replace("typed_", "")
            if base_name == name or t.table_name == name:
                found = True
                break

        result[name] = found

    return result


async def check_duckdb_tables(
    conn: duckdb.DuckDBPyConnection,
    table_names: list[str],
) -> dict[str, bool]:
    """Check which tables exist in DuckDB.

    Args:
        conn: DuckDB connection
        table_names: List of expected table names

    Returns:
        Dict mapping table_name -> exists
    """
    existing = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    return {name: name in existing for name in table_names}


async def get_typed_table_ids(session: AsyncSession) -> list[str]:
    """Get all typed table IDs from the metadata database.

    Returns:
        List of table IDs for tables with layer='typed'
    """
    from dataraum_context.storage import Table

    stmt = select(Table.table_id).where(Table.layer == "typed")
    result = await session.execute(stmt)
    return [row[0] for row in result.all()]


async def get_table_by_name(session: AsyncSession, table_name: str) -> Any:
    """Get a Table by name.

    Returns:
        Table object or None
    """
    from dataraum_context.storage import Table

    stmt = select(Table).where(Table.table_name == table_name)
    return (await session.execute(stmt)).scalar_one_or_none()


def print_phase_status(phase_name: str, complete: bool, details: str = "") -> None:
    """Print phase status in a consistent format."""
    status = "READY" if complete else "MISSING"
    symbol = "✓" if complete else "✗"
    print(f"  {symbol} {phase_name}: {status}")
    if details:
        print(f"      {details}")


async def cleanup_connections() -> None:
    """Close all connections. Call at end of test script."""
    global _duckdb_conn, _async_session, _engine

    if _duckdb_conn:
        _duckdb_conn.close()
        _duckdb_conn = None

    if _engine:
        await _engine.dispose()
        _engine = None
        _async_session = None


def list_duckdb_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """List all tables in DuckDB."""
    return [row[0] for row in conn.execute("SHOW TABLES").fetchall()]


async def print_database_summary(
    session: AsyncSession, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """Print summary of data in both databases."""
    from dataraum_context.storage import Column, Source, Table

    # SQLite summary
    sources = (await session.execute(select(Source))).scalars().all()
    tables = (await session.execute(select(Table))).scalars().all()
    columns = (await session.execute(select(Column))).scalars().all()

    print("\nDatabase Summary:")
    print("-" * 50)
    print("SQLite Metadata:")
    print(f"  Sources: {len(sources)}")
    print(f"  Tables: {len(tables)}")
    print(f"  Columns: {len(columns)}")

    if tables:
        print("\n  Tables by layer:")
        by_layer: dict[str, int] = {}
        for t in tables:
            by_layer[t.layer] = by_layer.get(t.layer, 0) + 1
        for layer, count in sorted(by_layer.items()):
            print(f"    {layer}: {count}")

    # DuckDB summary
    duckdb_tables = list_duckdb_tables(duckdb_conn)
    print(f"\nDuckDB Tables: {len(duckdb_tables)}")
    for tbl in sorted(duckdb_tables)[:10]:
        result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
        count = result[0] if result else 0
        print(f"  {tbl}: {count} rows")
    if len(duckdb_tables) > 10:
        print(f"  ... and {len(duckdb_tables) - 10} more")


# Example usage
if __name__ == "__main__":

    async def main() -> None:
        print("Test Infrastructure Demo")
        print("=" * 50)

        # Get connections (creates new DBs if they don't exist)
        session_factory = await get_test_session()
        duckdb_conn = get_duckdb_conn()

        async with session_factory() as session:
            await print_database_summary(session, duckdb_conn)

        await cleanup_connections()
        print("\nDone!")

    asyncio.run(main())
