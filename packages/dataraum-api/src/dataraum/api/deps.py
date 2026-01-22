"""FastAPI dependency injection.

Provides database sessions and DuckDB cursors for route handlers.
Uses the shared ConnectionManager from core/connections.py.
"""

from collections.abc import Generator
from typing import Annotated

import duckdb
from fastapi import Depends, Query
from sqlalchemy.orm import Session

from dataraum.core.connections import get_connection_manager


def get_session() -> Generator[Session]:
    """Get a sync SQLAlchemy session.

    FastAPI runs sync endpoints in a thread pool, so this is efficient.
    Uses the shared ConnectionManager's session_scope.
    """
    manager = get_connection_manager()
    with manager.session_scope() as session:
        yield session


def get_duckdb_cursor() -> Generator[duckdb.DuckDBPyConnection]:
    """Get a DuckDB cursor for read-only operations.

    Creates a new cursor from the static connection per request.
    Uses the shared ConnectionManager's duckdb_cursor context manager.
    """
    manager = get_connection_manager()
    with manager.duckdb_cursor() as cursor:
        yield cursor


# Type aliases for dependency injection
SessionDep = Annotated[Session, Depends(get_session)]
DuckDBDep = Annotated[duckdb.DuckDBPyConnection, Depends(get_duckdb_cursor)]


# Common query parameters
def pagination_params(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
) -> tuple[int, int]:
    """Common pagination parameters."""
    return skip, limit


PaginationDep = Annotated[tuple[int, int], Depends(pagination_params)]
