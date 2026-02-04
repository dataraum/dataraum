"""Source management endpoints."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select

from dataraum.api.deps import PaginationDep, SessionDep
from dataraum.api.schemas import SourceCreate, SourceListResponse, SourceResponse
from dataraum.storage import Source, Table

router = APIRouter()


@router.get("/sources", response_model=SourceListResponse)
def list_sources(
    session: SessionDep,
    pagination: PaginationDep,
) -> SourceListResponse:
    """List all sources with pagination."""
    skip, limit = pagination

    # Count total
    count_stmt = select(func.count()).select_from(Source)
    total = session.execute(count_stmt).scalar() or 0

    # Get sources with table counts
    stmt = (
        select(
            Source,
            func.count(Table.table_id).label("table_count"),
        )
        .outerjoin(Table, Source.source_id == Table.source_id)
        .group_by(Source.source_id)
        .offset(skip)
        .limit(limit)
    )
    result = session.execute(stmt)
    rows = result.all()

    sources = [
        SourceResponse(
            source_id=row.Source.source_id,
            name=row.Source.name,
            source_type=row.Source.source_type,
            path=row.Source.connection_config.get("path") if row.Source.connection_config else None,
            created_at=row.Source.created_at,
            table_count=row.table_count,
        )
        for row in rows
    ]

    return SourceListResponse(sources=sources, total=total)


@router.get("/sources/{source_id}", response_model=SourceResponse)
def get_source(
    source_id: str,
    session: SessionDep,
) -> SourceResponse:
    """Get a single source by ID."""
    # Get source with table count
    stmt = (
        select(
            Source,
            func.count(Table.table_id).label("table_count"),
        )
        .outerjoin(Table, Source.source_id == Table.source_id)
        .where(Source.source_id == source_id)
        .group_by(Source.source_id)
    )
    result = session.execute(stmt)
    row = result.first()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")

    return SourceResponse(
        source_id=row.Source.source_id,
        name=row.Source.name,
        source_type=row.Source.source_type,
        path=row.Source.connection_config.get("path") if row.Source.connection_config else None,
        created_at=row.Source.created_at,
        table_count=row.table_count,
    )


@router.post("/sources", response_model=SourceResponse, status_code=201)
def create_source(
    source: SourceCreate,
    session: SessionDep,
) -> SourceResponse:
    """Create a new source."""
    from uuid import uuid4

    # Store path in connection_config if provided
    connection_config = {"path": source.path} if source.path else None

    db_source = Source(
        source_id=str(uuid4()),
        name=source.name,
        source_type=source.source_type,
        connection_config=connection_config,
    )
    session.add(db_source)
    session.flush()
    session.refresh(db_source)

    return SourceResponse(
        source_id=db_source.source_id,
        name=db_source.name,
        source_type=db_source.source_type,
        path=db_source.connection_config.get("path") if db_source.connection_config else None,
        created_at=db_source.created_at,
        table_count=0,
    )
