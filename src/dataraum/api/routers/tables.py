"""Table and column browsing endpoints."""

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from dataraum.api.deps import PaginationDep, SessionDep
from dataraum.api.schemas import ColumnResponse, TableListResponse, TableResponse
from dataraum.storage import Column, Table

router = APIRouter()


def _get_column_semantic(column: Column) -> tuple[str | None, str | None, str | None]:
    """Extract semantic fields from column's annotation if present."""
    ann = column.semantic_annotation
    if ann is None:
        return None, None, None
    return ann.business_description, ann.semantic_role, ann.entity_type


@router.get("/tables", response_model=TableListResponse)
def list_tables(
    session: SessionDep,
    pagination: PaginationDep,
    source_id: str | None = Query(default=None, description="Filter by source ID"),
) -> TableListResponse:
    """List all tables with optional source filter."""
    skip, limit = pagination

    # Build base query
    base_query = select(Table)
    count_query = select(func.count()).select_from(Table)

    if source_id:
        base_query = base_query.where(Table.source_id == source_id)
        count_query = count_query.where(Table.source_id == source_id)

    # Count total
    total = session.execute(count_query).scalar() or 0

    # Get tables with columns and semantic annotations
    stmt = (
        base_query.options(selectinload(Table.columns).selectinload(Column.semantic_annotation))
        .offset(skip)
        .limit(limit)
    )
    result = session.execute(stmt)
    tables = result.scalars().all()

    return TableListResponse(
        tables=[
            TableResponse(
                table_id=t.table_id,
                name=t.table_name,
                source_id=t.source_id,
                row_count=t.row_count,
                columns=[
                    ColumnResponse(
                        column_id=c.column_id,
                        name=c.column_name,
                        position=c.column_position,
                        resolved_type=c.resolved_type,
                        nullable=True,  # Default, not tracked in Column model
                        business_description=_get_column_semantic(c)[0],
                        semantic_role=_get_column_semantic(c)[1],
                        entity_type=_get_column_semantic(c)[2],
                    )
                    for c in sorted(t.columns, key=lambda x: x.column_position)
                ],
            )
            for t in tables
        ],
        total=total,
    )


@router.get("/tables/{table_id}", response_model=TableResponse)
def get_table(
    table_id: str,
    session: SessionDep,
) -> TableResponse:
    """Get a single table with its columns."""
    stmt = (
        select(Table)
        .options(selectinload(Table.columns).selectinload(Column.semantic_annotation))
        .where(Table.table_id == table_id)
    )
    result = session.execute(stmt)
    table = result.scalar_one_or_none()

    if table is None:
        raise HTTPException(status_code=404, detail=f"Table {table_id} not found")

    return TableResponse(
        table_id=table.table_id,
        name=table.table_name,
        source_id=table.source_id,
        row_count=table.row_count,
        columns=[
            ColumnResponse(
                column_id=c.column_id,
                name=c.column_name,
                position=c.column_position,
                resolved_type=c.resolved_type,
                nullable=True,
                business_description=_get_column_semantic(c)[0],
                semantic_role=_get_column_semantic(c)[1],
                entity_type=_get_column_semantic(c)[2],
            )
            for c in sorted(table.columns, key=lambda x: x.column_position)
        ],
    )


@router.get("/columns/{column_id}", response_model=ColumnResponse)
def get_column(
    column_id: str,
    session: SessionDep,
) -> ColumnResponse:
    """Get a single column by ID."""
    stmt = (
        select(Column)
        .options(selectinload(Column.semantic_annotation))
        .where(Column.column_id == column_id)
    )
    result = session.execute(stmt)
    column = result.scalar_one_or_none()

    if column is None:
        raise HTTPException(status_code=404, detail=f"Column {column_id} not found")

    desc, role, entity = _get_column_semantic(column)

    return ColumnResponse(
        column_id=column.column_id,
        name=column.column_name,
        position=column.column_position,
        resolved_type=column.resolved_type,
        nullable=True,
        business_description=desc,
        semantic_role=role,
        entity_type=entity,
    )
