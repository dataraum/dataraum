"""Calculation graph endpoints."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select

from dataraum.api.deps import DuckDBDep, PaginationDep, SessionDep
from dataraum.api.schemas import (
    GraphExecuteRequest,
    GraphExecuteResponse,
    GraphListResponse,
    GraphResponse,
)
from dataraum.graphs.db_models import GeneratedCodeRecord

router = APIRouter()


@router.get("/graphs", response_model=GraphListResponse)  # type: ignore[untyped-decorator]
def list_graphs(
    session: SessionDep,
    pagination: PaginationDep,
) -> GraphListResponse:
    """List all generated code records (graphs).

    Note: This returns generated SQL code records, not abstract graph definitions.
    """
    skip, limit = pagination

    # Count total
    count_stmt = select(func.count()).select_from(GeneratedCodeRecord)
    total = session.execute(count_stmt).scalar() or 0

    # Get records
    stmt = select(GeneratedCodeRecord).offset(skip).limit(limit)
    result = session.execute(stmt)
    records = result.scalars().all()

    return GraphListResponse(
        graphs=[
            GraphResponse(
                graph_id=r.graph_id,
                name=f"Graph {r.graph_id[:8]}",
                description=f"Generated at {r.generated_at}",
                target_table=r.schema_mapping_id,
                metrics=[],
                created_at=r.generated_at,
            )
            for r in records
        ],
        total=total,
    )


@router.get("/graphs/{graph_id}", response_model=GraphResponse)  # type: ignore[untyped-decorator]
def get_graph(
    graph_id: str,
    session: SessionDep,
) -> GraphResponse:
    """Get a single generated code record."""
    stmt = select(GeneratedCodeRecord).where(GeneratedCodeRecord.graph_id == graph_id)
    result = session.execute(stmt)
    record = result.scalar_one_or_none()

    if record is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")

    return GraphResponse(
        graph_id=record.graph_id,
        name=f"Graph {record.graph_id[:8]}",
        description=f"Generated at {record.generated_at}",
        target_table=record.schema_mapping_id,
        metrics=[],
        created_at=record.generated_at,
    )


@router.post("/graphs/{graph_id}/execute", response_model=GraphExecuteResponse)  # type: ignore[untyped-decorator]
def execute_graph(
    graph_id: str,
    request: GraphExecuteRequest,
    session: SessionDep,
    duckdb: DuckDBDep,
) -> GraphExecuteResponse:
    """Execute a graph's SQL.

    Retrieves the generated SQL and executes it against DuckDB.
    """
    from uuid import uuid4

    # Get the generated code record
    stmt = select(GeneratedCodeRecord).where(GeneratedCodeRecord.graph_id == graph_id)
    result = session.execute(stmt)
    record = result.scalar_one_or_none()

    if record is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")

    execution_id = str(uuid4())

    try:
        # Execute the final SQL
        sql = record.final_sql
        query_result = duckdb.execute(sql)
        columns = [desc[0] for desc in query_result.description]
        rows = query_result.fetchall()

        # Convert to list of dicts
        result_data = []
        for row in rows[:1000]:  # Limit results
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                if hasattr(value, "isoformat"):
                    value = value.isoformat()
                elif hasattr(value, "__float__"):
                    value = float(value)
                row_dict[col] = value
            result_data.append(row_dict)

        return GraphExecuteResponse(
            execution_id=execution_id,
            graph_id=graph_id,
            sql=sql,
            result=result_data,
            row_count=len(result_data),
            assumptions=[],
            entropy_warnings=[],
        )

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Graph execution failed: {e!s}",
        ) from e
