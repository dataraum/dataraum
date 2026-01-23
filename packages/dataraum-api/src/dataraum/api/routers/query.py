"""SQL query execution endpoint."""

from typing import Any

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from dataraum.api.deps import DuckDBDep, SessionDep
from dataraum.api.schemas import (
    QueryAgentRequest,
    QueryAgentResponse,
    QueryAssumptionResponse,
    QueryRequest,
    QueryResponse,
)
from dataraum.storage import Source, Table

router = APIRouter()


@router.post("/query", response_model=QueryResponse)
def execute_query(
    request: QueryRequest,
    duckdb: DuckDBDep,
) -> QueryResponse:
    """Execute a read-only SQL query against DuckDB.

    The query is validated to ensure it's read-only (SELECT only).
    Results are limited to prevent memory issues.
    """
    sql = request.sql.strip()

    # Basic read-only validation
    # Note: This is a simple check - production should use proper SQL parsing
    sql_upper = sql.upper()
    if not sql_upper.startswith("SELECT"):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT queries are allowed",
        )

    # Check for dangerous keywords
    dangerous_keywords = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
    ]
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            raise HTTPException(
                status_code=400,
                detail=f"Query contains forbidden keyword: {keyword}",
            )

    # Add limit if not present
    if "LIMIT" not in sql_upper:
        sql = f"{sql} LIMIT {request.limit}"

    try:
        # Execute query
        result = duckdb.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows_raw = result.fetchmany(request.limit + 1)  # Fetch one extra to detect truncation

        # Check if truncated
        truncated = len(rows_raw) > request.limit
        if truncated:
            rows_raw = rows_raw[: request.limit]

        # Convert to list of dicts
        rows: list[dict[str, Any]] = []
        for row in rows_raw:
            row_dict: dict[str, Any] = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Convert non-JSON-serializable types
                if hasattr(value, "isoformat"):
                    value = value.isoformat()
                elif hasattr(value, "__float__"):
                    value = float(value)
                row_dict[col] = value
            rows.append(row_dict)

        return QueryResponse(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            truncated=truncated,
        )

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Query execution failed: {e!s}",
        ) from e


@router.post("/query/agent", response_model=QueryAgentResponse)
def query_agent(
    request: QueryAgentRequest,
    session: SessionDep,
    duckdb: DuckDBDep,
) -> QueryAgentResponse:
    """Answer a natural language question about the data.

    The Query Agent converts the question into SQL, executes it,
    and returns results with a confidence level based on data quality.

    Args:
        request: Query request with question and optional contract

    Returns:
        QueryAgentResponse with answer, data, and confidence level
    """
    from dataraum.query import answer_question

    # Verify source exists
    stmt = select(Source).where(Source.source_id == request.source_id)
    source = session.execute(stmt).scalar_one_or_none()

    if source is None:
        raise HTTPException(status_code=404, detail=f"Source not found: {request.source_id}")

    # Get tables
    tables_stmt = select(Table).where(Table.source_id == request.source_id)
    tables = list(session.execute(tables_stmt).scalars().all())

    if not tables:
        raise HTTPException(status_code=400, detail="Source has no tables")

    # Call the query agent
    result = answer_question(
        question=request.question,
        session=session,
        duckdb_conn=duckdb,
        source_id=request.source_id,
        contract=request.contract,
        auto_contract=request.auto_contract,
    )

    if not result.success or not result.value:
        raise HTTPException(
            status_code=500,
            detail=result.error or "Query agent failed",
        )

    query_result = result.value

    # Convert assumptions
    assumptions = [
        QueryAssumptionResponse(
            dimension=a.dimension,
            target=a.target,
            assumption=a.assumption,
            basis=a.basis.value,
            confidence=a.confidence,
        )
        for a in query_result.assumptions
    ]

    return QueryAgentResponse(
        execution_id=query_result.execution_id,
        question=query_result.question,
        answer=query_result.answer,
        sql=query_result.sql,
        data=query_result.data,
        columns=query_result.columns,
        confidence_level=query_result.confidence_level.value,
        confidence_emoji=query_result.confidence_level.emoji,
        confidence_label=query_result.confidence_level.label,
        entropy_score=round(query_result.entropy_score, 3),
        assumptions=assumptions,
        contract=query_result.contract,
        interpreted_question=query_result.interpreted_question,
        metric_type=query_result.metric_type,
        validation_notes=query_result.validation_notes,
        success=query_result.success,
        error=query_result.error,
    )
