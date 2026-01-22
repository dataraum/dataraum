"""SQL query execution endpoint."""

from typing import Any

from fastapi import APIRouter, HTTPException

from dataraum.api.deps import DuckDBDep
from dataraum.api.schemas import QueryRequest, QueryResponse

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
