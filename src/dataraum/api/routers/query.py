"""SQL query execution endpoint."""

from typing import Any

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from dataraum.api.deps import DuckDBDep, ManagerDep, SessionDep
from dataraum.api.schemas import (
    QueryAgentRequest,
    QueryAgentResponse,
    QueryAssumptionResponse,
    QueryLibraryEntryResponse,
    QueryLibraryListResponse,
    QueryLibrarySaveRequest,
    QueryLibrarySaveResponse,
    QueryLibrarySearchRequest,
    QueryLibrarySearchResponse,
    QueryLibrarySearchResult,
    QueryRequest,
    QueryResponse,
)
from dataraum.query.document import QueryAssumptionData, QueryDocument, SQLStep
from dataraum.storage import Source, Table

router = APIRouter()


@router.post("/query", response_model=QueryResponse)  # type: ignore[untyped-decorator]
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


@router.post("/query/agent", response_model=QueryAgentResponse)  # type: ignore[untyped-decorator]
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
        entropy_score=round(query_result.entropy_score, 3)
        if query_result.entropy_score is not None
        else None,
        assumptions=assumptions,
        contract=query_result.contract,
        interpreted_question=query_result.interpreted_question,
        metric_type=query_result.metric_type,
        validation_notes=query_result.validation_notes,
        success=query_result.success,
        error=query_result.error,
    )


# =============================================================================
# Query Library Endpoints
# =============================================================================


@router.get("/query/library/{source_id}", response_model=QueryLibraryListResponse)  # type: ignore[untyped-decorator]
def list_library_entries(
    source_id: str,
    session: SessionDep,
    manager: ManagerDep,
    limit: int = 100,
    offset: int = 0,
) -> QueryLibraryListResponse:
    """List query library entries for a source.

    Returns saved queries that can be reused by the Query Agent.
    """
    from dataraum.query.library import QueryLibrary, QueryLibraryError

    # Verify source exists
    stmt = select(Source).where(Source.source_id == source_id)
    source = session.execute(stmt).scalar_one_or_none()
    if source is None:
        raise HTTPException(status_code=404, detail=f"Source not found: {source_id}")

    try:
        library = QueryLibrary(session, manager)
    except QueryLibraryError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    entries = library.list_entries(source_id, limit=limit, offset=offset)
    total = library.count(source_id)

    return QueryLibraryListResponse(
        entries=[
            QueryLibraryEntryResponse(
                query_id=e.query_id,
                source_id=e.source_id,
                original_question=e.original_question,
                graph_id=e.graph_id,
                name=e.name,
                description=e.description,
                final_sql=e.final_sql,
                column_mappings=e.column_mappings,
                assumptions=e.assumptions,
                confidence_level=e.confidence_level,
                usage_count=e.usage_count,
                created_at=e.created_at,
                last_used_at=e.last_used_at,
            )
            for e in entries
        ],
        total=total,
    )


@router.get("/query/library/{source_id}/{query_id}", response_model=QueryLibraryEntryResponse)  # type: ignore[untyped-decorator]
def get_library_entry(
    source_id: str,
    query_id: str,
    session: SessionDep,
    manager: ManagerDep,
) -> QueryLibraryEntryResponse:
    """Get a specific query library entry."""
    from dataraum.query.library import QueryLibrary, QueryLibraryError

    try:
        library = QueryLibrary(session, manager)
    except QueryLibraryError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    entry = library.get_entry(query_id)

    if entry is None or entry.source_id != source_id:
        raise HTTPException(status_code=404, detail=f"Entry not found: {query_id}")

    return QueryLibraryEntryResponse(
        query_id=entry.query_id,
        source_id=entry.source_id,
        original_question=entry.original_question,
        graph_id=entry.graph_id,
        name=entry.name,
        description=entry.description,
        final_sql=entry.final_sql,
        column_mappings=entry.column_mappings,
        assumptions=entry.assumptions,
        confidence_level=entry.confidence_level,
        usage_count=entry.usage_count,
        created_at=entry.created_at,
        last_used_at=entry.last_used_at,
    )


@router.post("/query/library/{source_id}", response_model=QueryLibrarySaveResponse)  # type: ignore[untyped-decorator]
def save_to_library(
    source_id: str,
    request: QueryLibrarySaveRequest,
    session: SessionDep,
    manager: ManagerDep,
) -> QueryLibrarySaveResponse:
    """Save a query to the library for future reuse.

    The query will be indexed for semantic search, allowing
    similar questions to find and reuse this SQL.
    """
    from dataraum.query.library import QueryLibrary, QueryLibraryError

    # Verify source exists
    stmt = select(Source).where(Source.source_id == source_id)
    source = session.execute(stmt).scalar_one_or_none()
    if source is None:
        raise HTTPException(status_code=404, detail=f"Source not found: {source_id}")

    try:
        library = QueryLibrary(session, manager)
    except QueryLibraryError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e

    # Build QueryDocument from request
    document = QueryDocument(
        summary=request.summary,
        steps=[
            SQLStep(
                step_id=s.get("step_id", ""),
                sql=s.get("sql", ""),
                description=s.get("description", ""),
            )
            for s in request.steps
        ],
        final_sql=request.sql,
        column_mappings=request.column_mappings,
        assumptions=[
            QueryAssumptionData(
                dimension=a.get("dimension", ""),
                target=a.get("target", ""),
                assumption=a.get("assumption", ""),
                basis=a.get("basis", "inferred"),
                confidence=a.get("confidence", 0.5),
            )
            for a in request.assumptions
        ],
    )

    entry = library.save(
        source_id=source_id,
        document=document,
        original_question=request.question,
        name=request.name,
        description=request.description,
        confidence_level=request.confidence_level,
    )

    session.commit()

    return QueryLibrarySaveResponse(
        query_id=entry.query_id,
        message="Query saved to library",
    )


@router.post("/query/library/{source_id}/search", response_model=QueryLibrarySearchResponse)  # type: ignore[untyped-decorator]
def search_library(
    source_id: str,
    request: QueryLibrarySearchRequest,
    session: SessionDep,
    manager: ManagerDep,
) -> QueryLibrarySearchResponse:
    """Search the query library for similar questions.

    Uses semantic similarity to find queries that match the intent
    of the provided question.
    """
    from dataraum.query.library import QueryLibrary, QueryLibraryError

    # Verify source exists
    stmt = select(Source).where(Source.source_id == source_id)
    source = session.execute(stmt).scalar_one_or_none()
    if source is None:
        raise HTTPException(status_code=404, detail=f"Source not found: {source_id}")

    try:
        library = QueryLibrary(session, manager)
    except QueryLibraryError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    matches = library.find_similar_all(
        question=request.question,
        source_id=source_id,
        min_similarity=request.min_similarity,
        limit=request.limit,
    )

    return QueryLibrarySearchResponse(
        results=[
            QueryLibrarySearchResult(
                entry=QueryLibraryEntryResponse(
                    query_id=m.entry.query_id,
                    source_id=m.entry.source_id,
                    original_question=m.entry.original_question,
                    graph_id=m.entry.graph_id,
                    name=m.entry.name,
                    description=m.entry.description,
                    final_sql=m.entry.final_sql,
                    column_mappings=m.entry.column_mappings,
                    assumptions=m.entry.assumptions,
                    confidence_level=m.entry.confidence_level,
                    usage_count=m.entry.usage_count,
                    created_at=m.entry.created_at,
                    last_used_at=m.entry.last_used_at,
                ),
                similarity=round(m.similarity, 3),
            )
            for m in matches
        ],
        query=request.question,
    )


@router.delete("/query/library/{source_id}/{query_id}")  # type: ignore[untyped-decorator]
def delete_library_entry(
    source_id: str,
    query_id: str,
    session: SessionDep,
    manager: ManagerDep,
) -> dict[str, str]:
    """Delete a query from the library."""
    from dataraum.query.library import QueryLibrary, QueryLibraryError

    try:
        library = QueryLibrary(session, manager)
    except QueryLibraryError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e

    # Verify entry exists and belongs to source
    entry = library.get_entry(query_id)
    if entry is None or entry.source_id != source_id:
        raise HTTPException(status_code=404, detail=f"Entry not found: {query_id}")

    library.delete_entry(query_id)
    session.commit()

    return {"message": f"Entry {query_id} deleted"}
