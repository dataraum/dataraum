"""Context document endpoints."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from dataraum.api.deps import DuckDBDep, SessionDep
from dataraum.api.schemas import ContextResponse
from dataraum.graphs.context import build_execution_context, format_context_for_prompt
from dataraum.storage import Source, Table

router = APIRouter()


@router.get("/context/{source_id}", response_model=ContextResponse)  # type: ignore[untyped-decorator]
def get_context(
    source_id: str,
    session: SessionDep,
    duckdb: DuckDBDep,
) -> ContextResponse:
    """Get the full context document for a source.

    Returns the assembled context including:
    - Table and column metadata
    - Relationships
    - Entropy summary
    - Formatted prompt text for LLM consumption
    """
    # Verify source exists and get tables
    stmt = select(Source).where(Source.source_id == source_id)
    result = session.execute(stmt)
    source = result.scalar_one_or_none()

    if source is None:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")

    # Get table IDs for this source
    tables_stmt = select(Table.table_id).where(Table.source_id == source_id)
    tables_result = session.execute(tables_stmt)
    table_ids = [row[0] for row in tables_result.all()]

    if not table_ids:
        return ContextResponse(
            source_id=source_id,
            tables=[],
            relationships=[],
            entropy_summary=None,
            prompt_text="No tables found for this source.",
        )

    # Build execution context
    context = build_execution_context(
        session=session,
        duckdb_conn=duckdb,
        table_ids=table_ids,
    )

    # Format for prompt
    prompt_text = format_context_for_prompt(context)

    # Convert to response format
    tables_data = []
    for tc in context.tables:
        table_dict = {
            "table_id": tc.table_id,
            "name": tc.table_name,
            "row_count": tc.row_count,
            "table_entropy": tc.table_entropy,
            "readiness_for_use": tc.readiness_for_use,
            "columns": [
                {
                    "column_id": cc.column_id,
                    "name": cc.column_name,
                    "data_type": cc.data_type,
                    "semantic_role": cc.semantic_role,
                    "entity_type": cc.entity_type,
                    "business_concept": cc.business_concept,
                    "entropy_scores": cc.entropy_scores,
                    "resolution_hints": cc.resolution_hints,
                }
                for cc in tc.columns
            ],
        }
        tables_data.append(table_dict)

    relationships_data = []
    for rc in context.relationships:
        rel_dict = {
            "from_table": rc.from_table,
            "from_column": rc.from_column,
            "to_table": rc.to_table,
            "to_column": rc.to_column,
            "relationship_type": rc.relationship_type,
            "confidence": rc.confidence,
            "relationship_entropy": rc.relationship_entropy,
        }
        relationships_data.append(rel_dict)

    return ContextResponse(
        source_id=source_id,
        tables=tables_data,
        relationships=relationships_data,
        entropy_summary=context.entropy_summary,
        prompt_text=prompt_text,
    )
