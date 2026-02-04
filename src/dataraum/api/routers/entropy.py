"""Entropy dashboard endpoints."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from dataraum.api.deps import SessionDep
from dataraum.api.schemas import (
    ColumnEntropyResponse,
    CompoundRiskResponse,
    EntropyDashboardResponse,
    TableEntropyResponse,
)
from dataraum.entropy.views.dashboard_context import (
    build_column_response,
    build_for_dashboard,
    build_table_response,
)
from dataraum.storage import Column, Source, Table

router = APIRouter()


@router.get("/entropy/{source_id}", response_model=EntropyDashboardResponse)
def get_entropy_dashboard(
    source_id: str,
    session: SessionDep,
) -> EntropyDashboardResponse:
    """Get entropy dashboard for a source.

    Returns entropy profiles for all tables including:
    - Per-column entropy scores
    - High entropy dimensions
    - Compound risks
    - Resolution priorities
    """
    # Verify source exists
    stmt = select(Source).where(Source.source_id == source_id)
    result = session.execute(stmt)
    source = result.scalar_one_or_none()

    if source is None:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")

    # Build dashboard context using new views module (typed tables enforced internally)
    dashboard_ctx = build_for_dashboard(session, source_id)

    # Build response
    table_responses = []
    for table_summary in dashboard_ctx.tables:
        compound_risks = [
            CompoundRiskResponse(
                risk_id=risk.risk_id,
                dimensions=risk.dimensions,
                risk_level=risk.risk_level,
                impact=risk.impact,
                combined_score=risk.combined_score,
            )
            for risk in table_summary.compound_risks
        ]

        table_responses.append(
            TableEntropyResponse(
                table_id=table_summary.table_id,
                table_name=table_summary.table_name,
                avg_composite_score=table_summary.avg_composite_score,
                max_composite_score=table_summary.max_composite_score,
                blocked_column_count=len(table_summary.blocked_columns),
                total_columns=len(table_summary.columns),
                readiness=table_summary.readiness,
                compound_risks=compound_risks,
            )
        )

    return EntropyDashboardResponse(
        source_id=source_id,
        overall_readiness=dashboard_ctx.overall_readiness,
        tables=table_responses,
        high_priority_resolutions=dashboard_ctx.top_resolutions,
    )


@router.get("/entropy/table/{table_id}", response_model=TableEntropyResponse)
def get_table_entropy(
    table_id: str,
    session: SessionDep,
) -> TableEntropyResponse:
    """Get entropy profile for a single table."""
    # Verify table exists
    stmt = select(Table).where(Table.table_id == table_id)
    result = session.execute(stmt)
    table = result.scalar_one_or_none()

    if table is None:
        raise HTTPException(status_code=404, detail=f"Table {table_id} not found")

    # Build table response using new views module
    table_data = build_table_response(session, table_id)

    if "error" in table_data:
        return TableEntropyResponse(
            table_id=table_id,
            table_name=table.table_name,
            avg_composite_score=0.0,
            max_composite_score=0.0,
            blocked_column_count=0,
            total_columns=0,
            readiness="ready",
            compound_risks=[],
        )

    return TableEntropyResponse(
        table_id=table_data.get("table_id", table_id),
        table_name=table_data.get("table_name", table.table_name),
        avg_composite_score=table_data.get("avg_composite_score", 0.0),
        max_composite_score=table_data.get("max_composite_score", 0.0),
        blocked_column_count=table_data.get("blocked_column_count", 0),
        total_columns=table_data.get("total_columns", 0),
        readiness=table_data.get("readiness", "ready"),
        compound_risks=[],  # TODO: Add compound risks to build_table_response
    )


@router.get("/entropy/column/{column_id}", response_model=ColumnEntropyResponse)
def get_column_entropy(
    column_id: str,
    session: SessionDep,
) -> ColumnEntropyResponse:
    """Get entropy profile for a single column."""
    # Get column with table
    stmt = select(Column).where(Column.column_id == column_id)
    result = session.execute(stmt)
    column = result.scalar_one_or_none()

    if column is None:
        raise HTTPException(status_code=404, detail=f"Column {column_id} not found")

    # Build column response using new views module
    col_data = build_column_response(session, column_id)

    if "error" in col_data:
        return ColumnEntropyResponse(
            column_id=column_id,
            column_name=column.column_name,
            composite_score=0.0,
            readiness="ready",
            high_entropy_dimensions=[],
            resolution_hints=[],
            layer_scores={},
        )

    return ColumnEntropyResponse(
        column_id=column_id,
        column_name=col_data.get("column_name", column.column_name),
        composite_score=col_data.get("composite_score", 0.0),
        readiness=col_data.get("readiness", "ready"),
        high_entropy_dimensions=col_data.get("high_entropy_dimensions", []),
        resolution_hints=col_data.get("resolution_hints", []),
        layer_scores=col_data.get("layer_scores", {}),
    )
