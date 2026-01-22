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
from dataraum.entropy.context import build_entropy_context
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

    # Get table IDs for this source
    tables_stmt = select(Table).where(Table.source_id == source_id)
    tables_result = session.execute(tables_stmt)
    tables = list(tables_result.scalars().all())

    if not tables:
        return EntropyDashboardResponse(
            source_id=source_id,
            overall_readiness="ready",
            tables=[],
            high_priority_resolutions=[],
        )

    table_ids = [t.table_id for t in tables]

    # Build entropy context
    entropy_context = build_entropy_context(
        session=session,
        table_ids=table_ids,
    )

    # Convert to dashboard format using to_dashboard_dict()
    dashboard = entropy_context.to_dashboard_dict()

    # Build response
    table_responses = []
    for table_data in dashboard.get("tables", []):
        compound_risks = [
            CompoundRiskResponse(
                risk_id=risk.get("risk_id", ""),
                dimensions=risk.get("dimensions", []),
                risk_level=risk.get("risk_level", "unknown"),
                impact=risk.get("impact", ""),
                combined_score=risk.get("combined_score", 0.0),
            )
            for risk in table_data.get("compound_risks", [])
        ]

        table_responses.append(
            TableEntropyResponse(
                table_id=table_data.get("table_id", ""),
                table_name=table_data.get("table_name", ""),
                avg_composite_score=table_data.get("avg_composite_score", 0.0),
                max_composite_score=table_data.get("max_composite_score", 0.0),
                blocked_column_count=table_data.get("blocked_column_count", 0),
                total_columns=table_data.get("total_columns", 0),
                readiness=table_data.get("readiness", "ready"),
                compound_risks=compound_risks,
            )
        )

    return EntropyDashboardResponse(
        source_id=source_id,
        overall_readiness=dashboard.get("overall_readiness", "ready"),
        tables=table_responses,
        high_priority_resolutions=dashboard.get("high_priority_resolutions", []),
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

    # Build entropy context for single table
    entropy_context = build_entropy_context(
        session=session,
        table_ids=[table_id],
    )

    # Get table profile from context (dict keyed by table_name)
    table_profile = entropy_context.table_profiles.get(table.table_name)

    if table_profile is None:
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

    compound_risks = [
        CompoundRiskResponse(
            risk_id=risk.risk_id,
            dimensions=risk.dimensions,
            risk_level=risk.risk_level,
            impact=risk.impact,
            combined_score=risk.combined_score,
        )
        for risk in table_profile.compound_risks
    ]

    return TableEntropyResponse(
        table_id=table_profile.table_id,
        table_name=table_profile.table_name,
        avg_composite_score=table_profile.avg_composite_score,
        max_composite_score=table_profile.max_composite_score,
        blocked_column_count=len(table_profile.blocked_columns),
        total_columns=len(table_profile.column_profiles),
        readiness=table_profile.readiness,
        compound_risks=compound_risks,
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

    # Build entropy context for the column's table
    entropy_context = build_entropy_context(
        session=session,
        table_ids=[column.table_id],
    )

    # Get column profile from context
    column_profile = entropy_context.get_column_entropy(column.table_id, column.column_name)

    if column_profile is None:
        return ColumnEntropyResponse(
            column_id=column_id,
            column_name=column.column_name,
            composite_score=0.0,
            readiness="ready",
            high_entropy_dimensions=[],
            resolution_hints=[],
            layer_scores={},
        )

    # Build layer scores dict from individual attributes
    layer_scores = {
        "structural": column_profile.structural_entropy,
        "semantic": column_profile.semantic_entropy,
        "value": column_profile.value_entropy,
        "computational": column_profile.computational_entropy,
    }

    return ColumnEntropyResponse(
        column_id=column_id,
        column_name=column_profile.column_name,
        composite_score=column_profile.composite_score,
        readiness=column_profile.readiness,
        high_entropy_dimensions=column_profile.high_entropy_dimensions,
        resolution_hints=[r.action for r in column_profile.top_resolution_hints[:5]],
        layer_scores=layer_scores,
    )
