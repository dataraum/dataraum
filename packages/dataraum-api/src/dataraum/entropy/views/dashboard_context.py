"""Entropy context for dashboard/API.

Provides EntropyForDashboard view for api/routers/entropy.py consumption.
This view is optimized for JSON serialization and UI display.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.analysis.aggregator import (
    ColumnSummary,
    EntropyAggregator,
    TableSummary,
)
from dataraum.entropy.core.storage import EntropyRepository
from dataraum.entropy.models import CompoundRisk
from dataraum.entropy.resolution import find_top_resolutions

if TYPE_CHECKING:
    from dataraum.entropy.interpretation import EntropyInterpretation

logger = get_logger(__name__)


@dataclass
class EntropyForDashboard:
    """Entropy context optimized for dashboard/API.

    Provides a JSON-serializable structure for UI display with:
    - Table-level summaries
    - Column details with interpretations
    - Compound risks
    - Top resolution priorities
    """

    # Source information
    source_id: str | None = None

    # Overall readiness
    overall_readiness: str = "investigate"

    # Table summaries
    tables: list[TableSummary] = field(default_factory=list)

    # Compound risks across all tables
    compound_risks: list[CompoundRisk] = field(default_factory=list)

    # Top resolution priorities
    top_resolutions: list[dict[str, Any]] = field(default_factory=list)

    # Column interpretations (if available)
    interpretations: dict[str, EntropyInterpretation] = field(default_factory=dict)

    # Summary statistics
    total_tables: int = 0
    total_columns: int = 0
    high_entropy_count: int = 0
    critical_entropy_count: int = 0
    compound_risk_count: int = 0

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns:
            Dictionary suitable for API response
        """
        # Build table summaries
        tables_data: list[dict[str, Any]] = []
        for table in self.tables:
            table_data: dict[str, Any] = {
                "table_id": table.table_id,
                "table_name": table.table_name,
                "avg_entropy": round(table.avg_composite_score, 2),
                "max_entropy": round(table.max_composite_score, 2),
                "readiness": table.readiness,
                "column_count": len(table.columns),
                "high_entropy_columns": table.high_entropy_columns,
                "blocked_columns": table.blocked_columns,
                "avg_composite_score": round(table.avg_composite_score, 2),
                "max_composite_score": round(table.max_composite_score, 2),
                "blocked_column_count": len(table.blocked_columns),
                "total_columns": len(table.columns),
            }
            tables_data.append(table_data)

        # Build column summaries with interpretations
        columns_data: list[dict[str, Any]] = []
        for table in self.tables:
            for col in table.columns:
                col_key = f"{col.table_name}.{col.column_name}"
                col_data: dict[str, Any] = {
                    "column_key": col_key,
                    "column_id": col.column_id,
                    "table_name": col.table_name,
                    "column_name": col.column_name,
                    "composite_score": round(col.composite_score, 2),
                    "readiness": col.readiness,
                    "high_entropy_dimensions": col.high_entropy_dimensions,
                    "layer_scores": {k: round(v, 2) for k, v in col.layer_scores.items()},
                }

                # Add interpretation if available
                interpretation = self.interpretations.get(col_key)
                if interpretation:
                    col_data["interpretation"] = interpretation.to_dashboard_dict()

                columns_data.append(col_data)

        # Build compound risks summary
        risks_data: list[dict[str, Any]] = []
        for risk in self.compound_risks:
            risks_data.append(
                {
                    "risk_id": risk.risk_id,
                    "target": risk.target,
                    "dimensions": risk.dimensions,
                    "risk_level": risk.risk_level,
                    "impact": risk.impact,
                    "combined_score": round(risk.combined_score, 2),
                }
            )

        return {
            "summary": {
                "source_id": self.source_id,
                "overall_readiness": self.overall_readiness,
                "total_tables": self.total_tables,
                "total_columns": self.total_columns,
                "high_entropy_count": self.high_entropy_count,
                "critical_entropy_count": self.critical_entropy_count,
                "compound_risk_count": self.compound_risk_count,
                "computed_at": self.computed_at.isoformat(),
            },
            "tables": tables_data,
            "columns": columns_data,
            "compound_risks": risks_data,
            "top_resolution_hints": self.top_resolutions,
        }


def build_for_dashboard(
    session: Session,
    source_id: str,
    *,
    include_interpretations: bool = False,
) -> EntropyForDashboard:
    """Build entropy context for dashboard API.

    Loads entropy for all typed tables in the source and returns
    a view optimized for JSON serialization and UI display.

    Args:
        session: SQLAlchemy session
        source_id: Source ID to build dashboard for
        include_interpretations: Whether to include LLM interpretations

    Returns:
        EntropyForDashboard with all data needed for UI
    """
    repo = EntropyRepository(session)
    aggregator = EntropyAggregator()

    # Get all typed table IDs for this source
    typed_table_ids = repo.get_typed_table_ids_for_source(source_id)

    if not typed_table_ids:
        return EntropyForDashboard(
            source_id=source_id,
            overall_readiness="ready",
        )

    table_map, column_map = repo.get_table_column_mapping(typed_table_ids)

    # Load entropy objects
    entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

    if not entropy_objects:
        # Return empty dashboard with table structure
        tables: list[TableSummary] = []
        for table_id, table in table_map.items():
            tables.append(
                TableSummary(
                    table_id=table_id,
                    table_name=table.table_name,
                    readiness="ready",
                )
            )
        return EntropyForDashboard(
            source_id=source_id,
            overall_readiness="ready",
            tables=tables,
            total_tables=len(tables),
        )

    # Aggregate into summaries
    column_summaries, table_summaries = aggregator.summarize_columns_by_table(
        entropy_objects=entropy_objects,
        table_map=table_map,
        column_map=column_map,
    )

    # Collect all compound risks
    all_risks: list[CompoundRisk] = []
    for col in column_summaries.values():
        all_risks.extend(col.compound_risks)

    # Find top resolutions
    top_cascades = find_top_resolutions(entropy_objects, limit=5)
    top_resolutions: list[dict[str, Any]] = [
        {
            "action": cascade.action,
            "description": cascade.description,
            "targets": cascade.affected_targets,
            "priority_score": round(cascade.priority_score, 2),
            "effort": cascade.effort,
            "total_reduction": round(cascade.total_reduction, 2),
            "dimensions_improved": cascade.dimensions_improved,
        }
        for cascade in top_cascades
    ]

    # Calculate statistics
    from dataraum.entropy.config import get_entropy_config

    config = get_entropy_config()
    high_threshold = config.high_entropy_threshold
    critical_threshold = config.critical_entropy_threshold

    high_entropy_count = sum(
        1 for c in column_summaries.values() if c.composite_score >= high_threshold
    )
    critical_entropy_count = sum(
        1 for c in column_summaries.values() if c.composite_score >= critical_threshold
    )

    # Determine overall readiness
    if critical_entropy_count > 0:
        overall_readiness = "blocked"
    elif high_entropy_count > 0:
        overall_readiness = "investigate"
    else:
        overall_readiness = "ready"

    # Load interpretations if requested
    interpretations: dict[str, EntropyInterpretation] = {}
    if include_interpretations:
        interpretations = _load_interpretations(session, column_summaries)

    return EntropyForDashboard(
        source_id=source_id,
        overall_readiness=overall_readiness,
        tables=list(table_summaries.values()),
        compound_risks=all_risks,
        top_resolutions=top_resolutions,
        interpretations=interpretations,
        total_tables=len(table_summaries),
        total_columns=len(column_summaries),
        high_entropy_count=high_entropy_count,
        critical_entropy_count=critical_entropy_count,
        compound_risk_count=len(all_risks),
    )


def _load_interpretations(
    session: Session,
    column_summaries: dict[str, ColumnSummary],
) -> dict[str, EntropyInterpretation]:
    """Load stored interpretations for columns.

    Args:
        session: SQLAlchemy session
        column_summaries: Column summaries to load interpretations for

    Returns:
        Dictionary mapping column key to interpretation
    """
    from sqlalchemy import select

    from dataraum.entropy.db_models import EntropyInterpretationRecord
    from dataraum.entropy.interpretation import (
        Assumption,
        EntropyInterpretation,
        ResolutionAction,
    )

    interpretations: dict[str, EntropyInterpretation] = {}

    # Get column IDs
    column_ids = [s.column_id for s in column_summaries.values() if s.column_id]

    if not column_ids:
        return interpretations

    # Load interpretation records
    stmt = select(EntropyInterpretationRecord).where(
        EntropyInterpretationRecord.column_id.in_(column_ids)
    )
    records = session.execute(stmt).scalars().all()

    for record in records:
        # Parse assumptions
        assumptions: list[Assumption] = []
        if record.assumptions_json:
            for a in record.assumptions_json:
                assumptions.append(
                    Assumption(
                        dimension=a.get("dimension", ""),
                        assumption_text=a.get("assumption_text", ""),
                        confidence=a.get("confidence", "medium"),
                        impact=a.get("impact", ""),
                        basis=a.get("basis", "inferred"),
                    )
                )

        # Parse resolution actions
        resolution_actions: list[ResolutionAction] = []
        if record.resolution_actions_json:
            for r in record.resolution_actions_json:
                resolution_actions.append(
                    ResolutionAction(
                        action=r.get("action", ""),
                        description=r.get("description", ""),
                        priority=r.get("priority", "medium"),
                        effort=r.get("effort", "medium"),
                        expected_impact=r.get("expected_impact", ""),
                        parameters=r.get("parameters", {}),
                    )
                )

        interpretation = EntropyInterpretation(
            column_name=record.column_name,
            table_name=record.table_name,
            assumptions=assumptions,
            resolution_actions=resolution_actions,
            explanation=record.explanation,
            composite_score=record.composite_score,
            readiness=record.readiness,
            model_used=record.model_used,
            from_cache=bool(record.from_cache),
        )

        key = f"{record.table_name}.{record.column_name}"
        interpretations[key] = interpretation

    return interpretations


def build_table_response(
    session: Session,
    table_id: str,
) -> dict[str, Any]:
    """Build entropy response for a single table.

    Args:
        session: SQLAlchemy session
        table_id: Table ID to build response for

    Returns:
        Dictionary with table entropy data
    """
    repo = EntropyRepository(session)
    aggregator = EntropyAggregator()

    # Verify table is typed
    typed_ids = repo.get_typed_table_ids([table_id])
    if not typed_ids:
        return {
            "table_id": table_id,
            "error": "Table not found or not typed",
        }

    table_map, column_map = repo.get_table_column_mapping([table_id])
    table = table_map.get(table_id)

    if not table:
        return {"table_id": table_id, "error": "Table not found"}

    entropy_objects = repo.load_for_tables([table_id], enforce_typed=True)

    if not entropy_objects:
        return {
            "table_id": table_id,
            "table_name": table.table_name,
            "readiness": "ready",
            "avg_composite_score": 0.0,
            "max_composite_score": 0.0,
            "columns": [],
        }

    column_summaries, table_summaries = aggregator.summarize_columns_by_table(
        entropy_objects=entropy_objects,
        table_map=table_map,
        column_map=column_map,
    )

    table_summary = table_summaries.get(table.table_name)
    if not table_summary:
        return {
            "table_id": table_id,
            "table_name": table.table_name,
            "readiness": "ready",
            "avg_composite_score": 0.0,
            "max_composite_score": 0.0,
            "columns": [],
        }

    return {
        "table_id": table_summary.table_id,
        "table_name": table_summary.table_name,
        "readiness": table_summary.readiness,
        "avg_composite_score": round(table_summary.avg_composite_score, 2),
        "max_composite_score": round(table_summary.max_composite_score, 2),
        "high_entropy_columns": table_summary.high_entropy_columns,
        "blocked_columns": table_summary.blocked_columns,
        "blocked_column_count": len(table_summary.blocked_columns),
        "total_columns": len(table_summary.columns),
        "columns": [
            {
                "column_id": c.column_id,
                "column_name": c.column_name,
                "composite_score": round(c.composite_score, 2),
                "readiness": c.readiness,
                "layer_scores": {k: round(v, 2) for k, v in c.layer_scores.items()},
            }
            for c in table_summary.columns
        ],
    }


def build_column_response(
    session: Session,
    column_id: str,
) -> dict[str, Any]:
    """Build entropy response for a single column.

    Args:
        session: SQLAlchemy session
        column_id: Column ID to build response for

    Returns:
        Dictionary with column entropy data
    """
    from sqlalchemy import select

    from dataraum.storage import Column, Table

    # Get column and table info
    stmt = select(Column).where(Column.column_id == column_id)
    column = session.execute(stmt).scalar_one_or_none()

    if not column:
        return {"column_id": column_id, "error": "Column not found"}

    # Verify table is typed
    table_stmt = select(Table).where(
        Table.table_id == column.table_id,
        Table.layer == "typed",
    )
    table = session.execute(table_stmt).scalar_one_or_none()

    if not table:
        return {"column_id": column_id, "error": "Column not in typed table"}

    repo = EntropyRepository(session)
    entropy_objects = repo.load_for_columns([column_id], enforce_typed=True)

    if not entropy_objects:
        return {
            "column_id": column_id,
            "column_name": column.column_name,
            "table_name": table.table_name,
            "readiness": "ready",
            "composite_score": 0.0,
            "layer_scores": {},
            "high_entropy_dimensions": [],
            "resolution_hints": [],
        }

    aggregator = EntropyAggregator()
    summary = aggregator.summarize_column(
        column_id=column_id,
        column_name=column.column_name,
        table_id=table.table_id,
        table_name=table.table_name,
        entropy_objects=entropy_objects,
    )

    return {
        "column_id": column_id,
        "column_name": summary.column_name,
        "table_name": summary.table_name,
        "readiness": summary.readiness,
        "composite_score": round(summary.composite_score, 2),
        "layer_scores": {k: round(v, 2) for k, v in summary.layer_scores.items()},
        "high_entropy_dimensions": summary.high_entropy_dimensions,
        "resolution_hints": [h.action for h in summary.top_resolution_hints[:5]],
    }
