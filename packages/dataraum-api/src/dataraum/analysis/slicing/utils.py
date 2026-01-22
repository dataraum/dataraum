"""Utility functions for slicing analysis.

Provides helpers for loading context data from previous analysis phases.
"""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.db_models import (
    CategoricalAssociation,
    ColumnCorrelation,
)
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.statistics.db_models import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)
from dataraum.storage import Column, Table


def load_slicing_context(
    session: Session,
    table_ids: list[str],
) -> dict[str, Any]:
    """Load all context data needed for slicing analysis.

    Loads:
    - Table and column metadata
    - Statistical profiles
    - Semantic annotations
    - Correlation analysis results
    - Statistical quality metrics (Benford's Law, outliers)

    Args:
        session: Database session
        table_ids: Table IDs to load context for

    Returns:
        Dict with tables, statistics, semantic, correlations, quality keys
    """
    tables_data = _load_tables_with_columns(session, table_ids)
    statistics_data = _load_statistics(session, table_ids)
    semantic_data = _load_semantic_annotations(session, table_ids)
    correlations_data = _load_correlations(session, table_ids)
    quality_data = _load_quality_metrics(session, table_ids)

    return {
        "tables": tables_data,
        "statistics": statistics_data,
        "semantic": semantic_data,
        "correlations": correlations_data,
        "quality": quality_data,
    }


def _load_tables_with_columns(
    session: Session,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load table metadata with column information.

    Args:
        session: Database session
        table_ids: Table IDs to load

    Returns:
        List of table dicts with columns
    """
    result = []

    for table_id in table_ids:
        table = session.get(Table, table_id)
        if not table:
            continue

        # Load columns for this table
        columns_stmt = (
            select(Column).where(Column.table_id == table_id).order_by(Column.column_position)
        )
        columns_result = session.execute(columns_stmt)
        columns = columns_result.scalars().all()

        columns_data = [
            {
                "column_id": col.column_id,
                "column_name": col.column_name,
                "column_position": col.column_position,
                "raw_type": col.raw_type,
                "resolved_type": col.resolved_type,
            }
            for col in columns
        ]

        result.append(
            {
                "table_id": table.table_id,
                "table_name": table.table_name,
                "duckdb_path": table.duckdb_path,
                "layer": table.layer,
                "columns": columns_data,
            }
        )

    return result


def _load_statistics(
    session: Session,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load statistical profiles for tables.

    Args:
        session: Database session
        table_ids: Table IDs to load stats for

    Returns:
        List of statistics dicts
    """
    result = []

    # Get columns for these tables
    columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns_result = session.execute(columns_stmt)
    columns = columns_result.scalars().all()
    column_ids = [col.column_id for col in columns]

    # Build column lookup
    column_lookup = {col.column_id: col for col in columns}

    # Get table lookup
    table_lookup = {}
    for table_id in table_ids:
        table = session.get(Table, table_id)
        if table:
            table_lookup[table_id] = table

    # Load profiles
    profiles_stmt = (
        select(StatisticalProfile)
        .where(StatisticalProfile.column_id.in_(column_ids))
        .where(StatisticalProfile.layer == "typed")
    )
    profiles_result = session.execute(profiles_stmt)
    profiles = profiles_result.scalars().all()

    for profile in profiles:
        col = column_lookup.get(profile.column_id)
        if not col:
            continue

        table = table_lookup.get(col.table_id)
        table_name = table.table_name if table else "unknown"

        # Extract top values from profile_data
        profile_data = profile.profile_data or {}
        top_values = profile_data.get("top_values", [])

        result.append(
            {
                "table_id": col.table_id,
                "table_name": table_name,
                "column_id": profile.column_id,
                "column_name": col.column_name,
                "total_count": profile.total_count,
                "null_count": profile.null_count,
                "distinct_count": profile.distinct_count,
                "cardinality_ratio": profile.cardinality_ratio,
                "is_unique": profile.is_unique,
                "is_numeric": profile.is_numeric,
                "top_values": top_values,
            }
        )

    return result


def _load_semantic_annotations(
    session: Session,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load semantic annotations for tables.

    Args:
        session: Database session
        table_ids: Table IDs to load annotations for

    Returns:
        List of annotation dicts
    """
    result = []

    # Get columns for these tables
    columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns_result = session.execute(columns_stmt)
    columns = columns_result.scalars().all()
    column_ids = [col.column_id for col in columns]

    # Build lookups
    column_lookup = {col.column_id: col for col in columns}
    table_lookup = {}
    for table_id in table_ids:
        table = session.get(Table, table_id)
        if table:
            table_lookup[table_id] = table

    # Load annotations
    annotations_stmt = select(SemanticAnnotation).where(
        SemanticAnnotation.column_id.in_(column_ids)
    )
    annotations_result = session.execute(annotations_stmt)
    annotations = annotations_result.scalars().all()

    for ann in annotations:
        col = column_lookup.get(ann.column_id)
        if not col:
            continue

        table = table_lookup.get(col.table_id)
        table_name = table.table_name if table else "unknown"

        result.append(
            {
                "table_id": col.table_id,
                "table_name": table_name,
                "column_id": ann.column_id,
                "column_name": col.column_name,
                "semantic_role": ann.semantic_role,
                "entity_type": ann.entity_type,
                "business_name": ann.business_name,
                "business_description": ann.business_description,
                "business_concept": ann.business_concept,
            }
        )

    return result


def _load_correlations(
    session: Session,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load correlation analysis results for tables.

    Args:
        session: Database session
        table_ids: Table IDs to load correlations for

    Returns:
        List of correlation dicts
    """
    result = []

    # Get columns for these tables
    columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns_result = session.execute(columns_stmt)
    columns = columns_result.scalars().all()
    column_ids = [col.column_id for col in columns]

    # Build lookups
    column_lookup = {col.column_id: col for col in columns}

    # Load categorical associations (most relevant for slicing)
    assoc_stmt = select(CategoricalAssociation).where(
        CategoricalAssociation.column1_id.in_(column_ids)
    )
    assoc_result = session.execute(assoc_stmt)
    associations = assoc_result.scalars().all()

    for assoc in associations:
        col1 = column_lookup.get(assoc.column1_id)
        col2 = column_lookup.get(assoc.column2_id)
        if not col1 or not col2:
            continue

        result.append(
            {
                "type": "categorical_association",
                "column1_id": assoc.column1_id,
                "column1_name": col1.column_name,
                "column2_id": assoc.column2_id,
                "column2_name": col2.column_name,
                "cramers_v": assoc.cramers_v,
                "is_significant": assoc.is_significant,
            }
        )

    # Load numeric correlations
    corr_stmt = select(ColumnCorrelation).where(ColumnCorrelation.column1_id.in_(column_ids))
    corr_result = session.execute(corr_stmt)
    correlations = corr_result.scalars().all()

    for corr in correlations:
        col1 = column_lookup.get(corr.column1_id)
        col2 = column_lookup.get(corr.column2_id)
        if not col1 or not col2:
            continue

        result.append(
            {
                "type": "numeric_correlation",
                "column1_id": corr.column1_id,
                "column1_name": col1.column_name,
                "column2_id": corr.column2_id,
                "column2_name": col2.column_name,
                "pearson_r": corr.pearson_r,
                "spearman_rho": corr.spearman_rho,
                "is_significant": corr.is_significant,
            }
        )

    return result


def _load_quality_metrics(
    session: Session,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load statistical quality metrics for tables.

    Loads Benford's Law compliance and outlier detection results
    from Phase 3b analysis.

    Args:
        session: Database session
        table_ids: Table IDs to load quality metrics for

    Returns:
        List of quality metric dicts
    """
    result = []

    # Get columns for these tables
    columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns_result = session.execute(columns_stmt)
    columns = columns_result.scalars().all()
    column_ids = [col.column_id for col in columns]

    # Build lookups
    column_lookup = {col.column_id: col for col in columns}
    table_lookup = {}
    for table_id in table_ids:
        table = session.get(Table, table_id)
        if table:
            table_lookup[table_id] = table

    # Load quality metrics
    quality_stmt = select(StatisticalQualityMetrics).where(
        StatisticalQualityMetrics.column_id.in_(column_ids)
    )
    quality_result = session.execute(quality_stmt)
    quality_metrics = quality_result.scalars().all()

    for qm in quality_metrics:
        col = column_lookup.get(qm.column_id)
        if not col:
            continue

        table = table_lookup.get(col.table_id)
        table_name = table.table_name if table else "unknown"

        # Extract relevant data from quality_data JSON
        quality_data = qm.quality_data or {}
        benford_data = quality_data.get("benford_analysis") or {}
        outlier_data = quality_data.get("outlier_analysis") or {}

        result.append(
            {
                "table_id": col.table_id,
                "table_name": table_name,
                "column_id": qm.column_id,
                "column_name": col.column_name,
                "benford_compliant": qm.benford_compliant,
                "benford_p_value": benford_data.get("p_value"),
                "benford_chi_squared": benford_data.get("chi_squared"),
                "has_outliers": qm.has_outliers,
                "iqr_outlier_ratio": qm.iqr_outlier_ratio,
                "isolation_forest_anomaly_ratio": qm.isolation_forest_anomaly_ratio,
                "iqr_outlier_count": outlier_data.get("iqr_outlier_count"),
                "isolation_forest_outlier_count": outlier_data.get(
                    "isolation_forest_outlier_count"
                ),
            }
        )

    return result


__all__ = ["load_slicing_context"]
