"""Shared utility functions for semantic analysis."""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.storage import Column, Table


def load_table_mappings(
    session: Session,
    table_ids: list[str],
) -> dict[str, str]:
    """Load mapping of table_name -> table_id.

    Args:
        session: Database session
        table_ids: List of table IDs to load mappings for

    Returns:
        Dictionary mapping table_name to table_id
    """
    stmt = select(Table.table_name, Table.table_id).where(Table.table_id.in_(table_ids))
    result = session.execute(stmt)
    return dict(result.tuples().all())


def load_column_mappings(
    session: Session,
    table_ids: list[str],
) -> dict[tuple[str, str], str]:
    """Load mapping of (table_name, column_name) -> column_id.

    Args:
        session: Database session
        table_ids: List of table IDs to load mappings for

    Returns:
        Dictionary mapping (table_name, column_name) tuples to column_id
    """
    stmt = (
        select(Table.table_name, Column.column_name, Column.column_id)
        .join(Column)
        .where(Table.table_id.in_(table_ids))
    )
    result = session.execute(stmt)
    return {(table_name, col_name): col_id for table_name, col_name, col_id in result.all()}


def load_correlations_for_semantic(
    session: Session,
    table_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """Load within-table correlation data for semantic analysis context.

    Loads:
    - Functional dependencies (key identification)
    - Strong numeric correlations (column relationships)
    - Derived columns (computed columns)

    Args:
        session: Database session
        table_ids: List of table IDs to load correlations for

    Returns:
        Dictionary mapping table_name to correlation data:
        {
            "table_name": {
                "functional_dependencies": [...],
                "numeric_correlations": [...],
                "derived_columns": [...],
            }
        }
    """
    from dataraum.analysis.correlation.db_models import (
        ColumnCorrelation,
        DerivedColumn,
        FunctionalDependency,
    )

    # Load table name mapping
    table_map = load_table_mappings(session, table_ids)
    table_id_to_name = {v: k for k, v in table_map.items()}

    # Load column name mapping
    col_stmt = (
        select(Column.column_id, Column.column_name, Table.table_name)
        .join(Table)
        .where(Table.table_id.in_(table_ids))
    )
    col_result = session.execute(col_stmt)
    col_id_to_name = {col_id: col_name for col_id, col_name, _ in col_result.all()}

    result: dict[str, dict[str, Any]] = {}

    # Initialize result structure
    for table_name in table_map.keys():
        result[table_name] = {
            "functional_dependencies": [],
            "numeric_correlations": [],
            "derived_columns": [],
        }

    # Load functional dependencies
    fd_stmt = select(FunctionalDependency).where(FunctionalDependency.table_id.in_(table_ids))
    fd_result = session.execute(fd_stmt)

    for fd in fd_result.scalars().all():
        table_name_opt = table_id_to_name.get(fd.table_id)
        if not table_name_opt:
            continue
        table_name = table_name_opt

        # Resolve column names
        det_names = []
        for col_id in fd.determinant_column_ids:
            col_name = col_id_to_name.get(col_id, col_id)
            det_names.append(col_name)

        dep_name = col_id_to_name.get(fd.dependent_column_id, fd.dependent_column_id)

        result[table_name]["functional_dependencies"].append(
            {
                "determinant": det_names,
                "dependent": dep_name,
                "confidence": fd.confidence,
            }
        )

    # Load strong numeric correlations (only strong and very_strong)
    corr_stmt = select(ColumnCorrelation).where(
        ColumnCorrelation.table_id.in_(table_ids),
        ColumnCorrelation.correlation_strength.in_(["strong", "very_strong"]),
    )
    corr_result = session.execute(corr_stmt)

    for corr in corr_result.scalars().all():
        table_name_opt = table_id_to_name.get(corr.table_id)
        if not table_name_opt:
            continue
        table_name = table_name_opt

        col1_name = col_id_to_name.get(corr.column1_id, corr.column1_id)
        col2_name = col_id_to_name.get(corr.column2_id, corr.column2_id)

        result[table_name]["numeric_correlations"].append(
            {
                "column1": col1_name,
                "column2": col2_name,
                "pearson_r": corr.pearson_r,
                "spearman_rho": corr.spearman_rho,
                "strength": corr.correlation_strength,
            }
        )

    # Load derived columns
    derived_stmt = select(DerivedColumn).where(DerivedColumn.table_id.in_(table_ids))
    derived_result = session.execute(derived_stmt)

    for derived in derived_result.scalars().all():
        table_name_opt = table_id_to_name.get(derived.table_id)
        if not table_name_opt:
            continue
        table_name = table_name_opt

        derived_col_name = col_id_to_name.get(derived.derived_column_id, derived.derived_column_id)
        source_names = []
        for col_id in derived.source_column_ids:
            col_name = col_id_to_name.get(col_id, col_id)
            source_names.append(col_name)

        result[table_name]["derived_columns"].append(
            {
                "derived_column": derived_col_name,
                "source_columns": source_names,
                "formula": derived.formula,
                "match_rate": derived.match_rate,
            }
        )

    return result
