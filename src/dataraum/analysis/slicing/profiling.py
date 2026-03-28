"""Build ColumnSliceProfile records from per-slice StatisticalProfile data.

After slice_analysis creates per-slice statistical profiles, this module
aggregates them into ColumnSliceProfile records keyed by source column +
slice value. These records are consumed by the dimensional_entropy detector.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.slicing.db_models import ColumnSliceProfile, SliceDefinition
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def build_slice_profiles(
    session: Session,
    source_id: str,
) -> int:
    """Build ColumnSliceProfile records from per-slice statistical profiles.

    For each slice definition, reads StatisticalProfile records from the
    slice tables and creates ColumnSliceProfile records that map back to
    the source (or slicing_view) columns.

    Args:
        session: Database session.
        source_id: Source ID to process.

    Returns:
        Number of profiles created.
    """
    # Get typed tables
    typed_tables = list(
        session.execute(select(Table).where(Table.layer == "typed", Table.source_id == source_id))
        .scalars()
        .all()
    )
    if not typed_tables:
        return 0

    table_ids = [t.table_id for t in typed_tables]

    # Get slice definitions
    slice_defs = list(
        session.execute(select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids)))
        .scalars()
        .all()
    )
    if not slice_defs:
        return 0

    # Get all slice tables
    slice_tables = {
        t.table_name: t
        for t in session.execute(
            select(Table).where(Table.layer == "slice", Table.source_id == source_id)
        )
        .scalars()
        .all()
    }

    total_created = 0

    for slice_def in slice_defs:
        source_table = session.get(Table, slice_def.table_id)
        if not source_table:
            continue

        slice_column = session.get(Column, slice_def.column_id)
        effective_slice_col_name = slice_def.column_name or (
            slice_column.column_name if slice_column else "unknown"
        )

        # Delete existing profiles for this slice definition
        existing = list(
            session.execute(
                select(ColumnSliceProfile).where(
                    ColumnSliceProfile.slice_column_id == slice_def.column_id,
                    ColumnSliceProfile.slice_column_name == effective_slice_col_name,
                )
            )
            .scalars()
            .all()
        )
        for e in existing:
            session.delete(e)

        # Resolve effective table (prefer slicing_view for enriched columns)
        sv_table = session.execute(
            select(Table).where(
                Table.source_id == source_id,
                Table.table_name == f"slicing_{source_table.table_name}",
                Table.layer == "slicing_view",
            )
        ).scalar_one_or_none()
        effective_table = sv_table if sv_table else source_table

        # Get source column names to exclude slice definition columns
        slice_def_col_ids = set(
            session.execute(
                select(SliceDefinition.column_id).where(
                    SliceDefinition.table_id == source_table.table_id
                )
            )
            .scalars()
            .all()
        )
        slice_def_col_names = (
            set(
                session.execute(
                    select(Column.column_name).where(Column.column_id.in_(slice_def_col_ids))
                )
                .scalars()
                .all()
            )
            if slice_def_col_ids
            else set()
        )

        # Get effective table columns (excluding slice definition columns)
        effective_cols = [
            c
            for c in session.execute(
                select(Column).where(Column.table_id == effective_table.table_id)
            )
            .scalars()
            .all()
            if c.column_name not in slice_def_col_names
        ]
        effective_col_by_name = {c.column_name: c for c in effective_cols}

        # Process each slice value
        for slice_value in slice_def.distinct_values or []:
            # Find slice table
            import re

            safe_source = re.sub(r"[^a-zA-Z0-9]", "_", source_table.table_name)
            safe_source = re.sub(r"_+", "_", safe_source).strip("_").lower()
            safe_col = re.sub(r"[^a-zA-Z0-9]", "_", effective_slice_col_name)
            safe_col = re.sub(r"_+", "_", safe_col).strip("_").lower()
            safe_val = re.sub(r"[^a-zA-Z0-9]", "_", str(slice_value))
            safe_val = re.sub(r"_+", "_", safe_val).strip("_").lower() or "unknown"
            slice_table_name = f"slice_{safe_source}_{safe_col}_{safe_val}"

            slice_table = slice_tables.get(slice_table_name)
            if not slice_table:
                continue

            # Get statistical profiles for this slice table's columns
            slice_cols = list(
                session.execute(select(Column).where(Column.table_id == slice_table.table_id))
                .scalars()
                .all()
            )
            slice_col_ids = [c.column_id for c in slice_cols]
            if not slice_col_ids:
                continue

            profiles_by_col = {}
            for p in (
                session.execute(
                    select(StatisticalProfile).where(
                        StatisticalProfile.column_id.in_(slice_col_ids),
                        StatisticalProfile.layer == "typed",
                    )
                )
                .scalars()
                .all()
            ):
                col = next((c for c in slice_cols if c.column_id == p.column_id), None)
                if col:
                    profiles_by_col[col.column_name] = p

            # Create ColumnSliceProfile for each source column
            for col_name, eff_col in effective_col_by_name.items():
                stat_profile = profiles_by_col.get(col_name)
                if not stat_profile:
                    continue

                session.add(
                    ColumnSliceProfile(
                        source_column_id=eff_col.column_id,
                        slice_column_id=slice_def.column_id,
                        source_table_name=effective_table.table_name,
                        column_name=col_name,
                        slice_column_name=effective_slice_col_name,
                        slice_value=str(slice_value),
                        row_count=stat_profile.total_count,
                        null_ratio=stat_profile.null_ratio,
                        distinct_count=stat_profile.distinct_count,
                        quality_score=1.0 - (stat_profile.null_ratio or 0.0),
                        has_issues=(stat_profile.null_ratio or 0.0) > 0.2,
                        issue_count=1 if (stat_profile.null_ratio or 0.0) > 0.2 else 0,
                    )
                )
                total_created += 1

    logger.info("slice_profiles_built", source_id=source_id, profiles_created=total_created)
    return total_created
