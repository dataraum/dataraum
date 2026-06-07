"""Build ColumnSliceProfile records from per-slice StatisticalProfile data.

After slice_analysis creates per-slice statistical profiles, this module
aggregates them into ColumnSliceProfile records keyed by source column +
slice value. These records are consumed by the dimensional_entropy detector.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.slicing.db_models import ColumnSliceProfile, SliceDefinition
from dataraum.analysis.slicing.naming import slice_table_name, slicing_view_name
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def build_slice_profiles(
    session: Session,
    table_ids: list[str],
    *,
    session_id: str,
    run_id: str | None = None,
) -> int:
    """Build ColumnSliceProfile records from per-slice statistical profiles.

    For each slice definition, reads StatisticalProfile records from the
    slice tables and creates ColumnSliceProfile records that map back to
    the source (or slicing_view) columns.

    Source-free (DAT-403): scopes by the session's selected typed ``table_ids``
    (which may span sources) — never a single ``source_id``. The derived slice
    and slicing_view tables carry their fact table's source_id, so they are
    resolved through the session's source set rather than read directly off the
    selection.

    Args:
        session: Database session.
        table_ids: The session's selected typed table ids to process.
        session_id: Investigation session id stamped on the profiles.
        run_id: The begin_session run whose slice definitions to profile
            (run-versioned, DAT-448).

    Returns:
        Number of profiles created.
    """
    # Get typed tables
    typed_tables = list(
        session.execute(select(Table).where(Table.layer == "typed", Table.table_id.in_(table_ids)))
        .scalars()
        .all()
    )
    if not typed_tables:
        return 0

    typed_table_ids = [t.table_id for t in typed_tables]
    # Derived slice/slicing_view tables carry their fact table's source_id; scope
    # them by the session's source set.
    source_ids = {t.source_id for t in typed_tables}

    # Get THIS run's slice definitions (run-versioned, DAT-448)
    slice_defs = list(
        session.execute(
            select(SliceDefinition).where(
                SliceDefinition.table_id.in_(typed_table_ids),
                SliceDefinition.run_id == run_id,
            )
        )
        .scalars()
        .all()
    )
    if not slice_defs:
        return 0

    # Get all slice tables
    slice_tables = {
        t.table_name: t
        for t in session.execute(
            select(Table).where(Table.layer == "slice", Table.source_id.in_(source_ids))
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

        # Resolve effective table (prefer slicing_view for enriched columns).
        # The slicing_view shares its fact table's source_id (DAT-403) and is named
        # off its source-qualified duckdb_path (DAT-356).
        sv_table = session.execute(
            select(Table).where(
                Table.source_id == source_table.source_id,
                Table.table_name == slicing_view_name(source_table.duckdb_path or ""),
                Table.layer == "slicing_view",
            )
        ).scalar_one_or_none()
        effective_table = sv_table if sv_table else source_table

        # Get source column names to exclude slice definition columns
        slice_def_col_ids = set(
            session.execute(
                select(SliceDefinition.column_id).where(
                    SliceDefinition.table_id == source_table.table_id,
                    SliceDefinition.run_id == run_id,
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
            # Find the slice table by its source-qualified name (DAT-356).
            slice_name = slice_table_name(
                source_table.duckdb_path or "", effective_slice_col_name, slice_value
            )

            slice_table = slice_tables.get(slice_name)
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
                        session_id=session_id,
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

    logger.info(
        "slice_profiles_built", table_count=len(typed_table_ids), profiles_created=total_created
    )
    return total_created
