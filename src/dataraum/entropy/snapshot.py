"""Snapshot — run detectors on a target and return canonical scores.

Provides the core measurement mechanism for entropy gates:
- `take_snapshot()`: measure entropy for a column/table
- `Snapshot`: immutable result with scores, detectors run, timestamp

Each detector owns its data loading via `load_data()`. The snapshot
orchestrator resolves the target, creates a context per detector, and
lets each detector load only the data it needs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import (
    DetectorContext,
    get_default_registry,
)
from dataraum.entropy.models import EntropyObject

logger = get_logger(__name__)


@dataclass(frozen=True)
class Snapshot:
    """Immutable snapshot of detector scores for a target."""

    scores: dict[str, float]  # sub_dimension -> score
    detectors_run: list[str]  # detector_ids that were executed
    objects: tuple[EntropyObject, ...] = ()  # full EntropyObject instances
    measured_at: datetime = field(default_factory=lambda: datetime.now(UTC))


def _resolve_view_target(
    session: Session,
    target: str,
) -> tuple[str, str, str] | None:
    """Parse view target string and resolve to (view_id, view_name, fact_table_id).

    Supports format: "view:{view_name}"

    Returns None if target cannot be resolved.
    """
    from dataraum.analysis.views.db_models import EnrichedView

    ref = target.split(":", 1)[1] if ":" in target else target

    view = session.execute(
        select(EnrichedView).where(EnrichedView.view_name == ref)
    ).scalar_one_or_none()
    if not view:
        return None

    return view.view_id, view.view_name, view.fact_table_id


def _resolve_table_target(
    session: Session,
    target: str,
) -> tuple[str, str] | None:
    """Parse table target string and resolve to (table_id, table_name).

    Supports format: "table:table_name"

    Returns None if target cannot be resolved.
    """
    from dataraum.storage import Table

    ref = target.split(":", 1)[1] if ":" in target else target

    table = session.execute(
        select(Table).where(Table.table_name == ref, Table.layer == "typed")
    ).scalar_one_or_none()
    if not table:
        return None

    return table.table_id, ref


def _resolve_column_target(
    session: Session,
    target: str,
) -> tuple[str, str, str, str] | None:
    """Parse target string and resolve to (table_id, column_id, table_name, column_name).

    Supports formats:
    - "column:table_name.column_name"
    - "table_name.column_name"

    Returns None if target cannot be resolved.
    """
    from dataraum.storage import Column, Table

    # Parse target
    ref = target.split(":", 1)[1] if ":" in target else target
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return None

    table_name, column_name = parts

    table = session.execute(
        select(Table).where(Table.table_name == table_name, Table.layer == "typed")
    ).scalar_one_or_none()
    if not table:
        return None

    column = session.execute(
        select(Column).where(
            Column.table_id == table.table_id,
            Column.column_name == column_name,
        )
    ).scalar_one_or_none()
    if not column:
        return None

    return table.table_id, column.column_id, table_name, column_name


def _run_detectors(
    target: str,
    context: DetectorContext,
    detectors: list[Any],
) -> Snapshot:
    """Run a list of detectors against a context and return a Snapshot.

    Each detector gets a fresh DetectorContext copy. The detector calls
    load_data() to populate its required analysis keys, then can_run()
    checks they're present, and detect() produces EntropyObjects.
    """
    scores: dict[str, float] = {}
    detectors_run: list[str] = []
    all_objects: list[EntropyObject] = []

    for detector in detectors:
        # Build a fresh context per detector so load_data() results
        # don't leak between detectors.
        det_context = DetectorContext(
            session=context.session,
            source_id=context.source_id,
            table_id=context.table_id,
            table_name=context.table_name,
            column_id=context.column_id,
            column_name=context.column_name,
            view_name=context.view_name,
            duckdb_conn=context.duckdb_conn,
            # Copy pre-populated analysis_results (for test/legacy paths)
            analysis_results=dict(context.analysis_results),
        )

        try:
            detector.load_data(det_context)
        except Exception:
            logger.warning(
                f"Detector {detector.detector_id} load_data failed on {target}",
                exc_info=True,
            )
            continue

        if not detector.can_run(det_context):
            continue
        try:
            objects: list[EntropyObject] = detector.detect(det_context)
            detectors_run.append(detector.detector_id)
            all_objects.extend(objects)
            for obj in objects:
                scores[obj.sub_dimension] = obj.score
        except Exception:
            logger.warning(
                f"Hard detector {detector.detector_id} failed on {target}",
                exc_info=True,
            )

    return Snapshot(scores=scores, detectors_run=detectors_run, objects=tuple(all_objects))


def take_snapshot(
    target: str,
    session: Session,
    duckdb_conn: Any = None,
    dimensions: Sequence[str] | None = None,
) -> Snapshot:
    """Run detectors on a target and return canonical scores.

    Dispatches on target prefix:
    - "table:" -> table-scoped detectors (scope="table")
    - "column:" or default -> column-scoped detectors (scope="column")

    Each detector loads its own data via load_data(). The session is
    passed on the context so detectors can query the DB directly.

    Args:
        target: Target reference (e.g., "column:orders.amount" or "table:orders")
        session: SQLAlchemy session for loading analysis data
        duckdb_conn: DuckDB connection (unused currently, reserved for future)
        dimensions: Optional filter -- only run detectors for these sub_dimensions

    Returns:
        Snapshot with scores from all applicable detectors
    """
    registry = get_default_registry()
    is_view_target = target.startswith("view:")
    is_table_target = target.startswith("table:")

    if is_view_target:
        resolved_view = _resolve_view_target(session, target)
        if resolved_view is None:
            logger.warning(f"Cannot resolve view target for snapshot: {target}")
            return Snapshot(scores={}, detectors_run=[])

        view_id, view_name, fact_table_id = resolved_view

        context = DetectorContext(
            session=session,
            table_id=fact_table_id,
            view_name=view_name,
            duckdb_conn=duckdb_conn,
        )

        detectors = [d for d in registry.get_all_detectors() if d.scope == "view"]
    elif is_table_target:
        resolved = _resolve_table_target(session, target)
        if resolved is None:
            logger.warning(f"Cannot resolve table target for snapshot: {target}")
            return Snapshot(scores={}, detectors_run=[])

        table_id, table_name = resolved

        context = DetectorContext(
            session=session,
            table_id=table_id,
            table_name=table_name,
            duckdb_conn=duckdb_conn,
        )

        detectors = [d for d in registry.get_all_detectors() if d.scope == "table"]
    else:
        resolved_col = _resolve_column_target(session, target)
        if resolved_col is None:
            logger.warning(f"Cannot resolve target for snapshot: {target}")
            return Snapshot(scores={}, detectors_run=[])

        table_id, column_id, table_name, column_name = resolved_col

        context = DetectorContext(
            session=session,
            table_id=table_id,
            column_id=column_id,
            table_name=table_name,
            column_name=column_name,
            duckdb_conn=duckdb_conn,
        )

        detectors = [d for d in registry.get_all_detectors() if d.scope == "column"]

    # Filter by dimensions if specified
    if dimensions:
        dim_set = set(dimensions)
        detectors = [d for d in detectors if d.sub_dimension in dim_set]

    return _run_detectors(target, context, detectors)
