"""Entropy phase implementation.

Non-LLM entropy detection across all dimensions (structural, semantic, value, computational).
Runs detectors to quantify uncertainty in each column and table.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import (
    EntropyObjectRecord,
    EntropySnapshotRecord,
)
from dataraum.entropy.snapshot import take_snapshot
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.cleanup import exec_delete
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


# TODO: focus prioritization on actions that impact downstream context generation and LLM performance - e.g. structural issues that cause RI failures, semantic issues that cause misinterpretation, value issues that cause parsing failures, etc.
@analysis_phase
class EntropyPhase(BasePhase):
    """Entropy detection phase.

    Runs entropy detectors across all dimensions to quantify uncertainty
    in data. Produces entropy profiles for each column and table.

    Requires: statistics, semantic, relationships, correlations, quality_summary phases.
    """

    @property
    def name(self) -> str:
        return "entropy"

    @property
    def description(self) -> str:
        return "Entropy detection across all dimensions"

    @property
    def dependencies(self) -> list[str]:
        return [
            "typing",
            "column_eligibility",
            "semantic",
            "relationships",
            "computation_review",
        ]

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        count = exec_delete(
            session,
            delete(EntropyObjectRecord).where(EntropyObjectRecord.source_id == source_id),
        )
        count += exec_delete(
            session,
            delete(EntropySnapshotRecord).where(EntropySnapshotRecord.source_id == source_id),
        )
        return count

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.entropy import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all columns already have entropy profiles."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Count columns in these tables
        col_count_stmt = select(func.count(Column.column_id)).where(Column.table_id.in_(table_ids))
        total_columns = (ctx.session.execute(col_count_stmt)).scalar() or 0

        if total_columns == 0:
            return "No columns found in typed tables"

        # Count distinct columns with entropy records
        # (each column has multiple EntropyObjectRecords - one per detector/dimension)
        entropy_stmt = select(func.count(func.distinct(EntropyObjectRecord.column_id))).where(
            EntropyObjectRecord.column_id.in_(
                select(Column.column_id).where(Column.table_id.in_(table_ids))
            )
        )
        columns_with_entropy = (ctx.session.execute(entropy_stmt)).scalar() or 0

        if columns_with_entropy >= total_columns:
            return "All columns already have entropy profiles"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run entropy detection on all columns and tables."""
        # Verify detectors are registered
        from dataraum.entropy.detectors.base import get_default_registry

        registry = get_default_registry()
        all_detectors = registry.get_all_detectors()
        if not all_detectors:
            return PhaseResult.failed(
                "No entropy detectors registered. Cannot run entropy detection."
            )

        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Load all columns for counting and grouping
        columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = list(ctx.session.execute(columns_stmt).scalars().all())

        if not all_columns:
            return PhaseResult.failed("No columns found in typed tables.")

        # Group columns by table
        columns_by_table: dict[str, list[Column]] = {}
        for col in all_columns:
            columns_by_table.setdefault(col.table_id, []).append(col)

        # Build column_id lookup for table-scoped objects that reference columns
        column_id_by_table_col: dict[tuple[str, str], str] = {}
        for col in all_columns:
            column_id_by_table_col[(col.table_id, col.column_name)] = col.column_id

        total_entropy_objects = 0
        tables_processed = 0
        all_domain_objects: list[Any] = []  # Collect EntropyObject for network inference
        all_records: list[EntropyObjectRecord] = []  # Batch for session.add_all()

        # Build table name -> table_id lookup for target resolution
        table_id_by_name = {t.table_name: t.table_id for t in typed_tables}

        for table in typed_tables:
            table_columns = columns_by_table.get(table.table_id, [])
            if not table_columns:
                continue

            # --- Column-scoped detectors ---
            for col in table_columns:
                target = f"column:{table.table_name}.{col.column_name}"
                snapshot = take_snapshot(target=target, session=ctx.session)

                all_domain_objects.extend(snapshot.objects)

                for entropy_obj in snapshot.objects:
                    record = _make_record(
                        ctx=ctx,
                        entropy_obj=entropy_obj,
                        table_id=table.table_id,
                        column_id=col.column_id,
                    )
                    all_records.append(record)
                    total_entropy_objects += 1

            # --- Table-scoped detectors (dimensional_entropy, column_quality, etc.) ---
            table_snapshot = take_snapshot(target=f"table:{table.table_name}", session=ctx.session)
            all_domain_objects.extend(table_snapshot.objects)

            logger.debug(
                "table_scoped_detectors",
                table=table.table_name,
                entropy_objects=len(table_snapshot.objects),
            )

            for entropy_obj in table_snapshot.objects:
                # Resolve column_id from evidence if this is a column-level object
                # (e.g. column_quality produces per-column objects from table scope)
                record_column_id = _extract_column_id(entropy_obj, column_id_by_table_col)

                record = _make_record(
                    ctx=ctx,
                    entropy_obj=entropy_obj,
                    table_id=_resolve_table_id_from_target(
                        entropy_obj.target, table_id_by_name, table.table_id
                    ),
                    column_id=record_column_id,
                )
                all_records.append(record)
                total_entropy_objects += 1

            tables_processed += 1

        # Batch insert all entropy records at once
        ctx.session.add_all(all_records)

        # Compute summary statistics from in-memory domain objects.
        # No DB round-trip needed — the session hasn't committed yet and
        # uses autoflush=False, so re-querying the DB would see nothing.
        from dataraum.entropy.network.model import EntropyNetwork
        from dataraum.entropy.views.network_context import _assemble_network_context

        network = EntropyNetwork()
        network_ctx = _assemble_network_context(all_domain_objects, network)

        high_entropy_count = network_ctx.columns_blocked + network_ctx.columns_investigate
        critical_entropy_count = network_ctx.columns_blocked
        overall_readiness = network_ctx.overall_readiness

        # Average entropy score: per-target max, then mean across targets.
        # This prevents table-level dimensional entropy object counts from
        # dominating the average (each target contributes its worst score).
        target_max: dict[str, float] = {}
        for obj in all_domain_objects:
            if obj.target not in target_max or obj.score > target_max[obj.target]:
                target_max[obj.target] = obj.score
        avg_entropy = sum(target_max.values()) / len(target_max) if target_max else 0.0

        # Serialize Bayesian network state for downstream consumers
        snapshot_data: dict[str, Any] = {
            "node_states": {
                intent.intent_name: {
                    "worst_p_high": intent.worst_p_high,
                    "mean_p_high": intent.mean_p_high,
                    "columns_blocked": intent.columns_blocked,
                    "columns_investigate": intent.columns_investigate,
                    "columns_ready": intent.columns_ready,
                    "overall_readiness": intent.overall_readiness,
                }
                for intent in network_ctx.intents
            },
            "total_columns": network_ctx.total_columns,
            "columns_blocked": network_ctx.columns_blocked,
            "columns_investigate": network_ctx.columns_investigate,
            "columns_ready": network_ctx.columns_ready,
        }

        # Create snapshot record
        snapshot_record = EntropySnapshotRecord(
            source_id=ctx.source_id,
            total_entropy_objects=total_entropy_objects,
            high_entropy_count=high_entropy_count,
            critical_entropy_count=critical_entropy_count,
            overall_readiness=overall_readiness,
            avg_entropy_score=avg_entropy,
            snapshot_data=snapshot_data,
        )
        ctx.session.add(snapshot_record)

        # Note: commit handled by session_scope() in scheduler

        # Compute aggregated detector scores for gate checking and display.
        # Keys use full dimension paths (layer.dimension.sub_dimension) so they
        # match contract threshold prefix matching in the scheduler.
        scores_by_dim: dict[str, list[float]] = {}
        for obj in all_domain_objects:
            path = f"{obj.layer}.{obj.dimension}.{obj.sub_dimension}"
            scores_by_dim.setdefault(path, []).append(obj.score)

        entropy_scores = {
            dim: sum(scores) / len(scores) for dim, scores in scores_by_dim.items() if scores
        }

        return PhaseResult.success(
            outputs={
                "entropy_profiles": tables_processed,
                "entropy_objects": total_entropy_objects,
                "overall_readiness": overall_readiness,
                "high_entropy_columns": high_entropy_count,
                "critical_entropy_columns": critical_entropy_count,
                "entropy_scores": entropy_scores,
            },
            records_processed=len(all_columns),
            records_created=total_entropy_objects + 1,
            summary=f"{overall_readiness} readiness, {critical_entropy_count} critical columns",
        )


def _make_record(
    ctx: PhaseContext,
    entropy_obj: Any,
    table_id: str | None,
    column_id: str | None,
) -> EntropyObjectRecord:
    """Create an EntropyObjectRecord from an EntropyObject."""
    resolution_dicts = [
        {
            "action": opt.action,
            "parameters": opt.parameters,
            "effort": opt.effort,
            "description": opt.description,
        }
        for opt in entropy_obj.resolution_options
    ]

    return EntropyObjectRecord(
        source_id=ctx.source_id,
        table_id=table_id,
        column_id=column_id,
        target=entropy_obj.target,
        layer=entropy_obj.layer,
        dimension=entropy_obj.dimension,
        sub_dimension=entropy_obj.sub_dimension,
        score=entropy_obj.score,
        evidence=entropy_obj.evidence,
        resolution_options=resolution_dicts if resolution_dicts else None,
        detector_id=entropy_obj.detector_id,
    )


def _resolve_table_id_from_target(
    target: str,
    table_id_by_name: dict[str, str],
    fallback_table_id: str,
) -> str:
    """Resolve table_id from a target string like 'table:name' or 'column:name.col'."""
    if ":" in target:
        ref = target.split(":", 1)[1]
        table_name = ref.split(".")[0]
        return table_id_by_name.get(table_name, fallback_table_id)
    return fallback_table_id


def _extract_column_id(
    entropy_obj: Any,
    column_id_by_table_col: dict[tuple[str, str], str],
) -> str | None:
    """Extract column_id from an entropy object's evidence or target.

    For column-level objects produced by table-scoped detectors (e.g. column_quality),
    the evidence contains column_id and the target is 'column:table.col'.
    """
    # Check evidence for explicit column_id
    for ev in entropy_obj.evidence or []:
        col_id = ev.get("column_id")
        table_id = ev.get("table_id")
        if col_id and table_id:
            return str(col_id)

    # Fall back to parsing column target
    if entropy_obj.target.startswith("column:"):
        ref = entropy_obj.target.split(":", 1)[1]
        parts = ref.split(".", 1)
        if len(parts) == 2:
            # Try to find in our lookup — but we'd need table_id too
            # For now, return None for pure table-scoped objects
            pass

    return None
