"""Relationships phase implementation.

Detects relationships between typed tables:
- Value overlap analysis (Jaccard/containment similarity)
- Join column detection
- Cardinality analysis (one-to-one, one-to-many, etc.)
- Graph topology analysis
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, or_, select

from dataraum.analysis.relationships import detect_relationships
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.core.config import load_phase_config
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Table

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


@analysis_phase
class RelationshipsPhase(BasePhase):
    """Relationship detection phase.

    Detects relationships between typed tables using value overlap
    and structural analysis.
    """

    @property
    def name(self) -> str:
        return "relationships"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.relationships import db_models

        return [db_models]

    def _typed_tables(self, ctx: PhaseContext) -> list[Table]:
        """The session's selected tables (DAT-401, source-free).

        Scopes purely by ``ctx.table_ids`` — the begin_session selection, which
        may span sources. The ids are already validated as typed by
        ``begin_session_select``'s pre-flight (the single enforcement point), so
        no ``layer`` filter is repeated here. A source is meaningless past
        add_source, so this phase never reads ``ctx.source_id``
        (feedback-source-dies-at-addsource).
        """
        if not ctx.table_ids:
            return []
        stmt = select(Table).where(Table.table_id.in_(ctx.table_ids))
        return list(ctx.session.execute(stmt).scalars())

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if THIS session already detected relationships over its tables."""
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return "No typed tables found"

        if len(typed_tables) < 2:
            return "Need at least 2 tables to detect relationships"

        # Scoped to this session's own candidates (rows carry session_id): a
        # different session's candidates over a shared table must not make this
        # session skip detection (DAT-401).
        table_ids = [t.table_id for t in typed_tables]
        existing_count = (
            ctx.session.execute(
                select(func.count(Relationship.relationship_id)).where(
                    Relationship.session_id == ctx.require_session_id(),
                    Relationship.from_table_id.in_(table_ids),
                    Relationship.detection_method == "candidate",
                )
            )
        ).scalar() or 0

        if existing_count > 0:
            return f"Already detected {existing_count} relationship candidates"

        return None

    def replay_cleanup(self, ctx: PhaseContext, table_ids: list[str]) -> None:
        """Drop THIS session's candidate relationships for its tables (DAT-401/373).

        Deletes only the structural ``detection_method='candidate'`` rows this
        session wrote (scoped by ``session_id``) whose endpoints touch the scope
        — its OWN output. Never another session's rows, the ``'llm'`` rows (owned
        by ``semantic_per_table``), or the parent ``Table``: the FK cascade is
        NOT load-bearing, the delete is explicit and owner-scoped.
        """
        if not table_ids:
            return
        ctx.session.execute(
            delete(Relationship).where(
                Relationship.session_id == ctx.require_session_id(),
                Relationship.detection_method == "candidate",
                or_(
                    Relationship.from_table_id.in_(table_ids),
                    Relationship.to_table_id.in_(table_ids),
                ),
            )
        )
        ctx.session.flush()

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run relationship detection over the session's selected typed tables."""
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        if len(typed_tables) < 2:
            return PhaseResult.success(
                outputs={"relationship_candidates": [], "message": "Need at least 2 tables"},
                records_processed=0,
                records_created=0,
            )

        table_ids = [t.table_id for t in typed_tables]

        # Configuration from phase config (fallback to file for standalone usage)
        if "min_confidence" in ctx.config:
            config = ctx.config
        else:
            config = load_phase_config("relationships")
        min_confidence = config["min_confidence"]
        sample_percent = config["sample_percent"]

        # Run relationship detection
        detection_result = detect_relationships(
            table_ids=table_ids,
            duckdb_conn=ctx.duckdb_conn,
            session=ctx.session,
            session_id=ctx.require_session_id(),
            min_confidence=min_confidence,
            sample_percent=sample_percent,
            evaluate=True,
        )

        if not detection_result.success:
            return PhaseResult.failed(f"Relationship detection failed: {detection_result.error}")

        result_data = detection_result.unwrap()

        # Summarize findings
        candidates = result_data.candidates
        high_confidence = [
            c for c in candidates if any(jc.join_confidence >= 0.7 for jc in c.join_candidates)
        ]

        return PhaseResult.success(
            outputs={
                "relationship_candidates": [f"{c.table1} <-> {c.table2}" for c in candidates],
                "total_candidates": len(candidates),
                "high_confidence_count": len(high_confidence),
                "duration_seconds": result_data.duration_seconds,
            },
            records_processed=len(table_ids) * (len(table_ids) - 1) // 2,  # pairs analyzed
            records_created=len(candidates),
            summary=f"{len(candidates)} candidates ({len(high_confidence)} high-confidence)",
        )
