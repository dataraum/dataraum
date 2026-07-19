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

from sqlalchemy import select

from dataraum.analysis.relationships import detect_relationships
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
        add_source, so this phase is source-free
        (feedback-source-dies-at-addsource).
        """
        if not ctx.table_ids:
            return []
        # Ordered by name: downstream pair enumeration is order-sensitive on
        # symmetric evidence (see ``detector._load_tables``), so the selection
        # must not inherit Postgres physical row order.
        stmt = select(Table).where(Table.table_id.in_(ctx.table_ids)).order_by(Table.table_name)
        return list(ctx.session.execute(stmt).scalars())

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only on genuine preconditions — never because the session already ran.

        DAT-408: a versioned begin_session re-run MUST re-derive (candidates refresh,
        readiness re-promotes), so the old "already detected N candidates" idempotency
        branch is gone — it would make a replay a silent no-op (nothing re-derived,
        head never advances). The detection itself is idempotent (delete-before-insert
        on candidates; ``llm``/``manual`` durable).
        """
        typed_tables = self._typed_tables(ctx)
        if not typed_tables:
            return "No typed tables found"
        if len(typed_tables) < 2:
            return "Need at least 2 tables to detect relationships"
        return None

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

        # Run relationship detection
        detection_result = detect_relationships(
            table_ids=table_ids,
            duckdb_conn=ctx.duckdb_conn,
            session=ctx.session,
            min_confidence=min_confidence,
            evaluate=True,
            run_id=ctx.run_id,
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
