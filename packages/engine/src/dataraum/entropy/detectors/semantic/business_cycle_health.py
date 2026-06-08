"""Business cycle health entropy detector.

Consumes DetectedBusinessCycle records from the business_cycles phase.
Measures how well the pipeline detected business cycles — low completion
rates and low confidence indicate uncertain cycle detection.

Scope: table-level (cycles span multiple tables, score attaches to
each table involved in the cycle).

Scoring: per_cycle_score = max(1.0 - completion_rate, 1.0 - confidence)
         table_score = max(per_cycle_scores)

This is deliberately naive — refine after observing real output.
No injection exists for this detector; calibration is observation-based.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject

logger = get_logger(__name__)


class BusinessCycleHealthDetector(EntropyDetector):
    """Detect entropy from business cycle detection quality.

    Table-scoped detector that scores cycle completion rates and
    detection confidence. Poor cycle detection means downstream
    metrics (graph_execution) may be unreliable.
    """

    detector_id = "business_cycle_health"
    layer = Layer.SEMANTIC
    dimension = Dimension.CYCLES
    sub_dimension = SubDimension.BUSINESS_CYCLE_HEALTH
    scope = "table"
    required_analyses = [AnalysisKey.BUSINESS_CYCLES]
    description = "Business cycle detection quality: completion rates, confidence"

    def load_data(self, context: DetectorContext) -> None:
        """Load detected business cycles involving this table (DAT-455).

        Cycles are run-versioned and session-scoped, written by the
        operating_model stage — a *later* stage than the begin_session/add_source
        ``detect`` pass this detector runs in. So this reader resolves the
        session's PROMOTED ``operating_model`` head and reads cycles at that run,
        rather than the current detect run (``context.run_id``, which names this
        relationship-detect run, not the cycle run). With no promoted
        operating_model run — the common case, since this detector usually runs
        before operating_model exists — it reads NOTHING: fail-closed, never a
        cross-run read (DAT-429).
        """
        if context.session is None or not context.table_id or not context.session_id:
            return

        from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
        from dataraum.storage.snapshot_head import head_run_id, session_head_target

        om_run_id = head_run_id(
            context.session, session_head_target(context.session_id), "operating_model"
        )
        if om_run_id is None:
            return

        # Get all cycles for the session's promoted operating_model run
        cycles = list(
            context.session.execute(
                select(DetectedBusinessCycle).where(
                    DetectedBusinessCycle.session_id == context.session_id,
                    DetectedBusinessCycle.run_id == om_run_id,
                )
            )
            .scalars()
            .all()
        )

        # Filter to cycles involving this table
        matching = [c for c in cycles if context.table_name in (c.tables_involved or [])]

        if matching:
            context.analysis_results["business_cycles"] = matching

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Score cycle health for this table.

        Returns a single EntropyObject with score = max(per-cycle scores).
        Per-cycle score = max(1 - completion_rate, 1 - confidence).
        """
        cycles: list[Any] = context.get_analysis("business_cycles", [])
        if not cycles:
            return [
                self.create_entropy_object(
                    context=context,
                    score=0.0,
                    evidence=[{"reason": "no_cycles_involving_table"}],
                )
            ]

        per_cycle_scores: list[float] = []
        evidence: list[dict[str, Any]] = []

        for cycle in cycles:
            completion_rate = cycle.completion_rate if cycle.completion_rate is not None else 0.0
            confidence = cycle.confidence if cycle.confidence is not None else 0.0

            # High entropy when completion or confidence is low
            score = max(1.0 - completion_rate, 1.0 - confidence)
            per_cycle_scores.append(score)

            evidence.append(
                {
                    "cycle_name": cycle.cycle_name,
                    "cycle_type": cycle.cycle_type,
                    "canonical_type": cycle.canonical_type,
                    "confidence": confidence,
                    "completion_rate": completion_rate,
                    "score": score,
                    "total_records": cycle.total_records,
                    "completed_cycles": cycle.completed_cycles,
                }
            )

        final_score = max(per_cycle_scores) if per_cycle_scores else 0.0

        return [
            self.create_entropy_object(
                context=context,
                score=final_score,
                evidence=evidence,
            )
        ]
