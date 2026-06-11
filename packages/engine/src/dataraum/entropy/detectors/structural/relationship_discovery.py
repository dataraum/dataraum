"""Relationship-discovery adjudication detector (ADR-0009).

Pools the witness classes that already exist as ROWS on the focal directional
column pair — the structural candidate's value-overlap statistics (the data
witness), the LLM's confirmation, and the teach-materialized manual/keeper rows
— into the claim space {genuine, spurious}, and emits ONE witnessed
EntropyObject per relationship target whose score is the pooled conflict ``C``.
High ``C`` = the witnesses disagree about whether the relationship is genuine
(the orphan-broken-but-LLM-confirmed case detection-v1 injects); high
ignorance ``U`` = the pair entered the catalog with nobody qualified weighing
in. The shell contains no math (ADR-0009 piece 3); the measurement lives in
:mod:`dataraum.entropy.measurements.relationship_discovery`.
"""

from __future__ import annotations

from typing import Any

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.measurements.relationship_discovery import (
    CLAIM_SPACE,
    measure_relationship_discovery,
)
from dataraum.entropy.models import EntropyObject, WitnessClaim


class RelationshipDiscoveryDetector(EntropyDetector):
    """Adjudicate whether the focal relationship is genuine (witness pooling)."""

    detector_id = "relationship_discovery"
    layer = Layer.STRUCTURAL
    dimension = Dimension.RELATIONS
    sub_dimension = SubDimension.RELATIONSHIP_DISCOVERY
    required_analyses = [AnalysisKey.RELATIONSHIPS]
    scope = "relationship"
    description = "Adjudicates relationship genuineness across data/LLM/teach witnesses"

    def load_data(self, context: DetectorContext) -> None:
        """Load the focal pair's per-method rows + the calibrated reliabilities.

        Stored under the declared ``AnalysisKey.RELATIONSHIPS`` key — the SAME
        key ``can_run()`` gates on and ``detect()`` reads (the DAT-405 lesson:
        a divergent key silently disables the detector forever).
        """
        if (
            context.session is None
            or context.from_column_id is None
            or context.to_column_id is None
        ):
            return
        from dataraum.entropy.detectors.loaders import load_relationship_rows_for_pair
        from dataraum.entropy.reliabilities import get_reliability_config

        rows = load_relationship_rows_for_pair(
            context.session,
            context.from_column_id,
            context.to_column_id,
            session_id=context.session_id,
            run_id=context.run_id,
        )
        if rows:
            context.analysis_results[AnalysisKey.RELATIONSHIPS] = rows
        context.analysis_results["reliabilities"] = get_reliability_config().for_measurement(
            self.detector_id
        )

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Pool the pair's row-witnesses; emit one witnessed object (score = C)."""
        rows = context.get_analysis(AnalysisKey.RELATIONSHIPS, None)
        if not isinstance(rows, dict) or not rows:
            return []

        candidate = rows.get("candidate") or {}
        candidate_evidence: dict[str, Any] = candidate.get("evidence") or {}
        llm_row = rows.get("llm")
        manual_row = rows.get("manual")
        keeper_row = rows.get("keeper")
        reliabilities = context.get_analysis("reliabilities", None) or None

        adjudication = measure_relationship_discovery(
            join_confidence=candidate_evidence.get("join_confidence"),
            statistical_confidence=candidate_evidence.get("statistical_confidence"),
            llm_confidence=llm_row.get("confidence") if llm_row else None,
            manual_confidence=manual_row.get("confidence") if manual_row else None,
            keeper_confidence=keeper_row.get("confidence") if keeper_row else None,
            reliabilities=reliabilities,
        )
        if not adjudication.witnesses:
            # Nobody took a position (e.g. an overlay row with no confidence and
            # no measured candidate) — total ignorance, nothing to adjudicate.
            return []

        result = adjudication.result
        any_row = next(iter(rows.values()))
        # C/U → teach routing (DAT-447, same shape as temporal_behavior):
        # conflict means the witnesses disagree about genuineness — when the
        # posterior leans spurious, the resolving human verdict is ``reject``;
        # otherwise (contested-but-leaning-genuine, or pure ignorance where
        # nobody qualified weighed in) an explicit ``confirm`` is the missing
        # human witness. Both actions are the relationship overlay's executable
        # vocabulary (relationship_overlay_pairs: confirm/reject/add/keep on
        # the directional column pair).
        p_genuine = result.posterior[CLAIM_SPACE.index("genuine")]
        if result.conflict >= result.ignorance and p_genuine < 0.5:
            teach: dict[str, Any] = {
                "type": "relationship",
                "action": "reject",
                "from_column_id": context.from_column_id,
                "to_column_id": context.to_column_id,
            }
        else:
            teach = {
                "type": "relationship",
                "action": "confirm",
                "from_column_id": context.from_column_id,
                "to_column_id": context.to_column_id,
            }
        evidence = [
            {
                "claim_field": adjudication.claim_field,
                "from_table": context.from_table_name or any_row.get("from_table"),
                "to_table": context.to_table_name or any_row.get("to_table"),
                "conflict": result.conflict,
                "ignorance": result.ignorance,
                "posterior": dict(zip(CLAIM_SPACE, result.posterior, strict=True)),
                "methods_present": sorted(rows),
                # The decline signal, carried loud (not manufactured into an
                # opinion): a pair in the catalog without an llm row this run
                # was NOT (re-)confirmed by the selector.
                "llm_confirmed_this_run": llm_row is not None,
                "teach_suggestion": teach,
                "value_overlap": {
                    "join_confidence": candidate_evidence.get("join_confidence"),
                    "statistical_confidence": candidate_evidence.get("statistical_confidence"),
                    "algorithm": candidate_evidence.get("algorithm"),
                },
            }
        ]
        obj = self.create_entropy_object(context=context, score=result.conflict, evidence=evidence)
        obj.witnesses = [
            WitnessClaim(
                claim_field=adjudication.claim_field,
                witness_id=w.witness_id,
                distribution=dict(zip(CLAIM_SPACE, w.distribution, strict=True)),
                reliability=w.reliability,
            )
            for w in adjudication.witnesses
        ]
        return [obj]
