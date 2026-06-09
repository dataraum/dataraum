"""Temporal-behaviour (stock vs flow) detector — teach-first (ADR-0009, DAT-445).

Column-scoped semantic adjudication. For each column it pools the grounding-conditional
ontology prior (the concept's ``temporal_behavior`` + grounding confidence) against the
LLM's INDEPENDENT stock/flow claim (produced in ``semantic_per_column``), both read off
``SemanticAnnotation``. High conflict = the declared behaviour and the LLM read
disagree (the live ``debit_balance`` case: concept claims a balance, the LLM reads the
periodic trial_balance as flow); high ignorance = the behaviour is undetermined. Emits
one witnessed ``EntropyObject`` per column carrying the resolved behaviour, the conflict,
and a ranked ``concept_property`` / ``rebind`` teach suggestion.

No data trajectory and no cross-table reconciliation: the DAT-459 spike falsified the
time-series statistic, and the DAT-445 kill-gate showed an LLM reading a column's own
trajectory is confidently wrong on ambiguous shapes. The genuine data-reality witness
is the events→measure aggregation reconciliation (DAT-491) — a thin-consumer add-on
once that lineage is discovered, NOT part of this core.
"""

from __future__ import annotations

from typing import Any

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.entropy.measurements.temporal_behavior import (
    CLAIM_SPACE,
    measure_temporal_behavior,
    resolved_behaviour,
)
from dataraum.entropy.models import EntropyObject, WitnessClaim


class TemporalBehaviorDetector(EntropyDetector):
    """Pool the ontology prior vs the LLM stock/flow claim, per column."""

    detector_id = "temporal_behavior"
    layer = Layer.SEMANTIC
    dimension = Dimension.TEMPORAL
    sub_dimension = SubDimension.TEMPORAL_BEHAVIOR
    scope = "column"
    # No required_analyses: load_data reads the semantic annotation itself.
    description = "Stock vs flow: ontology prior vs LLM claim (teach-first)"

    def load_data(self, context: DetectorContext) -> None:
        """Load the column's semantic annotation (concept behaviour + LLM claim)."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_semantic
        from dataraum.entropy.reliabilities import get_reliability_config

        semantic = load_semantic(
            context.session, context.column_id, context.run_id, context.base_runs
        )
        if semantic is None:
            return
        context.analysis_results["semantic"] = semantic
        context.analysis_results["reliabilities"] = get_reliability_config().for_measurement(
            self.detector_id
        )

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Pool prior vs LLM claim; emit one per-column object when a witness opines."""
        semantic = context.get_analysis("semantic")
        if not semantic:
            return []
        reliabilities = context.get_analysis("reliabilities", None) or None

        adj = measure_temporal_behavior(
            context.table_name,
            context.column_name,
            ontology_behaviour=semantic.get("temporal_behavior"),
            grounding_confidence=semantic.get("confidence"),
            llm_claim=semantic.get("temporal_behavior_claim"),
            llm_confidence=semantic.get("temporal_behavior_claim_confidence"),
            reliabilities=reliabilities,
        )
        if not adj.witnesses:
            return []  # neither prior nor claim took a position → nothing to say

        label, contested = resolved_behaviour(adj.result)
        posterior = dict(zip(CLAIM_SPACE, adj.result.posterior, strict=True))
        # Conflict → fix the concept's behaviour; pure ignorance → rebind the concept.
        if adj.result.conflict >= adj.result.ignorance:
            teach: dict[str, Any] = {
                "type": "concept_property",
                "concept": semantic.get("business_concept"),
                "property": "temporal_behavior",
            }
        else:
            teach = {"type": "rebind", "column": context.column_name}
        evidence = [
            {
                "_table_name": context.table_name,
                "_column_name": context.column_name,
                "claim_field": adj.claim_field,
                "conflict": adj.result.conflict,
                "ignorance": adj.result.ignorance,
                "posterior": posterior,
                "resolved": label,
                "contested": contested,
                "ontology_behavior": semantic.get("temporal_behavior"),
                "llm_claim": semantic.get("temporal_behavior_claim"),
                "teach_suggestion": teach,
            }
        ]
        obj = EntropyObject(
            layer=self.layer,
            dimension=self.dimension,
            sub_dimension=self.sub_dimension,
            target=f"column:{context.table_name}.{context.column_name}",
            score=adj.result.conflict,
            evidence=evidence,
            detector_id=self.detector_id,
            witnesses=[
                WitnessClaim(
                    claim_field=adj.claim_field,
                    witness_id=w.witness_id,
                    distribution=dict(zip(CLAIM_SPACE, w.distribution, strict=True)),
                    reliability=w.reliability,
                )
                for w in adj.witnesses
            ],
        )
        return [obj]
