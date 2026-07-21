"""Temporal-behaviour (stock vs flow) detector — teach-first (ADR-0009, DAT-445).

Column-scoped semantic adjudication. For each column it pools the LLM's INDEPENDENT
stock/flow claim (produced in ``semantic_per_column``) against the data-grounded
structural reconciliation (DAT-491). Stock/flow is data-determined — the ontology no
longer votes (DAT-657): the same concept materializes as flow or stock, a format the
ontology can't declare. High conflict = the LLM read and the reconciliation disagree;
high ignorance = the behaviour is undetermined. A resolved verdict emits one witnessed
``EntropyObject`` carrying the behaviour and the conflict; a measure the pool could NOT
determine (total ignorance — no opinionated witness) emits a wave-2 ``abstained`` object
(``insufficient_data``, DAT-847) instead of a silent skip, so the undetermined column is
visible in the coverage/abstention trace and never reads as measured-clean. Neither path
carries a teach suggestion: stock/flow is data-determined, so the structural witness
already wins; there is no format for a human to teach here. A genuinely mis-grounded
column is corrected on the grounding path (bind/relationship), which owns that teach —
not manufactured into a second, misplaced ``rebind`` button off a temporal conflict.

No data-trajectory witness: the DAT-459 spike falsified the time-series statistic,
and the DAT-445 kill-gate showed an LLM reading a column's own trajectory is
confidently wrong on ambiguous shapes. The data-reality witness is instead the
events→measure aggregation reconciliation (DAT-491): when the begin_session
``aggregation_lineage`` phase reconciled the column against an event table THIS
run, ``structural_reconciliation`` joins the pool (``per_period`` → flow,
``cumulative`` → stock) — the only witness whose input is the data, not the name.
It abstains on every add_source detect (lineage rows are exact-run).
"""

from __future__ import annotations

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.entropy.measurements.temporal_behavior import (
    CLAIM_SPACE,
    measure_temporal_behavior,
    resolved_behaviour,
)
from dataraum.entropy.models import ABSTAIN_INSUFFICIENT_DATA, EntropyObject, WitnessClaim


class TemporalBehaviorDetector(EntropyDetector):
    """Pool the LLM stock/flow claim vs the data-grounded reconciliation, per column."""

    detector_id = "temporal_behavior"
    layer = Layer.SEMANTIC
    dimension = Dimension.TEMPORAL
    sub_dimension = SubDimension.TEMPORAL_BEHAVIOR
    scope = "column"
    # No required_analyses: load_data reads the semantic annotation itself.
    description = "Stock vs flow: LLM claim vs structural reconciliation (teach-first)"

    def load_data(self, context: DetectorContext) -> None:
        """Load the column's semantic annotation (the LLM stock/flow claim).

        Also loads the column's reconciled aggregation lineage when THIS run wrote
        one (DAT-491) — present only at a begin_session ``session_detect``, where
        the ``structural_reconciliation`` witness joins the pool.
        """
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import (
            load_semantic,
            load_structural_reconciliation,
        )
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
        structural = load_structural_reconciliation(
            context.session, context.column_id, context.run_id
        )
        if structural is not None:
            context.analysis_results["structural"] = structural

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Pool the LLM claim vs the reconciliation; emit a measurement or an abstention.

        A resolved stock/flow label → one measured object carrying the posterior and the
        pooled conflict/ignorance. Total ignorance — no opinionated witness, or a zero-
        reliability wash, so the pool resolved NO trustworthy label — is a wave-2
        ABSTENTION for a column the per-column agent read as a MEASURE
        (``semantic_role='measure'``): persisted as ``insufficient_data`` so the
        undetermined measure is visible in the coverage/abstention trace and never reads
        as measured-clean (DAT-847/DAT-853). A non-measure column (any other role) is not
        a stock/flow question → stay silent (no abstention flood over identifiers /
        dimensions — every column carries a mandatory ``unsure`` claim, so claim presence
        cannot discriminate; the role does).
        """
        semantic = context.get_analysis("semantic")
        if not semantic:
            return []
        reliabilities = context.get_analysis("reliabilities", None) or None
        structural = context.get_analysis("structural", None) or {}

        claim = semantic.get("temporal_behavior_claim")
        adj = measure_temporal_behavior(
            context.table_name,
            context.column_name,
            llm_claim=claim,
            llm_confidence=semantic.get("temporal_behavior_claim_confidence"),
            structural_pattern=structural.get("pattern"),
            structural_match_rate=structural.get("match_rate"),
            reliabilities=reliabilities,
        )

        label, contested = resolved_behaviour(adj)
        if label is None:
            # The pool determined nothing this run. Abstain ONLY for a column the
            # per-column agent read as a MEASURE, so the undetermined stock/flow gap is
            # loud (DAT-847). Every column carries a mandatory ``temporal_behavior_claim``
            # (a required field — ``unsure`` for non-measures, models.py), so the claim's
            # presence cannot discriminate; ``semantic_role`` is the measure signal. A
            # non-measure is not a stock/flow question → no row, so identifiers /
            # dimensions never wallpaper the coverage trace with insufficient_data.
            if semantic.get("semantic_role") != "measure":
                return []
            return [
                self.create_abstention(
                    context,
                    ABSTAIN_INSUFFICIENT_DATA,
                    evidence=[
                        {
                            "claim_field": adj.claim_field,
                            "ignorance": adj.result.ignorance,
                            "llm_claim": claim,
                            "structural_pattern": structural.get("pattern"),
                            "reason": (
                                "no opinionated stock/flow witness — behaviour undetermined "
                                "this run (lone name-read insufficient without data grounding)"
                            ),
                        }
                    ],
                )
            ]
        posterior = dict(zip(CLAIM_SPACE, adj.result.posterior, strict=True))
        # No teach_suggestion (DAT-657): stock/flow is data-determined, so the
        # structural witness already wins — nothing for a human to teach. A wrong
        # grounding is corrected on the grounding path, which owns that teach.
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
                "llm_claim": semantic.get("temporal_behavior_claim"),
                "structural_pattern": structural.get("pattern"),
                "structural_match_rate": structural.get("match_rate"),
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
