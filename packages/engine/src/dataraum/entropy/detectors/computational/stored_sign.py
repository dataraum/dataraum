"""Stored-sign detector — how a monetary balance is stored (ADR-0009, DAT-875).

Column-scoped semantic adjudication. Pools the catalogue agent's INDEPENDENT
storage-convention claim (authored in ``catalogue_semantics``) against the
data-grounded sign partition the ``aggregation_lineage`` phase measured — the
winning-pattern voter sets under a signed convention and under its negation. One
family under one sign ⇒ ``ledger_signed``; two disjoint families under opposite
signs ⇒ ``natural_balance``. See ``entropy/measurements/stored_sign.py`` for the
witnesses and why the claim alone cannot settle this.

A resolved verdict emits one witnessed ``EntropyObject`` carrying the convention and
the pooled conflict; a measure the pool could NOT determine emits a wave-2
``abstained`` object (``insufficient_data``) instead of a silent skip. Neither path
carries a teach suggestion: the storage convention is data-determined, so the
partition witness already wins, and there is no format for a human to teach here.

Both paths reach readiness because ``stored_sign`` HAS a ``loss.yaml`` row —
``readiness_context`` gates every object on ``LossConfig.is_loss_measurement``, so
without one an abstention would be dropped before ``abstained_loss`` (never a
``gap_abstained``, never moving ``coverage``) and a measured object would fall to
``direct_signals`` carrying zero banded risk. The row is what makes the coverage
claim above true. Its priors, and the ``reliabilities.yaml`` witness priors, are
declared PLACEHOLDERS (``calibrated: false`` per-measurement provenance, so the
file-global ``calibrated: true`` cannot vouch for them) — shaped after sibling rows,
never tuned to a metric, pending the eval rig.

This detector runs only where BOTH its inputs can exist — the begin_session
``session_detect``, where the catalogue run holds the ``ColumnConcept`` claim and the
lineage rows carry the partition. At add_source there is no ColumnConcept under the
run, so ``load_semantic`` returns no claim and the partition loader finds no
exact-run row; the detector stays silent rather than abstaining on a grain it was
never meant to answer at.
"""

from __future__ import annotations

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.entropy.measurements.stored_sign import (
    CLAIM_SPACE,
    measure_stored_sign,
    resolved_stored_sign,
)
from dataraum.entropy.models import ABSTAIN_INSUFFICIENT_DATA, EntropyObject, WitnessClaim


class StoredSignDetector(EntropyDetector):
    """Pool the LLM storage-convention claim vs the measured sign partition."""

    detector_id = "stored_sign"
    layer = Layer.SEMANTIC
    # UNITS, not TEMPORAL: a sign convention is a measure-EXPRESSION property, the
    # same family as unit_declaration / unit_source — what the number means before
    # any arithmetic is done with it.
    dimension = Dimension.UNITS
    sub_dimension = SubDimension.STORED_SIGN
    scope = "column"
    description = "Stored sign: LLM claim vs measured sign partition (data-determined)"

    def load_data(self, context: DetectorContext) -> None:
        """Load the column's catalogue claim and this run's measured sign partition.

        Both are exact-run reads at the catalogue grain — present at the
        begin_session ``session_detect`` and absent everywhere else (see the module
        docstring).
        """
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_semantic, load_sign_partition
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
        partition = load_sign_partition(context.session, context.column_id, context.run_id)
        if partition is not None:
            context.analysis_results["partition"] = partition

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Pool the claim vs the partition; emit a measurement or an abstention.

        A resolved convention → one measured object carrying the posterior and the
        pooled conflict/ignorance. Total ignorance is a wave-2 ABSTENTION for a column
        the per-column agent read as a MEASURE — persisted as ``insufficient_data`` so
        the undetermined measure is visible in the coverage/abstention trace and never
        reads as measured-clean (DAT-847/DAT-853). A non-measure column is not a
        storage-convention question → it is gated out ahead of both paths, so
        identifiers and dimensions never wallpaper the trace.

        The claim is NOT the measure signal here: a column with no ``ColumnConcept``
        row under this run carries no claim at all, so its absence would silence
        exactly the add_source grain the module docstring already excludes; and every
        catalogued column carries a mandatory claim (``unsure`` for the majority), so
        claim presence cannot discriminate. ``semantic_role`` does — the same
        discriminator temporal_behavior uses.
        """
        semantic = context.get_analysis("semantic")
        if not semantic:
            return []
        # The role gate governs BOTH paths. A non-measure is not a storage-convention
        # question, so it must not emit a MEASURED verdict either — a concept-bearing
        # dimension that happens to carry a lineage partition would otherwise be
        # labelled, and the label would be served. Every catalogued column carries a
        # mandatory claim, so claim presence cannot discriminate; the role does.
        if semantic.get("semantic_role") != "measure":
            return []
        reliabilities = context.get_analysis("reliabilities", None) or None
        partition = context.get_analysis("partition", None) or {}

        claim = semantic.get("stored_sign_claim")
        adj = measure_stored_sign(
            context.table_name,
            context.column_name,
            llm_claim=claim,
            llm_confidence=semantic.get("stored_sign_claim_confidence"),
            n_entities=partition.get("n_entities"),
            fired_primary=partition.get("fired_primary"),
            fired_mirror=partition.get("fired_mirror"),
            fired_both=partition.get("fired_both"),
            reliabilities=reliabilities,
        )

        label, contested = resolved_stored_sign(adj)
        if label is None:
            # Nothing determined this run. The role already gated above; what remains
            # is the GRAIN check — a column with no claim slot at all is at the
            # add_source grain (no ColumnConcept under this run), which is not an
            # undetermined column but a question that was never asked here.
            if claim is None:
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
                            "sign_fired_primary": partition.get("fired_primary"),
                            "sign_fired_mirror": partition.get("fired_mirror"),
                            "reason": (
                                "no opinionated stored-sign witness — storage convention "
                                "undetermined this run (a name read alone cannot settle "
                                "how values are stored)"
                            ),
                        }
                    ],
                )
            ]
        posterior = dict(zip(CLAIM_SPACE, adj.result.posterior, strict=True))
        # No teach_suggestion: the convention is data-determined, so the partition
        # witness already wins. A wrong reading is corrected on the grounding path.
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
                "llm_claim": claim,
                "sign_fired_primary": partition.get("fired_primary"),
                "sign_fired_mirror": partition.get("fired_mirror"),
                "sign_fired_both": partition.get("fired_both"),
                "n_entities": partition.get("n_entities"),
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
