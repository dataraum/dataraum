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

WHERE THOSE OBJECTS DO AND DO NOT SURFACE — ``stored_sign`` has NO entry in
``dataraum-config/entropy/loss.yaml`` yet, and that omission is deliberate: per-intent
loss weights would be invented numbers, the same argument that defers the
``reliabilities.yaml`` entry (see the measurement module). The consequence is
concrete and must not be overstated. ``readiness_context`` gates every object on
``LossConfig.is_loss_measurement``, so today:

* an ABSTENTION is dropped before ``abstained_loss`` — it never becomes a
  ``gap_abstained``, never moves ``coverage``, and never enters the abstention
  payload. It is NOT visible in the DAT-853 coverage trace.
* a MEASURED object routes to ``direct_signals``, not ``loss_objects``, so it is
  carried as a direct signal but contributes ZERO banded readiness risk.

Both objects ARE persisted to ``entropy_objects`` with their evidence, so the trace
exists in the table and the resolve pass reads it — the gap is purely in the
readiness/coverage rollup, and it closes when the loss entry is calibrated.

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
        the undetermined measure is a row in ``entropy_objects`` rather than a silent
        skip (it does NOT yet reach the readiness coverage trace — see the module
        docstring on the missing loss entry). A non-measure column is not a
        storage-convention question → stay silent, so identifiers and dimensions never
        wallpaper the trace.

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
            # Nothing determined this run. Abstain only where the question applies and
            # the catalogue grain exists: a measure column that HAS a claim slot. A
            # column with no claim is at the wrong grain (add_source), not undetermined.
            if semantic.get("semantic_role") != "measure" or claim is None:
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
