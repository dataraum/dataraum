"""Null-semantics adjudication detector (docs/architecture/entropy.md, DAT-457).

Pools three witnesses per *rejected* token into the claim space
{is-null, is-value} and emits ONE EntropyObject per column whose score is the
worst-token conflict ``C`` — high when the data treats a token as a null marker
but the curated vocabulary has never seen it (the novel-sentinel case that used
to need a hard-coded token). Per-token detail (``C``, ``U``, posterior,
witnesses) rides in evidence, so the provenance is loud.

This is adjudication entropy (witnesses disagree about a claim), not surprise:
it runs through the pooling engine. NOTE: dedicated ``claim_witnesses``-row
persistence is the next slice; here the witness breakdown lives in evidence.
"""

from __future__ import annotations

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.measurements.null_semantics import CLAIM_SPACE, measure_null_semantics
from dataraum.entropy.models import EntropyObject, WitnessClaim

_EMPTY_QUARANTINE = {"rejected_tokens": [], "total_rejected": 0}


class NullSemanticsDetector(EntropyDetector):
    """Adjudicate rejected tokens as null markers vs genuine values."""

    detector_id = "null_semantics"
    layer = Layer.VALUE
    dimension = Dimension.NULLS
    sub_dimension = SubDimension.NULL_SEMANTICS
    required_analyses = [AnalysisKey.TYPING]
    description = "Adjudicates rejected tokens as null markers vs values (witness pooling)"

    def load_data(self, context: DetectorContext) -> None:
        """Load typing, per-token quarantine counts, the null vocab, and reliabilities."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_quarantine_tokens, load_typing
        from dataraum.entropy.reliabilities import get_reliability_config
        from dataraum.sources.csv.null_values import load_null_value_config

        typing = load_typing(context.session, context.column_id, context.run_id)
        if typing is None:
            return
        context.analysis_results["typing"] = typing
        context.analysis_results["quarantine_tokens"] = load_quarantine_tokens(
            context.session, context.column_id, context.duckdb_conn, context.run_id
        ) or dict(_EMPTY_QUARANTINE)
        context.analysis_results["null_vocab"] = load_null_value_config().get_null_strings()
        # Calibrated witness reliabilities (DAT-450); empty → measurement's fallback.
        context.analysis_results["reliabilities"] = get_reliability_config().for_measurement(
            self.detector_id
        )

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Pool each rejected token; emit one per-column object (worst-token C)."""
        typing = context.get_analysis("typing")
        if not typing:
            return []
        quarantine = context.get_analysis("quarantine_tokens", dict(_EMPTY_QUARANTINE))
        vocab = context.get_analysis("null_vocab", [])
        reliabilities = context.get_analysis("reliabilities", None) or None

        adjudications = measure_null_semantics(
            quarantine, typing, vocab, reliabilities=reliabilities
        )
        if not adjudications:
            return []

        score = max(a.result.conflict for a in adjudications)
        # Evidence is the per-token pooled SUMMARY; the witness distributions go
        # to the claim_witnesses table via obj.witnesses (engine-persisted) — not
        # duplicated here.
        evidence = [
            {
                "token": a.token,
                "claim_field": a.claim_field,
                "conflict": a.result.conflict,
                "ignorance": a.result.ignorance,
                "posterior": dict(zip(CLAIM_SPACE, a.result.posterior, strict=True)),
            }
            for a in adjudications
        ]
        obj = self.create_entropy_object(context=context, score=score, evidence=evidence)
        obj.witnesses = [
            WitnessClaim(
                claim_field=a.claim_field,
                witness_id=w.witness_id,
                distribution=dict(zip(CLAIM_SPACE, w.distribution, strict=True)),
                reliability=w.reliability,
            )
            for a in adjudications
            for w in a.witnesses
        ]
        return [obj]
