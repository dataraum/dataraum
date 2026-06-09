"""Unit-consistency adjudication detector (ADR-0009, DAT-428).

Pools two witnesses — log-magnitude bimodality + the declared unit — per NUMERIC
column into {consistent, mixed} and emits ONE EntropyObject whose score is the
pooled conflict ``C``: high when the values span SCALES (kEUR among EUR) yet the
column is declared a single unit. This is the C-driven half of the ADR-0009 unit
split (the U-driven ``unit_declaration`` stays with the old ``unit_entropy``). The
per-witness breakdown rides on ``obj.witnesses`` → the ``claim_witnesses`` table.
"""

from __future__ import annotations

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.measurements.unit_consistency import CLAIM_SPACE, measure_unit_consistency
from dataraum.entropy.models import EntropyObject, WitnessClaim

# resolved_type prefixes that carry a numeric scale to mix.
_NUMERIC_TYPES = (
    "DECIMAL",
    "NUMERIC",
    "FLOAT",
    "DOUBLE",
    "REAL",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "SMALLINT",
    "TINYINT",
)


class UnitConsistencyDetector(EntropyDetector):
    """Adjudicate a numeric column's scale consistency (witness pooling)."""

    detector_id = "unit_consistency"
    layer = Layer.SEMANTIC
    dimension = Dimension.UNITS
    sub_dimension = SubDimension.UNIT_CONSISTENCY
    required_analyses = [AnalysisKey.TYPING]
    description = (
        "Adjudicates whether a numeric column mixes scales under one unit (witness pooling)"
    )

    def load_data(self, context: DetectorContext) -> None:
        """Load the column's numeric values + declared-unit confidence + reliabilities."""
        if context.session is None or context.column_id is None or context.duckdb_conn is None:
            return
        if context.table_name is None or context.column_name is None:
            return
        from dataraum.entropy.detectors.loaders import load_typing
        from dataraum.entropy.reliabilities import get_reliability_config

        typing = load_typing(context.session, context.column_id, context.run_id)
        if typing is None:
            return
        resolved = (typing.get("resolved_type") or "").upper()
        if not resolved.startswith(_NUMERIC_TYPES):
            return  # only a numeric column has a scale to mix
        context.analysis_results["unit_confidence"] = typing.get("unit_confidence")

        col = context.column_name.replace('"', '""')
        sql = f'SELECT "{col}" FROM lake.typed."{context.table_name}" WHERE "{col}" IS NOT NULL'
        context.analysis_results["values"] = [
            float(row[0])
            for row in context.duckdb_conn.execute(sql).fetchall()
            if row[0] is not None
        ]
        context.analysis_results["reliabilities"] = get_reliability_config().for_measurement(
            self.detector_id
        )

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Pool the two witnesses; emit one per-column object (pooled conflict C)."""
        values = context.get_analysis("values")
        if not values:
            return []
        unit_confidence = context.get_analysis("unit_confidence", None)
        reliabilities = context.get_analysis("reliabilities", None) or None

        adj = measure_unit_consistency(values, unit_confidence, reliabilities=reliabilities)
        posterior = (
            dict(zip(CLAIM_SPACE, adj.result.posterior, strict=True))
            if adj.result.posterior
            else {}
        )
        evidence = [
            {
                "claim_field": "unit",
                "conflict": adj.result.conflict,
                "ignorance": adj.result.ignorance,
                "posterior": posterior,
                "unit_confidence": unit_confidence,
            }
        ]
        obj = self.create_entropy_object(
            context=context, score=adj.result.conflict, evidence=evidence
        )
        obj.witnesses = [
            WitnessClaim(
                claim_field="unit",
                witness_id=w.witness_id,
                distribution=dict(zip(CLAIM_SPACE, w.distribution, strict=True)),
                reliability=w.reliability,
            )
            for w in adj.witnesses
        ]
        return [obj]
