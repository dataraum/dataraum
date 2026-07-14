"""Unit-source entropy detector (semantic / catalogue grain).

The SEMANTIC half of unit detection (DAT-647): can a measure's unit be
determined from the CATALOGUE — a sibling dimension column defines it (e.g.
``currency``), or the measure is inherently dimensionless? This is a
catalogue-grain question: its input ``ColumnConcept.unit_source_column`` is
authored by the ``semantic_per_table`` agent under the begin_session run, so
this detector runs at the **session detect** (declared on ``semantic_per_table``
in ``pipeline.yaml``), where ``load_semantic`` resolves the ColumnConcept.

Split from ``unit_entropy`` (DAT-647): the value-carried unit (a token in the
values, typed + cast at the typing phase → ``detected_unit``) is a distinct,
add_source-grain fact owned by :class:`UnitEntropyDetector`. Conflating the two
in one add_source-grain detector was the bug — the catalogue input can't exist
at add_source, so every currency measure read ``missing`` and blocked. Kept
separate here, each read at its own native grain.

Score: 0.0 when the unit is resolved (a dimension column or dimensionless),
1.0 when a measure has no determinable unit source. Aggregation-safety signal.

Source: semantic.semantic_role, semantic.unit_source_column (ColumnConcept).
"""

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class UnitSourceEntropyDetector(EntropyDetector):
    """Detector for whether a measure's unit is determinable from the catalogue.

    A measure whose unit is resolved by cross-column inference (a ``currency``
    dimension) or that is dimensionless has no unit-source entropy (→ 0). A
    measure with no determinable unit source is unsafe to aggregate (→ 1.0); the
    resolution is the concept vocabulary's ``unit_from_concept`` (declared via the
    ``frame`` stage's typed concepts, DAT-728), never a deterministic override.
    """

    detector_id = "unit_source"
    layer = Layer.SEMANTIC
    dimension = Dimension.UNITS
    sub_dimension = SubDimension.UNIT_SOURCE
    required_analyses = [AnalysisKey.SEMANTIC]
    description = "Measures whether a measure's unit is determinable from the catalogue"

    def load_data(self, context: DetectorContext) -> None:
        """Load semantic (role + catalogue-grain unit_source_column) for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_semantic

        sem = load_semantic(
            context.session, context.column_id, context.run_id, base_runs=context.base_runs
        )
        if sem is not None:
            context.analysis_results["semantic"] = sem

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect unit-source entropy. Only applies to columns with role 'measure'."""
        semantic = context.get_analysis("semantic", {})

        if hasattr(semantic, "semantic_role"):
            semantic_role = semantic.semantic_role
        else:
            semantic_role = semantic.get("semantic_role")

        # Only measures need a unit; dimensions/identifiers don't.
        if semantic_role != "measure":
            return []

        if hasattr(semantic, "unit_source_column"):
            unit_source_column = semantic.unit_source_column
        else:
            unit_source_column = semantic.get("unit_source_column")

        # The unit is KNOWN (→ 0) when a dimension column defines it or the
        # measure is dimensionless; otherwise no source is determinable (→ 1.0).
        if unit_source_column == "dimensionless":
            score, unit_status = 0.0, "dimensionless"
        elif unit_source_column:
            score, unit_status = 0.0, "resolved_from_dimension"
        else:
            score, unit_status = 1.0, "unresolved"

        evidence_dict: dict[str, object] = {
            "semantic_role": semantic_role,
            "unit_status": unit_status,
        }
        if unit_source_column:
            evidence_dict["unit_source_column"] = unit_source_column

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=[evidence_dict],
            )
        ]
