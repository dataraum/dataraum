"""Join path determinism entropy detector.

Measures uncertainty in join paths between tables.
Ambiguity (multiple paths to SAME table) indicates higher uncertainty,
not connectivity (paths to different tables, which is normal star schema).
"""

from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class JoinPathDeterminismDetector(EntropyDetector):
    """Detector for join path determinism — relationship-scoped (DAT-408).

    Measures whether the focal relationship is an *unambiguous* way to join its
    two tables. Multiple distinct column-pair paths between the SAME two tables =
    HIGH entropy (ambiguous which to use); a single path = LOW (deterministic). A
    user teach (``ConfigOverlay(type='relationship')``) that picks the path
    resolves the ambiguity.

    Source: the session's relationships (LLM-confirmed + candidates).
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "join_path_determinism"
    layer = Layer.STRUCTURAL
    dimension = Dimension.RELATIONS
    sub_dimension = SubDimension.JOIN_PATH_DETERMINISM
    required_analyses = [AnalysisKey.RELATIONSHIPS]
    scope = "relationship"
    description = "Measures ambiguity in join paths (not just connectivity)"

    def load_data(self, context: DetectorContext) -> None:
        """Load the whole session's relationships (ambiguity needs the full set)."""
        if context.session is None or context.session_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_session_relationships

        context.analysis_results["relationships"] = load_session_relationships(
            context.session, context.session_id
        )

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect join-path ambiguity for the focal relationship (DAT-408).

        Counts the distinct column-pair join paths between the focal relationship's
        two tables. More than one (and not resolved by a preferred-join teach) =
        ambiguous; exactly one = deterministic. Emits a single EntropyObject keyed
        ``relationship:{from}::{to}``.
        """
        config = get_entropy_config()
        detector_config = config.detector("join_path")

        score_deterministic = detector_config.get("score_deterministic", 0.1)
        score_ambiguous = detector_config.get("score_ambiguous", 0.7)

        from_table = context.from_table_name
        to_table = context.to_table_name
        if not from_table or not to_table:
            return []

        rels = context.get_analysis("relationships", [])
        if isinstance(rels, dict):
            rels = rels.get("relationships", [])
        elif not isinstance(rels, list):
            rels = []

        # Distinct column-pair paths connecting THIS pair of tables (either
        # direction). A frozenset of the two column ids dedups mirror rows; >1
        # distinct path = ambiguous which join to use.
        table_pair = {from_table, to_table}
        col_paths: set[frozenset[str]] = set()
        for rel in rels:
            if {self._get(rel, "from_table"), self._get(rel, "to_table")} != table_pair:
                continue
            fc = self._get(rel, "from_column_id")
            tc = self._get(rel, "to_column_id")
            if fc and tc:
                col_paths.add(frozenset({fc, tc}))

        # A user teach that picks the join path resolves the ambiguity.
        from dataraum.entropy.detectors.loaders import load_preferred_join_overlays

        overlays = load_preferred_join_overlays(context.session) if context.session else {}
        resolved = (
            f"{from_table}->{to_table}" in overlays or f"{to_table}->{from_table}" in overlays
        )

        if len(col_paths) > 1 and not resolved:
            score = score_ambiguous
            path_status = "ambiguous"
        else:
            score = score_deterministic
            path_status = "resolved" if resolved else "deterministic"

        evidence = [
            {
                "path_status": path_status,
                "from_table": from_table,
                "to_table": to_table,
                "distinct_join_paths": len(col_paths),
                "resolved_by_overlay": resolved,
            }
        ]

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
            )
        ]

    def _get(self, rel: Any, field: str) -> str | None:
        """Get a string field from a relationship dict or object."""
        if isinstance(rel, dict):
            value = rel.get(field)
        else:
            value = getattr(rel, field, None)
        return str(value) if value is not None else None
