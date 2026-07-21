"""Join path determinism entropy detector.

Measures uncertainty in join paths between tables.
Ambiguity (multiple paths to SAME table) indicates higher uncertainty,
not connectivity (paths to different tables, which is normal star schema).
"""

from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import (
    ABSTAIN_MISSING_INPUTS,
    ABSTAIN_NOT_APPLICABLE,
    EntropyObject,
)


class JoinPathDeterminismDetector(EntropyDetector):
    """Detector for join path determinism — relationship-scoped (DAT-408).

    Measures whether the focal relationship is an *unambiguous* way to join its
    two tables. Multiple distinct column-pair paths between the SAME two tables =
    HIGH entropy (ambiguous which to use); a user teach
    (``ConfigOverlay(type='relationship')``) that picks one path resolves the
    ambiguity (LOW).

    With ≤1 distinct path the ambiguity question is UNANSWERABLE — the loader
    excludes candidates (DAT-405) and the LLM confirms ~one relationship per
    pair, so a single path is the structural norm, not evidence of determinism.
    The detector ABSTAINS (DAT-851/853) instead of emitting a constant
    confident score; it measures only on genuine multi-path ambiguity.

    Source: the session's defined relationships (loaders.load_session_relationships).
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
        """Load the whole catalog's relationships (ambiguity needs the full set)."""
        if context.session is None:
            return
        from dataraum.entropy.detectors.loaders import load_session_relationships

        context.analysis_results["relationships"] = load_session_relationships(
            context.session, run_id=context.run_id
        )

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect join-path ambiguity for the focal relationship (DAT-408).

        Counts the distinct column-pair join paths between the focal relationship's
        two tables. ≤1 path: the ambiguity question doesn't arise — ABSTAIN
        (not_applicable, DAT-851). >1 paths: a real measurement — ambiguous, or
        deterministic when a preferred-join teach picked this path. Emits a single
        EntropyObject keyed ``relationship:{from}::{to}``.
        """
        config = get_entropy_config()
        detector_config = config.detector("join_path")

        score_deterministic = detector_config.get("score_deterministic", 0.1)
        score_ambiguous = detector_config.get("score_ambiguous", 0.7)

        from_table = context.from_table_name
        to_table = context.to_table_name
        if not from_table or not to_table:
            # A relationship context without endpoint names is an upstream
            # resolution gap — trace it, don't skip silently (DAT-853).
            return [
                self.create_abstention(
                    context,
                    ABSTAIN_MISSING_INPUTS,
                    evidence=[{"missing": "focal table names"}],
                )
            ]

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

        # ≤1 distinct path: "which of several joins?" doesn't arise, so a score
        # would be a confident answer to an unasked question (DAT-851: the old
        # constant 0.1 here made every relationship read "measured
        # deterministic" while the branch below was structurally unreachable).
        if len(col_paths) <= 1:
            return [
                self.create_abstention(
                    context,
                    ABSTAIN_NOT_APPLICABLE,
                    evidence=[
                        {
                            "path_status": "single_path",
                            "from_table": from_table,
                            "to_table": to_table,
                            "distinct_join_paths": len(col_paths),
                        }
                    ],
                )
            ]

        # A user teach confirming THIS join path resolves the ambiguity (DAT-409).
        # Keyed on the focal column pair (the path's identity), not the table pair:
        # confirming one path among several between the same two tables marks that
        # path deterministic, leaving the unconfirmed alternatives ambiguous.
        from dataraum.analysis.relationships.utils import load_confirmed_relationship_pairs

        confirmed = load_confirmed_relationship_pairs(context.session) if context.session else set()
        resolved = bool(
            context.from_column_id
            and context.to_column_id
            and frozenset({context.from_column_id, context.to_column_id}) in confirmed
        )

        if resolved:
            score = score_deterministic
            path_status = "resolved"
        else:
            score = score_ambiguous
            path_status = "ambiguous"

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
