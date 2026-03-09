"""Entropy lab Phase 5 — Fix handler tests.

Proves that each fix handler produces correct config patches and that
detectors respond to the resulting state changes (score reduction).

Categories:
  1. Accept-finding: benford, outlier_rate, null_ratio
  2a. Metadata-write: naming_clarity, unit_declaration, relationship_quality,
      join_path_determinism
"""

from __future__ import annotations

from typing import Any

import pytest

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.semantic.business_meaning import BusinessMeaningDetector
from dataraum.entropy.detectors.semantic.unit_entropy import UnitEntropyDetector
from dataraum.entropy.detectors.structural.relations import JoinPathDeterminismDetector
from dataraum.entropy.detectors.structural.relationship_entropy import (
    RelationshipEntropyDetector,
)
from dataraum.entropy.detectors.value.benford import BenfordDetector
from dataraum.entropy.detectors.value.null_semantics import NullRatioDetector
from dataraum.entropy.detectors.value.outliers import OutlierRateDetector
from dataraum.pipeline.fix_registry import get_default_fix_registry
from dataraum.pipeline.fixes import FixInput


def _make_context(
    table_name: str = "test_table",
    column_name: str = "test_column",
    **analysis_results: Any,
) -> DetectorContext:
    return DetectorContext(
        table_name=table_name,
        column_name=column_name,
        analysis_results=analysis_results,
    )


# ---------------------------------------------------------------------------
# Accept-finding handler — generic
# ---------------------------------------------------------------------------


class TestAcceptFindingHandler:
    """The accept_finding handler writes config patches to entropy/thresholds.yaml."""

    def _get_handler(self):
        registry = get_default_fix_registry()
        entry = registry.find("accept_finding")
        assert entry is not None
        return entry.handler

    def test_produces_correct_config_patch(self) -> None:
        handler = self._get_handler()
        result = handler(
            FixInput(
                action_name="accept_finding",
                affected_columns=["bank_transactions.amount"],
                parameters={"detector_id": "benford"},
                interpretation="Benford deviation expected for financial data",
            ),
            {},
        )
        assert len(result.config_patches) == 1
        patch = result.config_patches[0]
        assert patch.config_path == "entropy/thresholds.yaml"
        assert patch.key_path == ["detectors", "benford", "accepted_columns"]
        assert patch.value == "bank_transactions.amount"
        assert result.requires_rerun == ""

    def test_multiple_columns(self) -> None:
        handler = self._get_handler()
        result = handler(
            FixInput(
                action_name="accept_finding",
                affected_columns=["t.a", "t.b", "t.c"],
                parameters={"detector_id": "outlier_rate"},
            ),
            {},
        )
        assert len(result.config_patches) == 3
        assert all(
            p.key_path == ["detectors", "outlier_rate", "accepted_columns"]
            for p in result.config_patches
        )


# ---------------------------------------------------------------------------
# Category 1 — Accept-finding detector response
# ---------------------------------------------------------------------------


class TestOutlierAcceptance:
    """OutlierRateDetector reads accepted_columns and returns score_accepted."""

    def test_unaccepted_scores_normally(self) -> None:
        detector = OutlierRateDetector()
        context = _make_context(
            statistics={
                "quality": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.08,
                        "iqr_outlier_count": 80,
                        "iqr_lower_fence": 10,
                        "iqr_upper_fence": 200,
                    }
                }
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        assert len(objects) == 1
        score_before = objects[0].score
        assert score_before > 0.3, f"Expected significant score, got {score_before}"
        assert "accepted" not in objects[0].evidence[0]

    def test_accepted_scores_at_floor(self) -> None:
        """After acceptance, score drops to score_accepted (0.2 default)."""
        detector = OutlierRateDetector(
            config={"accepted_columns": ["test_table.test_column"]}
        )
        context = _make_context(
            statistics={
                "quality": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.08,
                        "iqr_outlier_count": 80,
                        "iqr_lower_fence": 10,
                        "iqr_upper_fence": 200,
                    }
                }
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        assert len(objects) == 1
        assert objects[0].score == 0.2
        assert objects[0].evidence[0]["accepted"] is True

    def test_acceptance_preserves_evidence(self) -> None:
        """Evidence should still contain original outlier data."""
        detector = OutlierRateDetector(
            config={"accepted_columns": ["test_table.test_column"]}
        )
        context = _make_context(
            statistics={
                "quality": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.08,
                        "iqr_outlier_count": 80,
                        "iqr_lower_fence": 10,
                        "iqr_upper_fence": 200,
                    }
                }
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        ev = objects[0].evidence[0]
        assert ev["outlier_ratio"] == 0.08
        assert ev["outlier_count"] == 80

    def test_resolution_options_include_accept(self) -> None:
        """Non-accepted columns should have accept_finding in resolution options."""
        detector = OutlierRateDetector()
        context = _make_context(
            statistics={
                "quality": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.03,
                        "iqr_outlier_count": 30,
                        "iqr_lower_fence": 10,
                        "iqr_upper_fence": 200,
                    }
                }
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        actions = {opt.action for opt in objects[0].resolution_options}
        assert "accept_finding" in actions


class TestBenfordAcceptance:
    """BenfordDetector reads accepted_columns and returns score_accepted."""

    def _benford_context(self) -> DetectorContext:
        return _make_context(
            statistics={
                "total_count": 8000,
                "quality": {
                    "benford_compliant": False,
                    "benford_analysis": {
                        "is_compliant": False,
                        "chi_square": 1917.0,
                        "p_value": 0.0,
                    },
                },
            },
            semantic={"semantic_role": "measure"},
        )

    def test_unaccepted_scores_normally(self) -> None:
        detector = BenfordDetector()
        objects = detector.detect(self._benford_context())
        score_before = objects[0].score
        assert score_before > 0.7
        assert "accepted" not in objects[0].evidence[0]

    def test_accepted_scores_at_floor(self) -> None:
        detector = BenfordDetector(
            config={"accepted_columns": ["test_table.test_column"]}
        )
        objects = detector.detect(self._benford_context())
        assert objects[0].score == 0.2
        assert objects[0].evidence[0]["accepted"] is True

    def test_resolution_options_include_accept(self) -> None:
        detector = BenfordDetector()
        objects = detector.detect(self._benford_context())
        actions = {opt.action for opt in objects[0].resolution_options}
        assert "accept_finding" in actions


class TestNullRatioAcceptance:
    """NullRatioDetector reads accepted_columns and returns score_accepted."""

    def test_unaccepted_scores_normally(self) -> None:
        detector = NullRatioDetector()
        context = _make_context(
            statistics={"null_ratio": 0.28, "null_count": 280, "total_count": 1000}
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.28
        assert "accepted" not in objects[0].evidence[0]

    def test_accepted_scores_at_floor(self) -> None:
        detector = NullRatioDetector(
            config={"accepted_columns": ["test_table.test_column"]}
        )
        context = _make_context(
            statistics={"null_ratio": 0.28, "null_count": 280, "total_count": 1000}
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.1  # null_ratio score_accepted default
        assert objects[0].evidence[0]["accepted"] is True

    def test_zero_nulls_not_affected(self) -> None:
        """Columns with zero nulls don't get the accept option."""
        detector = NullRatioDetector()
        context = _make_context(
            statistics={"null_ratio": 0.0, "null_count": 0, "total_count": 1000}
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.0
        actions = {opt.action for opt in objects[0].resolution_options}
        assert "accept_finding" not in actions


# ---------------------------------------------------------------------------
# Category 2a — Metadata-write handlers
# ---------------------------------------------------------------------------


class TestDocumentBusinessMeaningHandler:
    """document_business_meaning writes semantic overrides."""

    def _get_handler(self):
        registry = get_default_fix_registry()
        entry = registry.find("document_business_meaning")
        assert entry is not None
        return entry.handler

    def test_produces_correct_patch(self) -> None:
        handler = self._get_handler()
        result = handler(
            FixInput(
                action_name="document_business_meaning",
                affected_columns=["orders.amount"],
                parameters={
                    "business_name": "Order Amount",
                    "entity_type": "monetary_value",
                    "description": "Total order value in local currency",
                },
            ),
            {},
        )
        assert len(result.config_patches) == 1
        patch = result.config_patches[0]
        assert patch.config_path == "phases/semantic.yaml"
        assert patch.operation == "merge"
        assert patch.key_path == ["overrides", "business_meaning", "orders.amount"]
        assert patch.value["business_name"] == "Order Amount"
        assert result.requires_rerun == "semantic"

    def test_partial_fields(self) -> None:
        """Only specified fields appear in the patch."""
        handler = self._get_handler()
        result = handler(
            FixInput(
                action_name="document_business_meaning",
                affected_columns=["orders.amount"],
                parameters={"business_name": "Order Amount"},
            ),
            {},
        )
        patch = result.config_patches[0]
        assert "business_name" in patch.value
        assert "entity_type" not in patch.value
        assert "description" not in patch.value


class TestDocumentBusinessMeaningDetectorResponse:
    """Proves that filling missing metadata lowers the naming_clarity score."""

    def test_missing_everything_scores_high(self) -> None:
        detector = BusinessMeaningDetector()
        context = _make_context(
            semantic={
                "business_description": "",
                "business_name": None,
                "entity_type": None,
            }
        )
        objects = detector.detect(context)
        assert objects[0].score >= 0.9, f"Expected high score, got {objects[0].score}"

    def test_fully_documented_scores_zero(self) -> None:
        detector = BusinessMeaningDetector()
        context = _make_context(
            semantic={
                "business_description": "Total order value",
                "business_name": "Order Amount",
                "entity_type": "monetary_value",
                "confidence": 1.0,
            }
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.0

    def test_adding_fields_lowers_score(self) -> None:
        """Each additional field lowers the score monotonically."""
        detector = BusinessMeaningDetector()

        # No fields
        ctx0 = _make_context(
            semantic={"business_description": "", "business_name": None, "entity_type": None}
        )
        score0 = detector.detect(ctx0)[0].score

        # Add description
        ctx1 = _make_context(
            semantic={"business_description": "Some desc", "business_name": None, "entity_type": None}
        )
        score1 = detector.detect(ctx1)[0].score

        # Add business_name
        ctx2 = _make_context(
            semantic={
                "business_description": "Some desc",
                "business_name": "Amount",
                "entity_type": None,
            }
        )
        score2 = detector.detect(ctx2)[0].score

        # Add entity_type
        ctx3 = _make_context(
            semantic={
                "business_description": "Some desc",
                "business_name": "Amount",
                "entity_type": "monetary_value",
                "confidence": 1.0,
            }
        )
        score3 = detector.detect(ctx3)[0].score

        print(f"\n  Score progression: {score0:.3f} -> {score1:.3f} -> {score2:.3f} -> {score3:.3f}")
        assert score0 > score1 > score2 >= score3
        assert score3 == 0.0


class TestDeclareUnitHandler:
    """declare_unit writes unit overrides."""

    def _get_handler(self):
        registry = get_default_fix_registry()
        entry = registry.find("declare_unit")
        assert entry is not None
        return entry.handler

    def test_produces_correct_patch(self) -> None:
        handler = self._get_handler()
        result = handler(
            FixInput(
                action_name="declare_unit",
                affected_columns=["bank_transactions.amount"],
                parameters={"unit": "EUR", "unit_source_column": "currency"},
            ),
            {},
        )
        assert len(result.config_patches) == 1
        patch = result.config_patches[0]
        assert patch.config_path == "phases/semantic.yaml"
        assert patch.key_path == ["overrides", "units", "bank_transactions.amount"]
        assert patch.value["unit"] == "EUR"
        assert patch.value["unit_source_column"] == "currency"
        assert result.requires_rerun == "semantic"


class TestDeclareUnitDetectorResponse:
    """Proves that declaring a unit lowers the unit_declaration score."""

    def test_missing_unit_scores_high(self) -> None:
        detector = UnitEntropyDetector()
        context = _make_context(
            typing={"detected_unit": None, "unit_confidence": 0.0},
            semantic={"semantic_role": "measure", "unit_source_column": None},
        )
        objects = detector.detect(context)
        assert len(objects) == 1
        assert objects[0].score >= 0.7, f"Expected high score, got {objects[0].score}"

    def test_declared_unit_scores_low(self) -> None:
        detector = UnitEntropyDetector()
        context = _make_context(
            typing={"detected_unit": "EUR", "unit_confidence": 0.9},
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.1  # score_declared default

    def test_inferred_scores_between(self) -> None:
        detector = UnitEntropyDetector()
        context = _make_context(
            typing={"detected_unit": None, "unit_confidence": 0.0},
            semantic={"semantic_role": "measure", "unit_source_column": "currency"},
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.2  # score_inferred default

    def test_dimensionless_scores_declared(self) -> None:
        detector = UnitEntropyDetector()
        context = _make_context(
            typing={"detected_unit": None, "unit_confidence": 0.0},
            semantic={"semantic_role": "measure", "unit_source_column": "dimensionless"},
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.1  # same as score_declared


class TestConfirmRelationshipHandler:
    """confirm_relationship writes relationship confirmation."""

    def _get_handler(self):
        registry = get_default_fix_registry()
        entry = registry.find("confirm_relationship")
        assert entry is not None
        return entry.handler

    def test_produces_correct_patch(self) -> None:
        handler = self._get_handler()
        result = handler(
            FixInput(
                action_name="confirm_relationship",
                affected_columns=["bank_transactions.account_id"],
                parameters={
                    "from_table": "bank_transactions",
                    "to_table": "chart_of_accounts",
                    "relationship_type": "foreign_key",
                    "cardinality": "many_to_one",
                },
            ),
            {},
        )
        assert len(result.config_patches) == 1
        patch = result.config_patches[0]
        assert patch.config_path == "phases/relationships.yaml"
        assert patch.key_path == [
            "overrides",
            "confirmed_relationships",
            "bank_transactions->chart_of_accounts",
        ]
        assert patch.value["confirmed"] is True
        assert patch.value["relationship_type"] == "foreign_key"
        assert result.requires_rerun == "relationships"


class TestConfirmRelationshipDetectorResponse:
    """Proves that confirming a relationship lowers the relationship_quality score."""

    def _rel(self, is_confirmed: bool = False) -> dict[str, Any]:
        return {
            "from_table": "bank_transactions",
            "to_table": "chart_of_accounts",
            "relationship_type": "foreign_key",
            "cardinality": "many_to_one",
            "confidence": 0.8,
            "is_confirmed": is_confirmed,
            "evidence": {
                "left_referential_integrity": 100.0,
                "orphan_count": 0,
                "cardinality_verified": True,
            },
        }

    def test_unconfirmed_scores_higher(self) -> None:
        detector = RelationshipEntropyDetector()
        context = _make_context(
            relationships=[self._rel(is_confirmed=False)],
        )
        objects = detector.detect(context)
        assert len(objects) == 1
        score_unconfirmed = objects[0].score
        assert score_unconfirmed > 0, f"Expected nonzero, got {score_unconfirmed}"
        # Semantic component is 0.3 (unconfirmed) vs 0.1 (confirmed)
        print(f"\n  Unconfirmed: {score_unconfirmed:.3f}")

    def test_confirmed_scores_lower(self) -> None:
        detector = RelationshipEntropyDetector()
        ctx_before = _make_context(relationships=[self._rel(is_confirmed=False)])
        ctx_after = _make_context(relationships=[self._rel(is_confirmed=True)])

        score_before = detector.detect(ctx_before)[0].score
        score_after = detector.detect(ctx_after)[0].score

        print(f"\n  Before confirm: {score_before:.3f}")
        print(f"  After confirm:  {score_after:.3f}")
        assert score_after < score_before, "Confirmation should lower score"


class TestResolveJoinAmbiguityHandler:
    """resolve_join_ambiguity sets preferred join path."""

    def _get_handler(self):
        registry = get_default_fix_registry()
        entry = registry.find("resolve_join_ambiguity")
        assert entry is not None
        return entry.handler

    def test_produces_correct_patch(self) -> None:
        handler = self._get_handler()
        result = handler(
            FixInput(
                action_name="resolve_join_ambiguity",
                affected_columns=["orders.customer_id"],
                parameters={
                    "table": "orders",
                    "target_table": "customers",
                    "preferred_column": "customer_id",
                },
            ),
            {},
        )
        assert len(result.config_patches) == 1
        patch = result.config_patches[0]
        assert patch.config_path == "phases/relationships.yaml"
        assert patch.key_path == ["overrides", "preferred_joins", "orders->customers"]
        assert patch.value["column"] == "customer_id"
        assert result.requires_rerun == "relationships"


class TestResolveJoinAmbiguityDetectorResponse:
    """Proves join_path_determinism score reflects ambiguity state."""

    def test_ambiguous_scores_high(self) -> None:
        detector = JoinPathDeterminismDetector()
        context = _make_context(
            table_name="orders",
            relationships=[
                {"from_table": "orders", "to_table": "customers"},
                {"from_table": "orders", "to_table": "customers"},  # 2nd path → ambiguous
            ],
        )
        objects = detector.detect(context)
        assert objects[0].score > 0.3

    def test_deterministic_scores_low(self) -> None:
        detector = JoinPathDeterminismDetector()
        context = _make_context(
            table_name="orders",
            relationships=[
                {"from_table": "orders", "to_table": "customers"},
                {"from_table": "orders", "to_table": "products"},
            ],
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.1  # score_deterministic


# ---------------------------------------------------------------------------
# All handlers registered
# ---------------------------------------------------------------------------


class TestAllHandlersRegistered:
    """Every FixAction has a handler in the default registry."""

    def test_all_fix_actions_have_handlers(self) -> None:
        from dataraum.entropy.dimensions import FixAction

        registry = get_default_fix_registry()
        for action in FixAction:
            entry = registry.find(str(action))
            assert entry is not None, f"No handler for FixAction.{action.name}"

    def test_all_detectors_declare_fixable_actions(self) -> None:
        """Every gate-measurable detector declares at least one fixable action."""
        gate_detectors = [
            OutlierRateDetector(),
            BenfordDetector(),
            NullRatioDetector(),
            BusinessMeaningDetector(),
            UnitEntropyDetector(),
            RelationshipEntropyDetector(),
            JoinPathDeterminismDetector(),
        ]
        for detector in gate_detectors:
            assert len(detector.fixable_actions) > 0, (
                f"{detector.detector_id} has no fixable_actions"
            )
            print(f"  {detector.detector_id}: {[str(a) for a in detector.fixable_actions]}")
