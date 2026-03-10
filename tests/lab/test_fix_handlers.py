"""Fix bridge + detector response tests.

Proves that build_fix_documents() produces correct FixDocuments for each
action pattern, and that detectors respond to the resulting state changes
(score reduction after acceptance/documentation).

Categories:
  1. Accept-finding: benford, outlier_rate, null_ratio
  2a. Metadata-write: naming_clarity, unit_declaration, relationship_quality,
      join_path_determinism
"""

from __future__ import annotations

from typing import Any

from dataraum.entropy.detectors.base import DetectorContext, get_default_registry
from dataraum.entropy.detectors.semantic.business_meaning import BusinessMeaningDetector
from dataraum.entropy.detectors.semantic.unit_entropy import UnitEntropyDetector
from dataraum.entropy.detectors.structural.relations import JoinPathDeterminismDetector
from dataraum.entropy.detectors.structural.relationship_entropy import (
    RelationshipEntropyDetector,
)
from dataraum.entropy.detectors.value.benford import BenfordDetector
from dataraum.entropy.detectors.value.null_semantics import NullRatioDetector
from dataraum.entropy.detectors.value.outliers import OutlierRateDetector
from dataraum.pipeline.fixes import FixInput
from dataraum.pipeline.fixes.bridge import build_fix_documents


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
# Accept-finding bridge — generic
# ---------------------------------------------------------------------------


class TestAcceptFindingBridge:
    """The accept_finding bridge writes FixDocuments targeting entropy/thresholds.yaml."""

    def test_benford_produces_correct_document(self) -> None:
        schema = BenfordDetector().fix_schemas[0]
        docs = build_fix_documents(
            schema,
            FixInput(
                action_name="accept_finding",
                affected_columns=["bank_transactions.amount"],
                parameters={"detector_id": "benford"},
                interpretation="Benford deviation expected for financial data",
            ),
            "bank_transactions",
            "amount",
            "quality_review",
        )
        assert len(docs) == 1
        doc = docs[0]
        assert doc.target == "config"
        assert doc.action == "accept_finding"
        assert doc.payload["config_path"] == "entropy/thresholds.yaml"
        assert doc.payload["key_path"] == ["detectors", "benford", "accepted_columns"]
        assert doc.payload["operation"] == "append"
        assert doc.payload["value"] == "bank_transactions.amount"

    def test_multiple_columns(self) -> None:
        schema = OutlierRateDetector().fix_schemas[0]
        docs = build_fix_documents(
            schema,
            FixInput(
                action_name="accept_finding",
                affected_columns=["t.a", "t.b", "t.c"],
                parameters={"detector_id": "outlier_rate"},
            ),
            "t",
            "a",
            "quality_review",
        )
        assert len(docs) == 3
        assert all(
            d.payload["key_path"] == ["detectors", "outlier_rate", "accepted_columns"]
            for d in docs
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
# Category 2a — Metadata-write bridge
# ---------------------------------------------------------------------------


class TestDocumentBusinessMeaningBridge:
    """document_business_meaning writes semantic overrides via bridge."""

    def _get_schema(self):
        return BusinessMeaningDetector().fix_schemas[0]

    def test_produces_correct_document(self) -> None:
        schema = self._get_schema()
        docs = build_fix_documents(
            schema,
            FixInput(
                action_name="document_business_meaning",
                affected_columns=["orders.amount"],
                parameters={
                    "business_name": "Order Amount",
                    "entity_type": "monetary_value",
                    "description": "Total order value in local currency",
                },
            ),
            "orders",
            "amount",
            "semantic",
        )
        assert len(docs) == 1
        doc = docs[0]
        assert doc.target == "config"
        assert doc.payload["config_path"] == "phases/semantic.yaml"
        assert doc.payload["operation"] == "merge"
        assert doc.payload["key_path"] == ["overrides", "business_meaning", "orders.amount"]
        assert doc.payload["value"]["business_name"] == "Order Amount"

    def test_partial_fields(self) -> None:
        """Only specified fields appear in the value."""
        schema = self._get_schema()
        docs = build_fix_documents(
            schema,
            FixInput(
                action_name="document_business_meaning",
                affected_columns=["orders.amount"],
                parameters={"business_name": "Order Amount"},
            ),
            "orders",
            "amount",
            "semantic",
        )
        value = docs[0].payload["value"]
        assert "business_name" in value
        assert "entity_type" not in value
        assert "description" not in value


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

        assert score0 > score1 > score2 >= score3
        assert score3 == 0.0


class TestDeclareUnitBridge:
    """declare_unit writes unit overrides via bridge."""

    def _get_schema(self):
        return UnitEntropyDetector().fix_schemas[0]

    def test_produces_correct_document(self) -> None:
        schema = self._get_schema()
        docs = build_fix_documents(
            schema,
            FixInput(
                action_name="declare_unit",
                affected_columns=["bank_transactions.amount"],
                parameters={"unit": "EUR", "unit_source_column": "currency"},
            ),
            "bank_transactions",
            "amount",
            "semantic",
        )
        assert len(docs) == 1
        doc = docs[0]
        assert doc.payload["config_path"] == "phases/semantic.yaml"
        assert doc.payload["key_path"] == ["overrides", "units", "bank_transactions.amount"]
        assert doc.payload["value"]["unit"] == "EUR"
        assert doc.payload["value"]["unit_source_column"] == "currency"


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
        assert objects[0].score == 0.1  # score_inferred default

    def test_dimensionless_scores_declared(self) -> None:
        detector = UnitEntropyDetector()
        context = _make_context(
            typing={"detected_unit": None, "unit_confidence": 0.0},
            semantic={"semantic_role": "measure", "unit_source_column": "dimensionless"},
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.1  # same as score_declared


class TestConfirmRelationshipBridge:
    """confirm_relationship writes relationship confirmation via bridge."""

    def _get_schema(self):
        return RelationshipEntropyDetector().fix_schemas[0]

    def test_produces_correct_document(self) -> None:
        schema = self._get_schema()
        docs = build_fix_documents(
            schema,
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
            "bank_transactions",
            "account_id",
            "relationships",
        )
        assert len(docs) == 1
        doc = docs[0]
        assert doc.payload["config_path"] == "phases/relationships.yaml"
        assert doc.payload["key_path"] == [
            "overrides",
            "confirmed_relationships",
            "bank_transactions->chart_of_accounts",
        ]
        assert doc.payload["value"]["relationship_type"] == "foreign_key"
        assert doc.payload["value"]["cardinality"] == "many_to_one"
        # from_table/to_table are key_template fields — excluded from value
        assert "from_table" not in doc.payload["value"]
        assert "to_table" not in doc.payload["value"]


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

    def test_confirmed_scores_lower(self) -> None:
        detector = RelationshipEntropyDetector()
        ctx_before = _make_context(relationships=[self._rel(is_confirmed=False)])
        ctx_after = _make_context(relationships=[self._rel(is_confirmed=True)])

        score_before = detector.detect(ctx_before)[0].score
        score_after = detector.detect(ctx_after)[0].score

        assert score_after < score_before, "Confirmation should lower score"


class TestResolveJoinAmbiguityBridge:
    """resolve_join_ambiguity sets preferred join path via bridge."""

    def _get_schema(self):
        return JoinPathDeterminismDetector().fix_schemas[0]

    def test_produces_correct_document(self) -> None:
        schema = self._get_schema()
        docs = build_fix_documents(
            schema,
            FixInput(
                action_name="resolve_join_ambiguity",
                affected_columns=["orders.customer_id"],
                parameters={
                    "table": "orders",
                    "target_table": "customers",
                    "preferred_column": "customer_id",
                },
            ),
            "orders",
            "customer_id",
            "relationships",
        )
        assert len(docs) == 1
        doc = docs[0]
        assert doc.payload["config_path"] == "phases/relationships.yaml"
        assert doc.payload["key_path"] == ["overrides", "preferred_joins", "orders->customers"]
        assert doc.payload["value"]["preferred_column"] == "customer_id"
        # table/target_table are key_template fields — excluded from value
        assert "table" not in doc.payload["value"]
        assert "target_table" not in doc.payload["value"]


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
# Schema coverage
# ---------------------------------------------------------------------------


class TestFixSchemaCoverage:
    """Every fixable detector has fix_schemas and all FixActions are covered."""

    def test_all_fix_actions_have_schemas(self) -> None:
        """Every FixAction maps to at least one fix_schema in the registry."""
        from dataraum.entropy.dimensions import FixAction

        registry = get_default_registry()
        for action in FixAction:
            schema = registry.get_fix_schema(str(action))
            assert schema is not None, f"No fix_schema for FixAction.{action.name}"

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

    def test_all_detectors_declare_fix_schemas(self) -> None:
        """Every gate-measurable detector declares at least one fix_schema."""
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
            assert len(detector.fix_schemas) > 0, (
                f"{detector.detector_id} has no fix_schemas"
            )

    def test_fix_schemas_have_requires_rerun(self) -> None:
        """All fix_schemas declare which phase to re-run."""
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
            for schema in detector.fix_schemas:
                assert schema.requires_rerun is not None, (
                    f"{detector.detector_id}.{schema.action} missing requires_rerun"
                )
