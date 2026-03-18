"""Tests for the accept-finding fix via bridge (replaces old handler tests)."""

from __future__ import annotations

from dataraum.entropy.fix_schemas import get_schemas_for_detector
from dataraum.pipeline.fixes import FixInput
from dataraum.pipeline.fixes.bridge import build_fix_documents


class TestOutlierDetectorFixSchemas:
    def test_has_fix_schema(self) -> None:
        schemas = get_schemas_for_detector("outlier_rate")
        assert len(schemas) == 1
        assert schemas[0].action == "accept_finding"
        assert schemas[0].routing == "postprocess"
        assert schemas[0].gate == "quality_review"


class TestAcceptFindingBridge:
    def _get_schema(self):
        return get_schemas_for_detector("outlier_rate")[0]

    def test_single_column(self) -> None:
        schema = self._get_schema()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount"],
            parameters={"detector_id": "outlier_rate"},
            interpretation="Outliers are expected for this column",
        )
        docs = build_fix_documents(schema, fix_input, "orders", "amount", "quality_review")

        assert len(docs) == 1
        doc = docs[0]
        assert doc.target == "config"
        assert doc.action == "accept_finding"
        assert doc.payload["config_path"] == "entropy/thresholds.yaml"
        assert doc.payload["operation"] == "append"
        assert doc.payload["key_path"] == ["detectors", "outlier_rate", "accepted_columns"]
        assert doc.payload["value"] == "orders.amount"

    def test_multiple_columns(self) -> None:
        schema = self._get_schema()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount", "orders.quantity"],
            parameters={"detector_id": "outlier_rate"},
            interpretation="Outliers expected",
        )
        docs = build_fix_documents(schema, fix_input, "orders", "amount", "quality_review")

        assert len(docs) == 2
        values = [d.payload["value"] for d in docs]
        assert "orders.amount" in values
        assert "orders.quantity" in values

    def test_no_affected_columns(self) -> None:
        schema = self._get_schema()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=[],
            parameters={"detector_id": "outlier_rate"},
        )
        docs = build_fix_documents(schema, fix_input, "orders", "amount", "quality_review")
        assert docs == []

    def test_uses_interpretation_as_reason(self) -> None:
        schema = self._get_schema()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount"],
            parameters={"detector_id": "outlier_rate"},
            interpretation="User confirmed outliers are valid business data",
        )
        docs = build_fix_documents(schema, fix_input, "orders", "amount", "quality_review")
        assert docs[0].payload["reason"] == "User confirmed outliers are valid business data"
