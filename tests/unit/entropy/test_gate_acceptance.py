"""Tests for DataFix-based acceptance in the gate."""

from __future__ import annotations

from sqlalchemy.orm import Session

from dataraum.entropy.measurement import _get_accepted_targets, check_contracts
from dataraum.pipeline.fixes.models import DataFix
from dataraum.storage import Source


class TestAssessContractsAcceptance:
    """Test that accepted_targets excludes targets from violations."""

    def test_no_acceptance_all_targets_violate(self):
        scores = {"value.nulls.null_ratio": 0.7}
        thresholds = {"value.nulls": 0.3}
        column_details = {
            "value.nulls.null_ratio": {
                "column:orders.amount": 0.8,
                "column:orders.name": 0.5,
            }
        }

        issues = check_contracts(scores, thresholds, column_details, "gate")

        assert len(issues) == 1
        assert len(issues[0].affected_targets) == 2

    def test_accepted_target_excluded_from_violations(self):
        scores = {"value.nulls.null_ratio": 0.7}
        thresholds = {"value.nulls": 0.3}
        column_details = {
            "value.nulls.null_ratio": {
                "column:orders.amount": 0.8,
                "column:orders.name": 0.5,
            }
        }
        accepted = {
            "value.nulls.null_ratio": {"column:orders.amount"},
        }

        issues = check_contracts(
            scores,
            thresholds,
            column_details,
            "gate",
            accepted_targets=accepted,
        )

        assert len(issues) == 1
        assert "column:orders.amount" not in issues[0].affected_targets
        assert "column:orders.name" in issues[0].affected_targets

    def test_all_targets_accepted_skips_dimension(self):
        """Contract overrule: all violating targets accepted → no violation."""
        scores = {"value.nulls.null_ratio": 0.7}
        thresholds = {"value.nulls": 0.3}
        column_details = {
            "value.nulls.null_ratio": {
                "column:orders.amount": 0.8,
                "column:orders.name": 0.5,
            }
        }
        accepted = {
            "value.nulls.null_ratio": {
                "column:orders.amount",
                "column:orders.name",
            },
        }

        issues = check_contracts(
            scores,
            thresholds,
            column_details,
            "gate",
            accepted_targets=accepted,
        )

        assert issues == []

    def test_accepted_targets_none_is_safe(self):
        scores = {"value.nulls.null_ratio": 0.7}
        thresholds = {"value.nulls": 0.3}
        column_details = {
            "value.nulls.null_ratio": {
                "column:orders.amount": 0.8,
            }
        }

        issues = check_contracts(
            scores,
            thresholds,
            column_details,
            "gate",
            accepted_targets=None,
        )

        assert len(issues) == 1
        assert len(issues[0].affected_targets) == 1

    def test_accepted_for_different_dimension_no_effect(self):
        scores = {"value.nulls.null_ratio": 0.7}
        thresholds = {"value.nulls": 0.3}
        column_details = {
            "value.nulls.null_ratio": {
                "column:orders.amount": 0.8,
            }
        }
        accepted = {
            "value.outliers.outlier_rate": {"column:orders.amount"},
        }

        issues = check_contracts(
            scores,
            thresholds,
            column_details,
            "gate",
            accepted_targets=accepted,
        )

        assert len(issues) == 1
        assert "column:orders.amount" in issues[0].affected_targets


def _ensure_source(session: Session, source_id: str = "src-1") -> None:
    """Ensure a Source row exists for FK constraint."""
    existing = session.get(Source, source_id)
    if not existing:
        session.add(Source(source_id=source_id, name=source_id, source_type="csv"))
        session.flush()


class TestGetAcceptedTargets:
    """Test _get_accepted_targets queries DataFix correctly."""

    def test_empty_when_no_datafix(self, session: Session):
        _ensure_source(session)
        result = _get_accepted_targets(session, "src-1")
        assert result == {}

    def test_groups_by_dimension(self, session: Session):
        _ensure_source(session)
        session.add(
            DataFix(
                fix_id="f1",
                source_id="src-1",
                action="document_accepted_null_ratio",
                target="metadata",
                dimension="value.nulls.null_ratio",
                table_name="orders",
                column_name="amount",
                payload={},
                status="applied",
                ordinal=0,
            )
        )
        session.add(
            DataFix(
                fix_id="f2",
                source_id="src-1",
                action="document_accepted_outlier_rate",
                target="metadata",
                dimension="value.outliers.outlier_rate",
                table_name="orders",
                column_name="amount",
                payload={},
                status="applied",
                ordinal=0,
            )
        )
        session.flush()

        result = _get_accepted_targets(session, "src-1")

        assert "value.nulls.null_ratio" in result
        assert "column:orders.amount" in result["value.nulls.null_ratio"]
        assert "value.outliers.outlier_rate" in result
        assert "column:orders.amount" in result["value.outliers.outlier_rate"]

    def test_ignores_failed_datafix(self, session: Session):
        _ensure_source(session)
        session.add(
            DataFix(
                fix_id="f1",
                source_id="src-1",
                action="document_accepted_null_ratio",
                target="metadata",
                dimension="value.nulls.null_ratio",
                table_name="orders",
                column_name="amount",
                payload={},
                status="failed",
                ordinal=0,
            )
        )
        session.flush()

        result = _get_accepted_targets(session, "src-1")
        assert result == {}

    def test_ignores_non_acceptance_actions(self, session: Session):
        _ensure_source(session)
        session.add(
            DataFix(
                fix_id="f1",
                source_id="src-1",
                action="document_type_override",
                target="config",
                dimension="structural.types.type_fidelity",
                table_name="orders",
                column_name="amount",
                payload={},
                status="applied",
                ordinal=0,
            )
        )
        session.flush()

        result = _get_accepted_targets(session, "src-1")
        assert result == {}

    def test_table_scoped_target(self, session: Session):
        _ensure_source(session)
        session.add(
            DataFix(
                fix_id="f1",
                source_id="src-1",
                action="document_accepted_business_cycle",
                target="metadata",
                dimension="semantic.cycles.business_cycle_health",
                table_name="orders",
                column_name=None,
                payload={},
                status="applied",
                ordinal=0,
            )
        )
        session.flush()

        result = _get_accepted_targets(session, "src-1")
        dim = "semantic.cycles.business_cycle_health"
        assert dim in result
        assert "table:orders" in result[dim]

    def test_scoped_to_source_id(self, session: Session):
        _ensure_source(session, "src-1")
        _ensure_source(session, "src-2")
        session.add(
            DataFix(
                fix_id="f1",
                source_id="src-1",
                action="document_accepted_null_ratio",
                target="metadata",
                dimension="value.nulls.null_ratio",
                table_name="orders",
                column_name="amount",
                payload={},
                status="applied",
                ordinal=0,
            )
        )
        session.flush()

        result = _get_accepted_targets(session, "src-2")
        assert result == {}
