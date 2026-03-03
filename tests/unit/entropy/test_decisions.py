"""Tests for decision ledger data model."""

from dataraum.entropy.decisions import Decision, DecisionRecord


class TestDecision:
    def test_decision_is_frozen(self):
        d = Decision(gate_type="structural", action_type="override_type")
        assert d.gate_type == "structural"
        # frozen dataclass — mutation should raise
        try:
            d.gate_type = "semantic"  # type: ignore[misc]
            raise AssertionError("Should have raised FrozenInstanceError")
        except AttributeError:
            pass

    def test_decision_defaults(self):
        d = Decision()
        assert d.decision_id  # UUID auto-generated
        assert d.gate_type == ""
        assert d.before_scores == {}
        assert d.after_scores == {}
        assert d.improved is False
        assert d.actor == ""

    def test_decision_with_scores(self):
        d = Decision(
            gate_type="structural",
            action_type="override_type",
            target="column:orders.amount",
            before_scores={"type_fidelity": 0.6},
            after_scores={"type_fidelity": 0.1},
            improved=True,
        )
        assert d.before_scores["type_fidelity"] == 0.6
        assert d.after_scores["type_fidelity"] == 0.1
        assert d.improved is True


class TestDecisionRecord:
    def test_tablename(self):
        assert DecisionRecord.__tablename__ == "decisions"

    def test_record_fields(self):
        """Verify the model has all expected columns."""
        columns = {c.name for c in DecisionRecord.__table__.columns}
        expected = {
            "decision_id",
            "run_id",
            "source_id",
            "gate_type",
            "blocked_phase",
            "action_type",
            "target",
            "parameters",
            "actor",
            "before_scores",
            "after_scores",
            "improved",
            "source_hash",
            "evidence_summary",
            "decided_at",
            "sequence",
        }
        assert expected.issubset(columns)
