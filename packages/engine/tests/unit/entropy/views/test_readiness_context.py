"""Tests for the entropy readiness context assembler (loss-only, per-column).

Readiness rolls detector scores up the LOSS table (entropy/loss.yaml): per-intent
risk = clamp01(Σ weight·value), banded ready/investigate/blocked. No network DAG
(DAT-442). Test categories:
A. Dataclass defaults
B. Helper functions
C. Per-column assembly
D. Multi-measurement / multi-intent assembly
"""

from dataraum.entropy.views.readiness_context import (
    ColumnNodeEvidence,
    ColumnReadinessResult,
    DirectSignal,
    EntropyForReadiness,
    IntentReadiness,
    _object_to_direct_signal,
    assemble_readiness_context,
)

from .conftest import make_entropy_object

# ===================================================================
# A. Dataclass defaults
# ===================================================================


class TestDataclassDefaults:
    def test_column_node_evidence_defaults(self):
        cne = ColumnNodeEvidence()
        assert cne.node_name == ""
        assert cne.state == "low"
        assert cne.score == 0.0
        assert cne.impact_delta == 0.0
        assert cne.evidence == []
        assert cne.detector_id == ""

    def test_column_result_defaults(self):
        cnr = ColumnReadinessResult()
        assert cnr.target == ""
        assert cnr.node_evidence == []
        assert cnr.intents == []
        assert cnr.top_priority_node == ""
        assert cnr.top_priority_impact == 0.0
        assert cnr.nodes_observed == 0
        assert cnr.nodes_high == 0
        assert cnr.worst_intent_risk == 0.0
        assert cnr.readiness == "ready"

    def test_direct_signal_defaults(self):
        ds = DirectSignal()
        assert ds.dimension_path == ""
        assert ds.score == 0.0
        assert ds.evidence == []

    def test_intent_readiness_defaults(self):
        ir = IntentReadiness()
        assert ir.intent_name == ""
        assert ir.readiness == "ready"
        assert ir.risk == 0.0
        assert ir.drivers == []

    def test_entropy_for_readiness_defaults(self):
        efn = EntropyForReadiness()
        assert efn.columns == {}
        assert efn.direct_signals == []
        assert efn.total_columns == 0
        assert efn.columns_blocked == 0
        assert efn.columns_investigate == 0
        assert efn.columns_ready == 0
        assert efn.total_direct_signals == 0
        assert efn.overall_readiness == "ready"
        assert efn.avg_entropy_score == 0.0


# ===================================================================
# B. Helper functions
# ===================================================================


class TestObjectToDirectSignal:
    def test_correct_mapping(self):
        obj = make_entropy_object(
            layer="semantic",
            dimension="dimensional",
            sub_dimension="cross_column_patterns",
            target="table:sales",
            score=0.7,
            evidence=[{"pattern": "mixed_units"}],
            detector_id="dimensional_detector",
        )
        ds = _object_to_direct_signal(obj)
        assert ds.dimension_path == "semantic.dimensional.cross_column_patterns"
        assert ds.target == "table:sales"
        assert ds.score == 0.7
        assert ds.evidence == [{"pattern": "mixed_units"}]
        assert ds.detector_id == "dimensional_detector"


# ===================================================================
# C. Per-column assembly (loss table)
# ===================================================================


class TestPerColumnAssembly:
    def test_empty_objects_returns_default(self):
        result = assemble_readiness_context([])
        assert result.total_columns == 0
        assert result.columns == {}
        assert result.overall_readiness == "ready"

    def test_single_column_produces_column_result(self):
        """Two loss measurements on one column -> one ColumnReadinessResult."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.8, target="column:t.c1"),
            make_entropy_object(detector_id="type_fidelity", score=0.8, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        assert result.total_columns == 1
        col = result.columns["column:t.c1"]
        assert col.nodes_observed == 2
        assert {ne.node_name for ne in col.node_evidence} == {"null_ratio", "type_fidelity"}

    def test_relationship_target_produces_readiness_result(self):
        """A ``relationship:`` target rolls up like a column (DAT-408), not dropped."""
        rel = "relationship:orders.customer_id-customers.id"
        objects = [
            make_entropy_object(detector_id="relationship_entropy", score=0.8, target=rel),
        ]
        result = assemble_readiness_context(objects)
        assert rel in result.columns, "relationship target must produce a readiness result"
        res = result.columns[rel]
        assert res.target == rel
        assert res.nodes_observed == 1
        assert all(ds.target != rel for ds in result.direct_signals)

    def test_compute_rollup_false_keeps_evidence_drops_intents(self):
        """Rollup-free assembly (DAT-399 slice D) yields raw evidence, no bands."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.9, target="column:t.c1"),
        ]
        full = assemble_readiness_context(objects)
        cheap = assemble_readiness_context(objects, compute_rollup=False)

        # Same columns + same raw scores (the contract gate reads these).
        assert cheap.columns.keys() == full.columns.keys()
        full_scores = {
            ne.dimension_path: ne.score for ne in full.columns["column:t.c1"].node_evidence
        }
        cheap_scores = {
            ne.dimension_path: ne.score for ne in cheap.columns["column:t.c1"].node_evidence
        }
        assert cheap_scores == full_scores
        assert cheap.avg_entropy_score == full.avg_entropy_score

        # Rollup products are absent on the cheap path.
        cheap_col = cheap.columns["column:t.c1"]
        assert cheap_col.intents == []
        assert cheap_col.worst_intent_risk == 0.0
        assert cheap_col.readiness == "ready"
        assert all(ne.impact_delta == 0.0 for ne in cheap_col.node_evidence)
        # ...but the full rollup did produce intents for the same input.
        assert full.columns["column:t.c1"].intents

    def test_two_columns_independent_results(self):
        """Two columns with different scores -> independent results."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.9, target="column:t.c1"),
            make_entropy_object(detector_id="null_ratio", score=0.1, target="column:t.c2"),
        ]
        result = assemble_readiness_context(objects)
        assert result.total_columns == 2
        c1 = result.columns["column:t.c1"]
        c2 = result.columns["column:t.c2"]
        assert c1.worst_intent_risk > c2.worst_intent_risk
        assert c2.readiness == "ready"

    def test_non_loss_detector_no_column_result(self):
        """Column with only an informative-signal detector -> only DirectSignals."""
        objects = [
            make_entropy_object(
                layer="value",
                dimension="distribution",
                sub_dimension="benford_compliance",
                detector_id="benford",
                score=0.6,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects)
        assert result.total_columns == 0
        assert "column:t.c1" not in result.columns
        assert len(result.direct_signals) == 1
        assert result.direct_signals[0].detector_id == "benford"

    def test_mixed_loss_and_signal_within_column(self):
        """Loss measurements roll up; informative signals become direct signals."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.7, target="column:t.c1"),
            make_entropy_object(detector_id="benford", score=0.5, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        assert result.total_columns == 1
        assert len(result.direct_signals) == 1
        assert result.direct_signals[0].detector_id == "benford"

    def test_all_low_column_ready(self):
        """Column with all low scores -> readiness='ready'."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.1, target="column:t.c1"),
            make_entropy_object(detector_id="type_fidelity", score=0.1, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        col = result.columns["column:t.c1"]
        assert col.readiness == "ready"
        assert result.overall_readiness == "ready"
        assert all(ne.state == "low" for ne in col.node_evidence)

    def test_high_column_has_intent_readiness(self):
        """Column with a high measurement -> per-intent readiness with positive risk."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.8, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        col = result.columns["column:t.c1"]
        assert col.intents
        for intent in col.intents:
            assert intent.risk > 0
            assert intent.readiness in ("ready", "investigate", "blocked")

    def test_intent_carries_per_intent_drivers(self):
        """Each intent lists the measurements that drive ITS risk, ranked (DAT-394)."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.8, target="column:t.c1"),
            make_entropy_object(detector_id="type_fidelity", score=0.8, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        col = result.columns["column:t.c1"]
        query = next(i for i in col.intents if i.intent_name == "query_intent")

        # Both measurements weight query_intent in loss.yaml -> both are drivers.
        assert {d.node for d in query.drivers} == {"null_ratio", "type_fidelity"}
        assert all(d.impact_delta > 0 for d in query.drivers)
        assert all(d.state == "high" for d in query.drivers)  # 0.8 > 0.6
        # Self-describing: non-empty dimension_path + humanized label.
        assert all(d.dimension_path for d in query.drivers)
        labels = {d.node: d.label for d in query.drivers}
        assert labels["null_ratio"] == "Null ratio"
        assert labels["type_fidelity"] == "Type fidelity"
        # Ranked by impact, descending.
        deltas = [d.impact_delta for d in query.drivers]
        assert deltas == sorted(deltas, reverse=True)

    def test_table_target_rolls_up(self):
        """A ``table:`` target rolls up like a column (DAT-415)."""
        objects = [
            make_entropy_object(detector_id="dimension_coverage", score=0.8, target="table:sales"),
        ]
        result = assemble_readiness_context(objects)
        assert result.total_columns == 1
        assert "table:sales" in result.columns
        assert all(ds.target != "table:sales" for ds in result.direct_signals)

    def test_dimensional_entropy_is_a_direct_signal_not_a_band_driver(self):
        """Demoted (2026-06-16): dimensional_entropy is informative — excluded from the
        loss rollup, surfaced as a DirectSignal, never banding a table (benford lane)."""
        objects = [
            make_entropy_object(detector_id="dimensional_entropy", score=1.0, target="table:sales"),
        ]
        result = assemble_readiness_context(objects)
        assert "table:sales" not in result.columns  # no loss row → nothing to band
        assert any(
            ds.target == "table:sales" and ds.detector_id == "dimensional_entropy"
            for ds in result.direct_signals
        )

    def test_slice_conditional_null_is_a_direct_signal_not_a_band_driver(self):
        """Demoted (2026-06-22, DAT-540): slice_conditional_null is informative — excluded
        from the loss rollup, surfaced as a DirectSignal, never banding a column. A column
        whose ONLY object is slice_conditional_null has no loss row → nothing to band."""
        objects = [
            make_entropy_object(
                detector_id="slice_conditional_null", score=0.97, target="column:bank.payment_id"
            ),
        ]
        result = assemble_readiness_context(objects)
        assert "column:bank.payment_id" not in result.columns  # no loss row → nothing to band
        assert any(
            ds.target == "column:bank.payment_id" and ds.detector_id == "slice_conditional_null"
            for ds in result.direct_signals
        )

    def test_node_evidence_carries_raw_data(self):
        """ColumnNodeEvidence carries score, evidence, detector_id from the object."""
        evidence_data = [{"metric": "null_fraction", "value": 0.75}]
        objects = [
            make_entropy_object(
                detector_id="null_ratio",
                score=0.75,
                evidence=evidence_data,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects)
        col = result.columns["column:t.c1"]
        ne = next(n for n in col.node_evidence if n.node_name == "null_ratio")
        assert ne.score == 0.75
        assert ne.evidence == evidence_data
        assert ne.detector_id == "null_ratio"
        assert ne.state != "low"  # 0.75 > 0.6
        assert ne.label == "Null ratio"
        assert ne.dimension_path

    def test_column_top_priority_set(self):
        """A high measurement populates top_priority_node + a positive impact."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.9, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        col = result.columns["column:t.c1"]
        assert col.top_priority_node == "null_ratio"
        assert col.top_priority_impact > 0

    def test_zero_score_node_zero_impact(self):
        """A zero-score measurement contributes zero loss -> zero impact_delta."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.0, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        col = result.columns["column:t.c1"]
        ne = next(n for n in col.node_evidence if n.node_name == "null_ratio")
        assert ne.impact_delta == 0.0


# ===================================================================
# D. Multi-intent assembly over the loss table
# ===================================================================


class TestMultiIntentAssembly:
    def test_null_ratio_produces_all_three_intents(self):
        """null_ratio weights all three intents -> all three are produced."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.7, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        col = result.columns["column:t.c1"]
        assert {i.intent_name for i in col.intents} == {
            "query_intent",
            "aggregation_intent",
            "reporting_intent",
        }

    def test_multiple_columns_independent_intents(self):
        """Each column computes its own per-column intents independently."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.8, target="column:t.c1"),
            make_entropy_object(detector_id="null_ratio", score=0.8, target="column:t.c2"),
        ]
        result = assemble_readiness_context(objects)
        assert result.total_columns == 2
        for target in ("column:t.c1", "column:t.c2"):
            names = {i.intent_name for i in result.columns[target].intents}
            assert names == {"query_intent", "aggregation_intent", "reporting_intent"}

    def test_overall_readiness_blocked_when_high(self):
        """A very high measurement pushes its worst intent to blocked."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.95, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        col = result.columns["column:t.c1"]
        assert any(i.readiness == "blocked" for i in col.intents)
        assert result.overall_readiness == "blocked"

    def test_all_low_ready(self):
        """All-low measurements -> ready."""
        objects = [
            make_entropy_object(detector_id="null_ratio", score=0.1, target="column:t.c1"),
        ]
        result = assemble_readiness_context(objects)
        assert result.overall_readiness == "ready"


# ===================================================================
# E. Coverage — the third rollup outcome (DAT-853)
# ===================================================================


from .conftest import make_abstention  # noqa: E402


class TestCoverage:
    def test_all_measured_is_coverage_measured(self):
        """Existing behavior: a measured target rolls up with coverage='measured'."""
        ctx = assemble_readiness_context([make_entropy_object(score=0.9)])
        col = ctx.columns["column:test_table.col1"]
        assert col.coverage == "measured"
        assert col.abstentions == []
        assert ctx.columns_unmeasured == 0

    def test_all_abstained_is_unmeasured_not_silent(self):
        """Zero measured loss objects now yields a ROW: band vacuous, coverage says so.

        Before DAT-853 this target produced no result at all — indistinguishable
        from measured-clean (the missing→green line).
        """
        ctx = assemble_readiness_context(
            [make_abstention(detector_id="null_ratio", reason="missing_inputs")]
        )
        col = ctx.columns["column:test_table.col1"]
        assert col.coverage == "unmeasured"
        assert col.readiness == "ready"  # frozen band vocabulary — vacuous by design
        assert col.worst_intent_risk == 0.0
        assert col.intents == []
        assert col.abstentions == [
            {
                "detector": "null_ratio",
                "reason": "missing_inputs",
                "intents": ["aggregation_intent", "query_intent", "reporting_intent"],
            }
        ]
        # Counted apart from ready — an unmeasured column is not a clean one.
        assert ctx.columns_unmeasured == 1
        assert ctx.columns_ready == 0

    def test_mixed_is_partial_and_risk_unchanged(self):
        """A measured detector still drives risk; the abstention degrades coverage only."""
        measured = make_entropy_object(score=0.9)  # null_ratio, high risk
        abstained = make_abstention(
            detector_id="type_fidelity", sub_dimension="type_fidelity", reason="detector_error"
        )
        ctx = assemble_readiness_context([measured, abstained])
        col = ctx.columns["column:test_table.col1"]
        assert col.coverage == "partial"
        assert col.readiness == "blocked"  # 0.9 * 0.7 agg = 0.63 > 0.6
        assert [a["detector"] for a in col.abstentions] == ["type_fidelity"]
        # The measured rollup itself is byte-identical to the abstention-free one.
        ctx_without = assemble_readiness_context([make_entropy_object(score=0.9)])
        col_without = ctx_without.columns["column:test_table.col1"]
        assert col.worst_intent_risk == col_without.worst_intent_risk
        assert [i.risk for i in col.intents] == [i.risk for i in col_without.intents]

    def test_non_loss_abstention_yields_no_signal_and_no_row(self):
        """An abstained informative detector (benford) is trace-only: no DirectSignal."""
        ctx = assemble_readiness_context(
            [make_abstention(detector_id="benford", sub_dimension="benford_compliance")]
        )
        assert ctx.columns == {}
        assert ctx.direct_signals == []

    def test_cheap_path_carries_coverage_but_no_unmeasured_row(self):
        """compute_rollup=False (contract gate) keeps today's shape for unmeasured."""
        ctx = assemble_readiness_context(
            [make_abstention(detector_id="null_ratio")], compute_rollup=False
        )
        assert ctx.columns == {}  # gate reads scores; the trace is entropy_objects
        mixed = assemble_readiness_context(
            [make_entropy_object(score=0.5), make_abstention(detector_id="type_fidelity")],
            compute_rollup=False,
        )
        col = mixed.columns["column:test_table.col1"]
        assert col.coverage == "partial"
