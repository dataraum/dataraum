"""Tests for entropy readiness context assembler (per-column design).

Test categories:
A. Dataclass defaults
B. Helper functions
C. Per-column assembly with small_network (4-node)
E. Assembly with full_network (15-node)
"""

from dataraum.entropy.models import EntropyObject
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

    def test_column_network_result_defaults(self):
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

    def test_entropy_for_network_defaults(self):
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
# C. Per-column assembly with small_network (4-node)
# ===================================================================


class TestPerColumnAssembly:
    def test_empty_objects_returns_default(self, small_network):
        result = assemble_readiness_context([], small_network)
        assert result.total_columns == 0
        assert result.columns == {}
        assert result.overall_readiness == "ready"

    def test_single_column_produces_column_result(self, small_network):
        """One column with two mapped objects -> one ColumnReadinessResult."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.8,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.2,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        assert result.total_columns == 1
        assert "column:t.c1" in result.columns
        col = result.columns["column:t.c1"]
        assert col.nodes_observed == 2
        node_names = {ne.node_name for ne in col.node_evidence}
        assert "root_a" in node_names
        assert "root_b" in node_names

    def test_compute_rollup_false_keeps_evidence_drops_intents(self, small_network):
        """Rollup-free assembly (DAT-399 slice D) yields raw evidence, no bands.

        The query-time contract gate uses this cheap half: node evidence (raw
        scores keyed by dimension_path) must survive, while the noisy-OR products
        (intents, per-node impact_delta, banded readiness) are absent.
        """
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.9,
                target="column:t.c1",
            ),
        ]
        full = assemble_readiness_context(objects, small_network)
        cheap = assemble_readiness_context(objects, small_network, compute_rollup=False)

        # Same columns + same raw scores (the contract gate reads these).
        assert cheap.columns.keys() == full.columns.keys()
        full_scores = {
            ne.dimension_path: ne.score for ne in full.columns["column:t.c1"].node_evidence
        }
        cheap_scores = {
            ne.dimension_path: ne.score for ne in cheap.columns["column:t.c1"].node_evidence
        }
        assert cheap_scores == full_scores
        # avg_entropy_score is raw-derived -> still populated and identical.
        assert cheap.avg_entropy_score == full.avg_entropy_score

        # Rollup products are absent on the cheap path.
        cheap_col = cheap.columns["column:t.c1"]
        assert cheap_col.intents == []
        assert cheap_col.worst_intent_risk == 0.0
        assert cheap_col.readiness == "ready"
        assert all(ne.impact_delta == 0.0 for ne in cheap_col.node_evidence)
        # ...but the full rollup did produce intents for the same input.
        assert full.columns["column:t.c1"].intents

    def test_two_columns_independent_results(self, small_network):
        """Two columns with different scores -> independent results."""
        objects = [
            # Column 1: root_a high
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.9,
                target="column:t.c1",
            ),
            # Column 2: root_a low
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.1,
                target="column:t.c2",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        assert result.total_columns == 2
        c1 = result.columns["column:t.c1"]
        c2 = result.columns["column:t.c2"]
        # c1 should have higher risk than c2
        assert c1.worst_intent_risk > c2.worst_intent_risk
        assert c1.readiness != "ready" or c1.worst_intent_risk > 0
        assert c2.readiness == "ready"

    def test_column_only_unmapped_no_column_result(self, small_network):
        """Column with only unmapped objects -> no ColumnReadinessResult, only DirectSignals."""
        objects = [
            make_entropy_object(
                layer="semantic",
                dimension="dimensional",
                sub_dimension="cross_column_patterns",
                score=0.6,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        assert result.total_columns == 0
        assert "column:t.c1" not in result.columns
        assert len(result.direct_signals) == 1
        assert (
            result.direct_signals[0].dimension_path == "semantic.dimensional.cross_column_patterns"
        )

    def test_mixed_mapped_and_unmapped_within_column(self, small_network):
        """Mapped objects go to network, unmapped become direct signals."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.7,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="semantic",
                dimension="dimensional",
                sub_dimension="quality_assessment",
                score=0.5,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        assert result.total_columns == 1
        assert len(result.direct_signals) == 1

    def test_all_low_column_ready(self, small_network):
        """Column with all low evidence -> readiness='ready'."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.1,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.1,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        col = result.columns["column:t.c1"]
        assert col.readiness == "ready"
        assert result.overall_readiness == "ready"
        for ne in col.node_evidence:
            assert ne.state == "low"

    def test_high_column_has_intent_readiness(self, small_network):
        """Column with high evidence -> intent readiness reflects P(high)."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.8,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.7,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        col = result.columns["column:t.c1"]
        assert len(col.intents) == 1
        intent = col.intents[0]
        assert intent.intent_name == "leaf_z"
        assert intent.risk > 0
        assert intent.readiness in ("ready", "investigate", "blocked")

    def test_intent_carries_per_intent_drivers(self, small_network):
        """Each intent lists the observed nodes that lower ITS risk, ranked (DAT-394)."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.8,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.7,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        intent = result.columns["column:t.c1"].intents[0]

        # Both observed roots feed leaf_z through child_x, so both are drivers.
        driver_nodes = {d.node for d in intent.drivers}
        assert driver_nodes == {"root_a", "root_b"}
        # Each driver carries its discretized state and a positive per-intent impact.
        assert all(d.impact_delta > 0 for d in intent.drivers)
        assert all(d.state == "high" for d in intent.drivers)
        # Each driver is self-describing: non-empty dimension_path + humanized label.
        assert all(d.dimension_path for d in intent.drivers)
        labels = {d.node: d.label for d in intent.drivers}
        assert labels["root_a"] == "Root a"
        assert labels["root_b"] == "Root b"
        # Ranked by impact, descending.
        deltas = [d.impact_delta for d in intent.drivers]
        assert deltas == sorted(deltas, reverse=True)

    def test_table_target_becomes_direct_signal(self, small_network):
        """Table-level objects always become direct signals."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.8,
                target="table:sales",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        assert result.total_columns == 0
        assert len(result.direct_signals) == 1
        assert result.direct_signals[0].target == "table:sales"

    def test_node_evidence_carries_raw_data(self, small_network):
        """ColumnNodeEvidence has score, evidence from source object."""
        evidence_data = [{"metric": "type_mismatch_ratio", "value": 0.3}]
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.75,
                evidence=evidence_data,
                detector_id="type_detector",
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        col = result.columns["column:t.c1"]
        ne = next(n for n in col.node_evidence if n.node_name == "root_a")
        assert ne.score == 0.75
        assert ne.evidence == evidence_data
        assert ne.detector_id == "type_detector"
        # Non-low evidence carries a humanized label and its dimension_path.
        assert ne.state != "low"
        assert ne.label == "Root a"
        assert ne.dimension_path

    def test_column_top_priority_set(self, small_network):
        """Column with high node should have top_priority_node populated."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.9,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        col = result.columns["column:t.c1"]
        assert col.top_priority_node == "root_a"
        assert col.top_priority_impact > 0

    def test_node_evidence_has_impact_delta(self, small_network):
        """High node should have non-zero impact_delta from priorities."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.9,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        col = result.columns["column:t.c1"]
        ne = next(n for n in col.node_evidence if n.node_name == "root_a")
        assert ne.impact_delta > 0

    def test_low_node_has_zero_impact_delta(self, small_network):
        """Low node should have zero impact_delta (no fix needed)."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.1,
                target="column:t.c1",
            ),
        ]
        result = assemble_readiness_context(objects, small_network)
        col = result.columns["column:t.c1"]
        ne = next(n for n in col.node_evidence if n.node_name == "root_a")
        assert ne.impact_delta == 0.0


# ===================================================================
# E. Assembly with full_network (15-node)
# ===================================================================


class TestAssembleFullNetwork:
    def _make_root_objects(
        self,
        score: float = 0.7,
        target: str = "column:t.c1",
    ) -> list[EntropyObject]:
        """Create objects for all 8 root nodes targeting a single column."""
        roots = [
            ("structural", "types", "type_fidelity"),
            ("value", "nulls", "null_ratio"),
            ("value", "outliers", "outlier_rate"),
            ("semantic", "business_meaning", "naming_clarity"),
            ("semantic", "units", "unit_declaration"),
            ("semantic", "temporal", "time_role"),
            ("value", "temporal", "temporal_drift"),
            ("value", "distribution", "benford_compliance"),
        ]
        return [
            make_entropy_object(
                layer=layer,
                dimension=dim,
                sub_dimension=sub,
                score=score,
                target=target,
            )
            for layer, dim, sub in roots
        ]

    def test_all_roots_one_column_produces_3_intents(self, full_network):
        """With all 8 roots observed for one column, all 3 intents computed."""
        objects = self._make_root_objects(score=0.7)
        result = assemble_readiness_context(objects, full_network)
        assert result.total_columns == 1
        col = result.columns["column:t.c1"]
        intent_names = {i.intent_name for i in col.intents}
        assert intent_names == {"query_intent", "aggregation_intent", "reporting_intent"}

    def test_multiple_columns_compute_per_column_intents(self, full_network):
        """Each column computes its own per-column intents independently."""
        objects = self._make_root_objects(
            score=0.8, target="column:t.c1"
        ) + self._make_root_objects(score=0.8, target="column:t.c2")
        result = assemble_readiness_context(objects, full_network)
        assert result.total_columns == 2
        for target in ("column:t.c1", "column:t.c2"):
            names = {i.intent_name for i in result.columns[target].intents}
            assert names == {"query_intent", "aggregation_intent", "reporting_intent"}

    def test_unmapped_dimensional_signal(self, full_network):
        """semantic.dimensional.cross_column_patterns has no network node."""
        objects = self._make_root_objects(score=0.3)
        objects.append(
            make_entropy_object(
                layer="semantic",
                dimension="dimensional",
                sub_dimension="cross_column_patterns",
                score=0.6,
                target="table:sales",
            )
        )
        result = assemble_readiness_context(objects, full_network)
        assert result.total_direct_signals == 1
        assert (
            result.direct_signals[0].dimension_path == "semantic.dimensional.cross_column_patterns"
        )

    def test_overall_readiness_blocked_when_high(self, full_network):
        """With very high scores, overall readiness should be blocked."""
        objects = self._make_root_objects(score=0.95)
        result = assemble_readiness_context(objects, full_network)
        col = result.columns["column:t.c1"]
        blocked_intents = [i for i in col.intents if i.readiness == "blocked"]
        assert len(blocked_intents) > 0
        assert result.overall_readiness == "blocked"

    def test_all_low_roots_ready(self, full_network):
        """With all roots at low scores, should be ready."""
        objects = self._make_root_objects(score=0.1)
        result = assemble_readiness_context(objects, full_network)
        assert result.overall_readiness == "ready"

    def test_partial_low_evidence_subgraph_inference(self, full_network):
        """Column with partial low evidence uses dynamic subgraph.

        When only 4 of 9 root detectors fire (all with low scores), the
        dynamic subgraph removes unobserved roots. Remaining P(high)
        comes from CPT pessimistic shift — genuine conservatism, not
        prior leakage. The network may classify as "investigate" for
        intents where the pessimistic shift pushes P(high) just above 0.3.
        """
        # Only 4 roots — the common pattern for many baseline columns
        partial_roots = [
            ("structural", "types", "type_fidelity"),
            ("value", "nulls", "null_ratio"),
            ("value", "outliers", "outlier_rate"),
            ("semantic", "business_meaning", "naming_clarity"),
        ]
        objects = [
            make_entropy_object(
                layer=layer,
                dimension=dim,
                sub_dimension=sub,
                score=0.0,
                target="column:t.c1",
            )
            for layer, dim, sub in partial_roots
        ]
        result = assemble_readiness_context(objects, full_network)
        col = result.columns["column:t.c1"]

        # With dynamic subgraph, unobserved roots are excluded.
        # Remaining P(high) is from CPT pessimistic shift, not prior noise.
        # Most intents should be ready; some may be marginal "investigate".
        assert col.readiness in ("ready", "investigate")
        assert col.worst_intent_risk < 0.5  # No intent near "blocked"
