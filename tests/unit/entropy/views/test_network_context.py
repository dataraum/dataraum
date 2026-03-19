"""Tests for entropy network context assembler (per-column design).

Test categories:
A. Dataclass defaults
B. Helper functions
C. Per-column assembly with small_network (4-node)
D. Cross-column aggregation with small_network
E. Assembly with full_network (15-node)
F. Formatter tests
"""

import pytest

from dataraum.entropy.models import EntropyObject
from dataraum.entropy.views.network_context import (
    AggregateIntentReadiness,
    ColumnNetworkResult,
    ColumnNodeEvidence,
    CrossColumnFix,
    DirectSignal,
    EntropyForNetwork,
    IntentReadiness,
    _object_to_direct_signal,
    _serialize_resolution_options,
    assemble_network_context,
    format_network_context,
)

from .conftest import make_entropy_object, make_resolution_option

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
        assert cne.resolution_options == []
        assert cne.detector_id == ""

    def test_column_network_result_defaults(self):
        cnr = ColumnNetworkResult()
        assert cnr.target == ""
        assert cnr.node_evidence == []
        assert cnr.intents == []
        assert cnr.top_priority_node == ""
        assert cnr.top_priority_impact == 0.0
        assert cnr.nodes_observed == 0
        assert cnr.nodes_high == 0
        assert cnr.worst_intent_p_high == 0.0
        assert cnr.readiness == "ready"

    def test_aggregate_intent_readiness_defaults(self):
        air = AggregateIntentReadiness()
        assert air.intent_name == ""
        assert air.worst_p_high == 0.0
        assert air.mean_p_high == 0.0
        assert air.columns_blocked == 0
        assert air.columns_investigate == 0
        assert air.columns_ready == 0
        assert air.overall_readiness == "ready"

    def test_cross_column_fix_defaults(self):
        ccf = CrossColumnFix()
        assert ccf.node_name == ""
        assert ccf.dimension_path == ""
        assert ccf.columns_affected == 0
        assert ccf.total_intent_delta == 0.0
        assert ccf.example_columns == []
        assert ccf.resolution_options == []

    def test_direct_signal_defaults(self):
        ds = DirectSignal()
        assert ds.dimension_path == ""
        assert ds.score == 0.0
        assert ds.evidence == []

    def test_intent_readiness_defaults(self):
        ir = IntentReadiness()
        assert ir.intent_name == ""
        assert ir.readiness == "ready"
        assert ir.p_high == 0.0
        assert ir.dominant_state == "low"

    def test_entropy_for_network_defaults(self):
        efn = EntropyForNetwork()
        assert efn.columns == {}
        assert efn.direct_signals == []
        assert efn.intents == []
        assert efn.top_fix is None
        assert efn.total_columns == 0
        assert efn.columns_blocked == 0
        assert efn.columns_investigate == 0
        assert efn.columns_ready == 0
        assert efn.overall_readiness == "ready"


# ===================================================================
# B. Helper functions
# ===================================================================


class TestSerializeResolutionOptions:
    def test_empty_list(self):
        assert _serialize_resolution_options([]) == []

    def test_preserves_all_fields(self):
        opt = make_resolution_option(
            action="document_unit",
            effort="medium",
            description="Add unit annotation",
        )
        result = _serialize_resolution_options([opt])
        assert len(result) == 1
        d = result[0]
        assert d["action"] == "document_unit"
        assert d["effort"] == "medium"
        assert d["description"] == "Add unit annotation"
        assert d["parameters"] == {"key": "value"}

    def test_multiple_options(self):
        opts = [
            make_resolution_option(action="a"),
            make_resolution_option(action="b"),
        ]
        result = _serialize_resolution_options(opts)
        assert len(result) == 2
        assert result[0]["action"] == "a"
        assert result[1]["action"] == "b"


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

    def test_with_resolution_options(self):
        opt = make_resolution_option(action="fix_it")
        obj = make_entropy_object(resolution_options=[opt])
        ds = _object_to_direct_signal(obj)
        assert len(ds.resolution_options) == 1
        assert ds.resolution_options[0]["action"] == "fix_it"


# ===================================================================
# C. Per-column assembly with small_network (4-node)
# ===================================================================


class TestPerColumnAssembly:
    def test_empty_objects_returns_default(self, small_network):
        result = assemble_network_context([], small_network)
        assert result.total_columns == 0
        assert result.columns == {}
        assert result.overall_readiness == "ready"

    def test_single_column_produces_column_result(self, small_network):
        """One column with two mapped objects -> one ColumnNetworkResult."""
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
        result = assemble_network_context(objects, small_network)
        assert result.total_columns == 1
        assert "column:t.c1" in result.columns
        col = result.columns["column:t.c1"]
        assert col.nodes_observed == 2
        node_names = {ne.node_name for ne in col.node_evidence}
        assert "root_a" in node_names
        assert "root_b" in node_names

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
        result = assemble_network_context(objects, small_network)
        assert result.total_columns == 2
        c1 = result.columns["column:t.c1"]
        c2 = result.columns["column:t.c2"]
        # c1 should have higher risk than c2
        assert c1.worst_intent_p_high > c2.worst_intent_p_high
        assert c1.readiness != "ready" or c1.worst_intent_p_high > 0
        assert c2.readiness == "ready"

    def test_column_only_unmapped_no_column_result(self, small_network):
        """Column with only unmapped objects -> no ColumnNetworkResult, only DirectSignals."""
        objects = [
            make_entropy_object(
                layer="semantic",
                dimension="dimensional",
                sub_dimension="cross_column_patterns",
                score=0.6,
                target="column:t.c1",
            ),
        ]
        result = assemble_network_context(objects, small_network)
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
        result = assemble_network_context(objects, small_network)
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
        result = assemble_network_context(objects, small_network)
        col = result.columns["column:t.c1"]
        assert col.readiness == "ready"
        assert result.overall_readiness == "ready"
        assert result.top_fix is None
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
        result = assemble_network_context(objects, small_network)
        col = result.columns["column:t.c1"]
        assert len(col.intents) == 1
        intent = col.intents[0]
        assert intent.intent_name == "leaf_z"
        assert intent.p_high > 0
        assert intent.readiness in ("ready", "investigate", "blocked")
        assert sum(intent.posterior.values()) == pytest.approx(1.0, abs=0.01)

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
        result = assemble_network_context(objects, small_network)
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
        result = assemble_network_context(objects, small_network)
        col = result.columns["column:t.c1"]
        ne = next(n for n in col.node_evidence if n.node_name == "root_a")
        assert ne.score == 0.75
        assert ne.evidence == evidence_data
        assert ne.detector_id == "type_detector"

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
        result = assemble_network_context(objects, small_network)
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
        result = assemble_network_context(objects, small_network)
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
        result = assemble_network_context(objects, small_network)
        col = result.columns["column:t.c1"]
        ne = next(n for n in col.node_evidence if n.node_name == "root_a")
        assert ne.impact_delta == 0.0


# ===================================================================
# D. Cross-column aggregation with small_network
# ===================================================================


class TestCrossColumnAggregation:
    def test_aggregate_intent_worst_mean(self, small_network):
        """AggregateIntentReadiness computes worst/mean across columns."""
        objects = [
            # Column 1: high
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.9,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.8,
                target="column:t.c1",
            ),
            # Column 2: low
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.1,
                target="column:t.c2",
            ),
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.1,
                target="column:t.c2",
            ),
        ]
        result = assemble_network_context(objects, small_network)
        assert len(result.intents) == 1  # leaf_z only intent
        agg = result.intents[0]
        assert agg.intent_name == "leaf_z"
        # Worst from c1, mean across both
        c1_intent = result.columns["column:t.c1"].intents[0]
        c2_intent = result.columns["column:t.c2"].intents[0]
        assert agg.worst_p_high == pytest.approx(
            max(c1_intent.p_high, c2_intent.p_high),
            abs=0.001,
        )
        assert agg.mean_p_high == pytest.approx(
            (c1_intent.p_high + c2_intent.p_high) / 2,
            abs=0.001,
        )

    def test_cross_column_fix_picks_most_affected(self, small_network):
        """CrossColumnFix picks node affecting most columns."""
        objects = [
            # Both columns have root_a high
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.9,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.8,
                target="column:t.c2",
            ),
            # Only c1 has root_b high
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.7,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.1,
                target="column:t.c2",
            ),
        ]
        result = assemble_network_context(objects, small_network)
        assert result.top_fix is not None
        # root_a is non-low in 2 columns, root_b in 1 column
        assert result.top_fix.node_name == "root_a"
        assert result.top_fix.columns_affected == 2
        # total_intent_delta should be sum of actual causal deltas, not p_high
        assert result.top_fix.total_intent_delta > 0

    def test_cross_column_fix_uses_causal_impact(self, small_network):
        """total_intent_delta sums actual per-node impact, not worst_intent_p_high."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.9,
                target="column:t.c1",
            ),
        ]
        result = assemble_network_context(objects, small_network)
        assert result.top_fix is not None
        # The delta should equal the node's actual impact_delta from priorities
        col = result.columns["column:t.c1"]
        ne = next(n for n in col.node_evidence if n.node_name == "root_a")
        assert result.top_fix.total_intent_delta == pytest.approx(
            ne.impact_delta,
            abs=0.001,
        )

    def test_overall_readiness_from_worst_aggregate(self, small_network):
        """overall_readiness derives from worst aggregate intent."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.95,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                score=0.95,
                target="column:t.c1",
            ),
            # Another column all low
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.1,
                target="column:t.c2",
            ),
        ]
        result = assemble_network_context(objects, small_network)
        # c1 is blocked, so overall should be blocked
        assert result.columns_blocked >= 1 or result.columns_investigate >= 1
        assert result.overall_readiness != "ready"

    def test_column_counts_correct(self, small_network):
        """columns_blocked + columns_investigate + columns_ready = total_columns."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.9,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.1,
                target="column:t.c2",
            ),
        ]
        result = assemble_network_context(objects, small_network)
        assert (
            result.columns_blocked + result.columns_investigate + result.columns_ready
            == result.total_columns
        )

    def test_no_cross_column_fix_when_all_low(self, small_network):
        """No CrossColumnFix when all nodes are low."""
        objects = [
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.1,
                target="column:t.c1",
            ),
            make_entropy_object(
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                score=0.1,
                target="column:t.c2",
            ),
        ]
        result = assemble_network_context(objects, small_network)
        assert result.top_fix is None


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
        result = assemble_network_context(objects, full_network)
        assert result.total_columns == 1
        col = result.columns["column:t.c1"]
        intent_names = {i.intent_name for i in col.intents}
        assert intent_names == {"query_intent", "aggregation_intent", "reporting_intent"}

    def test_multiple_columns_aggregate_3_intents(self, full_network):
        """Multiple columns aggregate to 3 intent readiness entries."""
        objects = self._make_root_objects(
            score=0.8, target="column:t.c1"
        ) + self._make_root_objects(score=0.2, target="column:t.c2")
        result = assemble_network_context(objects, full_network)
        assert result.total_columns == 2
        agg_intent_names = {i.intent_name for i in result.intents}
        assert agg_intent_names == {"query_intent", "aggregation_intent", "reporting_intent"}

    def test_top_fix_identified_for_high_scores(self, full_network):
        """With high scores across columns, a top fix should be identified."""
        objects = self._make_root_objects(
            score=0.8, target="column:t.c1"
        ) + self._make_root_objects(score=0.9, target="column:t.c2")
        result = assemble_network_context(objects, full_network)
        assert result.top_fix is not None
        assert result.top_fix.columns_affected >= 1

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
        result = assemble_network_context(objects, full_network)
        assert result.total_direct_signals == 1
        assert (
            result.direct_signals[0].dimension_path == "semantic.dimensional.cross_column_patterns"
        )

    def test_overall_readiness_blocked_when_high(self, full_network):
        """With very high scores, overall readiness should be blocked."""
        objects = self._make_root_objects(score=0.95)
        result = assemble_network_context(objects, full_network)
        blocked_intents = [i for i in result.intents if i.overall_readiness == "blocked"]
        assert len(blocked_intents) > 0
        assert result.overall_readiness == "blocked"

    def test_all_low_roots_ready(self, full_network):
        """With all roots at low scores, should be ready."""
        objects = self._make_root_objects(score=0.1)
        result = assemble_network_context(objects, full_network)
        assert result.overall_readiness == "ready"
        assert result.top_fix is None

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
        result = assemble_network_context(objects, full_network)
        col = result.columns["column:t.c1"]

        # With dynamic subgraph, unobserved roots are excluded.
        # Remaining P(high) is from CPT pessimistic shift, not prior noise.
        # Most intents should be ready; some may be marginal "investigate".
        assert col.readiness in ("ready", "investigate")
        assert col.worst_intent_p_high < 0.5  # No intent near "blocked"


# ===================================================================
# F. Formatter tests
# ===================================================================


class TestFormatNetworkContext:
    def test_empty_context_shows_ready(self):
        ctx = EntropyForNetwork()
        result = format_network_context(ctx)
        assert "READY" in result

    def test_blocked_context(self):
        ctx = EntropyForNetwork(overall_readiness="blocked")
        result = format_network_context(ctx)
        assert "BLOCKED" in result

    def test_investigate_status(self):
        ctx = EntropyForNetwork(overall_readiness="investigate")
        result = format_network_context(ctx)
        assert "INVESTIGATE" in result

    def test_column_counts_in_header(self):
        ctx = EntropyForNetwork(
            total_columns=47,
            columns_blocked=3,
            columns_investigate=8,
            columns_ready=36,
            total_direct_signals=5,
        )
        result = format_network_context(ctx)
        assert "47 columns analyzed" in result
        assert "3 blocked" in result
        assert "8 investigate" in result
        assert "36 ready" in result
        assert "5 direct signals" in result

    def test_intent_readiness_table(self):
        ctx = EntropyForNetwork(
            intents=[
                AggregateIntentReadiness(
                    intent_name="aggregation_intent",
                    worst_p_high=0.645,
                    mean_p_high=0.120,
                    columns_blocked=2,
                    columns_investigate=5,
                    columns_ready=40,
                    overall_readiness="blocked",
                ),
            ],
        )
        result = format_network_context(ctx)
        assert "Intent Readiness" in result
        assert "aggregation_intent" in result
        assert "0.645" in result
        assert "0.120" in result

    def test_top_fix_shown(self):
        ctx = EntropyForNetwork(
            top_fix=CrossColumnFix(
                node_name="unit_declaration",
                dimension_path="semantic.units.unit_declaration",
                columns_affected=7,
                total_intent_delta=1.23,
                example_columns=["column:fx_rates.rate", "column:amounts.val"],
                resolution_options=[
                    {
                        "action": "document_unit",
                        "description": "Add unit annotation",
                        "effort": "low",
                        "parameters": {},
                    }
                ],
            ),
        )
        result = format_network_context(ctx)
        assert "Top Fix" in result
        assert "unit_declaration" in result
        assert "7 columns" in result
        assert "document_unit" in result

    def test_at_risk_columns_shown(self):
        ctx = EntropyForNetwork(
            total_columns=5,
            columns={
                "column:t.c1": ColumnNetworkResult(
                    target="column:t.c1",
                    readiness="blocked",
                    worst_intent_p_high=0.645,
                    node_evidence=[
                        ColumnNodeEvidence(
                            node_name="outlier_rate",
                            state="high",
                            score=1.0,
                            impact_delta=0.143,
                        ),
                    ],
                ),
                "column:t.c2": ColumnNetworkResult(
                    target="column:t.c2",
                    readiness="ready",
                    worst_intent_p_high=0.1,
                ),
            },
        )
        result = format_network_context(ctx)
        assert "At-Risk Columns" in result
        assert "column:t.c1" in result
        assert "0.645" in result
        assert "impact=0.143" in result

    def test_healthy_columns_shown(self):
        ctx = EntropyForNetwork(
            columns_ready=36,
        )
        result = format_network_context(ctx)
        assert "Healthy Columns" in result
        assert "36 columns" in result

    def test_direct_signals_listed(self):
        ctx = EntropyForNetwork(
            direct_signals=[
                DirectSignal(
                    dimension_path="semantic.dimensional.cross_column_patterns",
                    score=0.65,
                    target="table:sales",
                    evidence=[{"pattern": "mixed_currencies"}],
                ),
            ],
        )
        result = format_network_context(ctx)
        assert "Direct Signals" in result
        assert "cross_column_patterns" in result

    def test_empty_columns_no_at_risk(self):
        ctx = EntropyForNetwork()
        result = format_network_context(ctx)
        assert "At-Risk" not in result
