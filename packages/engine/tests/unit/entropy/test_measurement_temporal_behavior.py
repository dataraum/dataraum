"""Temporal-behaviour measurement — teach-first (ADR-0009, DAT-445, DAT-657).

Two witnesses: the independent LLM claim + the data-grounded structural
reconciliation (DAT-491). The ontology prior was DROPPED (DAT-657) — stock/flow is
a data-format property the ontology cannot declare. Asserts witness direction; that
the structural reconciliation is AUTHORITATIVE when it fires and a name-based claim
disagrees (DAT-764 — the data decides, with no manufactured conflict, so a
moderate-match verdict is not tipped by a confident-wrong LLM); the doc-trap
U-routing (a lone witness → ignorance, not low entropy); and that a column resolves
from the data alone. Properties/orderings, not point thresholds.
"""

from __future__ import annotations

import pytest

from dataraum.entropy.measurements.temporal_behavior import (
    CLAIM_SPACE,
    llm_claim_distribution,
    measure_temporal_behavior,
    resolved_behaviour,
    structural_reconciliation_distribution,
)

_STOCK = CLAIM_SPACE.index("stock")


# --- LLM claim ---------------------------------------------------------------
class TestLlmClaim:
    def test_stock_claim(self) -> None:
        assert llm_claim_distribution("stock", 0.9)["stock"] > 0.8

    def test_flow_claim(self) -> None:
        assert llm_claim_distribution("flow", 0.9)["stock"] < 0.2

    def test_unsure_or_absent_abstains(self) -> None:
        assert llm_claim_distribution("unsure", 0.9)["stock"] == 0.5
        assert llm_claim_distribution(None, 0.9)["stock"] == 0.5


# --- structural reconciliation witness (DAT-491) -----------------------------
class TestStructuralReconciliation:
    def test_cumulative_leans_stock(self) -> None:
        assert structural_reconciliation_distribution("cumulative", 0.9)["stock"] > 0.8

    def test_per_period_leans_flow(self) -> None:
        assert structural_reconciliation_distribution("per_period", 0.9)["stock"] < 0.2

    def test_match_rate_scales_the_lean(self) -> None:
        strong = structural_reconciliation_distribution("cumulative", 0.95)["stock"]
        weak = structural_reconciliation_distribution("cumulative", 0.2)["stock"]
        assert strong > weak > 0.5

    def test_unknown_pattern_abstains(self) -> None:
        assert structural_reconciliation_distribution("weird", 0.9)["stock"] == 0.5
        assert structural_reconciliation_distribution(None, 0.9)["stock"] == 0.5


# --- resolved layer ----------------------------------------------------------
class TestResolvedBehaviour:
    def test_agreement_resolves_uncontested(self) -> None:
        adj = measure_temporal_behavior(
            "t",
            "c",
            llm_claim="stock",
            llm_confidence=0.9,
            structural_pattern="cumulative",
            structural_match_rate=0.9,
        )
        label, contested = resolved_behaviour(adj)
        assert label == "point_in_time"
        assert contested is False

    def test_structural_overrules_a_disagreeing_llm_but_flags_contested(self) -> None:
        # DAT-764: the LLM name-reads the periodic trial_balance movement as stock;
        # the data reconciles it as per-period flow. The data is authoritative → the
        # label follows the data (additive), and the disagreement is flagged.
        adj = measure_temporal_behavior(
            "trial_balance",
            "debit_balance",
            llm_claim="stock",
            llm_confidence=0.8,
            structural_pattern="per_period",
            structural_match_rate=0.95,
        )
        label, contested = resolved_behaviour(adj)
        assert label == "additive"
        assert contested is True

    def test_structural_authority_is_symmetric_holds_stock(self) -> None:
        # The mirror of the debit_balance case: the data reconciles CUMULATIVE
        # (stock) while the LLM wrongly name-reads flow. Authority is direction-
        # agnostic — the data still decides, so the label stays point_in_time and no
        # flow-bias sneaks in. Guards against a one-sided overrule.
        adj = measure_temporal_behavior(
            "balance_sheet",
            "ending_balance",
            llm_claim="flow",
            llm_confidence=0.9,
            structural_pattern="cumulative",
            structural_match_rate=0.9,
        )
        label, contested = resolved_behaviour(adj)
        assert label == "point_in_time"
        assert contested is True
        assert adj.result.conflict < 0.05  # LLM pooled out — no manufactured conflict

    def test_total_ignorance_resolves_none(self) -> None:
        adj = measure_temporal_behavior("t", "c", llm_claim=None)
        assert resolved_behaviour(adj) == (None, False)


# --- pooled adjudication -----------------------------------------------------
class TestMeasure:
    def test_data_dissent_is_resolved_by_data_not_manufactured_conflict(self) -> None:
        """DAT-764: the LLM name-reads stock (wrong); the reconciliation says the
        column equals its per-period movement → flow. The data is authoritative, so
        the disagreeing name-read is pooled OUT — the posterior follows the data
        (flow) and NO readiness-blocking conflict is manufactured. Both raw reads are
        still recorded as provenance."""
        adj = measure_temporal_behavior(
            "tb",
            "debit_balance",
            llm_claim="stock",
            llm_confidence=0.9,
            structural_pattern="per_period",
            structural_match_rate=0.95,
        )
        assert adj.result.posterior[_STOCK] < 0.5  # resolves flow, not stock
        assert adj.result.conflict < 0.05  # the data decided — no manufactured conflict
        # …yet the two reads are kept for provenance.
        assert {w.witness_id for w in adj.witnesses} == {"llm_claim", "structural_reconciliation"}

    def test_moderate_match_structural_overpowers_confident_llm_stock(self) -> None:
        """The exact regression: trial_balance.debit_balance reconciled per_period at
        match_rate 0.75 (a real, gated verdict) while the LLM confidently name-read the
        'balance' column as stock. The symmetric pool tipped this to point_in_time;
        with structural authoritative it must resolve additive."""
        adj = measure_temporal_behavior(
            "trial_balance",
            "debit_balance",
            llm_claim="stock",
            llm_confidence=0.9,
            structural_pattern="per_period",
            structural_match_rate=0.75,
        )
        label, _ = resolved_behaviour(adj)
        assert label == "additive"
        assert adj.result.conflict < 0.05

    def test_weak_overrule_routes_to_ignorance_not_false_confidence(self) -> None:
        """Safety property of making structural authoritative: it removes the pooled
        CONFLICT signal, so a WEAK verdict (few entities reconciled → low match_rate)
        that overrules a confident LLM must NOT masquerade as a confident resolution.
        Its IGNORANCE stays high — the readiness/loss lane routes it to investigate —
        and shrinks only as match_rate rises. This is what keeps a near-noise verdict
        from silently greenlighting SUM-a-stock, WITHOUT an invented match_rate floor;
        the ignorance mechanism (grounded in the pool) already scales authority by
        coverage. Guards the cross-module invariant the loss lane depends on."""
        weak = measure_temporal_behavior(
            "t",
            "c",
            llm_claim="stock",
            llm_confidence=0.9,
            structural_pattern="per_period",
            structural_match_rate=0.05,
        )
        strong = measure_temporal_behavior(
            "t",
            "c",
            llm_claim="stock",
            llm_confidence=0.9,
            structural_pattern="per_period",
            structural_match_rate=1.0,
        )
        # Both resolve additive (data authoritative) with no manufactured conflict…
        assert resolved_behaviour(weak)[0] == "additive"
        assert weak.result.conflict < 0.05
        # …but the weak verdict carries much higher ignorance than the strong one.
        assert weak.result.ignorance > 0.9
        assert weak.result.ignorance > strong.result.ignorance

    def test_agreement_is_quiet(self) -> None:
        adj = measure_temporal_behavior(
            "balance_sheet",
            "ending_balance",
            llm_claim="stock",
            llm_confidence=0.9,
            structural_pattern="cumulative",
            structural_match_rate=0.9,
        )
        assert adj.result.conflict < 0.2
        assert adj.result.posterior[_STOCK] > 0.5

    def test_lone_claim_routes_to_ignorance_not_low_entropy(self) -> None:
        """Doc-trap: a lone witness (no counter) is ignorance about the column."""
        lone = measure_temporal_behavior("t", "c", llm_claim="stock", llm_confidence=0.9)
        both = measure_temporal_behavior(
            "t",
            "c",
            llm_claim="stock",
            llm_confidence=0.9,
            structural_pattern="cumulative",
            structural_match_rate=0.9,
        )
        assert {w.witness_id for w in lone.witnesses} == {"llm_claim"}
        assert lone.result.conflict < 0.05  # no disagreement…
        assert lone.result.ignorance > both.result.ignorance  # …but more ignorance

    def test_structural_alone_resolves_from_data(self) -> None:
        # Opaque column (LLM unsure) but the data reconciles → resolves from data,
        # not the doc-trap.
        adj = measure_temporal_behavior(
            "tb",
            "xq_v7kl",
            llm_claim="unsure",
            structural_pattern="per_period",
            structural_match_rate=0.9,
        )
        assert [w.witness_id for w in adj.witnesses] == ["structural_reconciliation"]
        label, contested = resolved_behaviour(adj)
        assert label == "additive"
        assert not contested

    def test_both_abstain_is_total_ignorance(self) -> None:
        adj = measure_temporal_behavior("t", "c", llm_claim="unsure")
        assert adj.witnesses == ()
        assert adj.result.conflict == 0.0
        assert adj.result.ignorance == pytest.approx(1.0)

    def test_claim_field_identity_and_witnesses(self) -> None:
        adj = measure_temporal_behavior(
            "trial_balance",
            "debit_balance",
            llm_claim="flow",
            llm_confidence=0.8,
            structural_pattern="per_period",
            structural_match_rate=0.9,
        )
        assert adj.claim_field == "temporal_behavior:trial_balance.debit_balance"
        assert {w.witness_id for w in adj.witnesses} == {"llm_claim", "structural_reconciliation"}
