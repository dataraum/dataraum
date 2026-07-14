"""Temporal-behaviour measurement — teach-first (ADR-0009, DAT-445, DAT-657).

Two witnesses: the independent LLM claim + the data-grounded structural
reconciliation (DAT-491). The ontology prior was DROPPED (DAT-657) — stock/flow is
a data-format property the ontology cannot declare. Asserts witness direction, the
data-dissent conflict (the LLM name-reads stock, the data reconciles flow), the
doc-trap U-routing (a lone witness → ignorance, not low entropy), and that a column
resolves from the data alone. Properties/orderings, not point thresholds.
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
        label, contested = resolved_behaviour(adj.result)
        assert label == "point_in_time"
        assert contested is False

    def test_conflict_resolves_contested(self) -> None:
        # The LLM name-reads the periodic trial_balance movement as stock; the data
        # reconciles it as per-period flow → conflict.
        adj = measure_temporal_behavior(
            "trial_balance",
            "debit_balance",
            llm_claim="stock",
            llm_confidence=0.8,
            structural_pattern="per_period",
            structural_match_rate=0.95,
        )
        _, contested = resolved_behaviour(adj.result)
        assert contested is True

    def test_total_ignorance_resolves_none(self) -> None:
        adj = measure_temporal_behavior("t", "c", llm_claim=None)
        assert resolved_behaviour(adj.result) == (None, False)


# --- pooled adjudication -----------------------------------------------------
class TestMeasure:
    def test_data_dissent_against_the_name_raises_conflict(self) -> None:
        """The DAT-491/657 case: the LLM name-reads stock (possibly wrong); the
        reconciliation says the column equals its per-period movement → flow.
        Conflict rises — the data witness escapes name-anchoring."""
        agreed = measure_temporal_behavior(
            "tb",
            "debit_balance",
            llm_claim="stock",
            structural_pattern="cumulative",
            structural_match_rate=0.9,
        )
        dissent = measure_temporal_behavior(
            "tb",
            "debit_balance",
            llm_claim="stock",
            structural_pattern="per_period",
            structural_match_rate=0.95,
        )
        assert dissent.result.conflict > agreed.result.conflict
        assert dissent.result.conflict > 0.05

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
        label, contested = resolved_behaviour(adj.result)
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
