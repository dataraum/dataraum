"""Temporal-behaviour measurement — teach-first (ADR-0009, DAT-445).

Two witnesses: grounding-conditional ontology prior + independent LLM claim. Asserts
the witnesses' direction, the grounding-conditional weakening (the contest AC), the
doc-trap U-routing (a lone/weak claim → ignorance, not low entropy), and the live
``debit_balance`` conflict (concept says stock, LLM reads flow). Properties/orderings,
not point thresholds.
"""

from __future__ import annotations

import pytest

from dataraum.entropy.measurements.temporal_behavior import (
    CLAIM_SPACE,
    llm_claim_distribution,
    measure_temporal_behavior,
    ontology_prior_distribution,
    resolved_behaviour,
)

_STOCK = CLAIM_SPACE.index("stock")


# --- ontology prior (grounding-conditional) ----------------------------------
class TestOntologyPrior:
    def test_point_in_time_leans_stock(self) -> None:
        assert ontology_prior_distribution("point_in_time", 0.9)["stock"] > 0.8

    def test_additive_leans_flow(self) -> None:
        assert ontology_prior_distribution("additive", 0.9)["stock"] < 0.2

    def test_unknown_behaviour_abstains(self) -> None:
        assert ontology_prior_distribution(None, 0.9)["stock"] == 0.5
        assert ontology_prior_distribution("weird", 0.9)["stock"] == 0.5

    def test_grounding_conditional_weakens_with_low_confidence(self) -> None:
        # THE contest AC: a contested/weak grounding collapses the prior toward 0.5.
        strong = ontology_prior_distribution("point_in_time", 0.95)["stock"]
        weak = ontology_prior_distribution("point_in_time", 0.2)["stock"]
        assert strong > weak
        assert abs(weak - 0.5) < abs(strong - 0.5)


# --- LLM claim ---------------------------------------------------------------
class TestLlmClaim:
    def test_stock_claim(self) -> None:
        assert llm_claim_distribution("stock", 0.9)["stock"] > 0.8

    def test_flow_claim(self) -> None:
        assert llm_claim_distribution("flow", 0.9)["stock"] < 0.2

    def test_unsure_or_absent_abstains(self) -> None:
        assert llm_claim_distribution("unsure", 0.9)["stock"] == 0.5
        assert llm_claim_distribution(None, 0.9)["stock"] == 0.5


# --- resolved layer ----------------------------------------------------------
class TestResolvedBehaviour:
    def test_agreement_resolves_uncontested(self) -> None:
        adj = measure_temporal_behavior(
            "t",
            "c",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.9,
            llm_claim="stock",
            llm_confidence=0.9,
        )
        label, contested = resolved_behaviour(adj.result)
        assert label == "point_in_time"
        assert contested is False

    def test_conflict_resolves_contested(self) -> None:
        adj = measure_temporal_behavior(
            "trial_balance",
            "debit_balance",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.9,
            llm_claim="flow",
            llm_confidence=0.8,
        )
        _, contested = resolved_behaviour(adj.result)
        assert contested is True

    def test_total_ignorance_resolves_none(self) -> None:
        adj = measure_temporal_behavior("t", "c", ontology_behaviour=None, llm_claim=None)
        assert resolved_behaviour(adj.result) == (None, False)


# --- pooled adjudication -----------------------------------------------------
class TestMeasure:
    def test_live_debit_balance_conflict(self) -> None:
        """concept says balance (stock), LLM reads the periodic TB as flow → conflict."""
        live = measure_temporal_behavior(
            "trial_balance",
            "debit_balance",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.9,
            llm_claim="flow",
            llm_confidence=0.8,
        )
        agree = measure_temporal_behavior(
            "balance_sheet",
            "ending_balance",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.9,
            llm_claim="stock",
            llm_confidence=0.9,
        )
        assert live.result.conflict > agree.result.conflict
        assert live.result.conflict > 0.3

    def test_agreement_is_quiet(self) -> None:
        adj = measure_temporal_behavior(
            "balance_sheet",
            "ending_balance",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.9,
            llm_claim="stock",
            llm_confidence=0.9,
        )
        assert adj.result.conflict < 0.2
        assert adj.result.posterior[_STOCK] > 0.5

    def test_weak_grounding_lowers_conflict(self) -> None:
        """A contested (low-confidence) grounding propagates as less conflict / more U."""
        strong = measure_temporal_behavior(
            "t",
            "c",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.95,
            llm_claim="flow",
            llm_confidence=0.9,
        )
        weak = measure_temporal_behavior(
            "t",
            "c",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.2,
            llm_claim="flow",
            llm_confidence=0.9,
        )
        assert strong.result.conflict > weak.result.conflict

    def test_lone_claim_routes_to_ignorance_not_low_entropy(self) -> None:
        """Doc-trap: a lone witness (no counter) is ignorance about the column, not resolved-quiet."""
        lone = measure_temporal_behavior(
            "t",
            "c",
            ontology_behaviour=None,
            llm_claim="stock",
            llm_confidence=0.9,
        )
        both = measure_temporal_behavior(
            "t",
            "c",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.9,
            llm_claim="stock",
            llm_confidence=0.9,
        )
        assert {w.witness_id for w in lone.witnesses} == {"llm_claim"}
        assert lone.result.conflict < 0.05  # no disagreement…
        assert (
            lone.result.ignorance > both.result.ignorance
        )  # …but more ignorance than the corroborated pair

    def test_both_abstain_is_total_ignorance(self) -> None:
        adj = measure_temporal_behavior("t", "c", ontology_behaviour=None, llm_claim="unsure")
        assert adj.witnesses == ()
        assert adj.result.conflict == 0.0
        assert adj.result.ignorance == pytest.approx(1.0)

    def test_claim_field_identity(self) -> None:
        adj = measure_temporal_behavior(
            "trial_balance",
            "debit_balance",
            ontology_behaviour="point_in_time",
            grounding_confidence=0.9,
            llm_claim="flow",
            llm_confidence=0.8,
        )
        assert adj.claim_field == "temporal_behavior:trial_balance.debit_balance"
        assert {w.witness_id for w in adj.witnesses} == {"ontology_prior", "llm_claim"}
