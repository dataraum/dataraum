"""Temporal-behaviour measurement (ADR-0009, DAT-445/DAT-459).

Pure; no DB/config. Asserts the witnesses' direction and — the point of the whole
exercise — that the live ``debit_balance`` case (a column whose data is a per-period
FLOW but whose declared concept claims STOCK) surfaces as elevated conflict rather
than being silently mislabelled. Properties/orderings, not point thresholds. The
structural cases are the DAT-459 grounding fixtures, including the two that
FALSIFIED the time-series persistence signature (trending flow, mean-reverting
stock) — both must classify correctly here.
"""

from __future__ import annotations

import pytest

from dataraum.entropy.measurements.temporal_behavior import (
    CLAIM_SPACE,
    measure_temporal_behavior,
    reconciliation_distribution,
    semantic_distribution,
)

_STOCK = CLAIM_SPACE.index("stock")


def _cumsum(xs: list[float]) -> list[float]:
    out, run = [], 0.0
    for x in xs:
        run += x
        out.append(run)
    return out


# A representative 12-period net-movement series (the independent anchor).
_M = [100.0, -50.0, 200.0, 30.0, -80.0, 150.0, 60.0, -40.0, 90.0, 20.0, -30.0, 70.0]


def _stock_account(movements: list[float] = _M) -> dict[str, list[float]]:
    """A carry-forward level: values = running total, Δvalue == movement."""
    return {"values": _cumsum(movements), "movements": movements}


def _flow_account(movements: list[float] = _M) -> dict[str, list[float]]:
    """A per-period movement: value == movement."""
    return {"values": list(movements), "movements": list(movements)}


# --- structural reconciliation witness ---------------------------------------
class TestReconciliationWitness:
    def test_genuine_stock_reads_stock(self) -> None:
        dist = reconciliation_distribution({"a": _stock_account()})
        assert dist["stock"] > 0.9

    def test_genuine_flow_reads_flow(self) -> None:
        dist = reconciliation_distribution({"a": _flow_account()})
        assert dist["stock"] < 0.1

    def test_trending_flow_reads_flow(self) -> None:
        # The rho1-killer #1: a ramping/seasonal flow. value == its own movement,
        # so it reconciles as flow despite a smooth, autocorrelated level.
        ramp = [100.0 + 15.0 * i for i in range(12)]
        dist = reconciliation_distribution({"a": _flow_account(ramp)})
        assert dist["stock"] < 0.1

    def test_mean_reverting_stock_reads_stock(self) -> None:
        # The rho1-killer #2: an AR(1) level that mean-reverts (phi<1). It still
        # carries forward (Δlevel == movement), so it reconciles as stock.
        level = [500.0, 520.0, 505.0, 515.0, 498.0, 510.0, 502.0, 508.0, 495.0, 506.0, 500.0, 504.0]
        movements = [level[0]] + [level[i] - level[i - 1] for i in range(1, len(level))]
        dist = reconciliation_distribution({"a": {"values": level, "movements": movements}})
        assert dist["stock"] > 0.9

    def test_wrong_anchor_abstains(self) -> None:
        # A genuine stock reconciled against the WRONG movements: neither hypothesis
        # fits → the witness abstains rather than confidently mislabelling.
        wrong = [7.0, -300.0, 11.0, 420.0, -5.0, 33.0, -90.0, 250.0, 4.0, -17.0, 88.0, -60.0]
        dist = reconciliation_distribution({"a": {"values": _cumsum(_M), "movements": wrong}})
        assert abs(dist["stock"] - 0.5) < 0.15

    def test_no_eligible_accounts_abstains(self) -> None:
        # Too few periods → nothing to reconcile.
        dist = reconciliation_distribution({"a": {"values": [1.0, 2.0], "movements": [1.0, 1.0]}})
        assert dist["stock"] == 0.5

    def test_zero_movement_account_abstains(self) -> None:
        dist = reconciliation_distribution({"a": {"values": [5.0] * 6, "movements": [0.0] * 6}})
        assert dist["stock"] == 0.5

    def test_multiple_accounts_aggregate(self) -> None:
        dist = reconciliation_distribution({"a": _stock_account(), "b": _stock_account()})
        assert dist["stock"] > 0.9

    def test_gross_flow_reconciles_against_matching_anchor(self) -> None:
        # The recall shape: a GROSS-debit flow. It does NOT equal the net movement,
        # but with the gross-debit candidate anchor present it reconciles as flow.
        gross_debit = [120.0, 80.0, 200.0, 50.0, 160.0, 90.0, 140.0, 70.0, 110.0, 60.0, 130.0, 100.0]
        gross_credit = [20.0, 130.0, 0.0, 45.0, 80.0, 30.0, 90.0, 40.0, 25.0, 70.0, 30.0, 55.0]
        net = [d - c for d, c in zip(gross_debit, gross_credit)]
        series = {"values": gross_debit, "anchors": [net, gross_credit, gross_debit]}
        assert reconciliation_distribution({"a": series})["stock"] < 0.1

    def test_gross_flow_abstains_without_matching_anchor(self) -> None:
        # Same gross-debit flow reconciled ONLY against net: neither hypothesis fits
        # (value ≠ net, Δvalue ≠ net) → abstain, not a false stock/flow call.
        gross_debit = [120.0, 80.0, 200.0, 50.0, 160.0, 90.0, 140.0, 70.0, 110.0, 60.0, 130.0, 100.0]
        gross_credit = [20.0, 130.0, 0.0, 45.0, 80.0, 30.0, 90.0, 40.0, 25.0, 70.0, 30.0, 55.0]
        net = [d - c for d, c in zip(gross_debit, gross_credit)]
        series = {"values": gross_debit, "anchors": [net]}
        assert abs(reconciliation_distribution({"a": series})["stock"] - 0.5) < 0.15


# --- semantic claim witness --------------------------------------------------
class TestSemanticWitness:
    def test_stock_claim(self) -> None:
        assert semantic_distribution({"stock": 0.9})["stock"] == 0.9

    def test_flow_claim(self) -> None:
        assert semantic_distribution({"flow": 0.8})["stock"] == pytest.approx(0.2)

    def test_none_abstains(self) -> None:
        assert semantic_distribution(None)["stock"] == 0.5

    def test_empty_abstains(self) -> None:
        assert semantic_distribution({})["stock"] == 0.5


# --- pooled adjudication -----------------------------------------------------
class TestMeasure:
    def test_live_bug_flow_data_claimed_stock_surfaces_conflict(self) -> None:
        # trial_balance.debit_balance: data is a per-period FLOW, concept claims STOCK.
        flow_data = {"a": _flow_account(), "b": _flow_account()}
        live = measure_temporal_behavior("trial_balance", "debit_balance", flow_data, {"stock": 0.9})
        agree = measure_temporal_behavior("trial_balance", "credit_balance", flow_data, {"flow": 0.9})
        # The disagreement (semantic stock vs structural flow) is the signal.
        assert live.result.conflict > agree.result.conflict
        assert live.result.conflict > 0.3

    def test_genuine_stock_claimed_stock_is_quiet(self) -> None:
        # balance_sheet.ending_balance: data is a STOCK, concept claims STOCK → agree.
        stock_data = {"a": _stock_account(), "b": _stock_account()}
        adj = measure_temporal_behavior("balance_sheet", "ending_balance", stock_data, {"stock": 0.9})
        assert adj.result.conflict < 0.2
        assert adj.result.posterior[_STOCK] > 0.5

    def test_genuine_flow_claimed_flow_is_quiet(self) -> None:
        flow_data = {"a": _flow_account()}
        adj = measure_temporal_behavior("journal_lines", "debit", flow_data, {"flow": 0.9})
        assert adj.result.conflict < 0.2
        assert adj.result.posterior[_STOCK] < 0.5

    def test_no_semantic_claim_no_conflict(self) -> None:
        # Without a declared behaviour there is nothing for the data to disagree with:
        # the abstaining semantic witness is dropped, leaving a single witness.
        stock_data = {"a": _stock_account()}
        adj = measure_temporal_behavior("t", "c", stock_data, None)
        assert adj.result.conflict < 0.05
        assert {w.witness_id for w in adj.witnesses} == {"structural_reconciliation"}

    def test_unreconcilable_data_with_claim_no_conflict(self) -> None:
        # A claim exists but the data won't reconcile (wrong/missing anchor):
        # the abstaining structural witness is dropped → ignorance, not conflict.
        wrong = [7.0, -300.0, 11.0, 420.0, -5.0, 33.0, -90.0, 250.0, 4.0, -17.0, 88.0, -60.0]
        data = {"a": {"values": _cumsum(_M), "movements": wrong}}
        adj = measure_temporal_behavior("t", "c", data, {"stock": 0.9})
        assert adj.result.conflict < 0.05
        assert {w.witness_id for w in adj.witnesses} == {"semantic_claim"}

    def test_claim_field_identity(self) -> None:
        adj = measure_temporal_behavior(
            "trial_balance", "debit_balance", {"a": _flow_account()}, {"stock": 0.9}
        )
        assert adj.claim_field == "temporal_behavior:trial_balance.debit_balance"
        assert {w.witness_id for w in adj.witnesses} == {"structural_reconciliation", "semantic_claim"}
