"""Structural reconciliation statistic — DAT-491 (port of the DAT-459 probe).

Asserts the grounded separation properties the probe measured, on deterministic
versions of the same adversarial cases that falsified the persistence statistic
(trending/seasonal flows, mean-reverting/sawtooth stocks), plus the wrong-anchor
abstain guardrail. Properties and orderings, not fitted thresholds.
"""

from __future__ import annotations

import pytest

from dataraum.analysis.lineage.models import PATTERN_CUMULATIVE, PATTERN_PER_PERIOD
from dataraum.analysis.lineage.reconcile import (
    FIRE_RESIDUAL_MAX,
    MIN_SEPARATION,
    classify_entity,
    classify_series,
    dispose,
    dispose_classified,
    reconcile,
    separation,
    wilson_lcb,
)

_T = 12


def _cumsum(movements: list[float]) -> list[float]:
    out, total = [], 0.0
    for m in movements:
        total += m
        out.append(total)
    return out


def _wiggle(i: int, scale: float) -> float:
    """Deterministic ±scale alternation (the probe used seeded noise)."""
    return scale if i % 2 == 0 else -scale


# --- per-entity classification ------------------------------------------------
class TestClassifyEntity:
    def test_trending_flow_is_per_period(self) -> None:
        # The rho1-killer: a trending flow LOOKS persistent, but it still equals
        # its own per-period movement → R_flow ≈ 0.
        y = [100.0 + 15.0 * t + _wiggle(t, 5.0) for t in range(_T)]
        r = classify_entity(y, y)
        assert r.label == PATTERN_PER_PERIOD
        assert r.r_flow < r.r_stock

    def test_seasonal_flow_is_per_period(self) -> None:
        y = [100.0 + (50.0 if t % 12 in (9, 10, 11) else 0.0) + _wiggle(t, 8.0) for t in range(_T)]
        assert classify_entity(y, y).label == PATTERN_PER_PERIOD

    def test_mean_reverting_stock_is_cumulative(self) -> None:
        # The other rho1-killer: a mean-reverting stock LOOKS like noise, but its
        # level still carries forward (Δy == movement) → R_stock ≈ 0.
        movements = [50.0, -45.0, 40.0, -38.0, 55.0, -50.0, 42.0, -40.0, 48.0, -44.0, 41.0, -39.0]
        # The base level cancels out of Δy, so only the carry-forward matters.
        y = [1000.0 + v for v in _cumsum(movements)]
        r = classify_entity(y, movements)
        assert r.label == PATTERN_CUMULATIVE
        assert r.r_stock < r.r_flow

    def test_sawtooth_stock_is_cumulative(self) -> None:
        # Quarterly-close stock: accumulates then a big closing movement zeroes it.
        movements = [30.0, 35.0, -65.0, 28.0, 33.0, -61.0, 31.0, 36.0, -67.0, 29.0, 34.0, -63.0]
        y = _cumsum(movements)
        assert classify_entity(y, movements).label == PATTERN_CUMULATIVE

    def test_reconciliation_noise_does_not_flip_the_verdict(self) -> None:
        # Probe robustness sweep: verdicts hold through reconciliation noise
        # (rounding, timing, missing transactions). Alternating noise is the
        # WORST case for the stock hypothesis — differencing doubles it — so
        # 0.2 of the movement scale here corresponds to a harsher perturbation
        # than the probe's Gaussian 0.25 sweep.
        movements = [40.0, 38.0, 45.0, 41.0, 39.0, 44.0, 42.0, 40.0, 43.0, 41.0, 39.0, 42.0]
        noise_scale = 0.2 * 41.0
        flow = [m + _wiggle(t, noise_scale) for t, m in enumerate(movements)]
        assert classify_entity(flow, movements).label == PATTERN_PER_PERIOD
        stock = [v + _wiggle(t, noise_scale) for t, v in enumerate(_cumsum(movements))]
        assert classify_entity(stock, movements).label == PATTERN_CUMULATIVE

    def test_wrong_anchor_abstains(self) -> None:
        # The guardrail: against a misaligned anchor (wrong entity/join/bridge)
        # BOTH residuals stay large → abstain, never a confident misclassification.
        movements = [40.0, 38.0, 45.0, 41.0, 39.0, 44.0, 42.0, 40.0, 43.0, 41.0, 39.0, 42.0]
        y = _cumsum(movements)
        wrong_anchor = [500.0 - 30.0 * t for t in range(_T)]
        assert classify_entity(y, wrong_anchor).label is None

    def test_short_series_abstains(self) -> None:
        assert classify_entity([1.0, 2.0, 3.0], [1.0, 1.0, 1.0]).label is None

    def test_dead_anchor_abstains(self) -> None:
        assert classify_entity([1.0] * _T, [0.0] * _T).label is None

    def test_reconcile_rejects_length_mismatch(self) -> None:
        with pytest.raises(ValueError, match="length mismatch"):
            reconcile([1.0, 2.0], [1.0])


# --- near-tie margin (DAT-847) -------------------------------------------------
def _plateau(width: int, height: float = 10.0) -> tuple[list[float], list[float]]:
    """A series whose movement is one terminal period plus a mid-series plateau.

    The anchor books everything in the last period; the measure additionally
    holds ``height`` over ``width`` mid-series periods. Both hypotheses can
    explain that plateau — flow pays for every period it is held (``width``
    units of error), stock pays only for stepping on and off it (2 units) — so
    the plateau width is a dial on how far apart the two residuals land, with
    NOTHING else about the series changing. It separates "which hypothesis wins"
    from "by how much", which is exactly what the margin gates.
    """
    anchor = [0.0] * (_T - 1) + [100.0]
    measure = [0.0] * _T
    for t in range(5, 5 + width):
        measure[t] = height
    measure[_T - 1] = 100.0
    return measure, anchor


class TestNearTieAbstention:
    """A verdict needs a MARGIN, not a strict inequality — a coin-flip is ignorance."""

    def test_indistinguishable_residuals_abstain(self) -> None:
        # width 3: R_flow = 0.30 vs R_stock = 0.20 — stock wins, but by a factor
        # of 1.5, inside the band this module already calls non-discriminating.
        # A bare `<` reported CUMULATIVE here with full confidence.
        # (`approx` on the residuals: the plateau numbers are exactly
        # representable today, but the RATIO is what this pins — an exact-equality
        # assert would turn any future fixture tweak into a float puzzle.)
        y, m = _plateau(3)
        r = classify_entity(y, m)
        assert r.r_flow == pytest.approx(0.30)
        assert r.r_stock == pytest.approx(0.20)
        assert min(r.r_flow, r.r_stock) <= FIRE_RESIDUAL_MAX  # the fit gate passes
        assert r.label is None  # ...and the margin gate does not

    def test_same_series_with_a_clear_winner_still_fires(self) -> None:
        # width 6 — the only thing that changed — puts stock 3× ahead of flow.
        y, m = _plateau(6)
        r = classify_entity(y, m)
        assert r.r_flow == pytest.approx(0.60)
        assert r.r_stock == pytest.approx(0.20)
        assert r.label == PATTERN_CUMULATIVE

    def test_exact_tie_abstains(self) -> None:
        # Equal residuals used to fall through to PER_PERIOD purely because the
        # comparison was `r_stock < r_flow` — the else-branch was a default, not
        # a finding.
        y = [20.0] + [0.0] * (_T - 2) + [100.0]
        m = [0.0] * (_T - 1) + [100.0]
        r = classify_entity(y, m)
        assert r.r_flow == r.r_stock
        assert r.label is None

    def test_series_that_fits_both_hypotheses_abstains(self) -> None:
        # A single terminal movement satisfies y == m AND Δy == m: both residuals
        # are 0, a perfect fit for either reading and evidence for neither.
        y = [0.0] * (_T - 1) + [100.0]
        r = classify_entity(y, list(y))
        assert (r.r_flow, r.r_stock) == (0.0, 0.0)
        assert r.label is None

    def test_margin_is_derived_from_the_fit_gate(self) -> None:
        # Not a tuned constant: it is the separation the weakest STILL-ADMISSIBLE
        # fit (FIRE_RESIDUAL_MAX) shows against the ≈1.0 the losing hypothesis
        # structurally sits at under a correct anchor.
        assert MIN_SEPARATION == separation(FIRE_RESIDUAL_MAX, 1.0)
        # Equivalently: the loser must be at least twice the winner, at any scale.
        assert separation(0.25, 0.5) == MIN_SEPARATION
        assert separation(2.0, 4.0) == MIN_SEPARATION


class TestSeparation:
    def test_is_scale_free_and_symmetric(self) -> None:
        assert separation(0.25, 0.5) == separation(0.5, 0.25)
        assert separation(0.25, 0.5) == separation(2.5, 5.0)

    def test_bounds(self) -> None:
        assert separation(0.3, 0.3) == 0.0
        assert separation(0.0, 0.5) == 1.0

    def test_a_dead_hypothesis_is_total_separation(self) -> None:
        # An infinite residual is a hypothesis whose normalizer died, not a bad
        # fit — the surviving hypothesis is unopposed.
        assert separation(0.1, float("inf")) == 1.0

    def test_two_dead_hypotheses_are_no_separation(self) -> None:
        assert separation(float("inf"), float("inf")) == 0.0


# --- candidate disposal --------------------------------------------------------
def _flow_entity(seed: int) -> tuple[list[float], list[float]]:
    y = [100.0 + 10.0 * seed + 12.0 * t + _wiggle(t, 4.0) for t in range(_T)]
    return y, list(y)


def _stock_entity(seed: int) -> tuple[list[float], list[float]]:
    movements = [30.0 + seed + _wiggle(t, 6.0) for t in range(_T)]
    return _cumsum(movements), movements


class TestDispose:
    def test_agreeing_entities_yield_a_verdict(self) -> None:
        verdict = dispose({f"acct{i}": _stock_entity(i) for i in range(5)})
        assert verdict is not None
        assert verdict.pattern == PATTERN_CUMULATIVE
        assert verdict.n_entities_fired == 5
        assert verdict.match_rate > 0.99

    def test_flow_entities_yield_per_period(self) -> None:
        verdict = dispose({f"acct{i}": _flow_entity(i) for i in range(4)})
        assert verdict is not None
        assert verdict.pattern == PATTERN_PER_PERIOD

    def test_split_vote_is_no_verdict(self) -> None:
        series = {f"s{i}": _stock_entity(i) for i in range(3)}
        series |= {f"f{i}": _flow_entity(i) for i in range(3)}
        assert dispose(series) is None

    def test_single_voting_entity_is_no_verdict(self) -> None:
        # A lone entity is ignorance, not lineage (MIN_ENTITIES_FIRED).
        assert dispose({"only": _stock_entity(1)}) is None

    def test_abstaining_entities_lower_match_rate(self) -> None:
        series = {f"acct{i}": _stock_entity(i) for i in range(4)}
        series["short"] = ([1.0, 2.0], [1.0, 1.0])  # abstains (too short)
        verdict = dispose(series)
        assert verdict is not None
        assert verdict.n_entities == 5
        assert verdict.n_entities_fired == 4
        assert verdict.match_rate < 0.99

    def test_empty_series_is_no_verdict(self) -> None:
        assert dispose({}) is None

    def test_dispose_is_dispose_classified_of_classify_series(self) -> None:
        # The DAT-759 split is a pure refactor: the composed path is identical.
        series = {f"acct{i}": _stock_entity(i) for i in range(4)}
        assert dispose(series) == dispose_classified(classify_series(series))


class TestWilsonLcb:
    """The DAT-759 support statistic: Wilson score lower bound of the vote rate."""

    def test_known_values(self) -> None:
        assert wilson_lcb(0, 0) == 0.0
        assert wilson_lcb(0, 10) == 0.0
        assert abs(wilson_lcb(4, 4) - 0.510) < 0.005
        assert abs(wilson_lcb(2, 4) - 0.150) < 0.005
        assert abs(wilson_lcb(20, 20) - 0.839) < 0.005

    def test_breadth_beats_perfect_subset(self) -> None:
        # The selection property the criterion exists for: 21/21 with the same
        # rate bound dominates a perfect small subset (the probe's real margins).
        assert wilson_lcb(21, 21) > wilson_lcb(15, 21) > wilson_lcb(4, 21)

    def test_common_denominator_is_load_bearing(self) -> None:
        # Probe leg b2: the same 10 votes rank near-top on their own subset
        # denominator and mid-field on the common one — callers must pass the
        # pairing universe, never the convention's aligned subset.
        assert wilson_lcb(10, 10) > 0.7
        assert wilson_lcb(10, 20) < 0.31
        assert wilson_lcb(20, 20) > wilson_lcb(10, 10)
