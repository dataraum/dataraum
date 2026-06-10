"""Structural reconciliation statistic — DAT-491 (port of the DAT-459 probe).

Asserts the grounded separation properties the probe measured, on deterministic
versions of the same adversarial cases that falsified the persistence statistic
(trending/seasonal flows, mean-reverting/sawtooth stocks), plus the wrong-anchor
abstain guardrail. Properties and orderings, not fitted thresholds.
"""

from __future__ import annotations

from dataraum.analysis.lineage.models import PATTERN_CUMULATIVE, PATTERN_PER_PERIOD
from dataraum.analysis.lineage.reconcile import classify_entity, dispose, reconcile

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
        import pytest

        with pytest.raises(ValueError, match="length mismatch"):
            reconcile([1.0, 2.0], [1.0])


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
