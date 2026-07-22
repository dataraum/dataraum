"""DAT-844 — the near-zero-baseline guard shared by every ``group_effects`` (targets.py).

``group_effects`` reports ``group / baseline − 1``. A baseline indistinguishable from
zero relative to the measure's own dispersion — most sharply, a within-entity de-meaned
residual, whose baseline is ~0 BY CONSTRUCTION — turns ordinary float summation noise
into a ``±10^18``-scale "effect" that would otherwise reach the LLM context verbatim
(the manual repro against ``test_flow_demeaned_residual_omits_all_effects``'s exact
fixture showed ``±10^14``-``10^15`` before the guard). Every target must OMIT every
slice for that node rather than emit or clamp such a number — generalizing
``EntityDemeanedRatioTarget``'s existing (always-empty) precedent to a data-derived
judgment: the healthy-baseline case is unaffected (golden equivalence pins it).
"""

from __future__ import annotations

import numpy as np
import pytest

from dataraum.analysis.drivers.criterion import build_codes
from dataraum.analysis.drivers.processor import (
    _within_entity_ratio_residual,
    _within_entity_residual,
)
from dataraum.analysis.drivers.targets import EntityMeanTarget, FlowTarget, RatioTarget

from .conftest import (
    CL_ENTITY,
    CL_RATIO_ROW_DRIVER,
    CL_ROW_DRIVER,
    _physical,
    columns,
    make_clustered_ratio_two_driver_corpus,
    make_clustered_two_driver_corpus,
    make_corpus,
    make_ratio_corpus,
)


class TestFlowTargetBaselineGuard:
    def test_demeaned_residual_omits_all_effects(self) -> None:
        """The DAT-844 signature fixture: a within-entity residual's baseline is ~0 by
        construction (float summation noise, not a real average) — the guard must
        return ``[]``, not the old ``±10^14``-scale effect."""
        df = make_clustered_two_driver_corpus(np.random.default_rng(0))
        residual = _within_entity_residual(df, CL_ENTITY, "measure")
        target = FlowTarget(residual, target_type="flow")
        phys, _labels = _physical(df[CL_ROW_DRIVER])
        codes, n_codes = build_codes(phys, target.observed, handle_nulls=True)
        assert target.group_effects(codes, n_codes, min_support=200) == []

    def test_healthy_baseline_unaffected(self) -> None:
        """A normal (non-demeaned) measure with a real driver still reports real,
        finite effects — the guard must be silent on the ordinary case."""
        df = make_corpus(np.random.default_rng(0))
        phys, measure = columns(df, "D_e60")
        target = FlowTarget(measure, target_type="flow")
        codes, n_codes = build_codes(phys, target.observed, handle_nulls=True)
        effects = target.group_effects(codes, n_codes, min_support=200)
        assert effects  # the ±60% driver clears support and reports
        assert all(np.isfinite(e) and abs(e) < 10 for _c, e, _s in effects)


class TestRatioTargetBaselineGuard:
    def test_demeaned_ratio_residual_omits_all_effects(self) -> None:
        """The ratio analogue: a plain ``RatioTarget`` fed the within-entity de-meaned
        residual (what ``EntityDemeanedRatioTarget`` special-cases to ``[]`` today) must
        independently hit the SAME near-zero-baseline guard, not just inherit the
        special case."""
        df = make_clustered_ratio_two_driver_corpus(np.random.default_rng(0))
        residual, weight = _within_entity_ratio_residual(df, CL_ENTITY, "numerator", "denominator")
        # Reconstruct a (numerator, denominator) pair whose Σnum/Σden IS exactly the
        # residual's weighted mean, so a plain RatioTarget sees the same baseline
        # EntityDemeanedRatioTarget would compute internally.
        target = RatioTarget(residual * weight, weight)
        phys, _labels = _physical(df[CL_RATIO_ROW_DRIVER])
        codes, n_codes = build_codes(phys, target.observed, handle_nulls=True)
        assert target.group_effects(codes, n_codes, min_support=200) == []

    def test_healthy_baseline_unaffected(self) -> None:
        df = make_ratio_corpus(np.random.default_rng(0))
        phys, _labels = _physical(df["R_e60"])
        target = RatioTarget(
            df["numerator"].to_numpy().astype(float),
            df["denominator"].to_numpy().astype(float),
        )
        codes, n_codes = build_codes(phys, target.observed, handle_nulls=True)
        effects = target.group_effects(codes, n_codes, min_support=200)
        assert effects
        assert all(np.isfinite(e) and abs(e) < 10 for _c, e, _s in effects)


class TestEntityMeanTargetBaselineGuard:
    """No production path currently feeds ``EntityMeanTarget`` a de-meaned residual
    (the entity-grain family is always primary over the row-wise de-mean under high
    ICC, DAT-561) — but the guard generalizes to it too, per the DAT-844 judgment: a
    baseline near-zero relative to the entity means' own spread is not comparable,
    residual or not. Constructed deterministically (exact cancellation) rather than
    via random noise — CLT noise averages down only as ``1/sqrt(n)``, nowhere near the
    relative float-precision floor the guard targets, so a random near-zero baseline
    isn't a reliable trigger at test-sized n.
    """

    def test_exact_zero_weighted_mean_omits_all_effects(self) -> None:
        means = np.concatenate([np.full(200, 5.0), np.full(200, -5.0)])
        sizes = np.full(400, 50.0)
        codes = np.concatenate([np.zeros(200, dtype=np.int64), np.ones(200, dtype=np.int64)])
        target = EntityMeanTarget(means, sizes, target_type="flow")
        assert target.group_effects(codes, 2, min_support=50) == []

    def test_healthy_baseline_unaffected(self) -> None:
        means = np.concatenate([np.full(200, 120.0), np.full(200, 80.0)])
        sizes = np.full(400, 50.0)
        codes = np.concatenate([np.zeros(200, dtype=np.int64), np.ones(200, dtype=np.int64)])
        target = EntityMeanTarget(means, sizes, target_type="flow")
        effects = target.group_effects(codes, 2, min_support=50)
        actual = {c: (e, s) for c, e, s in effects}
        assert actual.keys() == {0, 1}
        assert actual[0] == (pytest.approx(0.2), 200)
        assert actual[1] == (pytest.approx(-0.2), 200)
