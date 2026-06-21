"""DAT-545 P1 — the split criterion + (A)/(B) null gates.

Criterion-level only (no permutation null yet — that's the tree, P2): the gain
ranks a planted driver above an independent null, the (A) gate drops dim-null rows,
the (B) gate closes the measure-conditional-missingness leak, and min-support
suppresses a tiny-group dimension.
"""

from __future__ import annotations

import numpy as np

from dataraum.analysis.drivers.criterion import build_codes, variance_reduction

from .conftest import columns, make_corpus


def _gain(df, dim: str, *, handle_nulls: bool) -> float:
    phys, measure = columns(df, dim)
    codes, n_codes = build_codes(phys, measure, handle_nulls=handle_nulls)
    return variance_reduction(codes, n_codes, measure)


class TestVarianceReduction:
    def test_strong_driver_outranks_independent_null(self) -> None:
        df = make_corpus(np.random.default_rng(0))
        assert _gain(df, "D_e60", handle_nulls=True) > _gain(df, "N_lowcard", handle_nulls=True)
        # The effect-size ladder is monotone in expectation: the ±60% driver explains
        # more variance than the ±15% one.
        assert _gain(df, "D_e60", handle_nulls=True) > _gain(df, "D_e15", handle_nulls=True)

    def test_min_support_suppresses_tiny_groups(self) -> None:
        # N_highcard (400 levels over 20k rows ⇒ ~50/group) has no group clearing
        # min_support=200, so the gain collapses to 0 — the slice_variance wall avoided.
        df = make_corpus(np.random.default_rng(0))
        assert _gain(df, "N_highcard", handle_nulls=True) == 0.0

    def test_min_support_threshold_is_honoured(self) -> None:
        # Two clean groups below threshold → 0.0; above → positive.
        measure = np.concatenate([np.zeros(150), np.ones(150)])
        phys = np.array([0] * 150 + [1] * 150)
        codes, n = build_codes(phys, measure, handle_nulls=True)
        assert variance_reduction(codes, n, measure, min_support=200) == 0.0
        assert variance_reduction(codes, n, measure, min_support=100) > 0.9


class TestNullGates:
    def test_dim_present_gate_drops_null_rows(self) -> None:
        # (A): N_mnar is dim-null on ~half the rows — those must get code -1.
        df = make_corpus(np.random.default_rng(0))
        phys, measure = columns(df, "N_mnar")
        codes, _ = build_codes(phys, measure, handle_nulls=True)
        dim_null = phys == -1
        assert np.all(codes[dim_null] == -1)
        assert np.any(codes[~dim_null] >= 0)

    def test_missingness_gate_closes_measure_missing_leak(self) -> None:
        # (B): N_measure_missing concentrates measure-missingness in one slice value.
        # WITHOUT handling it manufactures a spurious gain; WITH the (B) gate the
        # offending slice is dropped and the gain falls sharply. This is the
        # load-bearing ablation from the spike.
        df = make_corpus(np.random.default_rng(0))
        leaked = _gain(df, "N_measure_missing", handle_nulls=False)
        gated = _gain(df, "N_measure_missing", handle_nulls=True)
        assert leaked > gated
        # The gate drops at least the offending slice value (fewer retained codes
        # than distinct present values).
        phys, measure = columns(df, "N_measure_missing")
        _, n_handled = build_codes(phys, measure, handle_nulls=True)
        _, n_raw = build_codes(phys, measure, handle_nulls=False)
        assert n_handled < n_raw
