"""Tier-1 synthetic proofs for the pure measurement statistics (entropy/stats.py).

Each measurement is a pure function: this asserts ordering, calibration, and edge
cases on synthetic inputs in microseconds — where measurement design happens.
The eval Tier-2 tests confirm the SAME functions
reproduce the pipeline's values on recorded data. Higher value = more entropy.
"""

from __future__ import annotations

import pytest

from dataraum.entropy import stats


class TestRate:
    def test_zero(self) -> None:
        assert stats.rate(0, 10) == 0.0

    def test_full(self) -> None:
        assert stats.rate(10, 10) == 1.0

    def test_empty_total_is_zero(self) -> None:
        assert stats.rate(0, 0) == 0.0

    def test_calibrates(self) -> None:
        assert stats.rate(2, 10) == pytest.approx(0.2)


class TestNullRatio:
    def test_empty_is_zero(self) -> None:
        assert stats.null_ratio([]) == 0.0

    def test_full_is_zero(self) -> None:
        assert stats.null_ratio(["a", "b", "c"]) == 0.0

    def test_all_missing_is_one(self) -> None:
        assert stats.null_ratio([None, "", None]) == 1.0

    def test_calibrates_to_fraction(self) -> None:
        assert stats.null_ratio(["a", None, "b", None]) == pytest.approx(0.5)

    def test_monotonic_in_missingness(self) -> None:
        assert stats.null_ratio([None, "a", "a", "a"]) < stats.null_ratio([None, None, "a", "a"])


class TestOrphanRate:
    def test_all_resolve(self) -> None:
        assert stats.orphan_rate([1, 2, 3], [1, 2, 3, 4]) == 0.0

    def test_all_orphan(self) -> None:
        assert stats.orphan_rate([7, 8], [1, 2]) == 1.0

    def test_calibrates(self) -> None:
        assert stats.orphan_rate([1, 2, 3, 4, 99], [1, 2, 3, 4]) == pytest.approx(0.2)

    def test_null_children_are_not_orphans(self) -> None:
        assert stats.orphan_rate([1, None, "", 2], [1, 2]) == 0.0


class TestTypeFidelity:
    def test_clean(self) -> None:
        assert stats.type_fidelity(1.0, 0.0) == 0.0

    def test_quarantine_drives_it(self) -> None:
        assert stats.type_fidelity(1.0, 0.08) == pytest.approx(0.08)

    def test_parse_failure_drives_it(self) -> None:
        assert stats.type_fidelity(0.9, 0.0) == pytest.approx(0.1)

    def test_worse_of_the_two(self) -> None:
        assert stats.type_fidelity(0.9, 0.05) == pytest.approx(0.1)


class TestTimeRoleMismatch:
    def test_aligned(self) -> None:
        assert stats.time_role_mismatch(is_temporal_type=True, is_timestamp_role=True) == 0.0

    def test_unparseable_timestamp(self) -> None:
        assert stats.time_role_mismatch(is_temporal_type=False, is_timestamp_role=True) == 1.0

    def test_not_a_time_column(self) -> None:
        assert stats.time_role_mismatch(is_temporal_type=False, is_timestamp_role=False) == 0.0


class TestConfidenceEntropy:
    def test_confident(self) -> None:
        assert stats.confidence_entropy(1.0) == 0.0

    def test_clueless(self) -> None:
        assert stats.confidence_entropy(0.0) == 1.0

    def test_calibrates(self) -> None:
        assert stats.confidence_entropy(0.3) == pytest.approx(0.7)


class TestNmi:
    def test_identical_is_one(self) -> None:
        assert stats.nmi([0, 1, 0, 1], [0, 1, 0, 1]) == pytest.approx(1.0)

    def test_perfect_mutex_is_one(self) -> None:
        # double-entry: debit non-zero XOR credit non-zero
        assert stats.nmi([1, 0, 1, 0], [0, 1, 0, 1]) == pytest.approx(1.0)

    def test_constant_column_is_zero(self) -> None:
        assert stats.nmi([1, 1, 1, 1], [0, 1, 0, 1]) == 0.0

    def test_independent_is_low(self) -> None:
        x = [0, 0, 1, 1, 0, 0, 1, 1]
        y = [0, 1, 0, 1, 0, 1, 0, 1]  # every (x,y) combo equally likely → independent
        assert stats.nmi(x, y) < 0.2

    def test_empty_is_zero(self) -> None:
        assert stats.nmi([], []) == 0.0


class TestCramersV:
    """Bias-corrected Cramér's V of is-null × slice (DAT-473). The pinned reference lives
    in dataraum-eval test_slice_null_gate.py; these guard the engine's copy of the math."""

    def _two_balanced_slices(self, n_each: int) -> list[str]:
        return ["A"] * n_each + ["B"] * n_each

    def test_independent_missingness_is_low(self) -> None:
        # MCAR: same null rate in both slices → association ≈ 0.
        slices = self._two_balanced_slices(100)
        is_null = ([True] * 20 + [False] * 80) + ([True] * 20 + [False] * 80)
        v = stats.cramers_v(is_null, slices)
        assert v is not None and v < 0.1

    def test_concentrated_missingness_is_high(self) -> None:
        # All nulls land in slice B → strong association.
        slices = self._two_balanced_slices(100)
        is_null = ([False] * 100) + ([True] * 50 + [False] * 50)
        v = stats.cramers_v(is_null, slices)
        assert v is not None and v > 0.4

    def test_concentration_orders_above_mcar(self) -> None:
        slices = self._two_balanced_slices(100)
        mcar = stats.cramers_v(([True] * 20 + [False] * 80) * 2, slices)
        conc = stats.cramers_v([False] * 100 + [True] * 40 + [False] * 60, slices)
        assert mcar is not None and conc is not None and conc > mcar

    def test_cochran_abstains_on_small_expected_cell(self) -> None:
        # A tiny slice (n=3) makes an expected null-cell < 5 → abstain, not an inflated V.
        slices = ["A"] * 200 + ["B"] * 3
        is_null = [i % 10 == 0 for i in range(200)] + [True, False, False]
        assert stats.cramers_v(is_null, slices) is None

    def test_abstains_on_single_slice(self) -> None:
        assert stats.cramers_v([True, False] * 50, ["only"] * 100) is None

    def test_abstains_on_no_null_and_all_null(self) -> None:
        slices = self._two_balanced_slices(100)
        assert stats.cramers_v([False] * 200, slices) is None
        assert stats.cramers_v([True] * 200, slices) is None

    def test_empty_abstains(self) -> None:
        assert stats.cramers_v([], []) is None
