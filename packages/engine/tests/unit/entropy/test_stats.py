"""Tier-1 synthetic proofs for the pure measurement statistics (entropy/stats.py).

Each measurement is a pure function: this asserts ordering, calibration, and edge
cases on synthetic inputs in microseconds — where measurement design happens
(entropy_eval_architecture.md). The eval Tier-2 tests confirm the SAME functions
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
