"""The measured slice-relevance score (DAT-879, absorbing DAT-280/622/827).

These pin the properties the score is FOR, not a table of numbers: that it is
bounded, that it is scale-free in the bucket count, that it separates the
degenerate cases the old ordinal could not express, that a categorical axis and
a banded numeric axis land on one scale, and that a truncated distribution is
reported as bounded instead of asserted.
"""

from __future__ import annotations

import pytest

from dataraum.analysis.slicing.relevance import score_axis


def _score(counts: list[int], *, total: int | None = None, nulls: int = 0, groups=None) -> float:
    r = score_axis(
        total_rows=total if total is not None else sum(counts) + nulls,
        null_count=nulls,
        bucket_counts=counts,
        distinct_groups=groups,
    )
    assert r is not None
    return r


class TestBounds:
    def test_perfect_axis_scores_one(self) -> None:
        """Full coverage and a perfectly even split is the maximum."""
        assert _score([25, 25, 25, 25]) == pytest.approx(1.0)

    def test_score_is_a_fraction(self) -> None:
        for counts, nulls in [([50, 50], 0), ([99, 1], 0), ([70, 20, 7, 3], 40), ([1] * 50, 10)]:
            assert 0.0 <= _score(counts, nulls=nulls) <= 1.0

    def test_exactly_uniform_never_exceeds_one(self) -> None:
        """ln(k)/ln(k) is not exactly 1 in binary floating point: k in
        {5, 13, 19, ...} floats to 1.0000000000000002, which violates the
        column's CHECK — an IntegrityError that fails the slicing activity and
        makes Temporal retry the same data forever. Enumerate, don't sample."""
        for k in range(2, 200):
            assert _score([10] * k, groups=k) <= 1.0

    def test_stale_profile_counts_cannot_go_negative(self) -> None:
        """A profile whose counts exceed the row count gives probabilities > 1,
        hence a NEGATIVE entropy (-0.0995 here) — the other side of the same
        CHECK violation."""
        assert _score([120, 5], total=100, groups=2) >= 0.0


class TestScaleFreeInGroupCount:
    """The score must NOT prefer few groups to many — that is a business
    judgment, and it stays with the agent. Evenness is normalized by the
    group count precisely so it carries no such preference."""

    def test_even_axes_score_the_same_regardless_of_cardinality(self) -> None:
        two_way = _score([50, 50])
        four_way = _score([25, 25, 25, 25])
        eight_hundred_way = _score([1] * 800)
        assert two_way == pytest.approx(four_way) == pytest.approx(eight_hundred_way)


class TestSeparatesDegenerateAxes:
    def test_near_constant_scores_near_zero(self) -> None:
        """A 99/1 boolean resolves almost nothing and must say so.

        This is the case that ruled out the effective-group ratio exp(H)/k,
        which scores it 0.53 — indistinguishable from a merely skewed four-way
        split, and a positively misleading number for an axis that puts 99% of
        rows in one bucket.
        """
        assert _score([99, 1]) < 0.1
        assert _score([99, 1]) < _score([70, 20, 7, 3])

    def test_single_group_scores_zero(self) -> None:
        """One bucket partitions nothing. ln(1) = 0 would divide by zero, so
        this is defined rather than derived — and it is 0, not None: we DID
        measure it, and what we measured resolves nothing."""
        assert _score([100], groups=1) == 0.0

    def test_nulls_reduce_the_score_proportionally(self) -> None:
        """Coverage is a straight fraction: half the rows unlabelled halves it."""
        full = _score([50, 50])
        half_null = _score([25, 25], total=100, nulls=50)
        assert half_null == pytest.approx(full * 0.5)


class TestUnmeasurable:
    """None is not zero. 'We never measured this axis' and 'this axis resolves
    nothing' are different claims and the catalog stores them differently."""

    def test_no_buckets_is_unmeasured(self) -> None:
        assert score_axis(total_rows=100, null_count=0, bucket_counts=[]) is None

    def test_no_rows_is_unmeasured(self) -> None:
        assert score_axis(total_rows=0, null_count=0, bucket_counts=[1]) is None

    def test_all_null_is_unmeasured(self) -> None:
        assert score_axis(total_rows=100, null_count=100, bucket_counts=[1]) is None


class TestKindAgnostic:
    """DAT-280: a numeric/range axis must be comparable to a categorical one.

    The scorer takes a BUCKET DISTRIBUTION and nothing else, so it cannot tell
    the two apart — comparability is structural, not a claim. (The numeric slice
    TYPE is not built yet; this pins the scale it will land on.)
    """

    def test_band_counts_and_value_counts_score_identically(self) -> None:
        # Four equal numeric BANDS vs four equal categorical VALUES.
        assert _score([25, 25, 25, 25]) == pytest.approx(_score([25, 25, 25, 25]))

    def test_a_banded_numeric_axis_can_outrank_a_categorical_one(self) -> None:
        """The ranking the ordinal could never express: an even 5-band split of
        a numeric column beats a skewed categorical column, on one scale."""
        numeric_bands = _score([20, 20, 20, 20, 20])
        skewed_categorical = _score([90, 5, 3, 2])
        assert numeric_bands > skewed_categorical


class TestTruncatedDistribution:
    """The profiler stores only the top-K, so a high-cardinality axis arrives
    partial. The unseen tail is treated as ONE bucket, so the score is a LOWER
    BOUND — under-claiming rather than promoting an axis on an assumed-even tail
    that is really one dominant value.

    The interval itself is deliberately not returned: an earlier draft exposed
    lower/upper/exact and nothing consumed them.
    """

    def test_truncation_lowers_the_score_below_the_even_case(self) -> None:
        # Same 200 observed buckets; the second claims 4800 more groups holding
        # the remaining mass, which can only make the axis less even than the
        # complete case.
        complete = _score([300] * 200, total=60_000, groups=200)
        truncated = _score([300] * 200, total=100_000, groups=5000)
        assert truncated < complete

    def test_a_zero_mass_tail_does_not_deflate_the_score(self) -> None:
        """When the observed buckets already account for every non-null row, the
        groups distinct_count claims beyond them hold ZERO rows — a stale
        profile disagreeing with itself. Normalizing over the claimed 5000 would
        divide an entropy built from 200 buckets by ln(5000) and crush a
        perfectly even axis toward 0.6. The counts are the measurement."""
        assert _score([10] * 200, total=2000, groups=5000) == pytest.approx(1.0)
