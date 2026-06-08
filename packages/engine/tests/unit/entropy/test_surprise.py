"""Surprise primitive — ``D_KL(observed ‖ reference)`` and its [0, 1) score.

The statistical-paradigm parallel to ``test_pooling.py``: where pooling measures
witness *disagreement*, surprise measures *distance from an expected reference*.
The defining property under test is sample-size invariance — the reason the
Cramér's-V / chi-square boost curves die.
"""

from __future__ import annotations

import math

import pytest

from dataraum.entropy.surprise import kl_divergence, surprise_score

# Benford's leading-digit reference (1..9), the canonical surprise reference.
_BENFORD = [math.log10(1 + 1 / d) for d in range(1, 10)]
_UNIFORM9 = [1 / 9] * 9


# --- kl_divergence ---------------------------------------------------------


def test_kl_identical_distributions_is_zero() -> None:
    assert kl_divergence(_BENFORD, _BENFORD) == pytest.approx(0.0, abs=1e-12)


def test_kl_is_non_negative() -> None:
    assert kl_divergence(_UNIFORM9, _BENFORD) > 0.0
    assert kl_divergence(_BENFORD, _UNIFORM9) > 0.0


def test_kl_is_asymmetric() -> None:
    # D_KL is a divergence, not a metric — direction matters.
    assert kl_divergence(_UNIFORM9, _BENFORD) != pytest.approx(
        kl_divergence(_BENFORD, _UNIFORM9)
    )


def test_kl_one_bit_for_point_mass_on_half() -> None:
    # Observed certainty on an outcome the reference gives prob 1/2 → exactly 1 bit.
    assert kl_divergence([1.0, 0.0], [0.5, 0.5]) == pytest.approx(1.0)


def test_kl_counts_equal_proportions() -> None:
    # Sample-size invariance: raw counts and the same proportions give equal KL.
    counts = [600, 300, 100]
    props = [0.6, 0.3, 0.1]
    ref = [0.5, 0.3, 0.2]
    assert kl_divergence(counts, ref) == pytest.approx(kl_divergence(props, ref))


def test_kl_scaling_observed_is_invariant() -> None:
    # 100× the same shape → identical surprise (intensive, not extensive).
    small = [6, 3, 1]
    large = [600, 300, 100]
    assert kl_divergence(small, _BENFORD[:3]) == pytest.approx(
        kl_divergence(large, _BENFORD[:3])
    )


def test_kl_reference_floor_keeps_it_finite() -> None:
    # Observed mass on an outcome the reference deems impossible → big but finite.
    div = kl_divergence([0.0, 1.0], [1.0, 0.0])
    assert math.isfinite(div)
    assert div > 25.0  # ~ -log2(_REF_FLOOR)


def test_kl_length_mismatch_raises() -> None:
    with pytest.raises(ValueError):
        kl_divergence([0.5, 0.5], [1.0])


def test_kl_zero_mass_raises() -> None:
    with pytest.raises(ValueError):
        kl_divergence([0.0, 0.0], [0.5, 0.5])


# --- surprise_score --------------------------------------------------------


def test_score_zero_when_matching_reference() -> None:
    assert surprise_score(_BENFORD, _BENFORD) == pytest.approx(0.0, abs=1e-12)


def test_score_in_unit_interval() -> None:
    for observed in (_UNIFORM9, [1.0] + [0.0] * 8, [0.0] * 8 + [1.0]):
        s = surprise_score(observed, _BENFORD)
        assert 0.0 <= s < 1.0


def test_score_half_at_one_bit() -> None:
    # D_KL = 1 bit → 1 - 2^-1 = 0.5 exactly.
    assert surprise_score([1.0, 0.0], [0.5, 0.5]) == pytest.approx(0.5)


def test_score_monotonic_in_distortion() -> None:
    # Drift the observed away from Benford toward uniform → surprise rises.
    def blend(t: float) -> list[float]:
        return [(1 - t) * b + t * u for b, u in zip(_BENFORD, _UNIFORM9, strict=True)]

    scores = [surprise_score(blend(t), _BENFORD) for t in (0.0, 0.25, 0.5, 0.75, 1.0)]
    assert scores == sorted(scores)
    assert scores[0] == pytest.approx(0.0, abs=1e-12)
    assert scores[-1] > scores[0]


def test_score_sample_size_invariant() -> None:
    # The whole point: a clean shape scores the same at n=100 and n=8000, so the
    # large-n chi-square false positive (clean → 0.7) cannot happen here.
    clean_shape = _BENFORD
    assert surprise_score([100 * c for c in clean_shape], _BENFORD) == pytest.approx(
        surprise_score([8000 * c for c in clean_shape], _BENFORD), abs=1e-12
    )


def test_strong_departure_is_surprising() -> None:
    # Mass piled where Benford expects little (digit 5: 0.45 vs 0.079; digit 9:
    # 0.20 vs 0.046) departs hard → a high surprise score, the recall signal.
    skewed = [0.05, 0.05, 0.05, 0.05, 0.45, 0.05, 0.05, 0.05, 0.20]
    assert surprise_score(skewed, _BENFORD) > 0.4

    # A merely-uniform digit distribution is only mildly surprising vs Benford —
    # the score is honest about how far the data actually sits, no boost curve.
    assert surprise_score(_UNIFORM9, _BENFORD) < 0.3
