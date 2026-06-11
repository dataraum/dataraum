"""Unit tests for the generic pooling engine (ADR-0009, DAT-457).

Pure math; no DB, no LLM. These assert the (C, U) *separation* that the whole
adjudication model rests on — the DAT-457 acceptance criteria, written as
orderings/properties rather than point thresholds (ADR-0009 eval grammar).
"""

from __future__ import annotations

import math

import pytest

from dataraum.entropy.pooling import (
    PoolResult,
    Witness,
    jensen_shannon_divergence,
    log_linear_pool,
    pool,
    shannon_entropy,
)

# --- claim space helpers (binary {is-null, is-value}-style space) -------------
SAYS_A = (1.0, 0.0)
SAYS_B = (0.0, 1.0)
UNSURE = (0.5, 0.5)


def _is_distribution(probs: tuple[float, ...]) -> bool:
    return math.isclose(math.fsum(probs), 1.0, abs_tol=1e-9) and all(p >= 0.0 for p in probs)


# --- math primitives ---------------------------------------------------------
class TestMathPrimitives:
    def test_shannon_entropy_uniform_binary_is_one_bit(self) -> None:
        assert shannon_entropy([0.5, 0.5]) == pytest.approx(1.0)

    def test_shannon_entropy_one_hot_is_zero(self) -> None:
        assert shannon_entropy([1.0, 0.0]) == pytest.approx(0.0)

    def test_shannon_entropy_uniform_quaternary_is_two_bits(self) -> None:
        assert shannon_entropy([0.25] * 4) == pytest.approx(2.0)

    def test_jsd_of_identical_distributions_is_zero(self) -> None:
        assert jensen_shannon_divergence([SAYS_A, SAYS_A], [0.5, 0.5]) == pytest.approx(0.0)

    def test_jsd_of_disjoint_equal_weight_is_one_bit(self) -> None:
        assert jensen_shannon_divergence([SAYS_A, SAYS_B], [0.5, 0.5]) == pytest.approx(1.0)

    def test_log_linear_pool_is_idempotent_on_one_hot(self) -> None:
        q = log_linear_pool([SAYS_A, SAYS_A], [1.0, 1.0])
        assert q[0] == pytest.approx(1.0, abs=1e-6)

    def test_log_linear_pool_sharpens_with_corroboration(self) -> None:
        # Two agreeing non-degenerate witnesses raise confidence (NOT idempotent):
        # q ∝ (0.7², 0.3²) → 0.49 / (0.49 + 0.09).
        q = log_linear_pool([(0.7, 0.3), (0.7, 0.3)], [1.0, 1.0])
        assert q[0] == pytest.approx(0.49 / 0.58, abs=1e-6)
        assert q[0] > 0.7


# --- the (C, U) separation: the DAT-457 acceptance criteria ------------------
class TestConflictIgnoranceSeparation:
    def test_conflicting_witnesses_high_C_low_U(self) -> None:
        result = pool([Witness("a", SAYS_A, 0.8), Witness("b", SAYS_B, 0.8)])
        assert result.conflict == pytest.approx(1.0)
        assert result.ignorance < 0.5

    def test_lone_reliable_witness_zero_conflict(self) -> None:
        result = pool([Witness("a", SAYS_A, 0.9)])
        assert result.conflict == pytest.approx(0.0)
        # posterior tracks the lone witness
        assert result.posterior[0] == pytest.approx(1.0, abs=1e-6)
        # one uncorroborated voice still leaves real ignorance
        assert 0.0 < result.ignorance < 1.0

    def test_no_witnesses_is_total_ignorance(self) -> None:
        result = pool([])
        assert result == PoolResult(
            posterior=(), conflict=0.0, ignorance=1.0, n_witnesses=0, evidence_mass=0.0
        )

    def test_all_untrusted_witnesses_uniform_posterior_high_ignorance(self) -> None:
        result = pool([Witness("a", SAYS_A, 0.0), Witness("b", SAYS_B, 0.0)])
        assert result.posterior == pytest.approx((0.5, 0.5))
        assert result.conflict == pytest.approx(0.0)
        assert result.ignorance > 0.9

    def test_vague_witnesses_are_ignorant_not_conflicted(self) -> None:
        # two reliable but uninformative (uniform) witnesses: agreement, but they
        # told us nothing -> low conflict, high ignorance.
        result = pool([Witness("a", UNSURE, 0.9), Witness("b", UNSURE, 0.9)])
        assert result.conflict == pytest.approx(0.0)
        assert result.ignorance > 0.9

    def test_agreement_is_quiet(self) -> None:
        result = pool([Witness("a", SAYS_A, 0.9), Witness("b", SAYS_A, 0.9)])
        assert result.conflict == pytest.approx(0.0)


# --- weight robustness: conflict flags regardless of who we trust ------------
class TestWeightRobustness:
    def test_full_disagreement_conflict_is_one_for_any_weights(self) -> None:
        balanced = pool([Witness("a", SAYS_A, 0.5), Witness("b", SAYS_B, 0.5)])
        skewed = pool([Witness("a", SAYS_A, 0.9), Witness("b", SAYS_B, 0.1)])
        very_skewed = pool([Witness("a", SAYS_A, 0.99), Witness("b", SAYS_B, 0.01)])
        assert balanced.conflict == pytest.approx(1.0)
        assert skewed.conflict == pytest.approx(1.0)
        assert very_skewed.conflict == pytest.approx(1.0)

    def test_weights_shift_the_posterior(self) -> None:
        balanced = pool([Witness("a", SAYS_A, 0.5), Witness("b", SAYS_B, 0.5)])
        skewed = pool([Witness("a", SAYS_A, 0.9), Witness("b", SAYS_B, 0.1)])
        # balanced -> indifferent; trusting A more -> posterior leans to claim A
        assert balanced.posterior[0] == pytest.approx(0.5)
        assert skewed.posterior[0] > 0.9


# --- orderings (monotonicity, not point thresholds) --------------------------
class TestOrderings:
    def test_more_corroboration_reduces_ignorance(self) -> None:
        one = pool([Witness("a", SAYS_A, 0.9)])
        three = pool(
            [Witness("a", SAYS_A, 0.9), Witness("b", SAYS_A, 0.9), Witness("c", SAYS_A, 0.9)]
        )
        assert three.ignorance < one.ignorance

    def test_more_separation_raises_conflict(self) -> None:
        mild = pool([Witness("a", (0.6, 0.4), 0.9), Witness("b", (0.4, 0.6), 0.9)])
        strong = pool([Witness("a", (0.9, 0.1), 0.9), Witness("b", (0.1, 0.9), 0.9)])
        assert strong.conflict > mild.conflict

    def test_higher_reliability_reduces_ignorance(self) -> None:
        weak = pool([Witness("a", SAYS_A, 0.2)])
        strong = pool([Witness("a", SAYS_A, 0.95)])
        assert strong.ignorance < weak.ignorance


# --- invariants --------------------------------------------------------------
class TestInvariants:
    @pytest.mark.parametrize(
        "witnesses",
        [
            [Witness("a", SAYS_A, 0.9)],
            [Witness("a", SAYS_A, 0.8), Witness("b", SAYS_B, 0.8)],
            [Witness("a", (0.2, 0.3, 0.5), 0.7), Witness("b", (0.6, 0.3, 0.1), 0.4)],
            [Witness("a", UNSURE, 0.5), Witness("b", UNSURE, 0.5)],
        ],
    )
    def test_outputs_are_in_range(self, witnesses: list[Witness]) -> None:
        result = pool(witnesses)
        assert 0.0 <= result.conflict <= 1.0
        assert 0.0 <= result.ignorance <= 1.0
        assert _is_distribution(result.posterior)

    def test_unnormalized_input_is_normalized(self) -> None:
        result = pool([Witness("a", (2.0, 2.0), 0.9)])  # -> (0.5, 0.5)
        assert result.posterior == pytest.approx((0.5, 0.5))

    def test_mismatched_claim_space_raises(self) -> None:
        with pytest.raises(ValueError, match="claim-space size"):
            pool([Witness("a", (1.0, 0.0), 0.9), Witness("b", (0.3, 0.3, 0.4), 0.9)])

    def test_three_way_claim_space(self) -> None:
        result = pool(
            [
                Witness("a", (1.0, 0.0, 0.0), 0.9),
                Witness("b", (0.0, 1.0, 0.0), 0.9),
                Witness("c", (0.0, 0.0, 1.0), 0.9),
            ]
        )
        # mutually disjoint one-hots -> maximal conflict
        assert result.conflict == pytest.approx(1.0)
        assert _is_distribution(result.posterior)
