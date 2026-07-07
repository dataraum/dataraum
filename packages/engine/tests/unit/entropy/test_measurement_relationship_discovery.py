"""Tests for the relationship-discovery adjudication measurement (docs/architecture/entropy.md).

The grounding result this pins (probe 2026-06-10, real pooling engine, observed
live values from the DAT-405 calibration runs): the pooled conflict ``C``
separates the injected family from clean BY ORDERING in both row regimes —

* confirmed family: a 20%-orphan FK the LLM confirms (join_confidence 0.59)
  conflicts measurably more than a clean containment-1.0 FK;
* keeper family: a silence-kept relationship over a weak overlap conflicts
  measurably more than a clean kept one.

Ordering assertions, never point thresholds (the eval grammar).
"""

from __future__ import annotations

import pytest

from dataraum.entropy.measurements.relationship_discovery import (
    CLAIM_SPACE,
    curation_distribution,
    llm_judgment_distribution,
    measure_relationship_discovery,
    value_overlap_distribution,
)


class TestWitnessDistributions:
    """Each extractor reads measured row values and abstains without them."""

    def test_value_overlap_exact_containment_asserts_genuine(self) -> None:
        dist = value_overlap_distribution(1.0, 1.0)
        assert dist["genuine"] == pytest.approx(1.0)

    def test_value_overlap_is_damped_by_statistical_confidence(self) -> None:
        exact = value_overlap_distribution(0.9, 1.0)
        sampled = value_overlap_distribution(0.9, 0.5)
        assert exact["genuine"] > sampled["genuine"] > 0.5

    def test_value_overlap_abstains_without_candidate_row(self) -> None:
        assert value_overlap_distribution(None, None)["genuine"] == pytest.approx(0.5)

    def test_llm_judgment_asserts_genuine_at_row_confidence(self) -> None:
        strong = llm_judgment_distribution(0.95)
        weak = llm_judgment_distribution(0.5)
        assert strong["genuine"] > weak["genuine"] > 0.5

    def test_llm_judgment_abstains_without_llm_row(self) -> None:
        assert llm_judgment_distribution(None)["genuine"] == pytest.approx(0.5)

    def test_curation_abstains_without_row(self) -> None:
        assert curation_distribution(None)["genuine"] == pytest.approx(0.5)


class TestAdjudication:
    """Pooling semantics over the row-witnesses."""

    def test_no_rows_is_total_ignorance(self) -> None:
        adjudication = measure_relationship_discovery()
        assert adjudication.witnesses == ()
        assert adjudication.result.conflict == pytest.approx(0.0)
        assert adjudication.result.ignorance == pytest.approx(1.0)

    def test_abstaining_witnesses_are_not_pooled(self) -> None:
        """An llm-only pair pools exactly one witness — abstainers are dropped."""
        adjudication = measure_relationship_discovery(llm_confidence=0.9)
        assert [w.witness_id for w in adjudication.witnesses] == ["llm_judgment"]

    def test_injected_confirmed_conflicts_more_than_clean_confirmed(self) -> None:
        """detection-v1 ordering: 20%-orphan FK (jc 0.59) > clean FK (jc 1.0)."""
        injected = measure_relationship_discovery(
            join_confidence=0.59, statistical_confidence=1.0, llm_confidence=0.9
        )
        clean = measure_relationship_discovery(
            join_confidence=1.0, statistical_confidence=1.0, llm_confidence=0.95
        )
        assert injected.result.conflict > clean.result.conflict
        # The injected pair is NOT genuine-and-quiet: conflict is non-trivial
        # while the clean pair stays near zero.
        assert clean.result.conflict == pytest.approx(0.0, abs=0.05)

    def test_keeper_over_weak_overlap_conflicts_more_than_clean_keeper(self) -> None:
        contested = measure_relationship_discovery(
            join_confidence=0.45, statistical_confidence=1.0, keeper_confidence=1.0
        )
        clean = measure_relationship_discovery(
            join_confidence=1.0, statistical_confidence=1.0, keeper_confidence=1.0
        )
        assert contested.result.conflict > clean.result.conflict
        assert clean.result.conflict == pytest.approx(0.0, abs=0.05)

    def test_posterior_layout_matches_claim_space(self) -> None:
        adjudication = measure_relationship_discovery(
            join_confidence=1.0, statistical_confidence=1.0
        )
        posterior = dict(zip(CLAIM_SPACE, adjudication.result.posterior, strict=True))
        assert posterior["genuine"] > posterior["spurious"]

    def test_reliabilities_are_threaded_onto_witnesses(self) -> None:
        adjudication = measure_relationship_discovery(
            join_confidence=0.8,
            statistical_confidence=1.0,
            llm_confidence=0.9,
            reliabilities={"value_overlap": 0.31, "llm_judgment": 0.62},
        )
        by_id = {w.witness_id: w.reliability for w in adjudication.witnesses}
        assert by_id == {"value_overlap": 0.31, "llm_judgment": 0.62}
