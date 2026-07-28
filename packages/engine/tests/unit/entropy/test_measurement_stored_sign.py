"""Stored-sign measurement — the pure adjudication layer (DAT-875).

Covers the vocabulary's single home, the sign-partition witness's reading of the
voter counts, and the pooling behaviour: the data witness overrules a disagreeing
name-based claim, an agreeing claim corroborates, and two abstentions resolve
nothing rather than tie-breaking into a label.

No DB, no LLM — the measurement module is pure by construction.
"""

from __future__ import annotations

from typing import get_args

import pytest

from dataraum.analysis.catalogue.models import STORED_SIGNS, StoredSignClaim
from dataraum.entropy.measurements.stored_sign import (
    CLAIM_SPACE,
    LEDGER_SIGNED,
    NATURAL_BALANCE,
    llm_claim_distribution,
    measure_stored_sign,
    resolved_stored_sign,
    sign_partition_distribution,
)


class TestVocabulary:
    """The measurement's claim space and the persisted vocabulary are ONE set."""

    def test_claim_space_matches_the_persisted_vocabulary(self):
        # The whole point of naming them identically: the resolve write round-trips
        # onto ColumnConcept with no translation table (unlike temporal_behavior).
        assert set(CLAIM_SPACE) == set(STORED_SIGNS)

    def test_claim_literal_is_the_vocabulary_plus_the_abstention(self):
        assert set(get_args(StoredSignClaim)) == set(STORED_SIGNS) | {"unsure"}

    def test_claim_space_has_no_undetermined_member(self):
        # Undetermined is NULL, never a label — a value no consumer can act on.
        assert "unsure" not in CLAIM_SPACE
        assert None not in CLAIM_SPACE


class TestSignPartitionWitness:
    """The data witness reads voter counts under a convention and its negation."""

    def test_one_family_under_one_sign_is_ledger_signed(self):
        # Every reconciling entity fits the SAME ledger direction => the stored
        # values were never normalized per account family.
        d = sign_partition_distribution(10, 10, 0, 0)
        assert d[LEDGER_SIGNED] > d[NATURAL_BALANCE]

    def test_two_disjoint_families_under_opposite_signs_is_natural_balance(self):
        # Half the entities need the anchor flipped — which is exactly what
        # normalizing a credit-normal family's sign does to the reconciliation.
        d = sign_partition_distribution(10, 5, 5, 0)
        assert d[NATURAL_BALANCE] > d[LEDGER_SIGNED]

    def test_absent_partition_abstains(self):
        assert sign_partition_distribution(None, None, None, None) == {
            NATURAL_BALANCE: 0.5,
            LEDGER_SIGNED: 0.5,
        }

    def test_no_voter_abstains(self):
        assert sign_partition_distribution(10, 0, 0, 0)[NATURAL_BALANCE] == 0.5

    def test_a_lone_voter_is_not_a_family(self):
        # One entity reconciling is ignorance, not a verdict — the same minimum the
        # reconciliation itself applies before disposing a candidate.
        assert sign_partition_distribution(10, 1, 0, 0)[NATURAL_BALANCE] == 0.5

    def test_a_single_dissenter_does_not_make_a_split(self):
        # 9 vs 1: the 1 is not a family, but it is also not nothing — rounding it
        # away to "uniform" would assert a convention over unexplained structure.
        assert sign_partition_distribution(10, 9, 1, 0)[NATURAL_BALANCE] == 0.5

    def test_degenerate_both_side_voters_are_removed_from_both(self):
        # An entity that fits either sign has a near-dead anchor; it cannot evidence
        # a partition. 6/6 with 5 shared leaves 1 vs 1 — under the family minimum.
        assert sign_partition_distribution(10, 6, 6, 5)[NATURAL_BALANCE] == 0.5

    def test_confidence_is_coverage_of_the_entity_population(self):
        # Same partition SHAPE, different share of the population explained: the
        # witness leans harder when it saw more of it. Unobserved entities are not
        # evidence, so coverage is the honest confidence — no threshold on top.
        wide = sign_partition_distribution(10, 10, 0, 0)[LEDGER_SIGNED]
        narrow = sign_partition_distribution(100, 10, 0, 0)[LEDGER_SIGNED]
        assert wide > narrow > 0.5

    def test_full_coverage_asserts_the_extreme(self):
        assert sign_partition_distribution(4, 4, 0, 0)[LEDGER_SIGNED] == pytest.approx(1.0)


class TestLLMClaimWitness:
    """The catalogue agent's name-and-marginal read."""

    @pytest.mark.parametrize("label", list(STORED_SIGNS))
    def test_a_confident_claim_asserts_its_label(self, label: str):
        assert llm_claim_distribution(label, 1.0)[label] == pytest.approx(1.0)

    def test_unsure_abstains(self):
        assert llm_claim_distribution("unsure", 0.0)[NATURAL_BALANCE] == 0.5

    def test_absent_claim_abstains(self):
        assert llm_claim_distribution(None, None)[NATURAL_BALANCE] == 0.5

    def test_zero_confidence_collapses_to_abstention(self):
        assert llm_claim_distribution(NATURAL_BALANCE, 0.0)[NATURAL_BALANCE] == 0.5


class TestPooling:
    """How the two witnesses combine into a resolved convention."""

    def test_data_overrules_a_disagreeing_claim(self):
        # The agent reads 'natural_balance' from the name; the data shows one
        # uniform family. The data wins — its input is the data, and the claim's
        # evidence provably does not contain the answer.
        adj = measure_stored_sign(
            "balance_sheet",
            "ending_balance",
            llm_claim=NATURAL_BALANCE,
            llm_confidence=0.9,
            n_entities=8,
            fired_primary=8,
            fired_mirror=0,
            fired_both=0,
        )
        label, contested = resolved_stored_sign(adj)
        assert label == LEDGER_SIGNED
        assert adj.overruled is True
        assert contested is True
        # Provenance keeps BOTH reads even though only one was pooled.
        assert {w.witness_id for w in adj.witnesses} == {"llm_claim", "sign_partition"}

    def test_an_agreeing_claim_is_kept_and_lowers_ignorance(self):
        agreed = measure_stored_sign(
            "balance_sheet",
            "ending_balance",
            llm_claim=LEDGER_SIGNED,
            llm_confidence=0.9,
            n_entities=8,
            fired_primary=8,
            fired_mirror=0,
            fired_both=0,
        )
        alone = measure_stored_sign(
            "balance_sheet",
            "ending_balance",
            llm_claim="unsure",
            n_entities=8,
            fired_primary=8,
            fired_mirror=0,
            fired_both=0,
        )
        assert agreed.overruled is False
        assert resolved_stored_sign(agreed)[0] == LEDGER_SIGNED
        assert agreed.result.ignorance < alone.result.ignorance

    def test_claim_stands_alone_when_the_partition_abstains(self):
        adj = measure_stored_sign(
            "balance_sheet",
            "ending_balance",
            llm_claim=NATURAL_BALANCE,
            llm_confidence=0.8,
        )
        assert resolved_stored_sign(adj)[0] == NATURAL_BALANCE
        assert adj.overruled is False

    def test_both_abstaining_resolves_nothing(self):
        adj = measure_stored_sign("t", "c", llm_claim="unsure")
        assert adj.witnesses == ()
        assert adj.result.ignorance == pytest.approx(1.0)
        assert resolved_stored_sign(adj) == (None, False)

    def test_a_zero_reliability_wash_resolves_nothing(self):
        # Nobody was trusted — do not mint a label off a >= tie-break.
        adj = measure_stored_sign(
            "t",
            "c",
            llm_claim=NATURAL_BALANCE,
            llm_confidence=1.0,
            reliabilities={"llm_claim": 0.0, "sign_partition": 0.0},
        )
        assert resolved_stored_sign(adj) == (None, False)

    def test_resolves_from_the_data_alone(self):
        # No claim at all (add_source grain has no ColumnConcept) — the partition
        # still determines the convention where it fired.
        adj = measure_stored_sign(
            "balance_sheet",
            "ending_balance",
            n_entities=6,
            fired_primary=3,
            fired_mirror=3,
            fired_both=0,
        )
        assert resolved_stored_sign(adj)[0] == NATURAL_BALANCE

    def test_claim_field_identifies_the_slot(self):
        adj = measure_stored_sign("balance_sheet", "ending_balance")
        assert adj.claim_field == "stored_sign:balance_sheet.ending_balance"
