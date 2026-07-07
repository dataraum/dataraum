"""Null-semantics measurement (docs/architecture/entropy.md, DAT-457).

Pure; no DB/config. Asserts the extractors' direction and — the point of the
whole exercise — that a *novel* sentinel (quarantine + type say null, vocabulary
has never seen it) surfaces as elevated conflict rather than being silently
mis-resolved. Properties/orderings, not point thresholds.
"""

from __future__ import annotations

from dataraum.entropy.measurements.null_semantics import (
    CLAIM_SPACE,
    measure_null_semantics,
    quarantine_distribution,
    resolved_null_tokens,
    type_distribution,
    vocabulary_distribution,
)

_NULL = CLAIM_SPACE.index("is-null")

# A clean strict-typed column whose listed cast failures include both tokens.
TYPED = {
    "resolved_type": "DECIMAL",
    "parse_success_rate": 0.9,
    "failed_examples": ["N/A", "PENDING_REVIEW"],
}
VOCAB = ["", "NULL", "N/A", "NA", "MISSING"]


# --- individual witnesses ----------------------------------------------------
class TestQuarantineWitness:
    def test_dominant_repeated_token_leans_null(self) -> None:
        # one token, 90 of 100 rejects, 2 distinct → concentrated + repeated
        assert quarantine_distribution(90, 100, 2)["is-null"] > 0.7

    def test_repetition_is_monotonic(self) -> None:
        # same cluster shape; a more-repeated token reads more strongly null
        low = quarantine_distribution(5, 100, 10)["is-null"]
        high = quarantine_distribution(50, 100, 10)["is-null"]
        assert high > low

    def test_co_occurring_sentinels_stay_strong_via_clustering(self) -> None:
        # the live-run case: 5 distinct sentinels, ~8 each over 40 rejects.
        # SHARE would dilute each to ~0.6; CLUSTERING keeps them strong.
        assert quarantine_distribution(8, 40, 5)["is-null"] > 0.7

    def test_high_cardinality_smear_is_not_a_cluster(self) -> None:
        # 50 distinct one-off rejects = corruption smear, not sentinels
        assert quarantine_distribution(1, 50, 50)["is-null"] < 0.55

    def test_thin_quarantine_is_damped(self) -> None:
        # a single rejected value is not a sentinel like 200 repeats are
        thin = quarantine_distribution(1, 1, 1)["is-null"]
        thick = quarantine_distribution(200, 200, 1)["is-null"]
        assert thin < thick

    def test_no_quarantine_is_neutral(self) -> None:
        assert quarantine_distribution(0, 0, 0)["is-null"] == 0.5


class TestTypeWitness:
    def test_typed_and_failed_leans_null(self) -> None:
        assert type_distribution("N/A", TYPED)["is-null"] > 0.7

    def test_varchar_column_abstains(self) -> None:
        d = type_distribution("N/A", {"resolved_type": "VARCHAR", "failed_examples": ["N/A"]})
        assert d["is-null"] == 0.5

    def test_token_not_in_failures_abstains(self) -> None:
        assert type_distribution("42", TYPED)["is-null"] == 0.5


class TestVocabularyWitness:
    def test_known_token_is_strong_null(self) -> None:
        assert vocabulary_distribution("N/A", VOCAB)["is-null"] > 0.7

    def test_case_and_whitespace_insensitive(self) -> None:
        assert vocabulary_distribution("  n/a ", VOCAB)["is-null"] > 0.7

    def test_unknown_token_leans_value(self) -> None:
        assert vocabulary_distribution("PENDING_REVIEW", VOCAB)["is-value"] > 0.5


# --- the pooled measurement --------------------------------------------------
class TestMeasureNullSemantics:
    def test_one_adjudication_per_rejected_token(self) -> None:
        quarantine = {
            "rejected_tokens": [
                {"token": "N/A", "count": 60},
                {"token": "PENDING_REVIEW", "count": 40},
            ],
            "total_rejected": 100,
        }
        result = measure_null_semantics(quarantine, TYPED, VOCAB)
        assert [a.token for a in result] == ["N/A", "PENDING_REVIEW"]
        assert result[0].claim_field == "null_token:N/A"

    def test_empty_quarantine_yields_nothing(self) -> None:
        assert (
            measure_null_semantics({"rejected_tokens": [], "total_rejected": 0}, TYPED, VOCAB) == []
        )

    def test_known_sentinel_is_quiet_agreement(self) -> None:
        # All three witnesses agree N/A is a null marker → low conflict, posterior null.
        quarantine = {"rejected_tokens": [{"token": "N/A", "count": 95}], "total_rejected": 100}
        (adj,) = measure_null_semantics(quarantine, TYPED, VOCAB)
        assert adj.result.conflict < 0.2
        assert adj.result.posterior[_NULL] > 0.8

    def test_novel_sentinel_surfaces_conflict(self) -> None:
        # quarantine + type say null; vocabulary has never seen PENDING_REVIEW →
        # the vocabulary witness dissents → conflict rises. THIS is the value:
        # the case that used to need a hard-coded token now bands investigate.
        novel = {
            "rejected_tokens": [{"token": "PENDING_REVIEW", "count": 95}],
            "total_rejected": 100,
        }
        known = {"rejected_tokens": [{"token": "N/A", "count": 95}], "total_rejected": 100}
        (novel_adj,) = measure_null_semantics(novel, TYPED, VOCAB)
        (known_adj,) = measure_null_semantics(known, TYPED, VOCAB)
        assert novel_adj.result.conflict > known_adj.result.conflict
        # Two reliable witnesses still carry the posterior toward is-null.
        assert novel_adj.result.posterior[_NULL] > 0.5

    def test_teaching_the_token_collapses_conflict(self) -> None:
        # The teach-delta at the measurement layer: adding the contested token to
        # the vocabulary — exactly what a null_value teach does — flips the
        # vocabulary witness to agreement, so conflict collapses while the
        # posterior stays confidently is-null. The teach enters as a WITNESS, never
        # an override. (Proven end-to-end live: DAT-457 teach-closure, C 0.255→0.019.)
        quarantine = {
            "rejected_tokens": [{"token": "PENDING_REVIEW", "count": 95}],
            "total_rejected": 100,
        }
        (before,) = measure_null_semantics(quarantine, TYPED, VOCAB)  # not in vocab
        (after,) = measure_null_semantics(quarantine, TYPED, [*VOCAB, "PENDING_REVIEW"])  # taught
        assert after.result.conflict < before.result.conflict  # teach drops conflict
        assert after.result.conflict < 0.05  # resolved — the three witnesses now agree
        assert after.result.posterior[_NULL] > 0.9  # still confidently a null marker

    def test_thin_one_off_value_is_ignorant_not_confident(self) -> None:
        # A single junk value, VARCHAR column (type abstains), not in vocab:
        # nobody informative weighed in → high ignorance.
        quarantine = {"rejected_tokens": [{"token": "x9q", "count": 1}], "total_rejected": 1}
        varchar = {"resolved_type": "VARCHAR", "failed_examples": []}
        strong = {"rejected_tokens": [{"token": "N/A", "count": 200}], "total_rejected": 200}
        (thin_adj,) = measure_null_semantics(quarantine, varchar, VOCAB)
        (strong_adj,) = measure_null_semantics(strong, TYPED, VOCAB)
        assert thin_adj.result.ignorance > strong_adj.result.ignorance

    def test_witnesses_are_carried_for_persistence(self) -> None:
        quarantine = {"rejected_tokens": [{"token": "N/A", "count": 95}], "total_rejected": 100}
        (adj,) = measure_null_semantics(quarantine, TYPED, VOCAB)
        assert {w.witness_id for w in adj.witnesses} == {
            "quarantine_clustering",
            "type_claim",
            "null_vocabulary",
        }


# --- resolved layer: evidence → null_tokens (3b) -----------------------------
class TestResolvedNullTokens:
    @staticmethod
    def _entry(token: str, is_null: float) -> dict:
        return {"token": token, "posterior": {"is-null": is_null, "is-value": 1.0 - is_null}}

    def test_keeps_is_null_tokens_drops_is_value(self) -> None:
        evidence = [self._entry("#ERR", 0.95), self._entry("12.5", 0.2), self._entry("TBD", 0.8)]
        assert resolved_null_tokens(evidence) == ["#ERR", "TBD"]

    def test_contested_token_still_resolves_is_null(self) -> None:
        # Novel sentinel: high conflict, but the pooled posterior is is-null → it
        # IS a null marker. The teach resolves the conflict, not the membership.
        evidence = [
            {
                "token": "PENDING",
                "posterior": {"is-null": 0.998, "is-value": 0.002},
                "conflict": 0.25,
            }
        ]
        assert resolved_null_tokens(evidence) == ["PENDING"]

    def test_empty_and_threshold_edges(self) -> None:
        assert resolved_null_tokens([]) == []
        # exactly at threshold is not "past" it
        assert resolved_null_tokens([self._entry("x", 0.7)]) == []
        assert resolved_null_tokens([self._entry("x", 0.71)]) == ["x"]


class TestAbstainerExclusion:
    """An abstaining witness is ignorance, not a diluting party (review C3)."""

    def test_abstaining_type_claim_does_not_dilute_novel_sentinel(self) -> None:
        from dataraum.entropy.measurements.null_semantics import measure_null_semantics

        # Novel sentinel: quarantine clusters hard (is-null), vocabulary has
        # never seen it (miss) — type_claim abstains (token absent from
        # failed_examples → uniform). The conflict must reflect the two
        # OPINIONATED witnesses disagreeing, undiluted by the abstainer.
        adjudications = measure_null_semantics(
            quarantine_data={
                "rejected_tokens": [{"token": "##MISSING##", "count": 40}],
                "total_rejected": 40,
            },
            typing_data={},
            null_tokens=[],
        )
        assert len(adjudications) == 1
        adj = adjudications[0]
        assert [w.witness_id for w in adj.witnesses] == [
            "quarantine_clustering",
            "null_vocabulary",
        ]
        # With the abstainer pooled the conflict sat at ~0.21 (below the 0.3
        # band edge); excluded, the genuine disagreement surfaces above it.
        assert adj.result.conflict > 0.3
