"""Null-semantics measurement (ADR-0009, DAT-457).

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
    def test_dominant_token_leans_null(self) -> None:
        assert quarantine_distribution(90, 100)["is-null"] > 0.7

    def test_share_is_monotonic(self) -> None:
        low = quarantine_distribution(10, 100)["is-null"]
        high = quarantine_distribution(80, 100)["is-null"]
        assert high > low

    def test_thin_quarantine_is_damped(self) -> None:
        # Same 100% share, but 1 rejection is not a sentinel like 200 are.
        thin = quarantine_distribution(1, 1)["is-null"]
        thick = quarantine_distribution(200, 200)["is-null"]
        assert thin < thick

    def test_no_quarantine_is_neutral(self) -> None:
        assert quarantine_distribution(0, 0)["is-null"] == 0.5


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
